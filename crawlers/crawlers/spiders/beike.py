import re
import json
import time
import ipdb
import requests
import traceback
from pprint import pprint
from redis import StrictRedis
from crawlers.items import BrokerItem, RentalItem, CommunityItem
from scrapy import Selector
from scrapy.http import Request
from scrapy.spiders import CrawlSpider
from scrapy_redis.spiders import RedisCrawlSpider

class BeikeRentalSpider(RedisCrawlSpider):
    """
    Only parse rental pages, given start urls, like ke.com/BJXXXXXX.html
    """
    name = 'beike_rental_spider'
    allowed_domains = ['m.ke.com']
    redis_key = '%s:start_urls' % name
    broker_url = 'https://m.ke.com/chuzu/aj/house/brokers?house_codes={rental_id}'
    custom_settings  = {
        # 'LOG_FILE': 
        'ITEM_PIPELINES': {
            'crawlers.pipelines.BeikeSaveItemToFilePipeline': 500,
        }
    }

    def parse(self, response):
        """
        https://m.ke.com/chuzu/bj/zufang/BJ2083729326825807872.html
        parse rental detail page
        """
        result = RentalItem()
        meta_info = dict()
        community_info = CommunityItem()
        result['rental_id'] = response.request.url.split('/')[-1].split('.')[0]
        selector = Selector(response)
        title = selector.xpath('.//div[@class="page-house-container"]//div[@class="box page-title-info"]//h2[@class="page-title-h2"]/text()').extract()
        if title:
            # never use extract_first, cuz '' has multiple meanings
            result['title'] = title[0].strip()
        price = selector.xpath('.//div[@class="page-house-container"]//div[@class="box content__detail--info"]//ul/li[1]/span[2]/text()').extract()
        if price:
            result['price'] = price[0].strip()
        payment = selector.xpath('.//div[@class="page-house-container"]//div[@class="box content__detail--info"]//ul/li[1]/span[1]/text()').extract()
        if payment:
            result['payment'] = payment[0].strip()
        layout = selector.xpath('.//div[@class="page-house-container"]//div[@class="box content__detail--info"]//ul/li[2]/span[2]/text()').extract()
        if layout:
            result['layout'] = layout[0].strip()
        area = selector.xpath('.//div[@class="page-house-container"]//div[@class="box content__detail--info"]//ul/li[3]/span[2]/text()').extract()
        if area:
            result['area'] = area[0].strip()
        tags = selector.xpath('.//div[@class="page-house-container"]//div[@class="box page-house-info"]//p[@class="content__item__tag--wrapper"]//i/text()').extract()
        if tags:
            result['tags'] = tags
        images = selector.xpath('.//div[@class="page-house-container"]//div[@id="mySwipe"]//ul[@class="slide__wrapper"]//img//@src').extract()
        if images:
            result['images'] = images
        floor = selector.xpath('.//div[@class="page-house-container"]//div[@class="box page-house-info"]/ul/li[5]/span/text()').extract()
        if floor:
            result['floor'] = floor[0].strip()
        publish_time = selector.xpath('.//div[@class="page-house-container"]//div[@class="box page-house-info"]//ul[@class="page-house-info-list"]/li[1]/span/text()').extract()
        if publish_time:
            result['publish_time'] = publish_time[0]
        community_id = selector.xpath('.//div[@class="page-house-container"]//div[@class="box page-house-info"]//p[@class="resblock"]/a/@href').extract()
        if community_id:
            community_info['community_id'] = community_id[0]
        community_name = selector.xpath('.//div[@class="page-house-container"]//div[@class="box page-house-info"]//p[@class="resblock"]/a/text()').extract()
        if community_name:
            community_info['name'] = community_name[0]
        company = selector.xpath('.//div[@class="page-house-container"]/div[@class="box brand-link"]/div/p[1]/text()').extract()
        if company:
            meta_info['company'] = company[0].strip()
        latitude = re.search("latitude: '([\d\.]+)'", response.text)
        if latitude:
            community_info['latitude'] = latitude.group(1)
        longitude = re.search("longitude: '([\d\.]+)'", response.text)
        if longitude:
            community_info['longitude'] = longitude.group(1)
        result['community_info'] = community_info
        meta_info['item'] = result
        broker_req = Request(url=self.broker_url.format(rental_id=result['rental_id']), callback=self.parse_broker, dont_filter=True, meta=meta_info)
        yield broker_req
    
    def parse_broker(self, response):
        result = BrokerItem()
        rental_item = response.meta.get('item', {})
        data = json.loads(response.text)
        if data.get('status') == 0:
            data = data['data']
            for _, broker_info in data.items():
                for _, temp in broker_info.items():
                    result['broker_id'] = temp['id']
                    result['name'] = temp['contact_name'].strip()
                    result['portrail'] = temp['contact_avatar']
                    result['phone'] = temp['tp_number']
                    if response.meta.get('company'):
                        result['company'] = response.meta['company']                        
                    rental_item['broker_info'] = result
                    return rental_item


def get_urls_from_pagenations():
    # get rental urls from page and add them to redis
    redis = StrictRedis()
    pagenations = {
        'entire': "https://m.ke.com/chuzu/bj/zufang/pg{page}rt200600000001/?ajax=1",
        "share": "https://m.ke.com/chuzu/bj/zufang/pg{page}rt200600000002/?ajax=1",
    }

    beike_cookies = {
        'lianjia_uuid': 'd51a3fb4-0d84-4754-b3d8-7b91ab8fc266',
        'ke_uuid': '0a241360d9aa8d6274b49c3874740f01',
        'sensorsdata2015jssdkcross': '%7B%22distinct_id%22%3A%22166687db766f6-0426c64c3fdff2-8383268-1049088-166687db769329%22%2C%22%24device_id%22%3A%22166687db766f6-0426c64c3fdff2-8383268-1049088-166687db769329%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_referrer%22%3A%22%22%2C%22%24latest_referrer_host%22%3A%22%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%7D%7D',
        'www_zufangzi_server': '0ca0f641b9b44a3586617f59ad4d8f06',
        'select_city': '110000',
        'lianjia_ssid': '4bd74bbc-cf13-4ddf-a383-4bdef814fe01',
    }
    beike_headers = {
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en,zh-CN;q=0.9,zh;q=0.8,ja;q=0.7',
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Mobile Safari/537.36',
        'Accept': 'application/json',
        'Referer': 'https://m.ke.com/chuzu/bj/zufang/rt200600000002/',
        'X-Requested-With': 'XMLHttpRequest',
        'Connection': 'keep-alive',
    }
    for i in range(1, 2000):
        for method in pagenations:
            if method == 'entire':
                beike_headers['Referer'] = "https://m.ke.com/chuzu/bj/zufang/rt200600000001/"
            url = pagenations[method].format(page=i)
            response = requests.get(url, timeout=30, headers=beike_headers, cookies=beike_cookies)
            if response.status_code != 200:
                continue
            ipdb.set_trace()
            data = json.loads(response.text)
            if data.get('no_more_data') != 1:
                break
            if data.get('error_no') != 0:
                pprint(data)
                time.sleep(5)
            selector = Selector(text=data['body'])
            for url in selector.xpath(''):
                redis.sadd(BeikeRentalSpider.redis_key, url)


class BeikePagenationSpider(CrawlSpider):
    """
    Only get rental urls from pagenation
    """
    start_urls =[
        "https://m.ke.com/chuzu/bj/zufang/pg1rt200600000001/?ajax=1",
        "https://m.ke.com/chuzu/bj/zufang/pg1rt200600000002/?ajax=1",
    ]
    name = "beike_pagenation_spider"
    redis_key = '%s:start_urls' % name
    custom_settings  = {
        'ITEM_PIPELINES': {
            'crawlers.pipelines.BeikePushUrlToRedisPipeline': 100,
        },
        'DOWNLOADER_MIDDLEWARES': {
            'crawlers.middlewares.BeikeHeaderMiddleware': 500, 
        }
        # 'EXTENSIONS': {
        #     'crawlers.extensions.CloseSpiderRedis': 100,
        # },
        # 'CLOSE_SPIDER_AFTER_IDLE_TIMES': 2,
    }

    def parse(self, response):
        current_page = re.search("/pg(\d+?)", response.request.url).group(1)
        ipdb.set_trace()
        data = json.loads(response.text)
        if data.get('no_more_data') != 1:
            print("No More Data")
            return 
        if data.get('error_no') != 0:
            pprint(data)
            time.sleep(5)
            return 
        selector = Selector(text=data['body'])
        for ele in selector.xpath('.//div[@data-el="houseItem"]//a/@href').extract():
            url = response.urljoin(ele)
            yield {'redis_key': BeikeRentalSpider.redis_key, 'url': url}
            # redis.sadd(BeikeRentalSpider.redis_key, url)
        next_page = response.request.url.replace('pg' + str(current_page), 'pg' + str(current_page + 1))
        next_request = Request(next_page, callback=self.parse)
        yield next_request


if __name__ == '__main__':
    """
    python3 -m crawlers.spiders.beike
    """
    get_urls_from_pagenations()
