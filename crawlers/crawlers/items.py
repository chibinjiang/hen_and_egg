# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.org/en/latest/topics/items.html

from scrapy import Item, Field


class BrokerItem(Item):
    broker_id = Field()
    name = Field()
    phone = Field()
    wechat = Field()
    qq_number = Field()
    portrail = Field()
    company = Field()
    company_url  = Field()
    crawled_time = Field()
    crawled_url = Field()


class CommunityItem(Item):
    community_id = Field()
    name = Field()
    latitude= Field()
    longitude = Field()
    address = Field()

class RentalItem(Item):
    title = Field()
    spider = Field()
    rent_type = Field()
    price = Field()  # price, or price range, seperated by -
    rental_id = Field()
    # community_id = Field()
    # community_name = Field()
    community_info = Field()  # aggregate community data
    payment = Field()
    address = Field()
    images= Field()
    floor = Field()
    crawled_time = Field()
    crawled_url = Field()
    layout = Field()
    area = Field()
    orientation = Field()
    window_type = Field()
    furniture = Field()
    description = Field()
    contact_info = Field()
    tags = Field()
    broker_info = Field()
    publish_time = Field()

