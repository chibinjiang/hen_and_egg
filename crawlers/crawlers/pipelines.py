# -*- coding: utf-8 -*-
import json
from redis import StrictRedis
from pprint import pprint
from .utils import get_current_datetime


class BeikeSaveItemToFilePipeline(object):

    def __init__(self):
        self.file = "Beike_rental_details_%s.txt" % get_current_datetime().replace(' ', '_')

    def process_item(self, item, spider):
        """
        Dump item to file
        """
        with open(self.file, "a") as fa:
            fa.write(json.dumps(dict(item)) + "\n")
        return item


class BeikePushUrlToRedisPipeline(object):
    def __init__(self):
        self.client = StrictRedis()

    def process_item(self, item, spider):
        """
        Dump item to file
        """
        self.client.sadd(item['redis_key'], item['url'])
        return item
