# -*- coding:utf-8 -*-
from pymongo import MongoClient, errors
import properties

"""
基于MongoDB的队列
"""

class MongoQueue:
    "状态定义"
    OUTSTADING, PROCESSING, COMPLETE = range(3)

    def __init__(self, client=None):
        self.client = MongoClient(properties.mongodb_addr, 27017) if client is None else client
        self.db = self.client.cache

    def __nonzero__(self):
        record = self.db.crawl_queue.find_one(
            {'status':{'$ne':self.COMPLETE}}
        )

        return  True if record else False

    def push(self, url):
        try:
            self.db.crawl_queue.insert(
                {'_id':url,'status':self.OUTSTADING}
            )
        except errors.DuplicateKeyError as e:
            pass

    def pop(self):
        record = self.db.crawl_queue.find_and_modify(
            query={'status':self.OUTSTADING},
            update={'$set':{'status':self.PROCESSING}}
        )

        if record:
            return record['_id']
        else:
            self.repair()
            raise KeyError

    def complete(self, url):
        self.db.crawl_queue.update(
            {'_id':url},
            {'$set':{'status':self.COMPLETE}}
        )

    def repair(self):
        record = self.db.crawl_queue.find_and_modify(
            query={'status':{'$ne':self.COMPLETE}},
            update={'$set':{'status':self.OUTSTADING}}
        )

        if record:
            print 'released:', record['_id']
