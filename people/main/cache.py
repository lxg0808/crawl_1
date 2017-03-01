# -*- coding:utf-8 -*-

import os.path
import cPickle

"""
根据url地址，对其html文件进行缓存
"""

"磁盘缓存"
class DiskCache:
    def __init__(self, root):
        self.root = root

    def __getitem__(self, item):
        filepath = os.path.join(self.root, item)
        with open(filepath, 'r') as fi:
            return cPickle.loads(fi.read())

    def __setitem__(self, key, value):
        filepath = os.path.join(self.root, key)
        with open(filepath, 'w') as fo:
            fo.write(cPickle.dumps(value))

"MongoDB缓存"
from pymongo import MongoClient
import zlib

class MongoCache:
    def __init__(self, client=None):
        self.client = MongoClient('localhost', 27017) if client is None else client
        self.db = self.client.cache

    # def __getitem__(self, url):
    #     record = self.db.webpage.find_one({'_id':url})
    #     if record:
    #         return record['result']
    #     else:
    #         raise KeyError(url + " does not exist")
    #
    # def __setitem__(self, url, result):
    #     record = {'result':result}
    #     self.db.webpage.update({'_id':url}, {'$set':record}, upsert=True)
    "增加数据压缩功能"
    def __getitem__(self, url):
        record = self.db.webpage.find_one({'_id': url})
        if record:
            return cPickle.loads(zlib.decompress(record['result']))
        else:
            raise KeyError(url + " does not exist")

    def __setitem__(self, url, result):
        record = {'result': zlib.compress(cPickle.dumps(result))}
        self.db.webpage.update({'_id': url}, {'$set': record}, upsert=True)