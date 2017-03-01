#  -*- coding:utf-8 -*-
import hashlib

"""
该部分主要定义一些工具类，对网页进行处理

"""

class Tool:
    __instance = None

    def __init__(self):
        pass

    "单例模式"
    @staticmethod
    def getTool():
        if Tool.__instance is None:
            Tool.__instance = Tool()
        return Tool.__instance

    "网页转码"
    def convertCode(self, content, fromCode, toCode):
        content = content.decode(fromCode).encode(toCode)
        return content.lower()

    "计算url的MD5值"
    def getMd5(self, url):
        url2md5 = hashlib.md5()
        url2md5.update(url)

        return url2md5.hexdigest()