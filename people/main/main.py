# -*- coding:utf-8 -*-
from queue import MongoQueue
from tools import Tool
import urllib2
import re
import properties
import os.path
from threadpool import ThreadPool
import threadpool
import multiprocessing
import urlparse

"""
根据人民网的sitemap爬取网站
将所有url保存在基于Mongodb的queue中，以状态的方式记录每一个url是否访问过
使用数据库服务器，文件存储在本地
"""
class Crawl:
    queue = None
    tool = None

    def __init__(self):
        self.queue = MongoQueue()
        self.tool = Tool.getTool()

    "第一步，解析sitemap，提取url"
    def parseSiteMap(self, sitemap):
        try:
            request = urllib2.Request(sitemap)
            response = urllib2.urlopen(request)
        except Exception as e:
            print e.message
            page = ""
        else:
            page = response.read()

        "提取url"
        urls = re.findall('<loc>\s*(.*?)\s*</loc>', page)

        return urls

    "第二步，解析所有url，存入队列"
    def saveURL(self, sitemap):
        count = 0
        child_sitemaps = self.parseSiteMap(sitemap)
        for chil_sitemap in child_sitemaps:
            urls = self.parseSiteMap(chil_sitemap)

            for url in urls:
                self.queue.push(url)
                count += 1
                print count

    "第三步，依次出队，爬取网页，并存储"
    def process(self, num):
        url = self.queue.pop()
        while url:
            try:
                request = urllib2.Request(url)
                response = urllib2.urlopen(request)
            except Exception as e:
                print e.message
            else:
                page = response.read()
                # page = self.tool.convertCode(page, 'gbk', 'utf-8')
                self.save(properties.data, url, page)
            self.queue.complete(url)
            url = self.queue.pop()

    "存储网页"
    def save(self, child_dir, url, page):
        md5 = self.tool.getMd5(url)
        filepath = os.path.join(child_dir, "%s.html" % md5)
        with open(filepath, 'w') as fo:
            fo.write(page)
        print "save successfully"

    "提取网页中url，拼接、过滤后添加到mongodb队列"
    def urlProcess(self, root_url, page):
        raw_urls = re.findall('<a\s*.*?href=\"(.*?)\"')
        for raw_url in raw_urls:
            if re.match('/', raw_url):
                pure_url = urlparse.urljoin(root_url, raw_url)
                self.queue.push(pure_url)
            else:
                self.queue.push(raw_url)

"多线程"
def multi_thread():
    crawl = Crawl()
    pool = ThreadPool(10)
    requests = threadpool.makeRequests(crawl.process, [i for i in range(8)])
    [ pool.putRequest(req) for req in requests ]
    pool.wait()

"多进程"
def multi_process():
    cpu_nums = multiprocessing.cpu_count()
    process_pool = []

    for i in range(cpu_nums):
        p = multiprocessing.Process(target=multi_thread)
        p.start()
        process_pool.append(p)

    for p in process_pool:
        p.join()

if __name__ == '__main__':
    sitemap = "http://www.people.com.cn/sitemap_index.xml"
    crawl = Crawl()
    crawl.saveURL(sitemap)
    # multi_process()