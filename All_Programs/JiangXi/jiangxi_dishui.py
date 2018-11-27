# coding=utf-8
import PackageTool
import re
import os
import traceback
import gevent
import gevent.monkey
import datetime
import requests
from lxml import html
import urlparse
import logging
from xml.etree import ElementTree
from tax import MyException, MySQL,MainFunc
from tax.config import config_one, config_two
from bs4 import  BeautifulSoup
import sys

reload(sys)

sys.setdefaultencoding('utf8')

class ShuiWu(MainFunc.MainFunc):
    def __init__(self,province,region,dishui_type=None):
        super(ShuiWu,self).__init__()
        self.pinyin='Jiang_Xi'
        self.region=region
        self.url=config_two[province][region][dishui_type]['url']

        self.encoding=config_two[province][region][dishui_type]['encoding']
        self.div_class = config_two[province][region][dishui_type]['divclass']
        self.province=province
        result=urlparse.urlparse(self.url)
        self.host_url=result.scheme+'://'+result.netloc
        self.stop_time='2017-10-01'

    def do_dishui(self):
        # print self.url
        r = self.requests_get(self.url, encoding='gb2312')
        try:
            self.get_dishui_title(r)
            if self.title_time < self.stop_time:
                return None
        except Exception as e:
            print e
        else:
            startpage=2
            while True:
                print '正在抓取page'+str(startpage)
                param='pageNo='+str(startpage)+'%5D'
                url=re.sub('pageNo=1',param,self.url)
                r = self.requests_get(url,encoding='gb2312')
                self.get_dishui_title(r)
                startpage+=1
                # print 'self.title_time',self.title_time
                if self.title_time < self.stop_time:
                    return None

    def get_dishui_title(self,r):
        ul = r.find_all(class_='list_content')[0].find_all('ul')[0]
        li_list = ul.select('li')
        for li in li_list:
            self.title_time = li.select('span')[0].text
            self.fbrq = li.select('span')[0].text
            a = li.select('a')[0]
            if re.search(u'(?:欠税公告|非正常户)', a.text):
                self.title = a.text
                link = a.get('href')
                detail_url = self.host_url + link
                print detail_url
                self.filename=re.findall('contentId=(.*?)category',detail_url)[0]+'.html'
                if not self.is_exist():
                    self.parse_detail(detail_url)

    def parse_detail(self,detail_url):
        tree=self.get_tree(detail_url)
        list_url=tree.xpath('//a/@href')
        down_load = False
        for url in list_url:
            if url.endswith('.doc') or url.endswith('.xls') or url.endswith('.xlsx') or url.endswith('.docx'):
                down_load = True
                print url
                self.doc_url=self.host_url + url
                try:
                    self.filename=re.findall('filename=(.*)',url)[0]
                except:
                    self.filename=url.split('/')[-1]
                self.download()
        if not down_load:
            self.doc_url = detail_url
            self.download()


if __name__ == '__main__':

    tasks=[]
    today=datetime.date.today()
    file_name = os.path.basename(__file__).split('.')[0]
    MainFunc.MainFunc.write_log(file_name)
    m='江西省'
    for n in config_two[m].keys():
         for x in config_two[m][n].keys():
             shuiwu=ShuiWu(m,n,dishui_type=x)
             dd=shuiwu.do_dishui
             tasks.append(gevent.spawn(dd))
    gevent.joinall(tasks)







