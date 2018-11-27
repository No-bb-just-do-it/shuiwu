# coding=utf-8
import PackageTool
import re
import os
import traceback
import gevent
import gevent.monkey
import datetime
import logging
import requests
from lxml import html
import urlparse
from tax import MyException,MySQL,MainFunc
from tax.config import config_one,config_two

class ShuiWu(MainFunc.MainFunc):
    def __init__(self,province,region,dishui_type=None):
        super(ShuiWu,self).__init__()
        self.pinyin='Hu_Bei'
        self.region=region
        self.url=config_two[province][region][dishui_type]['url']

        self.encoding=config_two[province][region][dishui_type]['encoding']
        self.province=province
        result=urlparse.urlparse(self.url)
        self.href_ahead=result.scheme+'://'+result.netloc
        self.oldest_time='2015-01-01'

    def look_for_a(self,url):
        self.doc_url = url
        if url.endswith('.doc') or url.endswith('.xls') or url.endswith('.xlsx') or url.endswith('.docx'):

            self.download_file()
        else:
            count=0
            tree=self.get_tree(url)
            all_a=tree.xpath('//a/@href')
            for a in all_a:
                #地税网站有很多干扰xls文件和网页无关 都是http开头
                if (a.endswith('.doc') or a.endswith('.xls') or a.endswith('.xlsx') or a.endswith('.pdf') or\
                            a.endswith('.docx')) and (not a.startswith('http')):
                    count+=1
                    if a.startswith('./'):
                        self.doc_url=re.findall('(.*\/)',url)[0]+a.replace('./','')
                    else:
                        self.doc_url=self.href_ahead+a
                    try:
                        self.filename = re.findall('filename=(.*)', self.doc_url)[0]
                    except:
                        self.filename = url.split('/')[-1]
                    self.download()

            if count==0:
                self.download()

    def do_dishui(self):
        tree=self.get_tree(self.url,encoding=self.encoding)
        try:
            self.get_dishui_title(tree)
        except MyException.TitleOverTimeException:
            pass
        else:
            startpage=1
            while True:
                param='index_'+str(startpage)+'.shtml'
                url=self.url+param
                tree=self.get_tree(url,encoding=self.encoding)
                try:
                    self.get_dishui_title(tree)
                except MyException.TitleOverTimeException:
                    break
                else:
                    startpage+=1

    def get_dishui_title(self,tree):
        news=tree.xpath('//div[@class="sdsj_wzy_p2"]/dl/dd')

        for gonggao in news:
            url=gonggao.xpath('a/@href')[0].replace('./','')
            self.title=gonggao.xpath('a/text()')[0].replace('\r','').replace('\n','')
            self.title_time=gonggao.xpath('span/text()')[0]
            if self.title_time < self.oldest_time:

                raise MyException.TitleOverTimeException
            url=self.url+url
            re_words = u'(?:欠税|非正常户)'
            if re.search(u'%s' % (re_words,), self.title):

                self.deal_title(url)
    def deal_title(self,url):
        if not self.is_exist():
            self.filename=url.split('/')[-1]
            self.look_for_a(url)
            logging.info('-----'+url)



if __name__ == '__main__':

    gevent.monkey.patch_all()
    tasks=[]
    today=datetime.date.today()
    file_name = os.path.basename(__file__).split('.')[0]
    MainFunc.MainFunc.write_log(file_name)

    m='湖北省'


    for n in config_two[m].keys():
         for x in config_two[m][n].keys():
             shuiwu=ShuiWu(m,n,dishui_type=x)
             dd=shuiwu.do_dishui
             tasks.append(gevent.spawn(dd))
    gevent.joinall(tasks)







