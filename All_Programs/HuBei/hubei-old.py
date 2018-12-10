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
from tax import MyException,MySQL,MainFunc
from tax.config import config_one,config_two

class ShuiWu(MainFunc.MainFunc):
    def __init__(self,province,region,dishui_type=None):
        super(ShuiWu,self).__init__()
        self.pinyin='Hu_Bei'
        self.region=region

        self.data={
            'col':'1',
            'appid':'1',
            'webid':'1',
            'path':'/',
            'columnid':config_one[province][region]['columnid'],
            'sourceContentType':'1',
            'unitid':config_one[province][region]['unitid'],
            'webname':config_one[province][region]['webname'],
            'permissiontype':'0'
        }
        self.url=config_one[province][region]['domain']
        self.province=province
        result=urlparse.urlparse(self.url)
        self.href_ahead=result.scheme+'://'+result.netloc
        self.oldest_time='2015-01-01'

    def do_guoshui(self):
        root = self.get_root()
        lst_node = re.findall('totalRecord=(.*?)\.',root)[0]
        params={
            'startrecord':'0',
            'endrecord':str(int(lst_node)-1),
            'perpage':str(lst_node)
        }
        root = self.get_root(params=params)
        results = re.findall('dataStore = \[(.*)\];',root,re.S)[0]
        data_list=results.split(',')

        for b in data_list:
            self.deal_node(b)

    def get_root(self,params=False):
        if params:
            a=requests.post(self.url,data=self.data,params=params,headers=self.headers)
        else:
            a=requests.post(self.url,data=self.data,headers=self.headers)

        root = a.text
        return root

    def deal_node(self,node):
        tree = html.fromstring(node.replace('\\\'', '\''))
        href = tree.xpath('//tr/td[2]/a/@href')[0]
        self.title = tree.xpath('//tr/td[2]/a/@title')[0]

        self.title_time=tree.xpath('//tr/td[3]/text()')[0]

        if self.title_time>self.oldest_time:
            re_words=u'(?:非正常户|欠税)'
            if re.search(u'%s' %(re_words,),self.title):
                if not self.is_exist():
                    self.filename=href.split('/')[-1]
                    real_href=self.href_ahead+href
                    logging.info('---------'+real_href)
                    self.look_for_a(real_href)

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

if __name__ == '__main__':
    gevent.monkey.patch_all()
    tasks=[]
    today=datetime.date.today()
    file_name = os.path.basename(__file__).split('.')[0]
    MainFunc.MainFunc.write_log(file_name)

    m='湖北省'
    for n in config_one[m].keys():
        shuiwu=ShuiWu(m,n)
        cc=shuiwu.do_guoshui
        tasks.append(gevent.spawn(cc))

    gevent.joinall(tasks)







