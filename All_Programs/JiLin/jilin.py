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
from xml.etree import ElementTree
from tax import MyException, MySQL,MainFunc
from tax.config import config_one, config_two
import logging

class ShuiWu(MainFunc.MainFunc):
    def __init__(self,province,region,dishui_type=None):
        super(ShuiWu,self).__init__()
        self.pinyin='Ji_Lin'
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
        lst_node = root.getiterator("totalrecord")[0].text
        params={
            'startrecord':'0',
            'endrecord':str(int(lst_node)-1),
            'perpage':str(lst_node)
        }
        root = self.get_root(params=params)
        results = root.getiterator("record")
        for node in results:
            self.deal_node(node)

    def get_root(self,params=False):
        if params:
            a=requests.post(self.url,data=self.data,params=params,headers=self.headers)
        else:
            a=requests.post(self.url,data=self.data,headers=self.headers)
        root = ElementTree.fromstring(a.text)
        return root

    def deal_node(self,node):

        self.title_time=re.findall("\d{4}-\d{2}-\d{2}",node.text)[0]
        if self.title_time>self.oldest_time:
            try:
                self.title=re.findall("title='(.*?)'",node.text)[0]
            except:
                self.title=re.findall('title="(.*?)"',node.text)[0]

            re_words=u'(?:欠税公告|欠税.*公告|欠缴税款|非正常户|欠费公告|关于清缴欠税的通告|非正户)'
            if re.search(u'%s' %(re_words,),self.title):
                if not self.is_exist():
                    href=re.findall('href="(.*?)"',node.text)[0]
                    self.filename=href.split('/')[-1]
                    real_href=self.href_ahead+href
                    logging.info('xls页面----------'+real_href)
                    self.look_for_a(real_href)

    def look_for_a(self,url):
        count=0

        tree=self.get_tree(url)
        all_a=tree.xpath('//a/@href')
        for a in all_a:
            if a.endswith('.doc') or a.endswith('.xls') or a.endswith('.xlsx') or a.endswith('.docx'):
                count+=1
                if a.startswith('http'):
                    self.doc_url=a
                else:
                    self.doc_url=self.href_ahead+a
                try:
                    self.filename=re.findall('filename=(.*)',a)[0]
                except:
                    self.filename=a.split('/')[-1]
                self.download()
        if count == 0:
            self.doc_url = url
            self.download()

if __name__ == '__main__':
    gevent.monkey.patch_all()
    tasks=[]
    today=datetime.date.today()
    file_name = os.path.basename(__file__).split('.')[0]
    MainFunc.MainFunc.write_log(file_name)

    m='吉林省'
    for n in config_one[m].keys():
        shuiwu=ShuiWu(m,n)
        cc=shuiwu.do_guoshui
        tasks.append(gevent.spawn(cc))

    gevent.joinall(tasks)







