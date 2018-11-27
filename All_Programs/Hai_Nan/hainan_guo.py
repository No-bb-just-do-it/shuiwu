# coding=utf-8
import PackageTool
import re
import os
import traceback
import gevent.monkey
import datetime
from lxml import html
import urlparse
from tax import MySQL,MyException,SpiderMan_FYH,MainFunc
import logging
requests=SpiderMan_FYH.SpiderMan()



class ShuiWu(MainFunc.MainFunc):
    def __init__(self,url,region):
        super(ShuiWu, self).__init__()
        self.province = '海南省'
        self.url=url
        self.region=region
        self.domain='http://www.hitax.gov.cn'
        self.oldest_time = '2015-01-01'
        self.pinyin='Hai_Nan'



    def get_title(self):
        tree = self.get_tree(self.url)
        try:
            self.get_content(tree)
        except MyException.TitleOverTimeException:
            pass
        else:
            last_page_a=tree.xpath('//ul[@class="cPage"]/a[last()]/@onclick')[0]
            last_page_href=re.findall("\('(.*?)'\)",last_page_a)[0]
            last_num=int(re.findall('index_(.*?)\.',last_page_href)[0])
            last_num_param='index_'+str(last_num)
            href_ahead=self.url.split('index')[0]
            for i in range(2,last_num+1):
                param='index_'+str(i)
                url=re.sub(last_num_param,param,last_page_href)
                whole_url= href_ahead+url
                tree = self.get_tree(whole_url)
                try:
                    self.get_content(tree)
                except MyException.TitleOverTimeException:
                    break

    def get_content(self,tree):
        all_title=tree.xpath('//ul[@class="mpgright_sum_block"]/li')
        for one_title in all_title:
            self.title=one_title.xpath('a/text()')[0]
            self.title_time = one_title.xpath('span/text()')[0]
            link = one_title.xpath('a/@href')[0]
            whole_url=self.domain+link
            if self.title_time > self.oldest_time:
                if not self.is_exist():
                    logging.info('-----'+whole_url)
                    self.look_for_a(whole_url)
            else:
                raise MyException.TitleOverTimeException
    def look_for_a(self,url):
        self.doc_url=url
        self.filename = url.split('/')[-1]
        count=0
        tree=self.get_tree(url)
        all_a = tree.xpath('//a/@href')
        for a in all_a:
            re_words = u'(?:\.doc|\.docx|\.xls|\.xlsx|\.pdf|\.xlt)'
            if re.search(u'%s' % (re_words,), a):
                self.filename = a.split('/')[-1]
                if self.filename.endswith('.xlt'):
                    self.filename=self.filename.replace('.xlt','.xls')
                count+=1
                self.doc_url=self.domain+a
                self.download()
        if count==0:
            self.download()

if __name__=='__main__':
    url_dict={
        u'海南省国税欠税':'http://www.hitax.gov.cn/xxgk_4_1/',
        u'海南省国税非正常户': 'http://www.hitax.gov.cn/xxgk_4_8/',
    }
    file_name = os.path.basename(__file__).split('.')[0]
    MainFunc.MainFunc.write_log(file_name)
    for m in url_dict.keys():
        url=url_dict[m]
        sw = ShuiWu(url,m)
        sw.get_title()








