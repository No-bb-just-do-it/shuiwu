# coding=utf-8
import PackageTool
import re
import os
import traceback
import gevent.monkey
import datetime
from lxml import html
import urlparse
import logging
from tax import MySQL,MyException,config,SpiderMan_FYH,MainFunc
from tax.Mysql_Config_Fyh import logger
requests=SpiderMan_FYH.SpiderMan()


class ShuiWu(MainFunc.MainFunc):
    def __init__(self,url,region):
        super(ShuiWu,self).__init__()
        self.province=u'海南省'
        self.url=url
        self.region=region
        self.domain='http://www.tax.hainan.gov.cn'
        self.oldest_time='2016-01-01'
        self.get_tk_url='http://www.tax.hainan.gov.cn/attachment_url.jspx'
        self.download_url='http://www.tax.hainan.gov.cn/attachment.jspx?'
        self.pinyin = 'Hai_Nan'

    def log(self, message):
        log_name = 'hai_nan_tax_gs.log'
        logger(log_name, message)

    def get_title(self):
        self.log(self.region)
        self.log('url:' + self.url)
        tree = self.get_tree(self.url)
        try:
            self.get_content(tree)
        except MyException.TitleOverTimeException as e:
            self.log(str(e))
            pass

    def get_content(self,tree):
        title_list=tree.xpath('//div[@class="listrightbj_list"]/table[1]/tr')
        for one_title in title_list:
            self.title=one_title.xpath('td[2]/a/text()')[0]
            link = one_title.xpath('td[2]/a/@href')[0]
            url_detail=self.domain+link
            self.fbrq = self.replace_word(one_title.xpath('td[3]/text()')[0])
            self.title_time = self.fbrq
            if self.fbrq>self.oldest_time:
                re_words = u'(欠税|非正常户)'
                if re.search(u'%s' % (re_words,), self.title):
                    print self.title,url_detail
                    if not self.is_exist():
                        print 'filename hasnot existed'
                        self.look_for_attachment(url_detail)
                    else:
                        print 'filename has existed'
                        self.look_for_attachment(url_detail)
            else:
                self.log(u'发布日期爬取到达设定最早日期')
                raise MyException.TitleOverTimeException

    def look_for_attachment(self,url):
        self.doc_url=url
        self.filename=url.split('/')[-1]
        content=requests.get(url)
        content.encoding='utf-8'
        try:
            inform=re.findall('Cms.attachment\((.*?)\)',content.text)[0]
        except:
            print url
        else:
            inform_list=inform.split(',')
            print 'inform_list',inform_list
            self.cid = self.replace_word(inform_list[1])
            n=self.replace_word(inform_list[2])
            #xls文件动态加载出来
            if int(n)>=1:
                print self.doc_url
                params={'cid':self.cid,'n':n}
                tks=self.get_tandk(params).split('","')
                print 'tks',tks,len(tks)
                i = 0
                for tk in tks:
                    self.doc_url=self.download_url+'cid='+self.cid+'&i=%d'%i + tk
                    print 'doc_url',self.doc_url
                    self.filename = re.findall('id="attach%d">(.*?)<'%i,content.text)[0]
                    print 'filename',self.filename
                    self.download()
                    i += 1
            #页面还有静态xls等文件
            else:
                count=0
                tree = html.fromstring(content.text)
                all_a = tree.xpath('//a/@href')
                for a in all_a:
                    re_words = u'(?:\.doc|\.docx|\.xls|\.xlsx|\.pdf|\.xlt)'
                    if re.search(u'%s' % (re_words,), a):
                        self.filename = a.split('/')[-1]
                        if self.filename.endswith('.xlt'):
                            self.filename = self.filename.replace('.xlt', '.xls')
                        count += 1
                        self.doc_url = self.domain + a

                        self.download()
                if count == 0:
                    self.download()

    def get_tandk(self,params):
        tk=requests.get(url=self.get_tk_url,params=params).text
        return re.findall('"(.*)"',tk)[0]

    def replace_word(self, word):
        return word.replace('\n', '').replace('\t', '').replace('"', '').strip()

def run():
    url_dict=config.hainan_config
    for k,url in url_dict.items():
        a=ShuiWu(url,k)
        a.get_title()


if __name__=='__main__':
    run()

