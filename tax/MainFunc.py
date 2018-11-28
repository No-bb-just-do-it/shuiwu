# coding=utf-8
import re
import os
import traceback
import gevent.monkey
import datetime
from lxml import html
import urlparse
import MySQL,MyException,config,SpiderMan
import LogConf
import logging
requests=SpiderMan.SpiderMan()
from bs4 import  BeautifulSoup


class MainFunc(object):
    def __init__(self):
        self.pinyin = ''
        self.filename = ''
        self.doc_url = ''
        self.title = ''
        self.fbrq = ''
        self.region = ''
        self.province = ''
        self.title_time = ''
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36'
        }

    def get_tree(self,url,encoding='utf-8'):
        html_content=requests.get(url)
        html_content.encoding=encoding
        if html_content.status_code!=200:
            print url
        tree=html.fromstring(html_content.text)
        return tree

    def requests_get(self, url, encoding='utf-8'):
        html_content = requests.get(url)
        html_content.encoding = encoding

        bs = BeautifulSoup(html_content.text, 'html5lib')
        return bs


    def download(self):
        self.insert_sql()
        if not os.path.exists('../../All_Files/%s/%s' % (self.pinyin, self.filename)):
            try:
                r = requests.get(self.doc_url)
            except:
                logging.error(self.filename+' download failed')
            else:
                with open('../../All_Files/%s/%s' % (self.pinyin, self.filename), 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024):
                        if chunk:
                            f.write(chunk)
                            f.flush()
                f.close()

    def is_exist(self):
        sql="select * from taxplayer_filename where title='%s' and fbrq='%s' and region='%s' " % (self.title, self.fbrq,self.region)
        print 'sql_query',sql
        return MySQL.execute_query(sql)

    def insert_sql(self):
        time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "insert into taxplayer_filename(province,region,fbrq,title,filename,url,last_update_time) values('%s','%s','%s','%s','%s','%s','%s')" % ( \
            self.province, self.region, self.title_time, self.title, self.filename, self.doc_url, time_now)
        print '111sql',sql
        try:
            MySQL.execute_update(sql)
        except:
            pass
        else:
            pass

    @classmethod
    def write_log(cls, filename):
        LogConf.create_logfile(filename)


if __name__=='__main__':
    pass

