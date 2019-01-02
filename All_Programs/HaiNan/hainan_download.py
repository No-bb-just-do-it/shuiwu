# coding=utf-8

import requests
from bs4 import BeautifulSoup
import sys
import os
from tax.config import TaxConfig
import time,datetime
import mysql.connector
import re
import gevent,gevent.monkey
from codecs import open
import chardet
from urllib.parse import urljoin
import threading
lock = threading.Lock()

class HaiNan(TaxConfig):
    def __init__(self):
        super(HaiNan,self).__init__()
        self.session = None
        self.province = "海南省"
        self.log_name = 'HaiNan.log'
        self.path = self.get_savefile_directory('HaiNan')
        self.url_host = 'http://www.hitax.gov.cn'

    def log(self,message):
        self.log_base(self.log_name,message)

    #海南省税务局欠税信息
    def qs_abnormal_province(self):
        for q in ['欠税','非正常']:
        # for q in ['非正常']:
            print('type:',q)
            self.last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')
            params = {
            'q':q,
            }
            url_start = 'http://www.hitax.gov.cn/search.jspx'
            # for t in titles:
            #     title = t.find(attrs={'class':'jsearch-result-title'})
            #     print(title)
            #     print(title.text)
            last_update_time = time.strftime("%Y-%m-%d %H:%M:%S")
            titles = self.get_titles(url_start, params=params)
            # print('len_titles ',len(titles))
            if not titles:
                print('无详情页列表信息，爬虫结束')
                break
            tList = []
            for num, title in enumerate(titles[::2]):
                # print('@'*100)
                # print('num:',num)
                # print(title)

                fbrq = titles[num*2+1].find_all(attrs={'class':'liulcs'})[1].text.replace('日期：','').strip()
                # print(fbrq)
                if not fbrq:
                    continue
                # fbrq = fbrq.replace('-','').replace(' ','').replace('年','-').replace('月','-').replace('日','')
                if fbrq <= self.fbrq_stop:
                    self.stop_crawl = True
                    print('发布日期爬取到达设定最早日期')
                    break
                # print(fbrq)
                # parse_detail = self.parse_detail(title,fbrq=fbrq)
                t = threading.Thread(target=self.parse_detail, args=(title, fbrq))
                t.daemon = True
                tList.append(t)
            for t in tList:
                t.start()
                t.join()
            # for t in tList:
            #     t.join()

    #解析列表页，返回title列表
    def get_titles(self,url,params=None,headers=None):
        for t in range(5):
            try:
                r = self.get(url,params=params)
                # r1 = requests.get(url,params=params,headers=headers)
                # print(r1.content)
                # print(r.text)
                if r.status_code == 200:
                    # r.encoding = 'gbk'
                    res = BeautifulSoup(r.text, 'html.parser')
                    box = res.find(attrs={'class':'search_result_block'})
                    title_list = box.find_all('div')[1:-2]
                    print(len(title_list))
                    # for i in title_list:
                    #     print(i)
                    return title_list
            except Exception as e:
                print(e)
                return None

    #解析详情页
    def parse_detail(self, title,fbrq):
        # div_url = title.find(attrs={'class':'jsearch-result-url'})
        # print('div_url  ',div_url)
        a = title.find_all('a')[1]
        href = a.get('href')
        url_detail = urljoin(self.url_host,href)
        print('url_detail: ',url_detail)
        html_filename = self.get_html_filename(url_detail)
        html_savepath = os.path.join(self.path,html_filename)
        titleText = a.text.strip()
        print(titleText)
        if '欠' in titleText or '缴' in titleText or '非正常户' in titleText or '失踪' in titleText:
            # url_detail = 'http://www.hhgtax.gov.cn/hhgtax/article_content_xxgk.jsp?id=20181106283800&smallclassid=20180629130174'
            r_inner = self.get(url_detail)
            # if not r_inner:
            #     return
            charset1 = chardet.detect(r_inner.content)['encoding']
            # print(charset1)
            r_inner.encoding = 'utf-8'
            res_inner = BeautifulSoup(r_inner.text, 'html.parser')
            # print(res_inner)
            res_inner_str = str(res_inner)
            # print(res_inner_str)
            a_detail_inners = re.findall(r'<a.*?href=.*?</a>|<A.*?href=.*?</A>', res_inner_str)
            # print(a_detail_inners)
            href_inners = self.get_href(a_detail_inners)
            # print(href_inners)
            if href_inners:
                for href_inner in href_inners:
                    download_url = self.url_host + href_inner
                    # print('download_url ',download_url)
                    # self.log('download_url: ' + download_url)
                    # print('download_url', download_url)
                    # filter_condition = self.check_download_url(download_url)
                    # if filter_condition:
                    filename = self.get_filename(download_url)
                    savepath = os.path.join(self.path, filename)
                    sql = "INSERT into taxplayer_filename VALUES('%s', '%s', '%s', '%s', " \
                          "'%s', '%s', '%s')" % (self.province, '',fbrq, titleText, filename,
                                                 download_url, self.last_update_time)
                    if os.path.isfile(savepath):
                        self.save_to_mysql(sql,self.log_name,lock=lock)
                    else:
                        # self.log('url_detail: ' + url_detail)
                        # self.log('download_url: ' + download_url)
                        print('download_url', download_url)
                        self.download_file(download_url, filename, savepath)
                        self.save_to_mysql(sql,self.log_name,lock=lock)
            else:
                sql = "INSERT into taxplayer_filename VALUES('%s', '%s', '%s', '%s', '%s', " \
                      "'%s', '%s')" % (self.province, '',fbrq, titleText, html_filename, url_detail,
                                       self.last_update_time)
                if os.path.isfile(html_savepath):

                    self.save_to_mysql(sql,self.log_name,lock=lock)
                else:
                    # self.log('url_detail_down_html: ' + url_detail)
                    print('url_detail_html ',url_detail)
                    with open(html_savepath, 'w',encoding='utf-8') as f:
                        # print(r_inner.content.decode('gbk'))
                        f.write(r_inner.content.decode(charset1,'ignore'))

                    self.save_to_mysql(sql,self.log_name,lock=lock)


if __name__ == '__main__':
    hainan = HaiNan()
    hainan.qs_abnormal_province()