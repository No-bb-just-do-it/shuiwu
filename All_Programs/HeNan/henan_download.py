# coding=utf-8

import requests
from bs4 import BeautifulSoup
import sys
import os
from tax.config import TaxConfig
import time,datetime
import mysql.connector
import re
from urllib.parse import urljoin
import gevent,gevent.monkey
from codecs import open
import chardet
import threading
lock = threading.Lock()
import json

class HeNan(TaxConfig):
    def __init__(self):
        super(HeNan,self).__init__()
        # self.session = None
        self.province = "河南省"
        self.log_name = 'HeNan.log'
        self.path = self.get_savefile_directory('HeNan')

    def log(self,message):
        self.log_base(self.log_name,message)

    #河南省税务局欠税信息
    def qs_abnormal_province(self):
        self.url_host = 'http://www.hl-n-tax.gov.cn'
        # self.url_host = 'http://ln-n-tax.gov.cn'
        headers = {
        'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Host': 'www.hntax.gov.cn',
        'Referer': 'http://www.hntax.gov.cn/zhuanti/qsgg/article_list.jsp?city_id=-1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36',
        # 'Cookie': '_gscu_887772128=43978900x0jor278; _gscbrs_887772128=1; UM_distinctid=1677c505c8c407-0a43ffd4fe983-35667607-1aeaa0-1677c505c8daa6; yfx_c_g_u_id_10003718=_ck18120511015611277569793762083; _gscu_2010918185=43979057f7qhjt53; _gscbrs_2010918185=1; CNZZDATA1273987317=845349678-1543974078-null%7C1544061654; yfx_f_l_v_t_10003718=f_t_1543978916120__r_t_1544065121800__v_t_1544065121800__r_c_1; yfx_mr_10003718=%3A%3Amarket_type_free_search%3A%3A%3A%3Abaidu%3A%3A%3A%3A%3A%3A%3A%3Awww.baidu.com%3A%3A%3A%3Apmf_from_free_search; yfx_mr_f_10003718=%3A%3Amarket_type_free_search%3A%3A%3A%3Abaidu%3A%3A%3A%3A%3A%3A%3A%3Awww.baidu.com%3A%3A%3A%3Apmf_from_free_search; yfx_key_10003718=; JSESSIONID=-PWBcJBTyz2mGbJ3zEiaUdF8DlsN7rNf1jpyT_olRTsRFugqZUG7!-1947031093; _gscs_887772128=t44065121eo2pqs17|pv:2; _gscs_2010918185=t44065131n43mnq53|pv:3'
        }
        titles_before = []
        for q in ['欠税公告']:
        # for q in ['欠税']:
                self.last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')
                print('type:',q)
                params = {
                    'siteCode': 'bm29160021',
                    'keyPlace': 1,
                    'qt': '欠税公告 OR 欠税公告  OR 欠税公告',
                    'tab': 'xxgk',
                    'pageSize': 5,
                    'redTitleLength': 28,
                    'combine': 'MD5TITLE',
                    'mode': 1
                }
                url_start = 'http://47.95.44.144/interest'
                r = self.post(url_start,params=params)
                # print(r.text)
                res = json.loads(r.text)
                # print(res)
                # print(res['totalHits'])
                # http: // www.jsgs.gov.cn / jrobot / search.do
                # for t in titles:
                #     title = t.find(attrs={'class':'jsearch-result-title'})
                #     print(title)
                #     print(title.text)
                titles = res['resultList']
                for num, title in enumerate(titles):
                    # print(fbrq)
                    # if fbrq <= self.fbrq_stop:
                    #     print('fbrq ',fbrq)
                    #     self.stop_crawl = True
                    #     print('发布日期爬取到达设定最早日期')
                    #     break
                    parse_detail = self.parse_detail(title)
                #     t = threading.Thread(target=self.parse_detail, args=(title,fbrq))
                #     t.daemon = True
                #     tList.append(t)
                # for t in tList:
                #     t.start()
                    # t.join()
                # for t in tList:
                #     t.join()

    #解析详情页
    def parse_detail(self, title):
        fbrq = title['docDate']
        print(fbrq)
        last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')
        url_detail = title['url']
        print(url_detail)
        # url_detail = url_source + href
        # print('url_detail: ',url_detail)
        html_filename = self.get_html_filename(url_detail)
        # print('html_filename:',html_filename)
        html_savepath = os.path.join(self.path,html_filename)
        titleText = title['title']
        print('titleText ',titleText)
        if '欠' in titleText or '缴' in titleText or '非正常户' in titleText or '失踪' in titleText:
            # url_detail = 'http://www.hhgtax.gov.cn/hhgtax/article_content_xxgk.jsp?id=20181106283800&smallclassid=20180629130174'
            r_detail  = self.get(url_detail,allow_redirects=True)

            # print('new_url:',r_detail)
            print('titleText ',titleText)
            print('url_detail:',url_detail)
            if not r_detail or not r_detail.content:
                # print('....')
                return
            # print(r_detail.content)
            # print(r_detail.text)
            charset1 = chardet.detect(r_detail.content)['encoding']
            # print(charset1)
            r_detail.encoding = 'utf-8'
            # 'http://hrb.hl-n-tax.gov.cn/module/download/downfile.jsp?classid=0&filename=c3fcb5b5f708431d9ff60067328d2a21.xls'

            res_inner = BeautifulSoup(r_detail.text, 'html.parser')

            # print(res_inner.text)
            res_inner_str = str(res_inner)
            # print(res_inner_str)
            a_detail_inners = re.findall(r'<a.*?href=.*?</a>|<A.*?href=.*?</A>', res_inner_str)
            # for a in a_detail_inners:
            #     print(a)
            # print(a_detail_inners)
            href_inners = self.get_href(a_detail_inners)
            # print(href_inners)
            if href_inners:
                for href_inner in href_inners:
                    href_inner = href_inner
                    if '哈尔滨' in titleText:
                        url_host = 'http://hrb.hl-n-tax.gov.cn'
                        download_url = urljoin(url_host,href_inner)
                    else:
                        download_url = 'http://xz.ha-n-tax.gov.cn/xz'+ href_inner
                        # download_url = href_inner
                    # download_url = self.url_host + href_inner
                    # print('download_url ',download_url)
                    # self.log('download_url: ' + download_url)
                    # print('download_url', download_url)
                    # filter_condition = self.check_download_url(download_url)
                    # if filter_condition:
                    filename = self.get_filename(download_url)
                    savepath = os.path.join(self.path, filename)
                    sql = "INSERT into taxplayer_filename VALUES('%s', '%s', '%s', '%s', " \
                          "'%s', '%s', '%s')" % (self.province, '',fbrq, titleText, filename,
                                                 download_url, last_update_time)
                    if os.path.isfile(savepath):
                        self.save_to_mysql(sql,self.log_name,lock)
                    else:
                        # self.log('url_detail: ' + url_detail)
                        # self.log('download_url: ' + download_url)
                        print('download_url', download_url)
                        self.download_file(download_url, filename, savepath)
                        self.save_to_mysql(sql,self.log_name,lock)
            else:
                sql = "INSERT into taxplayer_filename VALUES('%s', '%s', '%s', '%s', '%s', " \
                      "'%s', '%s')" % (self.province, '',fbrq, titleText, html_filename, url_detail,
                                       last_update_time)
                if os.path.isfile(html_savepath):

                    self.save_to_mysql(sql,self.log_name,lock)
                else:
                    # self.log('url_detail_down_html: ' + url_detail)
                    print('download_html_url ',url_detail)
                    try:
                        with open(html_savepath, 'w',encoding='utf8') as f:
                            # print(r_detail.content.decode('gbk'))
                            if charset1:
                                f.write(r_detail.content.decode(charset1,'ignore'))
                            else:
                                f.write(r_detail.content)
                    except Exception as e:
                        print(e)
                        with open(html_savepath, 'wb') as f:
                            # print(r_detail.content.decode('gbk'))
                            if charset1:
                                f.write(r_detail.content.decode(charset1, 'ignore'))
                            else:
                                f.write(r_detail.content)
                    self.save_to_mysql(sql,self.log_name,lock=lock)

if __name__ == '__main__':
    henan = HeNan()
    henan.qs_abnormal_province()
    # s='location.href = "http://www.dl-n-tax.gov.cn/art/2018/8/6/art_1792_102463.html";'
    # a = re.findall(r'location.href = "(.*)";',s)
    # print(a)