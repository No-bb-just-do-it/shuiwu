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
import threading
lock = threading.Lock()

class AnHui(TaxConfig):
    def __init__(self):
        super(AnHui,self).__init__()
        self.session = None
        self.province = "安徽省"
        self.log_name = 'AnHui.log'
        self.path = self.get_savefile_directory('AnHui')

    def log(self,message):
        self.log_base(self.log_name,message)

    #安徽省税务局欠税信息
    def qs_province(self):
        url_host = 'http://www.ah-n-tax.gov.cn'
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
        for q in ['欠税','非正常']:
            for p in range(0,5):
                self.last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')
                print('page:',p)
                params = {
                'webid':39,
                'pg':12,
                'p': p,
                'q':q,
                }
                url_start = 'http://www.ah-n-tax.gov.cn/jrobot/search.do'
                # for t in titles:
                #     title = t.find(attrs={'class':'jsearch-result-title'})
                #     print(title)
                #     print(title.text)
                last_update_time = time.strftime("%Y-%m-%d %H:%M:%S")
                titles = self.get_titles(url_start, params=params)
                # print('len_titles ',len(titles))
                if not titles or titles_before == titles:
                    print('无详情页列表信息，爬虫结束')
                    break
                titles_before = titles
                tList = []
                for num, title in enumerate(titles):
                    # print('@'*100)
                    # print(title)

                    fbrq = title.find(attrs={'jsearch-result-date'}).text.strip()
                    if not fbrq:
                        continue
                    fbrq = fbrq.replace('-','').replace(' ','').replace('年','-').replace('月','-').replace('日','')
                    if fbrq <= self.fbrq_stop:
                        self.stop_crawl = True
                        print(u'发布日期爬取到达设定最早日期')
                        break
                    # print(fbrq)
                    # parse_detail = self.parse_detail(title,url_host=url_host,fbrq=fbrq)
                    t = threading.Thread(target=self.parse_detail, args=(title, url_host, fbrq))
                    t.daemon = True
                    tList.append(t)
                for t in tList:
                    t.start()
                    # t.join()
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
                    title_list = res.find_all(attrs={'class': 'jsearch-result-box'})
                    # for i in title_list:
                    #     print(i)
                    return title_list
            except Exception as e:
                print(e)
                return None

    #解析详情页
    def parse_detail(self, title,url_host,fbrq):
        div_url = title.find(attrs={'class':'jsearch-result-url'})
        # print('div_url  ',div_url)
        a = div_url.find('a')
        url_detail = a.text.strip()
        print(url_detail)
        # url_detail = url_source + href
        # print('url_detail: ',url_detail)
        html_filename = self.get_html_filename(url_detail)
        html_savepath = os.path.join(self.path,html_filename)
        titleText = title.find(attrs={'class':'jsearch-result-title'}).text.strip()
        print(titleText)
        if '欠' in titleText or '缴' in titleText or '非正常户' in titleText or '失踪' in titleText:
            # url_detail = 'http://www.hhgtax.gov.cn/hhgtax/article_content_xxgk.jsp?id=20181106283800&smallclassid=20180629130174'
            r_inner = self.get(url_detail)
            if not r_inner:
                return
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
                    href_inner = href_inner.replace('http://79.12.67.6','')
                    download_url = url_host + href_inner
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
    anhui = AnHui()
    anhui.qs_province()