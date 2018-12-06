# coding=utf-8

import sys
import os

project = 'shuiwu'  # 工作项目根目录
sys.path.append(os.getcwd().split(project)[0] + project)
# print os.getcwd()
# print os.getcwd().split(project)
# print os.getcwd().split(project)[0]+project

from tax.SpiderMan import SpiderMan
from tax.taxplayer_download import TaxplayerDownload
from tax.Mysql_Config_Fyh import data_to_mysql
import re
import time
import gevent,gevent.monkey
gevent.monkey.patch_all()

# curPath = os.path.abspath(os.path.dirname(__file__))
# rootPath = os.path.split(curPath)[0]
# sys.path.append(rootPath)

import requests
from multiprocessing import pool
from bs4 import BeautifulSoup

import mysql.connector

class ShangHaiTaxplayerCrawler(TaxplayerDownload,SpiderMan):
    def __init__(self):
        super(ShangHaiTaxplayerCrawler, self).__init__()
        self.province = u'上海市'
        self.province_py = 'Shang_Hai'
        self.path = self.get_savefile_directory(self.province_py)
        print 'self.path ',self.path
        self.last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')
        self.order_nbr = '5fe6cf97-5592-11e7-be16-f45c89a63279'
        self.connect = mysql.connector.connect(host='172.16.0.76', port=3306, user='fengyuanhua', passwd='!@#qweASD', db='taxplayer',
                           charset='utf8')
        self.cursor = self.connect.cursor()

    def get_url_info(self):
        info = ''
        sql = "select * from taxplayer_url where pid = '1' and category_id = '2'"
        self.cursor.execute(sql)  # 返回符合条件的总数表
        # info = self.cursor.fetchmany(nums)
        info = self.cursor.fetchall()
        return info[11:]


    def log(self, message):
        log_name = 'sh_taxplayer_crawler.log'
        # self.logger(log_name, message)
        print message

    def get_tag_list(self,url):
        tag_list = []
        for i in range(5):
            r = self.get(url)
            r.encoding = 'utf-8'
            res = BeautifulSoup(r.text, 'html5lib')
            tag_div = res.findAll('div', {'class': 'list_content'})
            if tag_div:
                tag_list = tag_div[0].findAll('dd')
                return tag_list
        return tag_list

    def run(self):
        log_name = 'shang_hai_tax_qs.log'
        info = self.get_url_info()
        print len(info)
        for num_source in range(len(info)):
            self.stop_crawl = False
            region = info[num_source][6]
            print region
            xzqy_py = info[num_source][7]
            url_source = info[num_source][10]
            url_host = info[num_source][12]
            # self.log(region + ' ' + xzqy_py + ' ' + url_source)
            url = url_source + '/index.html'
            print url
            # self.log(region)
            for p in range(0, 30):
                if self.stop_crawl == True:
                    print region + u'爬虫结束，页码: ',p
                    break
                if p == 0:
                    url = url
                    print 'url1',url
                else:
                    url = url_source + '/index_' + str(p) + '.html'
                # self.log(region + '  ' + url)
                tag_list = self.get_tag_list(url)
                tasks = []
                if tag_list:
                    for tag in tag_list:
                        fbrq = re.findall(r'\d{4}-\d{2}-\d{2}', tag.text.strip())
                        if not fbrq:
                            continue
                        else:
                            fbrq = fbrq[0]
                        if fbrq <= self.fbrq_stop:
                            self.stop_crawl = True
                            print u'发布日期爬取到达设定最早日期'
                            break

                        parse_tag = self.parse_tag(num_source, tag,url_host,url_source,fbrq,log_name,region)
                        tasks.append(gevent.spawn(parse_tag))
                    # 通过协程处理每个详情页信息
                    gevent.joinall(tasks)
                else:
                    print u'tag_list为空,page_url: ',url
                    self.log(u'tag_list为空,page_url:' + url)
                    self.stop_crawl = True

    #解析详情页，参数：网站序号，host链接，起始链接，发布日期，日志名称，行政区域
    def parse_tag(self, num_source, tag,url_host,url_source,fbrq,log_name,region):
        a_tag = tag.find('a')
        #拼接标题链接地址
        url_now = 'http://' + url_source.split('//')[1]
        href = a_tag.get('href')
        if './' in href:
            href = a_tag.get('href').replace("./", "/", 1)
            url_inner = url_now + href
        else:
            url_inner = url_host + href
        self.log('url_inner: ' + url_inner)
        # print 'url_inner', url_inner

        html_filename = self.get_html_filename(url_inner)
        html_savepath = os.path.join(self.path,html_filename)
        title = a_tag.text.strip()
        if (u'欠税公告' in title or u'欠' in title or u'非正常' in title) and (u'催缴' not in title):
            print 'title',title,url_inner
            r_inner = self.get(url_inner)
            r_inner.encoding = 'utf-8'
            res_inner = BeautifulSoup(r_inner.text, 'html5lib')
            res_inner_str = res_inner.encode('utf8')
            a_tag_inners = re.findall(r'<a.*?href=.*?</a>|<A.*?href=.*?</A>', res_inner_str)
            #匹配详情页内链接地址
            href_inners = self.get_href(a_tag_inners)
            if href_inners:
                for href_inner in href_inners:
                    url_host_now = re.findall(r'(.*)/.*?', url_inner)[0]
                    if './' in str(href_inner):
                        # 下载地址拼接为当前路径+href
                        href_inner = href_inner.replace('./', '/', 1)
                        #拼接下载链接地址
                        download_url = url_host_now + href_inner
                    else:
                        download_url = url_host + href_inner
                    print 'download_url', download_url
                    filename = self.get_filename(download_url)
                    savepath = os.path.join(self.path,filename)

                    sql = "INSERT into taxplayer_filename VALUES('%s', '%s', '%s', '%s', " \
                          "'%s', '%s', '%s')" % (self.province, region, fbrq, title, filename,
                                                 download_url, self.last_update_time)
                    if os.path.isfile(savepath):
                        data_to_mysql(log_name, 0, sql)
                    else:
                        self.download_file(download_url, filename, savepath)
                        data_to_mysql(log_name, 0, sql)
                        print 'download_url_true', download_url
            else:
                sql = "INSERT into taxplayer_filename VALUES('%s', '%s', '%s', '%s', '%s', " \
                      "'%s', '%s')" % (self.province, region, fbrq, title, html_filename, url_inner,
                                       self.last_update_time)
                if os.path.isfile(html_savepath):
                    data_to_mysql(log_name, 0, sql)
                else:
                    self.download_htmlfile(r_inner, html_savepath)
                    data_to_mysql(log_name, 0, sql)
                    print 'html_savepath', html_savepath


if __name__ == '__main__':
    Crawler = ShangHaiTaxplayerCrawler()
    Crawler.run()
    # Crawler.get_url_info()
