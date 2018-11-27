# coding=utf-8
from tax.taxplayer_download import TaxplayerDownload
from tax.Mysql_Config_Fyh import logger
from tax.Mysql_Config_Fyh import data_to_mysql
from bs4 import BeautifulSoup
import os
import re
import sys
import time
import requests
import shutil
# from lxml import etree
import json
import gevent,gevent.monkey
gevent.monkey.patch_all()

class AnHuiTaxCrawler(TaxplayerDownload):
    def __init__(self):
        super(AnHuiTaxCrawler, self).__init__()
        self.province = u'山东省'
        self.province_py = 'Shan_Dong'
        self.path = self.get_savefile_directory(self.province_py)
        self.last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')
        self.get_directory(self.path)
        self.set_config()

    def set_config(self):
        self.xzqys = [u'青岛市',u'青岛市',u'山东省',]
        self.find_tags = ['newslist', 'newslist', 'dataStore']
        self.url_sources = [
            'http://www.qd-n-tax.gov.cn/ssgg/feizhengchanghu',              #青岛市非正常户
            'http://www.qd-n-tax.gov.cn/ssgg/qianshuigonggao',                #青岛市欠税公告
            'http://www.sd-n-tax.gov.cn/col/col43725/index.html',     #山东省地税
        ]

    def log(self, message):
        self
        log_name = 'shan_dong_tax_ds.log'
        logger(log_name, message)

    def get_tag_list(self, url, find_tag, num_surce,data = {}):
        tag_list = []
        big_tags = []
        for t in range(5):
            print url
            if num_surce == 0 or num_surce == 1:
                r = self.get(url)
            else:
                r = self.post(url,data=data)
            r.encoding = 'utf-8'
            res = BeautifulSoup(r.text, 'html5lib')

            if num_surce == 0 or num_surce == 1:
                table = res.find('div', {'class': find_tag})
                big_tags = table.findAll('li')
            elif num_surce == 2:
                big_tags = res.findAll('li')

            for big_tag in big_tags:
                tag_list.append(big_tag)
            if tag_list:
                return tag_list
        return tag_list

    def run(self):
        log_name = 'shan_dong_tax.log'
        for num_source in range(0, len(self.xzqys)):
            self.stop_crawl = False
            find_tag = self.find_tags[num_source]
            region = self.xzqys[num_source] + u'税务局'
            url_source = self.url_sources[num_source]
            url_host = 'http://' + url_source.split('/')[2]
            url_now = 'http://' + url_source.split('//')[1]
            print region, url_host
            self.log(region)
            for p in range(65):
                if self.stop_crawl == True:
                    print region + u'爬虫结束，页码: ',p
                    break
                data = {}
                if num_source == 2:
                    url = url_host + '/module/jslib/jquery/jpage/dataproxy.jsp?startrecord=%s&endrecord=%s&perpage' \
                                       '=20' % (str(p*20 + 1),str(p*20 + 21))

                    data = {'col': '1', 'appid': '1', 'webid': '25', 'path': '/', 'columnid': '43725',
                            'sourceContentType': '3',
                            'unitid': '307752', 'webname': '山东国税门户网站', 'permissiontype': '0'}
                else:
                    if p == 0:
                        url = url_source + '/index.htm'
                    else:
                        url = url_source + '/index_%s.htm' % str(p)
                self.log(region + '  ' + url)
                #取得大标题列表
                tag_list = self.get_tag_list(url, find_tag, num_source, data)
                tasks = []
                if tag_list:
                    for tag in tag_list:
                        fbrq = re.findall(r'\d{4}-\d{2}-\d{2}', tag.text.strip())

                        if not fbrq:
                            continue
                        else:
                            fbrq = fbrq[0]
                        print fbrq
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
                    self.stop_crawl = False

    #解析详情页，参数：网站序号，host链接，起始链接，发布日期，日志名称，行政区域
    def parse_tag(self, num_source, tag,url_host,url_source,fbrq,log_name,region):
        a_tag = tag.find('a')
        # print a_tag
        #拼接标题链接地址
        href = a_tag.get('href').replace("./", "/", 1)
        if num_source == 2:
            url_inner = url_host + href
        else:
            url_host_now = 'http://' + url_source.split('//')[1]
            url_inner = url_host_now + href
        self.log('url_inner: ' + url_inner)
        print 'url_inner', url_inner
        html_filename = self.get_html_filename(url_inner)
        html_savepath = self.path + html_filename
        # print 'html_savapath', html_savepath
        title = a_tag.text.strip()
        print title
        if u'欠' in title or u'缴' in title or u'非正常户' in title or u'失踪' in title:
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
                    savepath = self.path  + filename
                    sql = "INSERT into taxplayer_filename VALUES('%s', '%s', '%s', '%s', " \
                          "'%s', '%s', '%s')" % (self.province, region, fbrq, title, filename,
                                                 download_url, self.last_update_time)
                    if os.path.isfile(savepath):
                        data_to_mysql(log_name, 0, sql)
                    else:
                        self.download_file(download_url, filename, savepath)
                        data_to_mysql(log_name, 0, sql)

            else:
                sql = "INSERT into taxplayer_filename VALUES('%s', '%s', '%s', '%s', '%s', " \
                      "'%s', '%s')" % (self.province, region, fbrq, title, html_filename, url_inner,
                                       self.last_update_time)
                if os.path.isfile(html_savepath):
                    data_to_mysql(log_name, 0, sql)
                else:
                    self.download_htmlfile(r_inner, html_savepath)
                    data_to_mysql(log_name, 0, sql)



if __name__ == '__main__':
    crawler = AnHuiTaxCrawler()
    crawler.run()
