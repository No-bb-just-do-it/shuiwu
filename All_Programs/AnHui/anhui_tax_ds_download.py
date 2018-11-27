# coding=utf-8
import PackageTool
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
import gevent
import gevent.monkey
gevent.monkey.patch_all()


class AnHuiTaxCrawler(TaxplayerDownload):
    def __init__(self):
        super(AnHuiTaxCrawler, self).__init__()
        self.province = u'安徽省'
        self.province_py = 'An_hui'
        self.path = self.get_savefile_directory(self.province_py)
        self.last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')
        self.get_directory(self.path)
        self.set_config()

    def set_config(self):
        self.xzqys = [u'合肥市', u'淮北市', u'宿州市', u'蚌埠市', u'阜阳市', u'淮南市', u'滁州市', u'六安市',
                      u'马鞍山市', u'芜湖市', u'宣城市', u'铜陵市', u'池州市', u'安庆市', u'黄山市', u'合肥市',
                      u'六安市', u'马鞍山市', u'池州市']
        self.find_tags = ['td_x', 'boxxx', 'boxxx', 'list14', 'linex', 'boxxx', 'boxxx', 'boxxx', 'boxxh', 'news_list',
                          'boxxx', 'list_14xx', 'nylist14', 'boxxh', 'boxxx', 'td_x', 'boxxx', 'boxxh', 'nylist14']
        self.url_sources = [
            'http://www.ahhf-l-tax.gov.cn/hefei/bsfw/ssgg/qsgg',
            'http://www.ahhb-l-tax.gov.cn/huaibei/zwxxgk/qsgg',
            'http://60.166.52.36/suzhou/zwgk/ggl',
            'http://www.ahbb-l-tax.gov.cn/bengbu/swgk/qsgg',
            'http://www.ahfy-l-tax.gov.cn/fuyang/zwgk/ggxw/qsgg',  # 只有3个文件，已经手动好
            'http://www.ahhn-l-tax.gov.cn/huainan/bszx/sscx/qsgg',
            'http://60.166.52.36/chuzhou/nsfw/qsgg',
            'http://www.ahla-l-tax.gov.cn/liuan/zwgk/qsgg/qsgg',
            'http://www.ahma-l-tax.gov.cn/maanshan/nsfw/tzgg/sstz',
            'http://www.ahwh-l-tax.gov.cn/wuhu/dsfw/sscx/qsgg',
            'http://www.ahxc-l-tax.gov.cn/xuancheng/bsfw/tzgg/qsgg',
            'http://60.166.52.36/tltax/nsfw/tzgg/qsgg',
            'http://www.ahcz-l-tax.gov.cn/chizhou/bsfw/ssgg/qsgg',
            'http://www.ahaq-l-tax.gov.cn/anqing/zwgk/gggs',
            'http://www.ahhs-l-tax.gov.cn/hstax/bsfw/ssgg',
            'http://www.ahhf-l-tax.gov.cn/hefei/bsfw/ssgg/fzch',
            'http://www.ahla-l-tax.gov.cn/liuan/zwgk/qsgg/fzch',
            'http://www.ahma-l-tax.gov.cn/maanshan/nsfw/tzgg/fzch',
            'http://www.ahcz-l-tax.gov.cn/chizhou/bsfw/ssgg/fzchrd'
        ]

    def log(self, message):
        log_name = 'an_hui_tax_ds.log'
        logger(log_name, message)
    def get_tag_list(self, url, find_tag):
        tag_list = []
        for t in range(5):
            r = self.get(url)
            if r.status_code == 200:
                r.encoding = 'utf-8'
                res = BeautifulSoup(r.text, 'html5lib')
                big_tags = res.findAll('table', {'class': find_tag})
                for big_tag in big_tags:
                    tag_list.extend(big_tag.findAll('tr'))
                return tag_list
        return tag_list

    def run(self):
        self.log(self.last_update_time)
        log_name = 'an_hui_tax_ds.log'
        for num_source in range(len(self.xzqys)):
            self.stop_crawl = False
            find_tag = self.find_tags[num_source]
            region = self.xzqys[num_source] + u'税务局'
            url_source = self.url_sources[num_source]
            url_host = 'http://' + url_source.split('/')[2]
            self.log('-'*30 + region + '-'*30 + '\n')

            for p in range(50):
                if self.stop_crawl == True:
                    self.log(region + u'爬虫结束，当前列表页码: ' + str(p))
                    break
                if p == 0:
                    url = url_source + '/index.htm'
                else:
                    url = url_source + '/index_%s.htm' % str(p + 1)
                self.log(u'当前列表页地址:' + '  ' + url)
                tag_list = self.get_tag_list(url, find_tag)
                if not tag_list and p > 2:
                    break
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
                    self.stop_crawl = False

    #解析详情页，参数：网站序号，host链接，起始链接，发布日期，日志名称，行政区域
    def parse_tag(self, num_source, tag,url_host,url_source,fbrq,log_name,region):
        a_tag = tag.find('a')
        href = a_tag.get('href')
        url_inner = url_host + href
        self.log('url_inner: ' + url_inner)
        print 'url_inner', url_inner
        html_filename = self.get_html_filename(url_inner)
        html_savepath = self.path + '\\' + html_filename
        title = a_tag.text.strip()
        if u'欠' in title or u'缴' in title or u'非正常户' in title or u'失踪' in title:
            r_inner = self.get(url_inner)
            r_inner.encoding = 'utf-8'
            res_inner = BeautifulSoup(r_inner.text, 'html5lib')
            res_inner_str = res_inner.encode('utf8')
            a_tag_inners = re.findall(r'<a.*?href=.*?</a>|<A.*?href=.*?</A>', res_inner_str)
            href_inners = self.get_href(a_tag_inners)
            if href_inners:
                for href_inner in href_inners:
                    download_url = url_host + href_inner
                    # self.log('download_url: ' + download_url)
                    filename = self.get_filename(download_url)
                    savepath = self.path + '\\' + filename
                    sql = "INSERT into taxplayer_filename VALUES('%s', '%s', '%s', '%s', " \
                          "'%s', '%s', '%s')" % (self.province, region, fbrq, title, filename,
                                                 download_url, self.last_update_time)
                    if os.path.isfile(savepath):
                        data_to_mysql(log_name, 0, sql)
                    else:
                        self.download_file(download_url, filename, savepath)
                        self.log('download_url: ' + download_url)
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
