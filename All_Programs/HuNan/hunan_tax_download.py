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


class HuNanTaxCrawler(TaxplayerDownload):
    def __init__(self):
        super(HuNanTaxCrawler, self).__init__()
        self.province = u'湖南省'
        self.province_py = 'Hu_Nan'
        self.path = self.get_savefile_directory(self.province_py)
        self.last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')
        self.get_directory(self.path)
        self.set_config()

    def set_config(self):
        self.xzqys = [u'长沙市', u'株洲市', u'湘潭市', u'岳阳市', u'衡阳市', u'常德市', u'益阳市',
                      u'邵阳市', u'郴州市', u'永州市', u'娄底市', u'张家界市', u'怀化市', u'湘西自治州']
        self.xzqy_pys = ['cs', 'zz', 'xt', 'yy', 'hy', 'cd', 'yy', 'sy', 'cz', 'yz', 'ld', 'zjj', 'hh', 'xx']
        self.class_ids = ['20091203033149', '20091203033151', '20160303922318', '20160303922315', '20131214961797']
        # 奇数为非正常户公告，偶数为欠税公告。后面3个是特殊情况。

    def log(self, message):
        self
        log_name = 'hu_nan_tax.log'
        logger(log_name, message)

    def get_tag_list(self, num_source, url,params=None):
        tag_list = []
        for t in range(5):
            r = self.get(url)
            print r.content
            if r.status_code == 200:
                r.encoding = 'gbk'
                res = BeautifulSoup(r.text, 'html5lib')
                if self.xzqys[num_source] == u'衡阳市':
                    big_tags = res.findAll('ul', {'class': 'cls_ul_art_list'})
                else:
                    big_tags = res.findAll('div', {'class': 'listcont'})
                for big_tag in big_tags:
                    tag_list.extend(big_tag.findAll('li'))
                return tag_list
        return tag_list

    def run(self):
        log_name = 'hu_nan_tax.log'
        for num_source in range(0, len(self.xzqys)):
            self.stop_crawl = False
            region = self.xzqys[num_source] + u'国家税务局'
            xzqy_py = self.xzqy_pys[num_source]
            for j in range(2):
                for p in range(50):
                    if self.stop_crawl == True:
                        print region + u'爬虫结束，页码: ',p
                        break

                    if self.xzqys[num_source] == u'衡阳市':
                        class_id = self.class_ids[j + 2]
                    elif self.xzqys[num_source] == u'邵阳市' and j == 0:
                        class_id = self.class_ids[4]
                    else:
                        class_id = self.class_ids[j]
                    # url = 'http://www.%sgtax.gov.cn/%sgtax/article_list.jsp?' \
                    #       'pagenum=%s&smallclassid=%s' % (xzqy_py, xzqy_py, str(p), class_id)
                    url = 'http://www.csgtax.gov.cn/csgtax/article_list_xxgk_fl.jsp'
                    self.log(region + '  ' + url)
                    url_source = 'http://www.%sgtax.gov.cn/%sgtax/' % (xzqy_py, xzqy_py)
                    url_host = 'http://www.%sgtax.gov.cn' % xzqy_py
                    params = {
                        'pagenum': 0,
                        'smallclassid': 20180629130174
                    }
                    tag_list = self.get_tag_list(num_source, url,params=params)
                    if not tag_list and p > 2:
                        break
                    tasks = []
                    if tag_list:
                        for tag in tag_list:
                            fbrq = tag.find('span').text.strip()
                            if not fbrq:
                                continue
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
        href = a_tag.get('href')
        url_inner = url_source + href
        self.log('url_inner: ' + url_inner)
        # print 'url_inner', url_inner
        html_filename = self.get_html_filename(url_inner)
        html_savepath = self.path + '\\' + html_filename
        title = a_tag.get('title')
        if u'欠' in title or u'缴' in title or u'非正常户' in title or u'失踪' in title:
            r_inner = self.get(url_inner)
            r_inner.encoding = 'gbk'
            res_inner = BeautifulSoup(r_inner.text, 'html5lib')
            res_inner_str = res_inner.encode('utf8')
            a_tag_inners = re.findall(r'<a.*?href=.*?</a>|<A.*?href=.*?</A>', res_inner_str)
            href_inners = self.get_href(a_tag_inners)
            if href_inners:
                for href_inner in href_inners:
                    download_url = url_host + href_inner
                    self.log('download_url: ' + download_url)
                    print 'download_url', download_url
                    # filter_condition = self.check_download_url(download_url)
                    # if filter_condition:
                    filename = self.get_filename(download_url)
                    savepath = self.path + '\\' + filename
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
    crawler = HuNanTaxCrawler()
    crawler.run()
