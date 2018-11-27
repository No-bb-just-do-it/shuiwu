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
import gevent
import gevent.monkey
gevent.monkey.patch_all()


class HeBeiTaxCrawler(TaxplayerDownload):
    def __init__(self):
        super(HeBeiTaxCrawler, self).__init__()
        self.province = u'河北省'
        self.province_py = 'He_Bei'
        self.path = self.get_savefile_directory(self.province_py)
        print self.path
        self.last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')
        self.get_directory(self.path)
        self.set_config()

    def set_config(self):
        self.xzqys = [u'石家庄市', u'承德市', u'张家口', u'秦皇岛市', u'秦皇岛市', u'唐山市', u'廊坊市', u'保定市',
                      u'沧州市', u'衡水市', u'邢台市', u'邯郸市']
        self.xzqy_pys = ['sjz', 'cd', 'zjk', 'qhd', 'qhd', 'ts', 'lf', 'bd', 'cz', 'hs', 'xt', 'hd']
        self.url_sources = [
            'http://www.he-n-tax.gov.cn/sjzgsww_new/bsfw/qsgg',
            'http://www.he-n-tax.gov.cn/cdgsww_new/bsfw/ssgg/qsgg',
            'http://www.he-n-tax.gov.cn/zjkgsww_new/bsfw/ssgg/qsgg',
            'http://www.he-n-tax.gov.cn/qhdgsww_new/bsfw/swgg/qsgg',
            'http://www.he-n-tax.gov.cn/qhdgsww_new/bsfw/swgg/fzchgg',
            'http://www.he-n-tax.gov.cn/tsgsww_new/bsfw/ssgg/qsgg',
            'http://www.he-n-tax.gov.cn/lfgsww_new/bsfw/ssgg/qsgg',
            'http://www.he-n-tax.gov.cn/bdgsww_new/bsfw/ssgg/qsgg',
            'http://www.he-n-tax.gov.cn/czgsww/wfuwu/wqsgg/czgsgg_35111',
            'http://www.he-n-tax.gov.cn/hsgsww_new/bsfw/ssgg/qsgg',
            'http://www.he-n-tax.gov.cn/xtgsww_new/qsgg15',
            'http://www.he-n-tax.gov.cn/hdgsww/bsfw_19806/tzgg/4',
        ]

    def log(self, message):
        self
        log_name = 'he_bei_tax.log'
        logger(log_name, message)

    def get_tag_list(self, i, url):
        tag_list = []
        gbks = [0, 8, 10]
        for t in range(15):
            r = self.get(url)
            if r.status_code == 200:
                if i in gbks:
                    r.encoding = 'gbk'
                else:
                    r.encoding = 'utf-8'
                res = BeautifulSoup(r.text, 'html5lib')
                if i == 0:
                    big_tags = res.findAll('div', {'class': 'tabcontent'})
                else:
                    big_tags = res.findAll('ul', {'class': 'lr_list'})
                for big_tag in big_tags:
                    tag_list.extend(big_tag.findAll('li'))
                return tag_list
        return tag_list

    def run(self):
        log_name = 'he_bei_tax.log'
        url_host = 'http://www.he-n-tax.gov.cn'
        gbks = [0, 8, 10]
        for num_source in range(12):
            self.stop_crawl = False
            if num_source in gbks:
                decode_way = 'gbk'
            else:
                decode_way = 'utf-8'
            region = self.xzqys[num_source] + u'国家税务局'
            url_source = self.url_sources[num_source]
            self.log(region)
            print region
            tasks = []
            for p in range(50):
                if self.stop_crawl == True:
                    print region + u'爬虫结束，页码: ',p
                    break
                if p == 0:
                    url = url_source + '/index.htm'
                else:
                    url = url_source + '/index_%s.htm' % str(p)
                self.log('url: ' + url)
                print 'url_list',url
                tag_list = self.get_tag_list(num_source, url)
                if not tag_list and p > 2:
                    break
                if tag_list:
                    for tag in tag_list:
                        fbrq = re.findall(r'\d{4}-\d{2}-\d{2}', tag.encode('utf-8'))
                        if not fbrq:
                            continue
                        else:
                            fbrq = fbrq[0]
                        if fbrq <= self.fbrq_stop:
                            self.stop_crawl = True
                            self.log('发布日期爬取到达设定最早日期')
                            break

                        parse_tag = self.parse_tag(num_source, tag,url_host,url_source,fbrq,log_name,region,decode_way)
                        tasks.append(gevent.spawn(parse_tag))
                    # 通过协程处理每个详情页信息
                    gevent.joinall(tasks)
                else:
                    print 'tag_list为空,page_url: ',url
                    self.log('tag_list为空,page_url:' + url)
                    self.stop_crawl = True

    #解析详情页，参数：网站序号，host链接，起始链接，发布日期，日志名称，行政区域，解码方式
    def parse_tag(self, num_source, tag,url_host,url_source,fbrq,log_name,region,decode_way):
                        a_tag = tag.find('a')
                        href = a_tag.get('href')[1:]
                        connect_date = href.split('/')[1]
                        title = a_tag.text.strip()
                        url_inner = url_source + href
                        self.log('url_inner: ' + url_inner)
                        print 'url_inner', url_inner
                        html_filename = self.get_html_filename(url_inner)
                        html_savepath = self.path + '\\' + html_filename
                        if u'欠' in title or u'缴' in title or u'非正常户' in title or u'失踪' in title:
                            r_inner = self.get(url_inner)
                            r_inner.encoding = decode_way
                            res_inner = BeautifulSoup(r_inner.text, 'html5lib')
                            res_inner_str = res_inner.encode('utf8')
                            a_tag_inners = re.findall(r'<a.*?href=.*?</a>', res_inner_str)
                            # print a_tag_inners
                            href_inners = self.get_href(a_tag_inners)
                            if href_inners:
                                for href_inner in href_inners:
                                    print href_inner
                                    download_url = url_source + '/' + str(connect_date) + href_inner[1:]
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
    crawler = HeBeiTaxCrawler()
    crawler.run()
