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


class JiangSuTaxCrawler(TaxplayerDownload):
    def __init__(self):
        super(JiangSuTaxCrawler, self).__init__()
        self.province = u'江苏省'
        self.province_py = 'Jiang_Su'
        self.path = self.get_savefile_directory(self.province_py)
        self.last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')
        self.get_directory(self.path)
        self.set_config()

    def set_config(self):
        self.xzqys = [u'(南京)', u'(无锡)', u'(徐州)', u'(常州)', u'(苏州)', u'(南通)', u'(连云港)', u'(苏州园区)',
                      u'(淮安)', u'(盐城)', u'(扬州)', u'(镇江)', u'(泰州)', u'(宿迁)', u'(保税区)']
        self.coluids = ['48038', '48118', '48039', '48119', '48040', '48120', '48041', '48121', '48042', '48122',
                        '48043', '48123', '48044', '48124', '48045', '48125', '48046', '48126', '48047', '48127',
                        '48048', '48128', '48049', '48129', '48050', '48130', '48051', '48131', '48052', '48132']
        self.unitids = ['73729', '74015', '73729', '74015', '73729', '74015', '73729', '74015', '73729', '74015',
                        '73729', '74015', '73729', '74015', '73729', '74015', '73729', '74015', '73729', '74015',
                        '73729', '74015', '73729', '74015', '73729', '74015', '73729', '74015', '73729', '74015']

    def log(self, message):
        log_name = 'jiang_su_tax.log'
        logger(log_name, message)

    def get_tag_list(self, num_surce, url, data):
        tag_list = []
        for t in range(5):
            if num_surce == 30:
                r = self.get(url)
                if r.status_code == 200:
                    r.encoding = 'utf-8'
                    res = BeautifulSoup(r.text, 'html5lib')
                    big_tags = res.findAll('table', {'class': 'tb_main'})
                    for big_tag in big_tags:
                        tag_list.extend(big_tag.findAll('tr')[1:])
                    return tag_list
            else:
                r = self.post(url, data=data)
                if r.status_code == 200:
                    r.encoding = 'utf-8'
                    res = BeautifulSoup(r.text, 'html5lib')
                    pattern = '<a.*?</a>.*?\d{4}-\d{2}-\d{2}'
                    tag_list = re.findall(pattern, res.encode('utf-8'))
                    return tag_list
        return tag_list

    def run(self):
        log_name = 'jiang_su_tax.log'
        url_host = 'http://pub.jsds.gov.cn'
        for num_source in range(31):
            self.stop_crawl = False
            self.log(str(num_source))
            if num_source == 30:
                region = u'江苏省国家税务局'
                data = dict()
            else:
                region = u'江苏省地方税务局(' + self.xzqys[num_source / 2] + u')'
                data = {'appid': '1', 'col': '1', 'columnid': self.coluids[num_source], 'path': '/', 'permissiontype': '0',
                        'unitid': self.unitids[num_source], 'webid': '1', 'webname': '江苏省地方税务局'}
            self.log(region)
            print region
            for p in range(50):
                if self.stop_crawl == True:
                    print region + u'爬虫结束，页码: ',p
                    break
                if num_source == 30:
                    url = 'http://xxgk.jsgs.gov.cn/xxgk/jcms_files/jcms1/web1/site/zfxxgk/search.jsp?' \
                          'showsub=1&orderbysub=0&cid=43&cid=43&jdid=1&divid=div4869&currpage=%s' % (p + 1)
                else:
                    url = 'http://pub.jsds.gov.cn/module/jslib/jquery/jpage/dataproxy.jsp?' \
                          'perpage=40&startrecord=%s&endrecord=%s' % (p * 40 + 1, (p + 1) * 40)
                self.log('url: ' + url)
                tag_list = self.get_tag_list(num_source, url, data)
                if not tag_list and p > 2:
                    break
                tasks = []
                if tag_list:
                    for tag in tag_list:
                        if num_source == 30:
                            fbrq = re.findall(r'\d{4}-\d{2}-\d{2}', tag.text.strip())[0]
                            href = tag.find('a').get('href')
                            title = tag.find('a').get('mc')
                            url_inner = href
                        else:
                            fbrq = re.findall(r'\d{4}-\d{2}-\d{2}', tag)[0]
                            soup = BeautifulSoup(tag, 'html.parser')
                            a_tag = soup.find('a')
                            href = a_tag.get('href').split("'")[1].replace('\\', '')
                            title = a_tag.text.strip()
                            url_inner = url_host + href
                        if fbrq <= self.fbrq_stop:
                            self.stop_crawl = True
                            print u'发布日期爬取到达设定最早日期'
                            break
                        parse_tag = self.parse_tag(num_source, tag,url_host,url_inner,fbrq,log_name,region,title)
                        tasks.append(gevent.spawn(parse_tag))
                    # 通过协程处理每个详情页信息
                    gevent.joinall(tasks)
                else:
                    print u'tag_list为空,page_url: ',url
                    self.log(u'tag_list为空,page_url:' + url)
                    self.stop_crawl = True

    #解析详情页，参数：网站序号，host链接，详情页链接，发布日期，日志名称，行政区域，详情页标题
    def parse_tag(self, num_source, tag,url_host,url_inner,fbrq,log_name,region,title):
        self.log('url_inner: ' + url_inner)
        print 'url_inner', url_inner
        html_filename = self.get_html_filename(url_inner)
        html_savepath = self.path + '\\' + html_filename
        if u'欠' in title or u'缴' in title or u'非正常户' in title or u'失踪' in title:
            r_inner = self.get(url_inner)
            r_inner.encoding = 'utf-8'
            res_inner = BeautifulSoup(r_inner.text, 'html5lib')
            res_inner_str = res_inner.encode('utf8')
            a_tag_inners = re.findall(r'<a.*?href=.*?</a>', res_inner_str)
            href_inners = self.get_href(a_tag_inners)
            if href_inners:
                for href_inner in href_inners:
                    if num_source == 30:
                        download_url = 'http://xxgk.jsgs.gov.cn' + href_inner
                    else:
                        download_url = href_inner
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
    crawler = JiangSuTaxCrawler()
    crawler.run()
