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
import gevent,gevent.monkey
gevent.monkey.patch_all()


class GuangDongTaxCrawler(TaxplayerDownload):
    def __init__(self):
        super(GuangDongTaxCrawler, self).__init__()
        self.province = u'广东省'
        self.province_py = 'Guang_Dong'
        self.path = self.get_savefile_directory(self.province_py)
        self.last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')
        self.get_directory(self.path)
        self.set_config()

    def set_config(self):
        self.xzqys = [u'广东省', u'广州市', u'珠海市', u'汕头市', u'佛山市', u'韶关市', u'河源市', u'梅州市', u'惠州市',
                      u'汕尾市', u'东莞市', u'东莞市', u'中山市', u'江门市', u'阳江市', u'湛江市', u'茂名市', u'肇庆市',
                      u'清远市', u'潮州市', u'揭阳市', u'云浮市', u'深圳市']
        self.url_sources = [
            'http://www.gd-n-tax.gov.cn/pub/001021/xxgk/tzgg/ssgg',
            'http://www.gd-n-tax.gov.cn/pub/gzsgsww/xxgk/tzgg/ssgg',
            'http://www.gd-n-tax.gov.cn/pub/001002/xxgk/tzgg/ssgg',
            'http://www.gd-n-tax.gov.cn/pub/001021/xxgk/tzgg/ssgg',
            'http://www.gd-n-tax.gov.cn/pub/001028/xxgk/tzgg/ssgg/qsgg',
            'http://www.gd-n-tax.gov.cn/pub/001022/xxgk/tzgg/ssgg',
            'http://www.gd-n-tax.gov.cn/pub/001010/xxgk/tzgg/ssgg',
            'http://www.gd-n-tax.gov.cn/pub/001016/xxgk/tzgg/ssgg',
            'http://www.gd-n-tax.gov.cn/pub/001012/xxgk/tzgg/ssgg',
            'http://www.gd-n-tax.gov.cn/pub/001023/xxgk/tzgg/ssgg',
            'http://www.gd-n-tax.gov.cn/pub/001005/xxgk/tzgg/ssgg',
            'http://www.gd-n-tax.gov.cn/pub/001005/xxgk/tzgg/sjtzgg',
            'http://www.gd-n-tax.gov.cn/pub/001003/xxgk/tzgg/ssgg',
            'http://www.gd-n-tax.gov.cn/pub/001014/xxgk/tzgg/ssgg',
            'http://www.gd-n-tax.gov.cn/pub/001019/xxgk/tzgg/ssgg',
            'http://www.gd-n-tax.gov.cn/pub/001020/xxgk/tzgg/ssgg',
            'http://www.gd-n-tax.gov.cn/pub/001015/xxgk/tzgg/ssgg',
            'http://www.gd-n-tax.gov.cn/pub/001011/xxgk/tzgg/ssgg',
            'http://www.gd-n-tax.gov.cn/pub/001017/xxgk/tzgg/ssgg',
            'http://www.gd-n-tax.gov.cn/pub/001006/xxgk/tzgg/ssgg',
            'http://www.gd-n-tax.gov.cn/pub/001018/xxgk/tzgg/ssgg',
            'http://www.gd-n-tax.gov.cn/pub/001008/xxgk/tzgg/ssgg',
            'http://www.szgs.gov.cn/module/jslib/jquery/jpage/dataproxy.jsp'
        ]
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/58.0.3029.110 Safari/537.36'
        }

    def log(self, message):
        log_name = 'guang_dong_tax.log'
        logger(log_name, message)

    def get_tag_list(self, i, url):
        tag_list = []
        for t in range(15):
            if i == 22:
                data = {'col': '1', 'appid': '1', 'webid': '1', 'path': '/', 'columnid': '43', 'sourceContentType': '3',
                        'unitid': '2137', 'webname': '深圳国税', 'permissiontype': '0'}
                r = self.post(url, headers=self.headers, data=data)
            else:
                r = self.get(url)
            if r.status_code == 200:
                r.encoding = 'utf-8'
                res = BeautifulSoup(r.text, 'html5lib')
                if i == 22:
                    tag_list = res.findAll('record')
                    return tag_list
                else:
                    big_tags = res.findAll('div', {'class': 'tab_con_main box'})
                    for big_tag in big_tags:
                        tag_list.extend(big_tag.findAll('li'))
                    return tag_list
        return tag_list

    def download_sz(self, download_url, filename, savepath):
        for k in range(15):
            try:
                fs = self.get(download_url, headers=self.headers, timeout=15)
                if fs.status_code == 200:
                    pattern = 'http://.*?' + filename
                    download_url_news = re.findall(pattern, fs.content)
                    if download_url_news:
                        fs_new = self.get(download_url_news[0], headers=self.headers, timeout=15)
                        if fs_new.status_code == 200:
                            download_url_content = fs_new.content
                        else:
                            download_url_content = ''
                    else:
                        download_url_content = fs.content
                    with open(savepath, 'wb') as f:
                        f.write(download_url_content)
                    break
            except:
                if k == 14:
                    print u'第二次访问请求尝试结束', download_url
                else:
                    print u'第二次访问第' + str(k) + u'次请求尝试'

    def run(self):
        log_name = 'guang_dong_tax.log'
        for num_source in range(0, 23):
            self.stop_crawl = False
            region = self.xzqys[num_source] + u'国家税务局'
            url_source = self.url_sources[num_source]
            self.log(region)
            print region
            for p in range(50):
                if self.stop_crawl == True:
                    print region + u'爬虫结束，页码: ',p
                    break
                if num_source == 22:
                    url_host = 'http://www.szgs.gov.cn'
                    url = 'http://www.szgs.gov.cn/module/jslib/jquery/jpage/dataproxy.jsp?' \
                          'startrecord=%s&endrecord=%s&perpage=40' % (p * 40, (p + 1) * 40)
                else:
                    url_host = 'http://www.gd-n-tax.gov.cn'
                    if p == 0:
                        url = url_source + '/index.html'
                    else:
                        url = url_source + '/index_%s.html' % str(p)
                self.log('url: ' + url)
                tag_list = self.get_tag_list(num_source, url)
                if not tag_list and p > 2:
                    break
                tasks = []
                if tag_list:
                    for tag in tag_list:
                        fbrq = re.findall(r'\d{4}-\d{2}-\d{2}', tag.encode('utf-8'))
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
        connect_date = ''
        if num_source == 22:
            href = a_tag.get('href')
            url_inner = url_host + href
        else:
            href = a_tag.get('href')[1:]
            connect_date = re.findall('\d{6}', href)
            if connect_date:
                connect_date = connect_date[0]
            url_inner = url_source + href
        title = a_tag.text.strip()
        self.log('url_inner: ' + url_inner)
        print 'url_inner', url_inner
        html_filename = self.get_html_filename(url_inner)
        html_savepath = self.path + '\\' + html_filename
        if u'欠' in title or u'缴' in title or u'非正常户' in title or u'失踪' in title:
            file_formats = ['.doc', '.xls', '.pdf', '.rar', '.DOC']
            file_condition = True in [file_format in url_inner for file_format in file_formats]
            if file_condition and 'javascript' not in url_inner:
                filename = self.get_filename(url_inner)
                savepath = self.path + '\\' + filename
                sql = "INSERT into taxplayer_filename VALUES('%s', '%s', '%s', '%s', " \
                      "'%s', '%s', '%s')" % (self.province, region, fbrq, title, filename,
                                             url_inner, self.last_update_time)
                if os.path.isfile(savepath):
                    data_to_mysql(log_name, 0, sql)
                else:
                    self.download_file(url_inner, filename, savepath)
                    data_to_mysql(log_name, 0, sql)
            else:
                if num_source == 22:
                    r_inner = self.get(url_inner, headers=self.headers)
                else:
                    r_inner = self.get(url_inner)
                r_inner.encoding = 'utf-8'
                res_inner = BeautifulSoup(r_inner.text, 'html5lib')
                res_inner_str = res_inner.encode('utf8')
                a_tag_inners = re.findall(r'<a.*?href=.*?</a>', res_inner_str)
                href_inners = self.get_href(a_tag_inners)
                if href_inners:
                    download_url = ''
                    for href_inner in href_inners:
                        if num_source == 22:
                            download_url = url_host + href_inner
                        else:
                            download_url = url_source + '/' + connect_date + href_inner[1:]
                        if 'http' in href_inner or self.check_download_url(download_url):
                            continue
                    self.log('download_url: ' + download_url)
                    print 'download_url', download_url
                    filename = self.get_filename(download_url)
                    savepath = self.path + '\\' + filename
                    sql = "INSERT into taxplayer_filename VALUES('%s', '%s', '%s', '%s', " \
                          "'%s', '%s', '%s')" % (self.province, region, fbrq, title, filename,
                                                 download_url, self.last_update_time)
                    if os.path.isfile(savepath):
                        data_to_mysql(log_name, 0, sql)
                    else:
                        if num_source == 22:
                            self.download_sz(download_url, filename, savepath)
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
    crawler = GuangDongTaxCrawler()
    crawler.run()
