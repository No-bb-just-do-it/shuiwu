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
        self.xzqys = [u'合肥市', u'淮北市', u'亳州市', u'宿州市', u'蚌埠市', u'阜阳市', u'淮南市', u'滁州市', u'六安市',
                      u'马鞍山市', u'芜湖市', u'宣城市', u'铜陵市', u'池州市', u'安庆市', u'黄山市']
        self.xzqy_pys = ['hf', 'hb', 'bz', 'sz', 'bb', 'fy', 'hn', 'cz', 'la', 'mas', 'wh', 'xc', 'tl', 'chiz', 'aq',
                         'hs']
        self.web_ids = ['18', '4', '19', '5', '7', '6', '8', '9', '10', '12', '11', '16', '15', '20', '13', '17']
        self.content_types = ['3', '3', '1', '3', '1', '3', '1', '1', '3', '1', '3', '1', '1', '3', '3', '1']
        self.unit_ids = ['10915', '7292', '1491', '10800', '2752', '10806', '2900', '3048', '10837', '3344',
                         '6962', '3816', '3647', '10685', '10873', '409']
        self.column_ids = [
            '447,448,449,492,493,494,495,453,496,497,498,499,500,501,502,503,3682,452,468,469',
            '1100,1101,1102,1105,1140,1146,1147,1144,1148,1149,1150,1151,1145,1104,1120,1131,1132,1161,1162,1163,1164,'
            '1133,1165,1166,1167,1168,1134,1169,1170,1171,1172,1135,1173,1174,1175,1176,1136,1177,1178,1179,1180',
            '532',
            '1227,1228,1229,1232,1247,1267,1271,1277,1278,1279,1280,1231,1248,1258,1259,1288,1289,1290,1291,1260,1292,'
            '1293,1294,1295,1261,1296,1297,1298,1299,1262,1300,1301,1302,1303,1263,1304,1305,1306,1307,1264,1308,1309,'
            '1310,1311',
            '1375',
            '973,974,975,978,1017,1021,1022,1023,2377,2378,2379,2380,1025,1019,977,993,994,1004,1005,1036,1037,1006,'
            '1040,1041,1007,1044,1045,1008,1048,1049,1009,1052,1053,1010,1056,1057,1011,1060,1061,1012,1064,1065,1029,'
            '1068,1069',
            '1502', '1629',
            '1735,1736,1737,1781,1779,1784,1785,1787,1788,1789,1780,1739,1755,1756,1766,1767,1796,1797,1798,1799,1768,'
            '1800,1801,1802,1803,1769,1804,1805,1806,1807,1770,1808,1809,1810,1811,1771,1812,1813,1814,1815,1772,1816,'
            '1817,1818,1819,1773,1820,1821,1822,1823,1774,1824,1825,1826,1827',
            '1883',
            '846,847,848,851,886,893,890,894,2391,2392,2393,2394,895,896,897,2395,2396,2397,2398,898,899,900,2399,2400,'
            '2401,2402,2403,2324,2365,2427,2430,2431,2432,850,866,867,877,881,919,920,921,922,878,907,908,909,910,879,'
            '911,912,913,914,880,915,916,917,918',
            '2219', '2122',
            '719,720,721,765,766,763,2366,2367,2368,2369,2370,2371,2372,723,739,740,750,751,780,781,782,783,752,784,'
            '785,786,787,753,788,789,790,791,754,792,793,794,795,755,796,797,798,799,756,800,801,802,803',
            '1989,1990,1991,1994,2035,2421,2033,2041,2042,2043,2347,1993,2009,2010,2419,2420',
            '393'
        ]

    def log(self, message):
        log_name = 'an_hui_tax_gs.log'
        logger(log_name, message)

    def get_tag_list(self, url, data):
        for t in range(15):
            r = self.post(url, data=data)
            if r.status_code == 200:
                r.encoding = 'utf-8'
                res = BeautifulSoup(r.text, 'html5lib')
                recordset = res.find('recordset')
                if recordset:
                    records = recordset.findAll('record')
                    return records
            return []

    def run(self):
        log_name = 'an_hui_tax_gs.log'
        for num_source in range(0, len(self.xzqys)):
            xzqy_py = self.xzqy_pys[num_source]
            region = self.xzqys[num_source] + u'国家税务局'
            url_host = 'http://%s.ah-n-tax.gov.cn' % xzqy_py
            print region, url_host
            web_id = self.web_ids[num_source]
            content_type = self.content_types[num_source]
            unit_id = self.unit_ids[num_source]
            column_id = self.column_ids[num_source]
            data = {
                'col': '1',
                'appid': '1',
                'webid': web_id,
                'path': '/',
                'columnid': column_id,
                'sourceContentType': content_type,
                'unitid': unit_id,
                'webname': region,
            }
            self.log(region)
            for p in range(50):
                url = url_host + '/module/web/jpage/morecolumndataproxy.jsp?startrecord=%s&endrecord=%s&perpage=40' % \
                                 (p * 40, (p + 1) * 40)
                self.log(region + '  ' + url)
                tag_list = self.get_tag_list(url, data)
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
                            print '发布日期爬取到达设定最早日期'
                            break

                        parse_tag = self.parse_tag(num_source, tag,url_host,fbrq,log_name,region)
                        tasks.append(gevent.spawn(parse_tag))
                    # 通过协程处理每个详情页信息
                    gevent.joinall(tasks)
                else:
                    print 'tag_list为空,page_url: ',url
                    self.log('tag_list为空,page_url:' + url)
                    self.stop_crawl = False

    #解析详情页，参数：网站序号，host链接，起始链接，发布日期，日志名称，行政区域
    def parse_tag(self, num_source, tag,url_host,fbrq,log_name,region):
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
