# coding=utf-8
import time
import MySQLdb
from bs4 import BeautifulSoup
import requests
import re
import os

from Mysql_Config_Fyh import data_to_mysql
from Mysql_Config_Fyh import logger


class ShangHaiCrawler(object):
    def __init__(self):
        self.start_url = 'http://www.tax.sh.gov.cn/tycx/TYCXqjsknsrmdCtrl-getQjsknsrmd.pfv'
        self.last_up_datetime = time.strftime("%Y-%m-%d %H:%M:%S")
        self.province = u'上海市'

    def get_web_page(self, url, params):
        for k in range(15):
            try:
                r = requests.post(url, params=params)
                r.encoding = 'utf-8'
                res = BeautifulSoup(r.text, 'html5lib')
                # print res
                return res
            except:
                if k == 14:
                    self.log('请求服务器重试结束,'+ url)
                    os._exit(1)
                else:
                    self.log('请求服务器重试，第' + str(k) + '次重试')
            k += 1

    def log(self, message):
        log_name = 'Taxplayer_ShangHai.log'
        logger(log_name, message)

    def run(self):
        log_name = 'Taxplayer_ShangHai.log'
        max_repeat_time = 500
        taxplayer_types = ['QY', 'GT']
        temp_nsrmc = ''
        temp_swdjh = ''
        temp_fzrxm = ''
        temp_zjmc = ''
        temp_zjhm = ''
        for taxplayer_type in taxplayer_types:
            repeat_time = 0
            params = {
                'curPage': 1,
                'type': taxplayer_type
            }
            res = Crawler.get_web_page(self.start_url, params)
            # li = res.findAll('li', {'id': 'fenye'})[0]
            li = res.select('li#fenye')[0]
            print li
            a = li.findAll('a')[-1].encode('utf-8')
            total_page = int(re.findall('\d+', a)[0])
            self.log('total_page:' + str(total_page))
            if total_page:
                for i in range(1, total_page + 1):
                    break_condition = repeat_time > max_repeat_time
                    if break_condition:
                        self.log('break_condition: repeat_time > ' + str(max_repeat_time))
                        break
                    params = {
                        'curPage': i,
                        'type': taxplayer_type
                    }
                    res = Crawler.get_web_page(self.start_url, params)
                    table = res.findAll('table', {'class': 'csstable'})
                    if table:
                        trs = table[0].findAll('tr')
                        if len(trs) > 3:
                            for j in range(2, len(trs) - 1):
                                tds = trs[j].find_all('td')
                                nsrmc = tds[0].text.strip().replace('"', '')
                                swdjh = tds[1].text.strip().replace('"', '')
                                fzrxm = tds[2].text.strip().replace('"', '')
                                zjmc = tds[3].text.strip().replace('"', '')
                                zjhm = tds[4].text.strip().replace('"', '')
                                if nsrmc:
                                    temp_nsrmc = nsrmc
                                    temp_swdjh = swdjh
                                    temp_fzrxm = fzrxm
                                    temp_zjmc = zjmc
                                    temp_zjhm = zjhm
                                qssz = tds[5].text.strip().replace('"', '')
                                qsye_hj = tds[6].text.strip().replace('"', '')
                                qsye_cq = tds[7].text.strip().replace('"', '')
                                qsye_xq = tds[8].text.strip().replace('"', '')
                                swjg = tds[9].text.strip()
                                if nsrmc:
                                    sql = 'insert into taxplayer_sh VALUES ' \
                                          '("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s")' \
                                          % (nsrmc, swdjh, fzrxm, zjmc, qssz, zjhm, qsye_hj, qsye_cq,
                                             qsye_xq, swjg, self.province, self.last_up_datetime)
                                else:
                                    sql = 'insert into taxplayer_sh VALUES ' \
                                          '("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s")' \
                                          % (temp_nsrmc, temp_swdjh, temp_fzrxm, temp_zjmc, qssz, temp_zjhm, qsye_hj,
                                             qsye_cq,
                                             qsye_xq, swjg, self.province, self.last_up_datetime)
                                repeat_time = data_to_mysql(log_name, 0, sql, repeat_time)
                        else:
                            self.log('该页面没有数据')
                    else:
                        self.log('没有table表')
                self.log('repeat_time: ' + str(repeat_time))
            else:
                self.log('total_page没有获取到')


if __name__ == '__main__':
    Crawler = ShangHaiCrawler()
    Crawler.run()
