# _*_ coding=utf-8 _*_
import requests
import json
import MySQLdb
import logging
import time
import os


class ShenZhenCrawler(object):
    def __init__(self):
        self.mysql_conn()
        self.url = ''
        self.set_config()

    def set_config(self):
        self.url = 'http://www.szgs.gov.cn/bswmh/inspur.ssgg.fzch.FzchCmd.cmd?method=query'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
            'Host': 'www.szgs.gov.cn'
        }
        self.last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')

    def mysql_conn(self):
        self.conn = MySQLdb.connect(host='172.16.0.76', port=3306, user='fengyuanhua', passwd='!@#qweASD', \
                                    db='taxplayer', charset='utf8')
        self.cursor = self.conn.cursor()

    def run(self):
        url = self.url
        params = {'pageSize': 10, 'firstPage': 1}
        r = requests.post(url, headers=self.headers, data=params)
        result = json.loads(r.text)
        if len(result['Web']['response']) > 3:
            total_num = result['Web']['response'][4]['value']
            total_page = (int(total_num)) / 10 + 1
            for i in range(49382, total_page + 1, 1):
                k = 15
                while k > 0:
                    page = str(i)
                    params = {'pageSize': 10, 'firstPage': i}
                    try:
                        s = requests.post(url, headers=self.headers, data=params)
                        res = json.loads(s.text)
                        if len(res['Web']['response']) > 3:
                            k -= 20
                            log_filename = 'Taxplayer_ShenZhen.log'
                            logging.basicConfig(filename=log_filename, filemode='w')
                            logging.warning(msg='第' + page + '页')
                            outcome_list = res['Web']['response'][3]['value']
                            for outcome in outcome_list:
                                nsrsbh = outcome['value'][0]['value']
                                qymc = outcome['value'][1]['value']
                                fddbrmc = outcome['value'][2]['value']
                                sfzjhm = outcome['value'][3]['value']
                                jydz = outcome['value'][4]['value']
                                rddw = outcome['value'][5]['value']
                                sql = 'INSERT into taxplayer_sz(nsrsbh, qymc, fddbrmc, sfzjhm, jydz, rddw, last_update_time) VALUES("%s", "%s", "%s", "%s", "%s", "%s", "%s")' \
                                      % (nsrsbh, qymc, fddbrmc, sfzjhm, jydz, rddw, self.last_update_time)
                                print sql
                                os._exit(1)
                                try:
                                    self.cursor.execute(sql)
                                    self.conn.commit()
                                except:
                                    pass
                        elif k > 0:
                            k -= 1
                            print u'第' + str(k) + u'判断尝试'
                        else:
                            print u'判断尝试结束'
                            os._exit(1)
                    except:
                        if k > 0:
                            k -= 1
                            print u'第' + str(k) + u'尝试'
                        else:
                            print u'尝试结束'
                            os._exit(1)
        else:
            print u'请再试一次'


if __name__ == '__main__':
    Crawler = ShenZhenCrawler()
    Crawler.run()
