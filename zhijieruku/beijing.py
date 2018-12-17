# encoding=utf-8
import os
import sys
import time
import datetime
import urllib
# import MySQLdb
from bs4 import BeautifulSoup
import requests
import random
from selenium import webdriver
import time
import requests
import re
import os
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options

# from tax.Mysql_Config_Fyh import data_to_mysql
# from tax.Mysql_Config_Fyh import logger
import json
from tax.config import TaxConfig

# reload(sys)
# sys.setdefaultencoding('gbk')



class BeiJingSearcher(TaxConfig):
    def __init__(self):
        super(BeiJingSearcher,self).__init__()
        self.headers = ''
        self.set_config()
        self.log_name = 'BeiJing.log'

    def set_config(self):
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0",
                        "Host": "www.bjsat.gov.cn",
                        'Accept-Encoding': 'gzip, deflate',
                        'Accept-Language': 'en-US,en;q=0.5',
                        'Connection': 'keep-alive',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'Host': 'www.bjsat.gov.cn',
                        'Referer': 'http://www.bjsat.gov.cn/bjsat/office/jsp/qsgg/query.jsp',
                        'Content-Type': 'application/x-www-form-urlencoded',
                        }
        self.url = 'http://www.bjsat.gov.cn/bjsat/office/jsp/qsgg/query.jsp?'
        self.host = 'http://www.bjsat.gov.cn/bjsat/office/jsp/qsgg/'
        self.begin_time = self.get_date(8)
        self.begin_time = '2018-01-01'
        self.end_time = time.strftime("%Y-%m-%d")
        self.last_update_time = time.strftime("%Y-%m-%d %H:%M:%S")
        self.province = '北京市'

    def get_request(self, url, params=None):
        for k in range(5):
            try:
                r = requests.get(url, headers=self.headers, params=params)
                r.encoding = 'gbk'
                res = BeautifulSoup(r.text, 'html5lib')
                return res
            except:
                if k == 4:
                    print('请求服务器重试结束', url)
                    os._exit(1)
                else:
                    print('请求服务器重试，第' + str(k) + '次重试')
            k += 1

    def post_request(self, url, params):
        for k in range(15):
            try:
                r = requests.post(url, params=params, headers=self.headers)

                r.encoding = 'gbk'
                res = BeautifulSoup(r.text, 'html5lib')
                return res
            except:
                if k == 14:
                    print('请求服务器重试结束', url)
                    os._exit(1)
                else:
                    print('请求服务器重试，第' + str(k) + '次重试')

    def get_date(self, days):
        print((datetime.datetime.now() - datetime.timedelta(days=days)).strftime('%Y-%m-%d'))
        return (datetime.datetime.now() - datetime.timedelta(days=days)).strftime('%Y-%m-%d')

    def log(self, message):
        self.log_base(self.log_name, message)

    # def run(self):
    #     repeat_time = 0
    #
    #
    #     chrome_options = Options()
    #     # chrome_options.add_argument('--headless')
    #     # chrome_options.add_argument('--disable-gpu')
    #     # chrome_options.binary_location = r'C:\Users\chen.tan\AppData\Local\Google\Chrome\Application\chrome.exe'
    #     chrome_options.binary_location = r'C:\Program Files (x86)\Google\Chrome\Application\chrome.exe'
    #     driver = webdriver.Chrome(chrome_options=chrome_options)
    #     driver.get('http://www.bjsat.gov.cn/bjsat/office/jsp/qsgg/query.jsp')
    #     time.sleep(20)
    #     list_fbdw = driver.find_elements_by_xpath("//select[@name='fbdw']/option")[1:]
    #     list_nsrlx = driver.find_elements_by_xpath("//select[@name='nsrlx']/option")[1:]
    #     for i in range(len(list_fbdw)):
    #         for j in range(len(list_nsrlx)):
    #             list_fbdw = driver.find_elements_by_xpath("//select[@name='fbdw']/option")[1:]
    #             list_nsrlx = driver.find_elements_by_xpath("//select[@name='nsrlx']/option")[1:]
    #             driver.find_element_by_name('BeginTime').click()
    #             driver.find_element_by_xpath("//button[@value='<']").click()
    #             driver.find_element_by_xpath("//td[text()='1']").click()
    #             fbdw = list_fbdw[i].text.strip().encode('gbk')
    #             nsrlx = list_nsrlx[j].text.strip().encode('gbk')
    #             list_fbdw[i].click()
    #             list_nsrlx[j].click()
    #             tijiao = driver.find_element_by_xpath("//td/input")
    #             try:
    #                 tijiao.send_keys(Keys.ENTER)
    #                 time.sleep(3)
    #                 driver.set_page_load_timeout(10)
    #                 res = driver.page_source
    #                 res_2 = BeautifulSoup(res, 'html5lib')
    #             except:
    #                 continue
    #             table = res_2.find(bgcolor='#d5e2f3')
    #             page_span = table.findAll('span', {'class': 'font_spci01'})
    #             if page_span:
    #                 page_num = int(page_span[0].text.split('共')[1].split('页')[0])
    #                 if page_num:
    #                     print('page_num',page_num
    #                     for k in range(1, page_num + 1):
    #                         params = {
    #                             'BeginTime': self.begin_time,
    #                             'EndTime': self.end_time,
    #                             'fbdw': fbdw,
    #                             'nsrlx': nsrlx,
    #                             'page_num': str(k),
    #                         }
    #                         res_3 = self.post_request(self.url, params=params)
    #                         # print('res_3', res_3
    #                         table = res_3.find(bgcolor='#d5e2f3')
    #                         tr_list = table.findAll('tr')
    #                         if len(tr_list) > 3:
    #                             for tr in tr_list[2:-1]:
    #                                 tds = tr.findAll('td')
    #                                 # print('tds', tds
    #                                 dwmc = tds[0].text.strip()
    #                                 nsrsbh = tds[1].text.strip()
    #                                 fzrxm = tds[2].text.strip()
    #                                 zjhm = tds[3].text.strip()
    #                                 fbrq = tds[5].text.strip()
    #                                 href = tds[0].encode('utf8').split('href="')[1].split('"')[0]
    #                                 url_1 = self.host + href
    #                                 # if fbrq > '2017-10-12':
    #                                 if fbrq > self.begin_time:
    #                                     print( 'dwmc',dwmc
    #                                     print('fbrq',fbrq
    #                                     res_4 = searcher.get_request(url_1)
    #                                     table = res_4.findAll('table', {'bgcolor': '#d5e2f3'})
    #                                     if table:
    #                                         last_update_time = time.strftime("%Y-%m-%d %H:%M:%S")
    #                                         trs = table[0].findAll('tr')
    #                                         jydd = trs[5].findAll('td')[1].text.strip()
    #                                         qssz = trs[6].findAll('td')[1].text.strip()
    #                                         qsje = trs[7].findAll('td')[1].text.strip()
    #                                         xqsje = trs[8].findAll('td')[1].text.strip()
    #                                         sql = "insert into taxplayer_qsgg (province,nsrsbh,nsrmc,fddbr,qssz,qsje,dqsje," \
    #                                               "swjg,zjhm,jydz,fbrq,last_update_time) VALUES('%s','%s','%s','%s','%s'," \
    #                                               "'%s','%s','%s','%s','%s','%s','%s')" % \
    #                                               (self.province,nsrsbh,dwmc,fzrxm,qssz,qsje,xqsje,fbdw,zjhm,jydd,fbrq,self.last_update_time)
    #                                         print('qssz',qssz
    #                                         print('qsje',qsje
    #                                         repeat_time = data_to_mysql(self.log_name, 0, sql, repeat_time)
    #                                     else:
    #                                         self.log('no table')
    #                         else:
    #                             self.log('no data tr')
    #                 else:
    #                     self.log('page_num为0')
    #             else:
    #                 self.log('page_span没有获取到')
    #     driver.close()
    #     self.log('repeat_time:' + str(repeat_time))

    # def run_fzch1(self):
    #     repeat_time = 0
    #     province = '北京市'
    #     region = '北京市税务局'
    #
    #     for i in range(19):
    #         chrome_options = Options()
    #         # chrome_options.add_argument('--headless')
    #         # chrome_options.add_argument('--disable-gpu')
    #         # chrome_options.binary_location = r'C:\Users\chen.tan\AppData\Local\Google\Chrome\Application\chrome.exe'
    #         chrome_options.binary_location = r'C:\Program Files (x86)\Google\Chrome\Application\chrome.exe'
    #         driver = webdriver.Chrome(chrome_options=chrome_options)
    #         driver.get('http://www.bjsat.gov.cn/WSBST/qd/fzchgl/jsp/ggnr.jsp')
    #         time.sleep(2)
    #         list_fzch = driver.find_elements_by_xpath("//select[@name='select2']/option")[1:]
    #         # print(list_fzch[0].text
    #         # driver.find_element_by_name('BeginTime').click()
    #         # driver.find_element_by_xpath("//button[@value='<']").click()
    #         # driver.find_element_by_xpath("//td[text()='1']").click()
    #         fzch = list_fzch[i].text.strip()
    #         print(fzch
    #         list_fzch[i].click()
    #
    #         tijiao = driver.find_element_by_xpath("//div/input[@name='Submit']")
    #         try:
    #             tijiao.send_keys(Keys.ENTER)
    #             time.sleep(3)
    #             driver.set_page_load_timeout(10)
    #             res = driver.page_source
    #             res_2 = BeautifulSoup(res, 'html5lib')
    #             # print(res_2.text
    #             time.sleep(2)
    #         except:
    #             continue
    #         # infos = driver.find_elements_by_xpath("//tbody/tr")
    #         n_count = 0
    #         while True:
    #             if n_count %100 == 0:
    #                 print(fzch,n_count
    #             try:
    #                 infos = driver.find_elements_by_xpath("//tbody/tr")
    #                 for info in infos[2:]:
    #                     # print(len(infos)
    #                     # print(info
    #                     tds = info.text.split(' ')
    #                     # tds = info.find_elements_by_xpath("/td")
    #                     nsrmc = tds[1]
    #                     nsrsbh = tds[2]
    #                     fddbr = tds[4]
    #                     zjhm = tds[5]
    #                     jydz = tds[6]
    #                     rdrq = tds[7]
    #                     fbrq = tds[8]
    #                     last_update_time = time.strftime("%Y-%m-%d %H:%M:%S")
    #                     # print(self.last_update_time
    #                     sql = "insert into taxplayer_abnormal (province,region, nsrsbh, nsrmc, fddbr, zjzl," \
    #                           "zjhm,jydz,rdrq,fbrq,last_update_time) VALUES('%s','%s','%s','%s','%s'," \
    #                           "'%s','%s','%s','%s','%s','%s')" % \
    #                           (province, region, nsrsbh, nsrmc, fddbr, '居民身份证', zjhm, jydz, rdrq, fbrq, last_update_time)
    #                     # print(len(tds)
    #                     repeat_time = data_to_mysql(self.log_name, 0, sql, repeat_time)
    #
    #             except Exception as e:
    #                 print(e
    #             try:
    #                 # num_now = driver.find_element_by_xpath("//div[@class='pagination_box']/div/p[1]/span")
    #                 # num_all = driver.find_element_by_xpath("//div[@class='pagination_box']/div/p[2]/span")
    #                 # print(num_now.text
    #                 # print(num_all.text
    #
    #                 fanye = driver.find_element_by_xpath("//div[@class='btns_box']/input[2]")
    #                 fanye.click()
    #                 time.sleep(1.5)
    #                 n_count += 1
    #             except Exception as e:
    #                 print(e
    #                 fanye = driver.find_element_by_xpath("//div[@class='btns_box']/input[2]")
    #                 fanye.click()
    #                 time.sleep(3)
    #                 # driver.close()
    #                 # break
    #                 pass
    #             try:
    #                 num_now = driver.find_element_by_xpath("//div[@class='pagination_box']/div/p[1]/span")
    #                 num_all = driver.find_element_by_xpath("//div[@class='pagination_box']/div/p[2]/span")
    #                 print(num_now.text
    #                 print(num_all.text
    #                 if int(num_now.text.strip()) == int(num_all.text.strip()):
    #                     driver.close()
    #                     break
    #             except Exception as e:
    #                 print(e
    #                 continue
                # if int(num_now.text.strip()) == int(num_all.text.strip()):
                #     break

            # time.sleep(20)
    def run_fzch(self):
        province = '北京市'
        region = '北京市税务局'
        placeDict = {'1110101':247,'1110102':212,'1110106':591,'1110105':940,'1110108':1004, \
        '1110111':106,'1110112':267,'111113':64,'1110114':206,'1110224':291,'1110226':101,'1110227':124,
        '1110228':103,'1110229':20,'1110107':85,'1110109':136,'1110191':8,'1110192':21}
        # placeDict = {'1110106':591, \
        #              '1110107':85,'1110191':8,}
        for place,pageNum in placeDict.items():
            print(place)
            for num in range(pageNum):
            # for num in range(10):
                if num % 10 == 0:
                    print(num)
                postData = {
                'qxfj': place,
                'nsrsbh':'',
                'nsrmc':'',
                'pageNo': num+1,
                'pageSize': 100,
                }
                try:
                    response = requests.post('http://www.bjsat.gov.cn/WSBST//FZCHGL_FYServlet', data=postData)
                    # print(response.json()
                    # res = json.loads(response.text)

                    # print(json.dumps(json.loads(response.text.replace('\r', '').replace('\n', ''),strict=False), ensure_ascii=False)
                    res = json.loads(response.text.strip().replace('\\',''),strict=False)
                    for r in res['arrayList']:
                        try:
                            nsrsbh = r['NSRSBH']
                            nsrmc = r['MC']
                            fddbr = r['XM']
                            zjhm = r['ZJHM']
                            jydz = r['JYDD']
                            rdrq = r['RDRQ']
                            fbrq = r['GXRQ']


                            last_update_time = time.strftime("%Y-%m-%d %H:%M:%S")
                            # print(self.last_update_time
                            sql = "insert into taxplayer_abnormal (province,region, nsrsbh, nsrmc, fddbr, zjzl," \
                                  "zjhm,jydz,rdrq,fbrq,last_update_time) VALUES('%s','%s','%s','%s','%s'," \
                                  "'%s','%s','%s','%s','%s','%s')" % \
                                  (province, region, nsrsbh, nsrmc, fddbr, '居民身份证', zjhm, jydz, rdrq, fbrq, last_update_time)
                            # print(len(tds)
                            self.save_to_mysql(sql,self.log_name)
                        except Exception as e:
                            print(e)
                except Exception as e:
                    print(response.text)
                    print(e)
                # print(res)
if __name__ == '__main__':
    searcher = BeiJingSearcher()
    # searcher.run()
    searcher.run_fzch()