# coding=utf-8

import requests
from bs4 import BeautifulSoup
import sys
import os
from tax.config import TaxConfig
import time,datetime
import mysql.connector
import re
import gevent,gevent.monkey
from codecs import open
import chardet
from urllib.parse import urlparse
from urllib import parse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
import  pytesseract
from PIL import Image

import threading
# lock = threading.Lock()

class GuangDong(TaxConfig):
    def __init__(self):
        super(GuangDong,self).__init__()
        self.session = None
        self.province = "广东省"
        self.log_name = 'GuangDong.log'
        self.path = self.get_savefile_directory('GuangDong')

    def log(self,message):
        self.log_base(self.log_name,message)

    def qs_province(self):

        rs = requests.get('http://www.gd-n-tax.gov.cn/gdsw/qsgg/common_tt.shtml')
        session = requests.session()
        # print(requests.session())
        # print(r.content)
        a = datetime.datetime.now().strftime('%a %b %d %Y %H:%M:%S')
        a +=' GMT 0800 (中国标准时间)'
        params = {
            'dt':a,
        }
        # print(a)
        r = session.get('http://www.gd-n-tax.gov.cn/siteapps/webpage/gdtax/qsgg/image_code.jsp',params=a)
        fp = os.getcwd()
        savePath = os.path.join(fp,'../../All_Files/image_guangdong/img.png')
        with open(savePath, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
                    f.flush()
            f.close()
        img = Image.open(savePath)
        s = pytesseract.image_to_string(img)
        print(s)
        if not s.isdigit() or len(s) != 4:
            self.qs_province()
        for i in range(1,31454):
            if i % 10 == 0:
                print('page:',i)
            last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')
            params = {
                'maxPage': 0,
                'no_first_yzm': 1,
                'no_init_flag': 1,
                'status': 1,
                'yzm': s,
                'pagination_input': i,
            }
            r = session.post('http://www.gd-n-tax.gov.cn/siteapps/webpage/gdtax/qsgg/qsgg_search_list.jsp',params=params)
            res = BeautifulSoup(r.content,'html.parser')
            trs = res.find_all('tr')[1:]
            # print(len(trs))
            if len(trs) != 10:
                print('trs:',len(trs))
            for tr in trs:
                tds = tr.find_all('td')
                # print(tds[0].find('a').get('href'))
                # print(tds)
                nsrmc = tds[0].text.strip()
                nsrsbh = tds[1].text.strip()
                fddbr = tds[2].text.strip()
                qsje = tds[3].text.strip()
                fbrq = tds[4].text.strip()[:4] + '-07-01'
                # for td in tds:
                #     print(td.text)
                sql = "insert into taxplayer_qsgg (province,nsrmc,nsrsbh,fddbr,qsje,fbrq,last_update_time) values ('%s','%s','%s','%s','%s','%s','%s')" % (self.province,nsrmc,nsrsbh,fddbr,qsje,fbrq,last_update_time)
                # print(sql)
                self.save_to_mysql(sql,log_name=self.log_name)
                # print(tr)
                # time.sleep(1000)



if __name__ == '__main__':
    guangdong = GuangDong()
    guangdong.qs_province()
