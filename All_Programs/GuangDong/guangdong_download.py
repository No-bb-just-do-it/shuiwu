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

    def down_image(self):
        timeNow = datetime.datetime.now().strftime('%a %b %d %Y %H:%M:%S')
        timeNow +=' GMT 0800 (中国标准时间)'
        params = {'dt':timeNow}
        r = self.session.get('http://www.gd-n-tax.gov.cn/siteapps/webpage/gdtax/qsgg/image_code.jsp',params=timeNow)
        fp = os.getcwd()
        savePath = os.path.join(fp,'../../All_Files/image_guangdong/img.png')
        with open(savePath, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
                    f.flush()
            f.close()
        return savePath

    #识别验证码
    def recognition_img(self):
        savePath = self.down_image()
        try:
            img = Image.open(savePath)
        except:
            self.recognition_img()
        captcha = pytesseract.image_to_string(img)
        print('captcha:',captcha)
        if not captcha.isdigit() or len(captcha) != 4:
            self.recognition_img()
        return captcha



    #获取列表页
    def get_tags(self,captcha=None,page=None):
        params = {
            'maxPage': 31454,
            'no_first_yzm': 0,
            'no_init_flag': 1,
            # 'status': 1,
            'yzm': captcha,
            'pagination_input': page,
        }
        r = self.session.post('http://www.gd-n-tax.gov.cn/siteapps/webpage/gdtax/qsgg/qsgg_search_list.jsp', params=params)
        res = BeautifulSoup(r.content, 'html.parser')
        trs = res.find_all('tr')[1:]
        # print(len(trs))
        # for tr in trs:
        #     print(tr)
        if len(trs) != 10:
            print('len_trs:',len(trs))
        if len(trs) == 0:
            print('wrong_trs:', len(trs))
            print('wrong_page:',page)
            captcha = self.recognition_img()
            trs = self.get_tags(captcha)
        return trs

    def qs_province(self):

        requests.get('http://www.gd-n-tax.gov.cn/gdsw/qsgg/common_tt.shtml')
        self.session = requests.session()
        captcha = self.recognition_img()
        # for p in range(3840,31454):
        for p in range(1,31454):
            if p % 10 == 0:
                print('page:',p)
            last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')
            trs = self.get_tags(captcha,p)
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
                # print(sql)
                self.save_to_mysql(sql,log_name=self.log_name)
                # print(tr)
                # time.sleep(1000)



if __name__ == '__main__':
    guangdong = GuangDong()
    guangdong.qs_province()
