# coding=utf-8
import os
import time
import mysql.connector
from tax.SpiderMan import SpiderMan
from bs4 import BeautifulSoup
import re
from codecs import open
import chardet
from threading import Lock
import traceback
import sys
from peewee import *

class TaxConfig(SpiderMan):
    def __init__(self):
        super(TaxConfig,self).__init__()
        self.fbrq_stop = '2018-01-01'
        self.conn = None
        self.cursor = None
        # self.test = True
        self.test = False
        self.my_conn()

    def my_conn(self):
        if self.test:
            self.conn = mysql.connector.connect(host='172.16.0.76', port=3306,
                        user='fengyuanhua', passwd='!@#qweASD', db='shuiwu',charset='utf8')
        else:
            self.conn = mysql.connector.connect(host='172.16.0.76', port=3306,
                        user='fengyuanhua', passwd='!@#qweASD', db='taxplayer',charset='utf8')
        self.cursor = self.conn.cursor()

    def log_read(self, log_name, message):
        """
        记录日志信息
        :param log_name:日志名
        :param message:日志信息
        :return:
        """
        parent_dir = os.path.join(os.path.dirname(__file__), '../logs')
        today = time.strftime('%Y-%m-%d')
        # today = '2017-11-29'
        write_time = time.strftime('%H:%M:%S')
        log_directory = os.path.join(parent_dir, today)
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)
        log_path = os.path.join(log_directory,log_name)
        if type(message) == list:
            key = '['
            val = ''
            for m in message:
                if type(m) == dict:
                    key += '{' + str(m.keys()[0]) + ': ' + str(m.values()[0]) + '},'
                    val += str(m.values()[0]) + ','
                elif type(m) == str:
                    key += m + ','
            key = key[:-1]
            val = val[:-1]
            key += ']'
            with open(log_path, 'a') as f:
                f.write(key)
                f.write(val)
                f.write('\n')
        elif type(message) == int:
            with open(log_path, 'a') as f:
                f.write(write_time + '    ' + str(message))
        else:
            if type(message) == str:
                message = message.encode('utf8')
            with open(log_path, 'a') as f:
                f.write(write_time + '    ' + message + '\n')

    def save_to_mysql(self, sql, log_name = None,lock = None):
        lock = None
        try:
            # print(sql)
            if lock:
                lock.acquire()
            # print(sql)
            # conn =
            self.cursor.execute(sql)
            self.conn.commit()
            # self.conn.close()
            if lock:
                lock.release()
        except Exception as e:
            # exType, exValue, exTrace = sys.exc_info()
            # print(exType, exValue, sep="\n")
            # print(traceback.print_tb(exTrace))
            # print(sql)
            # print(e.args[0])
            # print(e)
            self.log_base(log_name,sql)
            self.log_base(log_name,e)
            # print(e.args)
            if e.args[0] == 2006:
                time.sleep(2)
                self.save_to_mysql(sql)
            if e.args[0] != 1062:
                print(e.args[0])
                # exType, exValue, exTrace = sys.exc_info()
                # print(exType, exValue, sep="\n")
                # print(traceback.print_tb(exTrace))
                print(sql)
                print(e)

    # 获得需要保存的html文件名
    def get_html_filename(self, url_detail):
        html_filename = url_detail.split('/')[-1]
        if '=' in url_detail:
            # if '.htm' not in url_detail:
            #     html_filename = html_filename.split('=')[-1] + '.html'
            # else:
            #     html_filename = html_filename.split('=')[-1]
            html_filename = html_filename.split('=')[-1]
        if not html_filename.endswith('.htm') or not html_filename.endswith(''):
            html_filename += '.html'
        return html_filename

     # 判断链接列表中需要下载的文件的链接，返回链接列表
    def get_href(self, a_tag_list):

        href_list = []
        for a_tag in a_tag_list:
            soup = BeautifulSoup(a_tag, "html.parser")
            href = soup.find('a').get('href', ' ')
            file_formats = ['.doc', '.xls', '.pdf', '.rar', '.DOC']
            file_condition = True in [file_format in href for file_format in file_formats]
            if file_condition and 'javascript' not in href:
                href_list.append(href)



        return href_list

    def log_base(self,log_name, message):
        parent_dir = os.path.join(os.path.dirname(__file__), '../logs')
        #print('parent_dir', parent_dir)
        today = time.strftime('%Y-%m-%d')
        write_time = time.strftime('%H:%M:%S')
        log_directory = os.path.join(parent_dir, today)
        log_path = os.path.join(log_directory, log_name)
        if type(message) == str:
            message = message.encode('utf8')
        # log_directory = log_name.replace(log_name.split('\\')[-1], '')
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)
        with open(log_path, 'a',encoding='utf-8') as f:
            f.write(write_time + '    ' + str(message) + '\n')
        #print(write_time, '  ', message)

    def get_filename(self, url):
        filename = url.split('/')[-1]
        if '=' in filename:
            filename = filename.split('=')[-1]
        return filename

    def get_savefile_directory(self, province_py):
        parent_dir = os.path.join(os.path.dirname(__file__), '../All_Files')
        save_directory = os.path.join(parent_dir, province_py)
        if not os.path.exists(save_directory):
            os.makedirs(save_directory)
        return save_directory

    # 下载文件
    def download_file(self, download_url, filename, savepath):
        #print('filename ',filename)
        #print('savepath ',savepath)
        for k in range(5):
            try:
                fs = self.get(download_url, timeout=15)
                # print(fs)
                if fs and fs.status_code == 200:
                    pattern = 'http://.*?' + filename
                    download_url_news = re.findall(pattern, str(fs.content))
                    # print('download_url_news',download_url_news)
                    if download_url_news:
                        fs_new = self.get(download_url_news[0], timeout=15)
                        if  fs_new and fs_new.status_code == 200:
                            download_url_content = fs_new.content
                        else:
                            download_url_content = ''
                    else:
                        download_url_content = fs.content
                    with open(savepath, 'wb') as f:
                        f.write(download_url_content)
                    break
            except Exception as e:
                print(e)
                if k == 4:
                    print('下载失败:', download_url)
                    # logger('download_url','下载失败')

                # else:
                #     print(u'第' + str(k) + u'次下载请求')