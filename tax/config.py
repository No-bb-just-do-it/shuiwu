# coding=utf-8
import os
import time
import mysql.connector

class TaxConfig(object):
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.my_conn()

    def my_conn(self):
        self.conn = mysql.connector.connect(host='172.16.0.76', port=3306,
                    user='fengyuanhua', passwd='!@#qweASD', db='taxplayer',charset='utf8')
        self.cursor = self.conn.cursor()


    def logger(self, log_name, message):
        """
        记录日志信息
        :param log_name:日志名
        :param message:日志信息
        :return:
        """
        parent_dir = os.path.join(os.path.dirname(__file__), '../logs/readerlogs')
        today = time.strftime('%Y-%m-%d')
        # today = '2017-11-29'
        write_time = time.strftime('%H:%M:%S')
        log_directory = os.path.join(parent_dir, today)
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)
        log_path = log_directory + '\\' + log_name
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
            if type(message) == unicode:
                message = message.encode('utf8')
            with open(log_path, 'a') as f:
                f.write(write_time + '    ' + message + '\n')

