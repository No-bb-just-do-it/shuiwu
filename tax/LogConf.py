# coding=utf-8

import datetime
import logging
import os

def create_logfile(file_name):
    today = str(datetime.datetime.now().strftime('%Y-%m-%d'))
    if not os.path.exists('log'):
        os.mkdir(r'log')
    if not os.path.exists('log/%s' %(file_name,)):
        os.mkdir(r'log/%s' %(file_name,))
    today = str(datetime.date.today())
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='log/%s/%s.log' % (file_name,today),
                        filemode='w')
