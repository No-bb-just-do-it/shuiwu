# coding=utf-8

import datetime
import MySQLdb
import traceback

updatetime = datetime.datetime.now().strftime('%Y-%m-%d')

conn_1 = MySQLdb.connect(host='172.16.0.20', port=3306, user='zhangxiaogang', passwd='gangxiaozhang', db='court_notice',
                         charset='utf8')
cursor_1 = conn_1.cursor()
conn_2 = MySQLdb.connect(host='172.16.0.20', port=3306, user='zhangxiaogang', passwd='gangxiaozhang', db='job_info',
                         charset='utf8')
cursor_2 = conn_2.cursor()


def execute_insert(sql):
    try:
        cursor_1.execute(sql)
        conn_1.commit()
        # conn_1.close()
    except Exception, e:
        print u'数据库已有该数据'
        # print 'traceback.print_exc():'; traceback.print_exc()


def execute_start(name):
    start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sql_1 = "UPDATE ktgg_job set start_time= '%s' , updatetime= '%s' where name='%s' " % \
            (start_time, updatetime, name)
    # print 'sql_1:', sql_1, id(start_time)
    try:
        cursor_2.execute(sql_1)
        conn_2.commit()
    except:
        print u'更改监控开始时间报错'


def execute_sop(name):
    stop_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sql_4 = "UPDATE ktgg_job set status=1 ,stop_time='%s' where name='%s'  " % (stop_time, name)
    # print 'sql_4:', sql_4, id(stop_time)
    try:
        cursor_2.execute(sql_4)
        conn_2.commit()
    except:
        print u'更改结束时间报错'
