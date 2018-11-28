# coding=utf-8
import os
import MySQLdb
import time


def my_conn(db_no):
    dbs = ['taxplayer', 'job_info']
    conn = MySQLdb.connect(host='172.16.0.76', port=3306, user='fengyuanhua', passwd='!@#qweASD', db=dbs[db_no],
                           charset='utf8')
    return conn


def data_to_mysql(log_name, db_no, sql, repeat_time=0, val=''):
    try:
        conn = my_conn(db_no)
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            conn.commit()
        except Exception as e:
            if e[0] == 2006:
                time.sleep(7)
                repeat_time = data_to_mysql(log_name, db_no, sql, repeat_time)
                return repeat_time
            elif e[0] != 1062:
                logger(log_name, str(e[0]) + ':' + e[1])
                logger(log_name, sql)
                if val:
                    logger(log_name, 'val: ' + val)
            else:
                repeat_time += 1
        return repeat_time
    except Exception as e:
        if e[0] == 2003:
            time.sleep(7)
            repeat_time = data_to_mysql(log_name, db_no, sql, repeat_time)
            return repeat_time
        else:
            logger(log_name, 'repeat_time: ' + str(repeat_time))
            return repeat_time


def logger(log_name, message):
    parent_dir = os.path.join(os.path.dirname(__file__), '../logs/downloadlogs')
    print 'parent_dir',parent_dir
    today = time.strftime('%Y-%m-%d')
    write_time = time.strftime('%H:%M:%S')
    log_directory = os.path.join(parent_dir, today)
    log_path = log_directory + '\\' + log_name
    if type(message) == unicode:
        message = message.encode('utf8')
    # log_directory = log_name.replace(log_name.split('\\')[-1], '')
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    with open(log_path, 'a') as f:
        f.write(write_time + '    ' + message + '\n')
    print write_time,'  ',message
