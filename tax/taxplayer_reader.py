# coding=utf-8
from Mysql_Config_Fyh import my_conn
import os
import time
import xlwt
import xlrd
import win32com
from win32com.client import Dispatch
from xlutils.copy import copy
import re

import bs4
from bs4 import BeautifulSoup


class TaxplayerReader(object):
    def __init__(self):
        self.province = u''
        self.province_py = ''
        self.conn = None
        self.cursor = None
        self.mysql_conn()
        self.abnormal_fields = []
        self.qs_fields = []
        # self.today = "'%2017-09-08%'"
        self.today = "'%" + time.strftime('%Y-%m-%d') + "%'"
        self.last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')
        self.fieldnames_directory = self.get_fieldnames_directory()
        self.row = -1
        self.db_table = 'test'
        self.db_table = ''

    def set_config(self):
        self.path = self.get_savefile_directory(self.province_py)
        self.log_name = '%s_reader.log' % self.province_py
        self.test_log_name = '%s_reader_test.log' % self.province_py
        self.abnormal_fields = [
            {'nsrsbh': u'社会信用代码（纳税人识别号）,税务登记证号,纳税人识别号码,税务登记号,税务登记代码,纳税识别号'},
            {'nsrmc': u'纳税人名称,企业或单位名称,非正常户欠税人名称,企业名称'},
            {'fddbr': u'法人代表,法定代表人姓名,法定代表人(负责人)姓名,法定代表人（负责人）姓名,法人名称,业主姓名,法人姓名'},
            {'rdrq': u'非正常户认定日期'},
            {'djrq': u'登记日期'},
            {'jydz': u'生产经营地址,实际经营地址,经营（注册）地址'},
            {'zcdz': u'注册地址'},
            {'zjzl': u'证件种类'},
            {'zjhm': u'证件号码'},
            {'qyxz': u'登记注册类型'},
            {'swjg': u'认定税务机关，主管税务机关,主管税务所（科、分局）,管理科所,主管税务所（科、分局）'}
        ]
        self.qs_fields = [
            {'nsrsbh': u'税号,社会信用代码（纳税人识别号）,纳税人识别码,税务登记代码,税务登记证号,税务登记号码,'
                       u'纳税识别号'},
            {'nsrmc': u'纳税人名称,企业名称,企业（单位）名称,单位名称,欠税纳税人名称,业主名称,企业或单位名称,纳税人全称'
                      u'企业（单位、业主或个人）名称,欠税企业名称,单位（个人）名称,企业或单位的名称,非正常户欠税人名称,'
                      u'纳税人名称(姓名),单位名称（业主姓名）,企业、单位名称,企业或单位（业主）名称'},
            {'fddbr': u'法定代表人（负责人）/业主姓名,法人（负责人）,法定代表人（负责人、业主）,'
                      u'法定代表人姓名,法定代表人（负责人）姓名,法人（业主）姓名,法人代表,法人(负责人)姓名,法人姓名'},
            {'zjzl': u'证件种类'},
            {'zjhm': u'证件号码'},
            {'jydz': u'生产经营地址,生产经营地点,经营（注册）地址'},
            {'ssqs': u'税款所属期起,税费所属期起,所属时期起'},
            {'ssqz': u'税款所属期止,税费所属期止,所属时期止'},
            {'xjrq': u'限缴日期,缴款期限,税款限缴日期'},
            {'qssz': u'欠税税种,征收项目,欠税项目,欠税（费）种,欠税（费）种（地税）,欠缴税种,欠税（费）种类，所欠税种'},
            {'swjg': u'主管税务所（科、分局）,主管税务机关'},
            {'cqsje': u'陈欠余额,期末陈欠,陈欠金额'},
            {'qsje': u'欠税余额,应补(退)税额,欠税余额（元）,欠税金额,总计欠税金额,未缴金额,欠缴税款金额,欠缴税费余额,'
                     u'欠税金额（元）,求和项:应补(退)税额,实缴额（单位：元）,实缴额,欠税总额(万元),欠税总额（万元）,'
                     u'欠税金额（元角分）,欠税额,欠缴金额（元）,欠税金额（万元）,期未欠税（元）,欠税额度（元）,'
                     u'欠税（费）余额（元）,欠税(担保欠税)余额(元),欠税（担保欠税）余额金额（元）,税额（元）'
                     u'欠税(担保欠税)余额金额(元),欠税金额(元),截止本期拖欠税款情况，欠缴税款总金额'
                     u'欠税公告金额（元）,截止本期拖欠税款情况'},
            {'dqsje': u'当期'},

        ]

    def log(self, message):
        if self.db_table:
            self.logger(self.test_log_name, message)
            print message
        else:
            self.logger(self.log_name, message)
            print message

    def word_to_html(self):
        sql = "SELECT * from taxplayer_filename where (title like '%欠税%' or title like '%缴%' or title " \
              "like '%非正常户%' or title like '%非正户%') and filename like '%.doc%' and province = '" + \
              self.province.encode('utf8') + "' and last_update_time like " + self.today
        save_directory = self.path + '\word_to_html\\'
        self.convert_word_to_html(sql, self.path, save_directory)

    def get_abnormal_excel_fieldnames(self):
        sql = "SELECT * from taxplayer_filename where (title like '%非正常户%' or title like '%非正户%') and " \
              "filename like '%.xls%' and province = '" + self.province.encode('utf8') + \
              "' and last_update_time like " + self.today
        savepath = self.fieldnames_directory + '\\' + '%s_fields_read.xls' % self.province_py
        self.row = self.read_excel_fieldnames(sql, self.path, savepath, self.row)

    def get_abnormal_html_fieldnames(self):
        sql = "SELECT * from taxplayer_filename where (title like '%非正常户%' or title like '%非正户%') and " \
              "(filename like '%.doc%' or filename like '%.htm%') and province = '" + self.province.encode('utf8') + \
              "' and last_update_time like " + self.today
        savepath = self.fieldnames_directory + '\\' + '%s_fields_read.xls' % self.province_py
        self.row = self.read_html_field_info(sql, self.path, savepath, self.row)

    def get_qsgg_excel_fieldnames(self):
        sql = "SELECT * from taxplayer_filename where (title like '%欠税%' or title like '%缴%')" \
              " and filename like '%.xls%' and province = '" + self.province.encode('utf8') + \
              "' and last_update_time like " + self.today
        savepath = self.fieldnames_directory + '\\' + '%s_fields_read.xls' % self.province_py
        self.row = self.read_excel_fieldnames(sql, self.path, savepath, self.row)

    def get_qsgg_html_fieldnames(self):
        sql = "SELECT * from taxplayer_filename where (title like '%欠税%' or title like '%缴%')" \
              " and (filename like '%.doc%' or filename like '%.%htm%') and province = '" \
              + self.province.encode('utf8') + "' and last_update_time like " + self.today
        savepath = self.fieldnames_directory + '\\' + '%s_fields_read.xls' % self.province_py
        self.row = self.read_html_field_info(sql, self.path, savepath, self.row)

    def mysql_conn(self):
        try:
            self.conn = my_conn(0)
            self.cursor = self.conn.cursor()
        except Exception as e:
            if e[0] == 2003:
                time.sleep(7)
                self.mysql_conn()

    def get_directory(self, directory):
        """
        生成空的文件夹
        :param directory:
        :return:
        """
        if not os.path.exists(directory):
            os.makedirs(directory)

    def logger(self, log_name, message):
        """
        记录日志信息
        :param log_name:日志名
        :param message:日志信息
        :return:
        """
        parent_dir = os.path.join(os.path.dirname(__file__), '../logs/readerlogs')
        # today = time.strftime('%Y-%m-%d')
        today = '2017-11-29'
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

    def get_savefile_directory(self, province_py):
        """
        :param province_py:省份拼音
        :return: 返回改省份的文件路径
        """
        parent_dir = os.path.join(os.path.dirname(__file__), '../All_Files')
        save_directory = os.path.join(parent_dir, province_py)
        self.get_directory(save_directory)
        return save_directory + '\\'

    def get_fieldnames_directory(self):
        """
        :return: 创建并返回日志路径
        """
        today = time.strftime('%Y-%m-%d')
        parent_dir = os.path.join(os.path.dirname(__file__), '../logs/readerlogs')
        log_directory = os.path.join(parent_dir, today)
        self.get_directory(log_directory)
        return log_directory

    def data_to_mysql(self, sql, num_repeat, num_info, num_fail):
        """
        用来将数据插入到mysql数据库，并记录插入异常，另外返回重复次数。
        :param sql: insert 插入语句
        :param num_repeat: 插入语句执行重复条数
        :param num_info: 字段序号
        :param nun_fail：执行sql失败条数
        """
        log_name = '%s_wrong_sqls.log' % self.province_py
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            data_nums = [num_repeat, num_fail]
            return data_nums
        except Exception as e:
            if e[0] == 2006:
                time.sleep(3)
                data_nums = self.data_to_mysql(sql, num_repeat, num_fail)
                return data_nums
            elif e[0] != 1062:
                num_fail += 1
                print num_info + 1, sql, e
                self.logger(log_name, self.province_py)
                self.logger(log_name, str(num_info + 1))
                self.logger(log_name, str(e[0]))
                self.logger(log_name, sql)
            else:
                num_repeat += 1
            data_nums = [num_repeat, num_fail]
            return data_nums

    def get_province_info(self, sql):
        """
        获取需要解析的文件相关信息。
        :param sql: sql查询语句。
        :return: 返回需要解析的文件信息。
        """
        print sql
        info = []
        nums = self.cursor.execute(sql)
        if nums:
            info = self.cursor.fetchmany(nums)
        return info

    # 以下到excel_reader()的函数用来解析excel
    #
    #

    def get_excel(self, filepath):
        """
        :param filepath: excel文件路径
        :return: 返回excel对象
        """
        self
        try:
            excel = xlrd.open_workbook(filepath)
        except:
            excel = xlrd.open_workbook(filepath, formatting_info=True)
        return excel

    # def get_excel_start_idx(self, table, rows):
    #     self
    #     start_idx = 0
    #     for _idx in range(0, rows):
    #         cols = []
    #         col_twos = []
    #         for col in table.row_values(_idx):
    #             if isinstance(col, float):
    #                 cols.append(col)
    #             elif col.strip():
    #                 cols.append(_idx)
    #         col_nums = len(cols)
    #         for col_two in table.row_values(_idx + 1):
    #             if isinstance(col_two, float):
    #                 col_twos.append(col_two)
    #             elif col_two.strip():
    #                 col_twos.append(_idx + 1)
    #         col_two_nums = len(col_twos)
    #         print col_nums, col_two_nums
    #         if col_two_nums > 2 and col_two_nums > col_nums:
    #             start_idx = _idx + 2
    #             break
    #         elif col_two_nums == col_nums > 2:
    #             start_idx = _idx + 1
    #             break
    #         elif col_nums > 2:
    #             start_idx = _idx + 1
    #             break
    #     return start_idx

    def get_excel_start_idx(self, table, rows):
        """
        获取excel的数据起始行
        :param table: excel的sheet表
        :param rows: sheet表的总行数
        :return: 返回excel的数据起始行
        """
        start_idx = 0
        for _idx in range(0, rows):
            col_val = u''
            for col in table.row_values(_idx):
                if isinstance(col, float):
                    col_val += str(col)
                elif col.strip():
                    col_val += col.strip()
            match_fields = [u'纳税人识别号', u'纳税人名称', u'纳税人识别码', u'税务登记号码', u'企业或单位名称',
                            u'税务登记证号', u'税务登记号', u'纳税识别号', u'纳税人名称', u'业户名称', u'企业名称',
                            u'经营地点']
            match_condition = True in [match_field in col_val for match_field in match_fields]
            if match_condition:
                start_idx = _idx + 1
                break
        return start_idx

    def read_excel_fieldnames(self, sql, file_directory, savepath, row_num):
        """
        读取excel字段及字段后面两行内容
        :param sql: 选取要读取文件的sql语句
        :param file_directory: 读取文件的路径
        :param savepath: 保存读取结果的excel路径
        :param row_num: 写入表的起始位置
        :return:
        """
        print sql
        nums = self.cursor.execute(sql)
        print nums
        # break
        if nums:
            info = self.cursor.fetchmany(nums)
            if os.path.isfile(savepath) and row_num == -1:
                os.remove(savepath)
                f = xlwt.Workbook(encoding='utf-8')
            elif os.path.isfile(savepath):
                rb = xlrd.open_workbook(savepath)
                f = copy(rb)
            else:
                f = xlwt.Workbook(encoding='utf-8')
            row = 0
            for t in range(3):
                row = row_num
                row += 1
                if os.path.isfile(savepath):
                    sheet = f.get_sheet(t)
                    sheet.write(row, 0, 'excel')
                else:
                    sheet = f.add_sheet('fieldnames' + str(t), cell_overwrite_ok=True)
                    sheet.write(row, 0, 'excel')
                for n in range(0, nums):
                    filepath = file_directory + info[n][4]
                    try:
                        excel = self.get_excel(filepath)
                        sheets = excel.sheets()
                        count = len(sheets)  # sheet数量
                        for m in range(count):
                            row += 1
                            try:
                                table = excel.sheets()[m]
                                rows = table.nrows
                                start_idx = self.get_excel_start_idx(table, rows)
                                if start_idx < 1:
                                    start_idx = 1
                                # print start_idx, start_idx + t - 1, start_idx + t
                                for line in range(start_idx + t - 1, start_idx + t, 1):
                                    sheet.write(row, 0, n + 1)
                                    row_val = table.row_values(line)
                                    for j in range(len(row_val)):
                                        if t == 0:
                                            val = row_val[j].strip().replace(u'\n', u'').replace(u' ', u'')
                                            val = ''.join(val.split())
                                        else:
                                            val = row_val[j]
                                        sheet.write(row, j + 1, val)
                            except:
                                sheet.write(row, 0, n + 1)
                                sheet.write(row, 1, '?')
                    except Exception as e:
                        # if e[0] == "Unsupported format, or corrupt file: Expected BOF record; found '<html xm'":
                        if '<html' in str(e[0]) or '<!DOCT' in str(e[0]):
                            row += 1
                            try:
                                soup = self.get_soup(filepath)
                                tr_list, inner_signal = self.get_tr_list(soup)
                                new_tr_list = self.get_html_start_info(tr_list)
                                dt = len(tr_list) - len(new_tr_list)
                                if new_tr_list:
                                    tr_values = tr_list[dt + t].findAll('td')
                                    th_values = tr_list[dt + t].findAll('th')
                                    if tr_values:
                                        tag_values = tr_values
                                    else:
                                        tag_values = th_values
                                    sheet.write(row, 0, n + 1)
                                    for j in range(len(tag_values)):
                                        if t == 0:
                                            val = tag_values[j].text.strip().replace(u'\n', u'').replace(u' ', u'')
                                            val = ''.join(val.split())
                                        else:
                                            val = tag_values[j].text
                                        sheet.write(row, j + 1, val)
                            except Exception as e:
                                print e
                                print n + 1, info[n][4]
                                sheet.write(row, 0, n + 1)
                                sheet.write(row, 1, '?')
                        else:
                            print e
                            print n + 1, info[n][4]
                            row += 1
                            sheet.write(row, 0, n + 1)
                            sheet.write(row, 1, '?')
                for k in range(17):
                    col = sheet.col(k)
                    col.width = 256 * 25
            f.save(savepath)
            return row
        else:
            return row_num

    def get_row_val(self, val):
        """
        获取某一行的一个数据
        :param val: 某行的一个数据
        :return: 处理后的数据
        """
        if isinstance(val, float):
            val = str(val).decode('utf8').replace(u'\n', '').replace(u' ', u'')
        else:
            val = val.strip().replace(u'\n', '').replace(u' ', u'')
        val = ''.join(val.split())
        return val

    def get_money_dw(self, table, rows):
        """
        获取excel表字段前出现的欠税单位，如果没有，返回False.
        :param table: excel文件的一个sheet表
        :param rows: sheet表的所有行数
        :return:
        """
        start_idx = self.get_excel_start_idx(table, rows)
        dw = False
        if start_idx:
            for i in range(0, start_idx):
                col_val = u''
                for col in table.row_values(i):
                    if isinstance(col, float):
                        col_val += str(col)
                    elif col.strip():
                        col_val += col.strip()
                fields = [u': 万', u':万', u'：万', u'： 万']
                wan_condition = True in [field in col_val for field in fields]
                if wan_condition:
                    dw = True
                    break
        return dw

    def get_sz_time(self, vals):
        """
        返回从税种字段开始连续两列税种次数，次数大于1，不匹配税种字段。反之，则继续进行字段匹配。
        :param vals: excel或html连续两格数据
        :return: 税种出现次数
        """
        sz_time = 0
        for val in vals:
            if str(type(val)) == "<class 'bs4.element.Tag'>":
                temp_val = self.get_tag_val(val)
            else:
                temp_val = self.get_row_val(val)
            if re.findall(u'.+税', temp_val) and u'欠税' not in temp_val:
                sz_time += 1
        return sz_time

    def get_excel_qsgg_field_info(self, table, rows, fields=None):
        """
        获取匹配字段的匹配结果以及欠税金额单位是否是万元。wan=True表示欠税金额单位是万元，反之欠税金额单位是元。匹配结果是一
        个list,里面是dict类型，key表示字段名，value表示字段名对应的位置。
        :param table: excel的一个sheet表。
        :param rows: sheet表的所有行数。
        :param fields: 进行匹配的初始字段集。
        :return:
        """
        dw = self.get_money_dw(table, rows)
        if not fields:
            fields = self.qs_fields
        col_val = []
        match_fields = []
        wan = False
        keys = []
        zjhms = [u'证件号码', u'身份证号', u'法人证件号', u'身份号码', u'有效证件号', u'法人代表人身份证']
        repeat_qsjes = [u'截止', u'欠税情况', u'欠缴地方税金额（单位：元）', u'欠税税种及欠税金额']
        qsjes = [u'欠税余额', u'欠税金额',u'欠缴税款金额']
        dqsjes = [u'当期', u'其中', u'新增', u'新发生', u'新欠']
        filter_rqs = [u'日期', u'税款所属期']
        rq_keys = ['xjrq', 'ssqs', 'ssqz']
        start_idx = self.get_excel_start_idx(table, rows)
        if start_idx == 0:
            return match_fields
        else:
            j = start_idx - 1
        for col in table.row_values(j):
            if isinstance(col, float):
                col_val.append(col)
            elif col.strip():
                col_val.append(col)
        col_nums = len(col_val)
        if col_nums > 2:
            row_val = table.row_values(j)
            for k in range(len(row_val)):
                val = self.get_row_val(row_val[k])
                # print 'val', val, len(val)
                for fds in range(len(fields)):
                    match_field = fields[fds].values()[0]
                    match_key = fields[fds].keys()[0]
                    match_condition = val in match_field and val
                    zjhm_condition = True in [zjhm in val for zjhm in zjhms]
                    repeat_qsje_condition = True in [qsje in val for qsje in repeat_qsjes]
                    qsje_condition = True in [qsje in val for qsje in qsjes]
                    dqsje_condition = True in [dqsje in val for dqsje in dqsjes]
                    filter_rq_condition = True in [rq == val for rq in filter_rqs]
                    if u'纳税人识别号' in match_field:
                        if match_key not in keys:
                            if u'纳税人识别号' in val or match_condition:
                                match_fields.append({match_key: k})
                                keys.append(match_key)
                    elif match_field == u'证件种类':
                        if match_key not in keys:
                            if match_field in val or u'证件类型' in val or u'证件名称' in val:
                                match_fields.append({match_key: k})
                                keys.append(match_key)
                    elif match_field == u'证件号码':
                        if match_key not in keys:
                            if zjhm_condition:
                                match_fields.append({match_key: k})
                                keys.append(match_key)
                    elif match_field == u'当期':
                        if match_key not in keys:
                            if dqsje_condition:
                                match_fields.append({match_key: k})
                                keys.append(match_key)
                    elif match_key in rq_keys:
                        if match_key not in keys and not filter_rq_condition:
                            if match_condition:
                                match_fields.append({match_key: k})
                                keys.append(match_key)
                    elif match_key == 'qssz':
                        if match_key not in keys and match_condition:
                            temp_row_vals = table.row_values(j + 1)
                            if len(temp_row_vals) >= k + 1:
                                sz_time = self.get_sz_time(temp_row_vals[k: k + 2])
                            else:
                                sz_time = 1
                            if sz_time <= 1:
                                match_fields.append({match_key: k})
                                keys.append(match_key)
                    elif u'法定代表人' in match_field:
                        if match_key not in keys:
                            temp_row_val = table.row_values(j + 1)
                            for t in range(len(temp_row_val)):
                                temp_val = self.get_row_val(temp_row_val[t])
                                zjhm_condition_temp = True in [zjhm in temp_val for zjhm in zjhms]
                                if u'姓名' in temp_val:
                                    match_fields.append({match_key: t})
                                    keys.append(match_key)
                                elif u'证件名称' in temp_val or u'证件种类' in temp_val or u'证件类型' in temp_val:
                                    if 'zjzl' not in keys:
                                        match_fields.append({'zjzl': t})
                                        keys.append('zjzl')
                                elif zjhm_condition_temp:
                                    if 'zjhm' not in keys:
                                        match_fields.append({'zjhm': t})
                                        keys.append('zjhm')
                            if match_key not in keys:
                                if (u'法定代表' in val or u'法人' in val or match_condition) and not zjhm_condition:
                                    match_fields.append({match_key: k})
                                    keys.append(match_key)
                    elif u'欠税余额' in match_field:
                        if match_key not in keys:
                            if u'合计' in val:
                                if match_key not in keys:
                                    match_fields.append({match_key: k})
                                    keys.append(match_key)
                            elif repeat_qsje_condition:
                                temp_row_vals = table.row_values(j + 1)
                                if len(temp_row_val) >= k:
                                    sz_time = self.get_sz_time(temp_row_vals[k: k + 2])
                                else:
                                    sz_time = 1
                                for t in range(len(temp_row_val)):
                                    temp_val = self.get_row_val(temp_row_val[t])
                                    dqsje_condition_temp = True in [dqsje in temp_val for dqsje in dqsjes]
                                    if u'税种' in temp_val:
                                        if 'qssz' not in keys and sz_time <= 1:
                                            match_fields.append({'qssz': t})
                                            keys.append('qssz')
                                    elif u'合计' in temp_val:
                                        if match_key not in keys:
                                            wan = wan or dw
                                            match_fields.append({match_key: t})
                                            keys.append(match_key)
                                            break
                                    elif temp_val in match_field and temp_val and u'万元' in temp_val:
                                        if match_key not in keys:
                                            wan = True
                                            match_fields.append({match_key: t})
                                            keys.append(match_key)
                                    elif temp_val in match_field and temp_val:
                                        if match_key not in keys:
                                            wan = wan or dw
                                            match_fields.append({match_key: t})
                                            keys.append(match_key)
                                    elif dqsje_condition_temp:
                                        if 'dqsje' not in keys:
                                            wan = wan or dw
                                            match_fields.append({'dqsje': t})
                                            keys.append('dqsje')
                                if match_key not in keys:
                                    if (match_condition or qsje_condition) and not dqsje_condition and u'万元' in val:
                                        wan = True
                                        match_fields.append({match_key: k})
                                        keys.append(match_key)
                                    elif (match_condition or qsje_condition) and not dqsje_condition:
                                        wan = wan or dw
                                        match_fields.append({match_key: k})
                                        keys.append(match_key)
                            else:
                                if match_key not in keys:
                                    if (match_condition or qsje_condition) and not dqsje_condition and u'万元' in val:
                                        wan = True
                                        match_fields.append({match_key: k})
                                        keys.append(match_key)
                                    elif (match_condition or qsje_condition) and not dqsje_condition:
                                        wan = wan or dw
                                        match_fields.append({match_key: k})
                                        keys.append(match_key)
                    elif match_condition:
                        if match_key not in keys:
                            match_fields.append({match_key: k})
                            keys.append(match_key)
        else:
            print col_nums
        return match_fields, wan

    def get_excel_abnormal_field_info(self, table, rows, fields=None):
        """
        获取非正常户excel文件的字段信息
        :param table: excel的一个sheet表
        :param rows: excel的总行数
        :param fields: 用来匹配的字段信息
        :return: 匹配字段结果
        """
        if not fields:
            fields = self.abnormal_fields
        col_val = []
        match_fields = []
        keys = []
        zjhms = [u'证件号码', u'身份证号码', u'法人证件号', u'证件名称', u'身份号码']
        # repeat_fddbrs = [u'法定代表人或业主']
        start_idx = self.get_excel_start_idx(table, rows)
        if start_idx == 0:
            return match_fields
        else:
            j = start_idx - 1
        for col in table.row_values(j):
            if len(col.strip()) > 0:
                col_val.append(col)
        col_nums = len(col_val)
        if col_nums > 1:
            row_val = table.row_values(j)
            for k in range(len(row_val)):
                if isinstance(row_val[k], float):
                    val = str(row_val[k]).decode('utf8').replace(u'\n', '').replace(u' ', u'')
                else:
                    val = row_val[k].strip().replace(u'\n', '').replace(u' ', u'')
                val = ''.join(val.split())
                # print 'val', val, len(val)
                for fds in range(len(fields)):
                    match_field = fields[fds].values()[0]
                    match_key = fields[fds].keys()[0]
                    match_condition = val in match_field and val
                    zjhm_condition = True in [zjhm in val for zjhm in zjhms]
                    # repeat_fddbr_condtion = True in [fddbr == val for fddbr in repeat_fddbrs]
                    if u'纳税人识别号' in match_field:
                        if match_key not in keys:
                            if u'纳税人识别号' in val or match_condition:
                                match_fields.append({match_key: k})
                                keys.append(match_key)
                    elif match_field == u'证件号码':
                        if match_key not in keys:
                            if zjhm_condition:
                                match_fields.append({match_key: k})
                                keys.append(match_key)
                    elif match_field == u'证件种类':
                        if match_key not in keys:
                            if match_field in val or u'证件类型' in val:
                                match_fields.append({match_key: k})
                                keys.append(match_key)
                    elif match_field == u'注册地址':
                        if match_key not in keys:
                            if match_condition and u'注册' in val:
                                match_fields.append({match_key: k})
                                keys.append(match_key)
                    elif u'法定代表人' in match_field:
                        # if repeat_fddbr_condtion:
                        temp_vals = table.row_values(j + 1)
                        for t in range(len(temp_vals)):
                            if isinstance(temp_vals[t], float):
                                temp_val = str(temp_vals[t]).decode('utf8').replace(u'\n', '').replace(u' ', u'')
                            else:
                                temp_val = temp_vals[t].strip().replace(u'\n', '').replace(u' ', u'')
                            temp_val = ''.join(temp_val.split())
                            zjhm_condition_temp = True in [zjhm in temp_val for zjhm in zjhms]
                            if u'姓名' in temp_val and not zjhm_condition_temp:
                                if match_key not in keys:
                                    match_fields.append({match_key: t})
                                    keys.append(match_key)
                            elif zjhm_condition_temp:
                                if 'zjhm' not in keys:
                                    match_fields.append({'zjhm': t})
                                    keys.append('zjhm')
                            elif u'经营地址' in temp_val:
                                if 'jydz' not in keys:
                                    match_fields.append({'jydz': t})
                                    keys.append('jydz')
                        if match_key not in keys:
                            if (u'法定代表人' in val or u'法人代表' in val or match_condition) and not zjhm_condition:
                                match_fields.append({match_key: k})
                                keys.append(match_key)
                    elif match_condition:
                        if match_key not in keys:
                            match_fields.append({match_key: k})
                            keys.append(match_key)
        else:
            print col_nums
        return match_fields

    def get_merge_cells(self, start_idx, table):
        """
        获取合并单元格信息
        :param start_idx: 读取excel的sheet表的起始位置
        :param table: excel的一个sheet表
        :return: 返回sheet表起始行开始后前两列的合并单元格
        """
        self
        new_merge_cells = []
        merge_cells = table.merged_cells
        merge_cells.sort()
        # print merge_cells, '**************'
        for merge_cell in merge_cells:
            if merge_cell[2] == 1 and merge_cell[0] >= start_idx:
                new_merge_cells.append(merge_cell[0:2])
        return new_merge_cells

    def get_merge_cells_row(self, j, merge_cells):
        """
        获取当前行的位置，如果当前行在合并单元格中，取合并行的首行；反之，返回输入的当前行。
        :param j:当前行
        :param merge_cells:前两列的所有合并单元格
        :return:
        """
        self
        for merge_cell in merge_cells:
            if merge_cell[0] < j < merge_cell[1]:
                return merge_cell[0]
        return j

    def get_field_val(self, val, j, table, merge_cells, position):
        """
        获取某个单元格的值，如果是合并单元格，返回合并单元格首行的值
        :param val:当前单元格的值
        :param j:当前单元格的行数
        :param table:当前sheet表
        :param merge_cells:sheet表前两行的合并单元格
        :param position:单元格的列数
        :return: 单元格的值
        """
        if not val and merge_cells:
            row = self.get_merge_cells_row(j, merge_cells)
            val = table.cell(row, position).value
            return val
        elif not val and not merge_cells:
            row = j - 1
            val = table.cell(row, position).value
            if not val:
                val = self.get_field_val(val, row, table, merge_cells, position)
                if val:
                    return val
            else:
                return val
        elif val:
            return val

    def get_int_field(self, val):
        """
        获取可能是整数字段的值
        :param val: 单元格的值
        :return: 单元格值的str的格式
        """
        if isinstance(val, int):
            return str(val)
        elif isinstance(val, float):
            return str(int(val))
        else:
            val = val.encode('utf8').replace(',', '').replace('，', '')
            return val

    def get_money_field(self, val, wan=False):
        """
        :param val:金额数值
        :param wan: 如果wan=True，表示money单位是万元，反之表示单位是万元。
        :return:处理后的字符串数值
        """
        if wan:
            if val:
                if isinstance(val, int):
                    return str(int(val) * 10000)
                elif isinstance(val, float):
                    return str(val * 10000)
                else:
                    val = val.encode('utf8')
                    val = val.replace(',', '').replace('元', '').replace('，', '')
                    return str(float(val) * 10000)
            else:
                return str(val)
        else:
            if isinstance(val, int):
                return str(int(val))
            elif isinstance(val, float):
                return str(val)
            else:
                val = val.encode('utf8').replace(',', '').replace('元', '').replace('，', '')
                return val

    def get_date(self, table, row, column):
        """
        获取excel表里的日期
        :param table:excel里的一张sheet表
        :param row:sheet表的当前行
        :param column:sheet表的当前列
        :return:
        """
        if table.cell_type(row, column) == 3:
            rq = str(xlrd.xldate.xldate_as_datetime(table.cell(row, column).value, 0))[0:10]
        elif isinstance(table.cell(row, column).value, float):
            rq = str(int(table.cell(row, column).value))[0:10]
            if len(rq) == 8:
                rq = rq[0:4] + '-' + rq[4:6] + '-' + rq[6:8]
        else:
            val = table.cell(row, column).value.strip()
            if type(val) == unicode:
                rq = val.replace(u'年', u'-').replace(u'月', u'-').replace(u'日', u'-')
                rq = rq.encode('utf8')[0:10]
            elif len(val) > 9:
                rq = val[0:10]
            elif len(val) == 8:
                rq = val[0:4] + '-' + val[4:6] + '-' + val[6:8]
            else:
                rq = val
        rq = rq.replace('/', '-').replace('.', '-')
        return rq

    def excel_reader(self):
        pass

    # 以下到html_reader()的函数用来解析word和html

    def convert_word_to_html(self, sql, file_directory, save_directory):
        """
        将word格式文件转成html格式文件
        :param sql:选取需要转格式的文件sql语句
        :param file_directory:需要转格式的文件的路径
        :param save_directory:转格式保存新格式的文件路径
        :return:
        """
        self.get_directory(save_directory)
        print sql
        nums = self.cursor.execute(sql)  # 返回符合条件的总数表
        self.log('num_info:' + str(nums))
        info = self.cursor.fetchmany(nums)
        for n in range(0, nums):
            w = win32com.client.Dispatch('Word.Application')
            filepath = file_directory + info[n][4]
            if os.path.isfile(filepath):
                try:
                    doc = w.Documents.Open(filepath)
                    self.get_directory(save_directory)
                    savepath = save_directory + info[n][4].split('.')[0] + '.html'
                    if os.path.isfile(savepath):
                        doc.Close()
                    else:
                        doc.SaveAs(savepath, 8)
                        doc.Close()
                except:
                    print n + 1, filepath
            else:
                print filepath + ' does not existed'

    def get_filepath(self, filename, file_directory):
        """
        获取文件路径
        :param filename: 文件名
        :param file_directory:保存的文件所在文件夹
        :return:文件的绝对路径
        """
        if '.doc' in filename or '.DOC' in filename:
            return file_directory + '\\word_to_html\\' + filename.split('.')[0] + '.html'
        else:
            return file_directory + filename

    def get_decode_way(self, filename):
        """
        获取解析html的时的编码格式
        :param filename: 文件名
        :return: 如果是word格式文件，返回gbk,反之，返回空字符串。这是因为html格式用Beautifulsoup会自动解析，不需要指定
        解析编码格式，具体的见本公共模块中的get_soup()函数。
        """
        if '.doc' in filename or '.DOC' in filename:
            decode_way = 'gbk'
        else:
            decode_way = ''
        return decode_way

    def get_soup(self, filepath, decode_way=None):
        """
        获取beautifulsoup后的html文件
        :param filepath: 文件路径
        :param decode_way: 解码格式
        :return:
        """
        try:
            htmlfile = open(filepath, 'r')  # 以只读的方式打开本地html文件
        except IOError:
            return ''
        else:
            if decode_way:
                htmlpage = htmlfile.read().decode(decode_way, 'ignore').encode('utf8')
            else:
                htmlpage = htmlfile.read()
            soup = BeautifulSoup(htmlpage, "html.parser")  # 实例化一个BeautifulSoup对象
            htmlfile.close()
            return soup

    def get_html_start_info(self, tr_list):
        """
        获取html文件中table表里除字段行的所有tr
        :param tr_list: table表里所有的tr
        :return:
        """
        new_tr_list = []
        for i in range(len(tr_list)):
            tr_text = tr_list[i].text.strip().replace(u'\n', '').replace(u' ', u'').replace(' ', '')
            # print tr_text
            if u'纳税人识别号' in tr_text or u'纳税人名称' in tr_text or u'纳税人识别码' in tr_text \
                    or u'税务登记号码' in tr_text or u'企业或单位名称' in tr_text or u'税务登记证号' in tr_text \
                    or u'税务登记号' in tr_text or u'纳税识别号' in tr_text or u'纳税人名称' in tr_text \
                    or u'业户名称' in tr_text:
                new_tr_list = tr_list[i:]
                break
        return new_tr_list

    def get_inner_table(self, table):
        """
        获取标签最内层的table表
        :param table: 传入的table对象
        :return:如果table标签里层有table标签，返回最里层的table标签。反之返回输入的table对象。
        """
        new_table = table.find('table')
        if not new_table:
            return table
        else:
            return self.get_inner_table(new_table)

    def find_tr_list(self, inner_table):
        """
        获取table表里的满足条件的所有tr
        :param inner_table:
        :return:
        """
        tr_list = inner_table.findAll('tr')
        new_tr_list = self.get_html_start_info(tr_list)
        return new_tr_list

    def get_tr_list(self, soup):
        """
        获取html里特定table的所有tr
        :param soup:
        :return:
        """
        inner_signal = False
        if soup:
            tables = soup.findAll('table')
            tr_list = []
            if tables:
                for table in tables:
                    inner_table = self.get_inner_table(table)
                    tr_list = self.find_tr_list(inner_table)
                    if tr_list:
                        return tr_list, inner_signal
                for table in tables:
                    tr_list = self.find_tr_list(table)
                    if tr_list:
                        inner_signal = True
                        return tr_list, inner_signal
            else:
                print 'no tables'
            return tr_list, inner_signal
        else:
            return [], inner_signal

    def read_html_field_info(self, sql, file_directory, savepath, row_num):
        """
        读取html的字段信息
        :param sql: 选取要读取文件的sql语句
        :param file_directory: 读取文件所在文件夹
        :param savepath: 保存读取字段excel文件的路径
        :param row_num: 写入sheet表的当前行
        :return:
        """
        nums = self.cursor.execute(sql)
        print sql
        print nums
        if nums:
            info = self.cursor.fetchmany(nums)
            if os.path.isfile(savepath):
                rb = xlrd.open_workbook(savepath)
                f = copy(rb)
            else:
                f = xlwt.Workbook(encoding='utf-8')
            row = 0
            for t in range(3):
                row = row_num
                row += 1
                if os.path.isfile(savepath):
                    sheet = f.get_sheet(t)
                    sheet.write(row, 0, 'html')
                else:
                    sheet = f.add_sheet('fieldnames' + str(t), cell_overwrite_ok=True)
                    sheet.write(row, 0, 'html')
                for n in range(0, nums):
                    # print n + 1, '111111'
                    row += 1
                    province = info[n][0]
                    filepath = self.get_filepath(info[n][4], file_directory)
                    decode_way = self.get_decode_way(info[n][4])
                    try:
                        soup = self.get_soup(filepath, decode_way)
                        tr_list, inner_signal = self.get_tr_list(soup)
                        new_tr_list = self.get_html_start_info(tr_list)
                        dt = len(tr_list) - len(new_tr_list)
                        if new_tr_list:
                            tr_values = tr_list[dt + t].findAll('td')
                            th_values = tr_list[dt + t].findAll('th')
                            if tr_values:
                                tag_values = tr_values
                            else:
                                tag_values = th_values
                            sheet.write(row, 0, n + 1)
                            for j in range(len(tag_values)):
                                if t == 0:
                                    val = tag_values[j].text.strip().replace(u'\n', u'').replace(u' ', u'')
                                    val = ''.join(val.split())
                                else:
                                    val = tag_values[j].text
                                sheet.write(row, j + 1, val)
                        else:
                            print n + 1, 'no tr_list return', filepath
                            sheet.write(row, 0, n + 1)
                            sheet.write(row, 1, '?')
                    except Exception as e:
                        print n + 1, filepath, e
                        sheet.write(row, 0, n + 1)
                        sheet.write(row, 1, '?')
                for k in range(10):
                    col = sheet.col(k)
                    col.width = 256 * 25
            f.save(savepath)
            return row
        else:
            return row_num

    def get_max_position(self, positions, i):
        """
        获取满足条件的位置
        :param positions: 已匹配的字段所有位置list
        :param i:当前用来匹配的位置
        :return:
        """
        self
        if i > max(positions):
            return i
        else:
            return max(positions) + 1

    def get_tag_val(self, val):
        """
        获取一个标签的所有的text，并去掉各种空格
        :param val: 属性为tag的标签
        :return: 标签的text
        """
        self
        val = val.text.strip().replace(u'\n', '').replace(u' ', u'')
        val = ''.join(val.split())
        return val

    def get_money_dw_html(self, soup):
        """
        获取html格式的欠税单位
        :param soup: 经过beautifulsoup后的html
        :return: True表示单位为万元，反之，表示单位为元。
        """
        self
        if soup:
            if u': 万元' in soup.text or u':万元' in soup.text or u'：万元' in soup.text or u'： 万元' in soup.text:
                return True
            else:
                return False
        else:
            return False

    def get_html_qsgg_field_info(self, tr_list, fields=None):
        """
        获取html格式欠税的字段信息
        :param tr_list: html里选取的table表里所有的tr标签
        :param fields: 用来匹配的初始字段信息
        :return: 匹配后的字段结果
        """
        self
        if not fields:
            fields = self.qs_fields
        match_fields = []
        rq_keys = ['xjrq', 'ssqs', 'ssqz']
        special_keys = ['fddbr', 'qsje']
        wan = False
        keys = []
        positions = []
        repeat_qsjes = [u'合计', u'截止', u'欠税情况', u'欠税额度（元）', u'欠税余额情况']
        repeat_fddbrs = [u'法人代表', u'法定代表人或业主']
        zjhms = [u'证件号码', u'身份证号码', u'法人证件号', u'身份号码', u'（护照）号码', u'身份证号', u'有效证件号',
                 u'（有效证件）号码']
        filter_nsrmcs = [u'纳税人']
        dqsjes = [u'新发生', u'新欠', u'新增欠税']
        filter_qsjes = [u'上期']
        filter_rqs = [u'日期', u'税款所属期', u'所属期']
        tr_title = tr_list[0]
        td = tr_title.findAll('td')
        th = tr_title.findAll('th')
        temp_tds = []
        if len(tr_list) >= 2:
            temp_tds = tr_list[1].findAll('td')
        if td:
            tags = td
        else:
            tags = th
        for i in range(len(tags)):
            val = self.get_tag_val(tags[i])
            # print 'val', val, len(val)
            for fds in range(len(fields)):
                match_field = fields[fds].values()[0]
                match_key = fields[fds].keys()[0]
                # print val, match_order_condition, match_key
                match_condition = val in match_field and val
                filter_nsrmc_condition = True in [nsrmc == val for nsrmc in filter_nsrmcs]
                dqsje_condition = True in [dqsje in val for dqsje in dqsjes]
                filter_qsje_condition = True in [qsje in val for qsje in filter_qsjes]
                repeat_qsje_condition = True in [qsje in val for qsje in repeat_qsjes]
                repeat_fddbr_condition = True in [fddbr == val for fddbr in repeat_fddbrs]
                zjhm_condition = True in [zjhm in val for zjhm in zjhms]
                filter_rq_condition = True in [rq == val for rq in filter_rqs]
                if u'纳税人识别号' in match_field:
                    if match_key not in keys:
                        if u'纳税人识别号' in val or match_condition and not filter_nsrmc_condition:
                            match_fields.append({match_key: i})
                            keys.append(match_key)
                            positions.append(i)
                elif match_field == u'证件号码':
                    if match_key not in keys:
                        if zjhm_condition or val == u'身份证':
                            match_fields.append({match_key: i})
                            keys.append(match_key)
                            positions.append(i)
                elif match_field == u'证件种类':
                    if match_key not in keys:
                        if match_field in val or u'证件类型' in val or u'证件名称' in val:
                            match_fields.append({match_key: i})
                            keys.append(match_key)
                            positions.append(i)
                elif u'法定代表人' in match_field:
                    if repeat_fddbr_condition:
                        for t in range(len(temp_tds)):
                            temp_val = self.get_tag_val(temp_tds[t])
                            non_repetition_temp = match_key not in keys
                            if u'姓名' in temp_val:
                                if non_repetition_temp:
                                    match_fields.append({match_key: i + t})
                                    keys.append(match_key)
                                    positions.append(i + t)
                            elif u'证件名称' in temp_val or u'证件种类' in temp_val or u'证件类型' in temp_val:
                                if 'zjzl' not in keys:
                                    match_fields.append({'zjzl': i + t})
                                    keys.append('zjzl')
                                    positions.append(i + t)
                            elif u'证件号码' in temp_val or u'身份证号' in temp_val or u'居民身份证' in temp_val:
                                if 'zjhm' not in keys:
                                    match_fields.append({'zjhm': i + t})
                                    keys.append('zjhm')
                                    positions.append(i + t)
                            elif (u'法定代表' in val or u'法人代表' in val or match_condition) and not zjhm_condition:
                                if non_repetition_temp:
                                    match_fields.append({match_key: i})
                                    keys.append(match_key)
                                    positions.append(i + t)
                    else:
                        if match_key not in keys:
                            if (u'法定代表' in val or u'法人代表' in val or match_condition) and not zjhm_condition:
                                match_fields.append({match_key: i})
                                keys.append(match_key)
                                positions.append(i)
                elif match_key == 'qssz':
                    if match_condition:
                        if match_key not in keys:
                            if len(temp_tds) >= 4:
                                sz_time = self.get_sz_time(temp_tds[2: 4])
                            else:
                                sz_time = 0
                            if sz_time <= 1:
                                if 'qsje' in keys or i in positions:
                                    max_position = self.get_max_position(positions, i)
                                    match_fields.append({match_key: max_position})
                                    keys.append(match_key)
                                    positions.append(max_position)
                                else:
                                    match_fields.append({match_key: i})
                                    keys.append(match_key)
                                    positions.append(i)
                elif match_key == 'jydz':
                    if match_condition:
                        if match_key not in keys:
                            if 'zjhm' in keys or i in positions:
                                max_position = self.get_max_position(positions, i)
                                match_fields.append({match_key: max_position})
                                keys.append(match_key)
                                positions.append(max_position)
                            else:
                                match_fields.append({match_key: i})
                                keys.append(match_key)
                                positions.append(i)
                elif match_key in rq_keys:
                    if match_key not in keys and not filter_rq_condition:
                        if match_condition:
                            match_fields.append({match_key: i})
                            keys.append(match_key)
                            positions.append(i)
                elif u'欠税余额' in match_field:
                    if repeat_qsje_condition:
                        if u'合计' in val:
                            if match_key not in keys:
                                match_fields.append({match_key: i})
                                keys.append(match_key)
                                positions.append(i)
                        else:
                            for t in range(len(temp_tds)):
                                temp_val = self.get_tag_val(temp_tds[t])
                                dqsje_condition_temp = True in [dqsje in temp_val for dqsje in dqsjes]
                                non_repetition_temp = match_key not in keys
                                if u'合计' in temp_val:
                                    if 'qsje' not in keys:
                                        max_position = self.get_max_position(positions, i + t)
                                        match_fields.append({match_key: max_position})
                                        keys.append(match_key)
                                        positions.append(max_position)
                                        break
                                elif u'税种' in temp_val:
                                    if 'qssz' not in keys:
                                        match_fields.append({'qssz': i + t})
                                        keys.append('qssz')
                                        positions.append(i + t)
                                elif temp_val in match_field and u'万元' in temp_val:
                                    if non_repetition_temp:
                                        max_position = self.get_max_position(positions, i + t)
                                        wan = True
                                        match_fields.append({match_key: max_position})
                                        keys.append(match_key)
                                        positions.append(max_position)
                                elif temp_val in match_field and temp_val:
                                    if non_repetition_temp:
                                        max_position = self.get_max_position(positions, i + t)
                                        match_fields.append({match_key: max_position})
                                        keys.append(match_key)
                                        positions.append(max_position)
                                elif dqsje_condition_temp:
                                    if 'dqsje' not in keys:
                                        max_position = self.get_max_position(positions, i + t)
                                        match_fields.append({'dqsje': max_position})
                                        keys.append('dqsje')
                                        positions.append(max_position)
                            if match_key not in keys:
                                if match_condition and u'万元' in val:
                                    wan = True
                                    max_position = self.get_max_position(positions, i)
                                    match_fields.append({match_key: max_position})
                                    keys.append(match_key)
                                    positions.append(max_position)
                                elif (match_condition or u'欠税余额' in val) and not dqsje_condition \
                                        and not filter_qsje_condition:
                                    max_position = self.get_max_position(positions, i)
                                    match_fields.append({match_key: max_position})
                                    keys.append(match_key)
                                    positions.append(max_position)

                    else:
                        if match_key not in keys:
                            if match_condition and u'万元' in val:
                                wan = True
                                max_position = self.get_max_position(positions, i)
                                match_fields.append({match_key: max_position})
                                keys.append(match_key)
                                positions.append(max_position)
                            elif (match_condition or u'欠税余额' in val) and not dqsje_condition \
                                    and not filter_qsje_condition:
                                max_position = self.get_max_position(positions, i)
                                match_fields.append({match_key: max_position})
                                keys.append(match_key)
                                positions.append(max_position)
                elif match_field == u'当期':
                    if match_key not in keys:
                        if dqsje_condition:
                            max_position = self.get_max_position(positions, i)
                            match_fields.append({match_key: max_position})
                            keys.append(match_key)
                            positions.append(max_position)
                elif match_condition:
                    if match_key not in keys and match_key not in special_keys:
                        match_fields.append({match_key: i})
                        keys.append(match_key)
                        positions.append(i)
        return match_fields, wan

    def get_html_abnormal_field_info(self, tr_list, fields=None):
        """
        获取html格式非正常户的字段信息
        :param tr_list: html选取的table表里所有的tr标签
        :param fields: 用来匹配的初始字段信息
        :return: 匹配后的字段结果
        """
        judge = None
        if not fields:
            fields = self.abnormal_fields
        match_fields = []
        keys = []
        zjhms = [u'证件号码', u'身份证号', u'法人证件号', u'身份号码']
        repeat_fddbrs = [u'法定代表人或负责人']
        tr_title = tr_list[0]
        if len(tr_list) >= 2:
            temp_tds = tr_list[1].findAll('td')
        td = tr_title.findAll('td')
        th = tr_title.findAll('th')
        if td:
            tags = td
        else:
            tags = th
        for i in range(len(tags)):
            val = tags[i].text.strip().replace(' ', '')
            val = val.replace(u'\n', '')
            val = ''.join(val.split())
            # print 'val', val, len(val)
            for fds in range(len(fields)):
                match_field = fields[fds].values()[0]
                match_key = fields[fds].keys()[0]
                match_condition = val in match_field and val
                zjhm_condition = True in [zjhm in val for zjhm in zjhms]
                repeat_fddbr_condition = True in [fddbr == val for fddbr in repeat_fddbrs]
                # print val
                # print match_field, match_key
                if match_field == u'证件号码':
                    if match_key not in keys:
                        if zjhm_condition:
                            match_fields.append({match_key: i})
                            keys.append(match_key)
                elif match_field == u'证件种类':
                    if match_key not in keys:
                        if match_field in val or u'证件类型' in val or u'证件名称' in val:
                            match_fields.append({match_key: i})
                            keys.append(match_key)
                elif match_field == u'注册地址':
                    if match_key not in keys:
                        if match_condition and u'注册' in val:
                            match_fields.append({match_key: i})
                # 标题合并单元格判断
                elif u'法定代表人' in match_field:
                    if repeat_fddbr_condition:
                        judge = True
                        print '@@@@@@@@'
                        for t in range(len(temp_tds)):
                            print '##########', len(temp_tds)
                            temp_val = self.get_tag_val(temp_tds[t])
                            if u'姓名' in temp_val:
                                if match_key not in keys:
                                    match_fields.append({match_key: i + t})
                                    print '$$$$$$$$$$$$$', match_fields
                                    keys.append(match_key)
                                    print '!!!!!!!!!!!', keys
                            elif u'居民身份证或其他有效身份证件号码' in temp_val:
                                if 'zjhm' not in keys:
                                    match_fields.append({'zjhm': i + t})
                                    keys.append('zjzl')
                            keys.append(match_key)
                    else:
                        if match_key not in keys:
                            if u'法定代表人' in val or match_condition and not zjhm_condition:
                                match_fields.append({match_key: i})
                                keys.append(match_key)
                # elif u'法定代表人' in match_field:
                #     if match_key not in keys:
                #         if u'法定代表人' in val or match_condition and not zjhm_condition:
                #             match_fields.append({match_key: i})
                #             keys.append(match_key)
                elif match_condition:
                    if match_key not in keys:
                        match_fields.append({match_key: i})
                        keys.append(match_key)
        return match_fields

    def get_formate_date(self, val):
        """
        获取格式化的日期，默认结果表示为"2017-03-08"格式
        :param val: 原始的日期
        :return: 格式化后的日期
        """
        self
        val = val.replace('年', '-').replace('月', '-').replace('日', '')
        if '星期' in val:
            val = val.replace('星期', '').replace('一', '').replace('二', '').replace('三', '')
            val = val.replace('四', '').replace('五', '')
        val = val.replace('/', '-')
        val = val.replace('.', '-')
        if '-' not in val and len(val) == 8:
            val = val[0:4] + '-' + val[4:6] + '-' + val[6:8]
        elif len(val) >= 10:
            val = val[:10]
        elif '-' not in val[-4:]:
            val = val[:-4] + val[-4:-2] + '-' + val[-2:]
        vals = val.split('-')
        new_date = ''
        for va in vals:
            if len(va) == 4 or len(va) == 2:
                new_date += va + '-'
            elif len(va) == 1:
                new_date += '0' + va + '-'
        new_date = new_date[:-1]
        return new_date

    def get_hb_cell_val(self, j, new_tr_list, position, inner_signal):
        """
        获取html合并单元格的值
        :param j: 当前的行数
        :param new_tr_list: 所有的tr行list
        :param position: 当前读取的列
        :param inner_signal: tr内层是否嵌套table的标志，True表示默认嵌套，False表示默认不嵌套。
        :return:返回合并单元格的值
        """
        if inner_signal:
            tds = new_tr_list[j].children
            td_texts = [td.text.strip() for td in tds if isinstance(td, bs4.element.Tag)]
        else:
            tds = new_tr_list[j].findAll('td')
            td_texts = [td.text.strip() for td in tds]
        val = td_texts[position].encode('utf8')
        if not val and j > 0:
            j -= 1
            val = self.get_hb_cell_val(j, new_tr_list, position, inner_signal)
            return val
        else:
            return val

    def html_reader(self):
        pass


    # 记录日志以及打印每省插入mysql数据库信息
    def log_province(self,province,num_sql_all,num_repetition_all,num_fail_all):
        self.log(province + u'一共插入' + str(num_sql_all) + u'条')
        self.log(province + u'一共重复' + str(num_repetition_all) + u'条')
        self.log(province + u'一共失败' + str(num_fail_all) + u'条')

    def print_chart(self,num_info,num_sql,num_repetition,num_fail):
        print str(num_info + 1) + u'表插入' + str(num_sql) + u'条'
        print str(num_info + 1) + u'表重复' + str(num_repetition) + u'条'
        print str(num_info + 1) + u'表失败' + str(num_fail) + u'条'



if __name__ == '__main__':
    reader = TaxplayerReader()
    # print reader.abnormal_fields[0]['nsrsbh']
    # reader.get_soup()
