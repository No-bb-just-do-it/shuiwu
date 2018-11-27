# coding=utf-8
import xlrd
import xlwt
import MySQLdb
import os
import sys
import time
import logging
import re


class TaxplayerSHexcelreader(object):
    def __init__(self):
        self.mysql_conn()
        self.set_config()

    def set_config(self):
        self.field = [{'nsrsbm': u'税务登记证,税务登记号,纳税人识别号,纳税人识别码,纳税人税务登记号,纳税识别号'},
                      {'nsrdm': u'纳税人代码'},
                      {'qymc': u'纳税人名称,业户名称'},
                      {'fddbr': u'法定代表人姓名,法定代表人或负责人姓名,法定代表人（负责人）姓名,'
                                u'法定代表人或负责人名称,法人代表,业主名称,法定代表人或负责人姓名,业主姓名'},
                      {'qssz': u'欠税税种'},
                      # u'征收项目名称'
                      {'qsje': u'欠税余额'},
                      {'xjrq': u'限缴日期'},
                      # {'swjg': u''},
                      {'bldd': u'办理地点'},
                      {'rddw': u'国地税'},
                      {'zjzl': u'种类'},
                      {'zjhm': u'号码'},
                      {'zcdz': u'法定注册地址'},
                      # {'djrq': u'登记日期'},
                      {'jydz': u'生产经营地址'},
                      {'ssqs': u'税费所属期起'},
                      {'ssqz': u'税费所属期止'},
                      ]
        self.path = sys.path[0] + '\All_files\\'
        self.nsrlb = [u'非个体', u'个体']
        self.update_time = "'2018-01-15%'"
        self.last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')

    def mysql_conn(self):
        self.conn = MySQLdb.connect(host='172.16.0.76', port=3306, user='fengyuanhua', passwd='!@#qweASD',
                                    db='taxplayer', charset='utf8')
        self.cursor = self.conn.cursor()

    def get_directory(self, directory):
        if not os.path.exists(directory):
            os.makedirs(directory)

    def get_intfield(self, val):
        if isinstance(val, float):
            return str(int(val)).replace("'", "")
        elif isinstance(val, int):
            return str(int(val)).replace("'", "")
        else:
            val = val.strip().replace("'", "").encode('utf8')
            return val

    def get_money_field(self, val):
        if isinstance(val, int):
            return str(int(val))
        elif isinstance(val, float):
            return str(val)
        else:
            val = val.encode('utf8')
            val = val.replace(',', '')
            return val

    def get_date(self, table, row, column):
        if table.cell_type(row, column) == 3:
            rdrq = str(xlrd.xldate.xldate_as_datetime(table.cell(row, column).value, 0))[0:10]
        elif isinstance(table.cell(row, column).value, float):
            rdrq = str(int(table.cell(row, column).value))[0:10]
            if len(rdrq) == 8:
                rdrq = rdrq[0:4] + '-' + rdrq[4:6] + '-' + rdrq[6:8]
        else:
            val = table.cell(row, column).value.strip()
            if type(val) == unicode:
                rdrq = val.encode('utf8')
                rdrq = rdrq.replace('年', '-')
                rdrq = rdrq.replace('月', '-')
                rdrq = rdrq.replace('日', '')
                rdrq = rdrq[0:10]
            elif len(val) > 9:
                rdrq = val[0:10]
            elif len(val) == 8:
                rdrq = val[0:4] + '-' + val[4:6] + '-' + val[6:8]
            else:
                rdrq = val
        return rdrq

    def get_start_info(self, table, rows):
        start_idx = 0
        col_nums = 0
        for _idx in range(0, rows):
            cols = []
            for col in table.row_values(_idx):
                if isinstance(col, float):
                    cols.append(col)
                elif len(col.strip()) > 0:
                    cols.append(_idx)
            col_nums = len(cols)
            if col_nums > 3:
                start_idx = _idx + 1
                break
        start_info = [start_idx, col_nums]
        return start_info

    def logger(self, xzqy_py, m, n):
        log_filename = 'Excelreader_SH_' + xzqy_py + '_' + str(m) + '.log'
        logging.basicConfig(filename=log_filename, filemode='w')
        logging.warning(msg='  ' + u'第' + str(n + 1) + u'张非正常认定户表')

    def data_to_mysql(self, sql, num, k1, n):
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            num += 1
            data_nums = [num, k1]
            return data_nums
        except Exception as e:
            if e[0] != 1062:
                print n + 1, sql, e
            k1 += 1
            data_nums = [num, k1]
            return data_nums

    def get_all_info(self):
        sql = "select * from taxplayer_filename where last_update_time like " + self.update_time + \
              "and title like '%欠税公告%' and filename like '%.xls%'"
        # sql = "select * from taxplayer_sh_newoo_filename where title like '%欠税公告%' and filename like '%.xls%'"
        print sql
        nums = self.cursor.execute(sql)
        if nums > 0:
            info = self.cursor.fetchmany(nums)
            return info
        else:
            print u'没有需要解析的文件'
            os._exit(1)

    def get_field_info(self, table, rows, fields):
        col_val = []
        match_fields = []
        start_idx = reader.get_start_info(table, rows)[0]
        if start_idx == 0:
            return match_fields
        else:
            j = start_idx - 1
        for col in table.row_values(j):
            if len(col.strip()) > 0:
                col_val.append(col)
        col_nums = len(col_val)
        # print col_nums
        if col_nums > 2:
            row_val = table.row_values(j)
            match_time = 0
            for k in range(len(row_val)):
                val = row_val[k].strip().replace(u'\n', '').replace(u' ', u'')
                # print 'val', val, len(val)
                for fds in range(len(fields)):
                    match_field = fields[fds].values()[0]
                    match_key = fields[fds].keys()[0]
                    if match_field == u'欠税税种':
                        if u'税种' in val or u'征收项目' in val:
                            match_fields.append({match_key: k})
                    elif match_field == u'种类':
                        if u'证件种类' in val or u'证件名称' in val:
                            match_fields.append({match_key: k})
                    elif match_field == u'欠税余额':
                        if u'合计' in val:
                            match_fields.append({match_key: k})
                        elif u'截止' in val:
                            temp_row_val = table.row_values(j + 1)
                            for t in range(len(temp_row_val)):
                                if isinstance(temp_row_val[t], float):
                                    temp_val = str(temp_row_val[t]).decode('utf8').replace(u'\n', '')
                                else:
                                    temp_val = temp_row_val[t].strip().replace(u'\n', '').replace(' ', '')
                                if u'合计' in temp_val:
                                    match_fields.append({match_key: t})
                                    break
                        else:
                            if u'欠税余额' in val:
                                match_fields.append({match_key: k})
                    elif match_field == u'号码':
                        if u'证件号' in val or u'身份证号码' in val:
                            match_fields.append({match_key: k})
                    elif val in match_field and val:
                        match_fields.append({match_key: k})
        else:
            print col_nums
        return match_fields

    def read_fieldnames(self):
        sql = "select * from taxplayer_sh_newoo_filename where title like '%欠税公告%' and filename like '%.xls%'"
        nums = self.cursor.execute(sql)
        print nums
        # break
        if nums:
            info = self.cursor.fetchmany(nums)
            f = xlwt.Workbook(encoding='utf-8')
            for t in range(3):
                row = -1
                sheet = f.add_sheet('fieldnames' + str(t), cell_overwrite_ok=True)
                for n in range(0, nums):
                    row += 1
                    try:
                        filepath = self.path + info[n][3]
                        # print filepath
                        # os._exit(1)
                        excel = xlrd.open_workbook(filepath, 'utf8')
                        sheets = excel.sheets()
                        count = len(sheets)  # sheet数量
                        # print count
                        for m in range(count):
                            row += 1
                            try:
                                table = excel.sheets()[m]
                                rows = table.nrows
                                start_idx = reader.get_start_info(table, rows)[0]
                                if start_idx < 5:
                                    for line in range(t, t + 1, 1):
                                        sheet.write(row, 0, n + 1)
                                        row_val = table.row_values(line)
                                        for j in range(len(row_val)):
                                            # print row, j + 1, row_val[j]
                                            val = row_val[j].strip().replace(u'\n', '').replace(' ', '')
                                            val = ''.join(val.split('  '))
                                            val = val.replace(' ', '')
                                            sheet.write(row, j + 1, val)
                                else:
                                    for line in range(t + start_idx - 2, t + start_idx - 1, 1):
                                        sheet.write(row, 0, n + 1)
                                        row_val = table.row_values(line)
                                        for j in range(len(row_val)):
                                            val = row_val[j].strip().replace(u'\n', '').replace(' ', '')
                                            val = ''.join(val.split())
                                            val = val.replace(' ', '')
                                            sheet.write(row, j + 1, val)
                            except:
                                sheet.write(row, 0, n + 1)
                                sheet.write(row, 1, '?')
                    except:
                        print n, info[n][3]
                        row += 1
                        sheet.write(row, 0, n + 1)
                        sheet.write(row, 1, '?')
                for k in range(17):
                    col = sheet.col(k)
                    col.width = 256 * 25
            save_directory = sys.path[0] + '\\qsgg_excel_read\\filednames'
            savepath = save_directory + '\\' + 'qsgg_excel_read_filednames.xls'
            reader.get_directory(save_directory)
            f.save(savepath)

    def field_collect(self):
        # for i in range(len(self.provinces)):
        # 收集特定某省份字段所在行下一行的数据
        directory = sys.path[0] + '\\qsgg_excel_read\\filednames'
        filepath = directory + '\\' + 'qsgg_excel_read_filednames.xls'
        f = xlwt.Workbook(encoding='utf-8')
        sheet = f.add_sheet('fieldnames', cell_overwrite_ok=True)
        row = -1
        try:
            excel = xlrd.open_workbook(filepath, 'utf8')
            sheets = excel.sheets()
            count = len(sheets)  # sheet数量
            for m in range(0, count):
                table = excel.sheets()[m]
                rows = table.nrows
                # start_idx = reader.get_start_info(table, rows)[0]
                for j in range(0, rows):
                    # print len(table.row_values(j))
                    col_val = []
                    for col in table.row_values(j):
                        if isinstance(col, float):
                            col = str(col)
                        else:
                            col = col.encode('utf8')
                        if len(col.strip()) > 0:
                            col_val.append(col)
                    col_nums = len(col_val)
                    if col_nums > 3:
                        row_val = table.row_values(j)
                        match = ''
                        for k in range(len(row_val)):
                            if isinstance(row_val[k], float):
                                val = str(int(row_val[k]))
                            else:
                                val = row_val[k].encode('utf8')
                            match += val
                        # print type(match), match
                        # os._exit(1)
                        if '纳税人识别' in match or '纳税人名称' in match or '税务登记号码' in match \
                                or '序号' in match or '纳税人识别号' in match:
                            row += 1
                            table2 = excel.sheets()[m]
                            # rows2 = table.nrows
                            try:
                                row_val = table2.row_values(j)
                                # 收集字段所在行下一行的数据
                                # row_val = table2.row_values(j + 1)
                                for k in range(len(row_val)):
                                    sheet.write(row, k, row_val[k])
                            except:
                                print j
            for col_line in range(10):
                col = sheet.col(col_line)
                col.width = 256 * 25
            save_directory = sys.path[0] + '\\qsgg_excel_read\\filednames'
            savepath = save_directory + '\\' + 'qsgg_excel_read_field_collect.xls'
            # 收集字段所在行下一行的数据的保存路径
            # savepath = save_directory + '\\' + province_py + '_field_next_collect.xls'
            reader.get_directory(save_directory)
            f.save(savepath)
        except Exception as e:
            print filepath, e

    def get_merge_cells(self, start_idx, table):
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
        self
        for merge_cell in merge_cells:
            if merge_cell[0] < j < merge_cell[1]:
                # print merge_cell[0], 'fsdfsfsdfsfsd'
                return merge_cell[0]
        return j

    def get_field_val(self, val, j, table, merge_cells, position):
        self
        if not val and merge_cells:
            row = reader.get_merge_cells_row(j, merge_cells)
            val = table.cell(row, position).value
            return val
        elif not val and not merge_cells:
            row = j - 1
            val = table.cell(row, position).value
            if not val:
                val = reader.get_field_val(val, row, table, merge_cells, position)
                if val:
                    return val
            else:
                return val
        else:
            return val

    def excelread(self):
        fields = self.field
        info = reader.get_all_info()
        all_num = 0
        k2 = 0
        for n in range(0, len(info)):
            k1 = 0
            num = 0
            xzqy = info[n][0]
            ggrq = info[n][1]
            filepath = self.path + info[n][3]
            # print filepath
            # filter_nums = [3]
            # if n + 1 not in filter_nums:
            #     continue
            try:
                if '.xlsx' in filepath:
                    excel = xlrd.open_workbook(filepath)
                else:
                    excel = xlrd.open_workbook(filepath, formatting_info=True)
                sheets = excel.sheets()
                count = len(sheets)  # sheet数量
                for m in range(count):
                    try:
                        table = excel.sheets()[m]
                        rows = table.nrows
                        match_fields = reader.get_field_info(table, rows, fields)
                        if match_fields:
                            print n + 1, match_fields
                            continue
                            start_idx = reader.get_start_info(table, rows)[0]
                            merge_cells = reader.get_merge_cells(start_idx, table)
                            int_fields = 'nsrsbm,zjhm'
                            money_fields = 'qsje'
                            rq_fields = 'xjrq,ggrq,ssqs,ssqz'
                            keep_fields = 'qsje,qssz'
                            for j in range(start_idx, rows):
                                row_vals = table.row_values(j)
                                field_keys = 'xzqy,last_update_time,'
                                val = "'" + xzqy.encode(
                                    'utf8') + "','" + self.last_update_time + "','"
                                for md in range(len(match_fields)):
                                    key = match_fields[md].keys()[0]
                                    position = match_fields[md].values()[0]
                                    row_val = row_vals[position]
                                    if key in keep_fields:
                                        row_val_new = row_val
                                    else:
                                        row_val_new = reader.get_field_val(row_val, j, table, merge_cells, position)
                                    if key in int_fields:
                                        val += reader.get_intfield(row_val_new) + "','"
                                        field_keys += key + ','
                                    elif key in money_fields:
                                        if type(row_val_new) == unicode:
                                            if u'合' in row_val_new or not row_val_new:
                                                break
                                            else:
                                                val += reader.get_money_field(row_val_new) + "','"
                                                field_keys += key + ','
                                        else:
                                            if row_val_new:
                                                val += reader.get_money_field(row_val_new) + "','"
                                                field_keys += key + ','
                                            else:
                                                break
                                    elif key in rq_fields:
                                        val += reader.get_date(table, j, position) + "','"
                                        field_keys += key + ','
                                    elif isinstance(row_val_new, float):
                                        val += str(int(row_val_new)) + "','"
                                        field_keys += key + ','
                                    elif key == 'qssz':
                                        if u'小计' in row_val_new or u'合计' in row_val_new:
                                            break
                                        else:
                                            val += row_val_new.strip().encode('utf8') + "','"
                                            field_keys += key + ','
                                    else:
                                        val += row_val_new.strip().encode('utf8') + "','"
                                        field_keys += key + ','
                                field_keys = field_keys[0:-1]
                                val = val[0:-2]
                                if 'qsje' not in field_keys:
                                    continue
                                elif 'ggrq' not in field_keys:
                                    field_keys += ',ggrq'
                                    val += ",'" + ggrq.encode('utf8') + "'"
                                sql = 'insert into taxplayer_sh_qjsw (' + field_keys + ') values (' + val + ')'
                                # sql = "insert into test_qsgg_sh (" + field_keys + ") values (" + val + ")"
                                data_nums = reader.data_to_mysql(sql, num, k1, n)
                                num = data_nums[0]
                                k1 = data_nums[1]
                                # print n + 1, sql
                                # break
                    except Exception as e:
                        print e
                        print n + 1, filepath, 'aaaaaaaa'
            except:
                print n + 1, filepath, 'bbbbbb'
            print str(n + 1) + u'表插入' + str(num) + u'条'
            print str(n + 1) + u'表重复' + str(k1) + u'条'
            k2 += k1
            all_num += num
        print u'一共插入' + str(all_num) + u'条'
        print u'一共失败' + str(k2) + u'条'


if __name__ == '__main__':
    reader = TaxplayerSHexcelreader()
    reader.excelread()
    # reader.read_fieldnames()
    # reader.field_collect()
