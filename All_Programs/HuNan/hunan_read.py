# coding=utf-8
# import PackageTool
from tax.taxplayer_reader import TaxplayerReader
import sys
import os
import time
import bs4
import re


class HuNanTaxplayerReader(TaxplayerReader):
    def __init__(self):
        super(HuNanTaxplayerReader, self).__init__()
        self.province = u'湖南省'
        self.province_py = 'HuNan'
        self.set_config()
        # self.today = "'%2017-12-06%'"

    def abnormal_excel_reader(self):
        self.log('abnormal_excel_reader')
        fields = list()
        sql = "SELECT * from taxplayer_filename where title like '%非正常户%' and filename like '%.xls%' " \
              "and province = '" + self.province + "' and last_update_time like " + self.today
        self.log(sql)
        info = self.get_province_info(sql)
        num_sql_all, num_repetition_all, num_fail_all = 0, 0, 0
        for num_info in range(0, len(info)):
            num_sql, num_repetition, num_fail = 0, 0, 0
            region = info[num_info][1]
            fbrq = info[num_info][2]
            filepath = os.path.join(self.path,info[num_info][4])
            try:
                excel = self.get_excel(filepath)
                sheets = excel.sheets()
                count = len(sheets)  # sheet数量
                for m in range(count):
                    try:
                    # if True:
                        table = excel.sheets()[m]
                        rows = table.nrows
                        match_fields = self.get_excel_abnormal_field_info(table, rows, fields)
                    except Exception as e:
                        self.log(str(num_info+1) + 'filepath  ' + filepath)
                        self.log("读取非正常户excel字段信息错误（get_excel_abnormal_field_info):e:"  + str(e))
                        continue
                    if match_fields:
                        self.log(str(num_info+1)+str(match_fields))
                        if len(match_fields) <= 1:
                            continue
                        start_idx = self.get_excel_start_idx(table, rows)
                        int_fields = 'nsrsbh,zjhm,sfzhm'
                        rq_fields = 'rdrq,fbrq,djrq,dsrdsj'
                        for j in range(start_idx, rows):
                            row_vals = table.row_values(j)
                            field_keys = 'province,region,last_update_time,'
                            val = "'" + self.province + "','" + region + "','" + self.last_update_time + "','"
                            for md in range(len(match_fields)):
                                key = list(match_fields[md].keys())[0]
                                position = list(match_fields[md].values())[0]
                                if key in int_fields:
                                    val += self.get_int_field(row_vals[position]) + "','"
                                    field_keys += key + ','
                                elif key in rq_fields:
                                    val += self.get_formate_date(self.get_date(table, j, position)) + "','"
                                    field_keys += key + ','
                                elif key == 'nsrmc':
                                    if row_vals[position].strip():
                                        val += row_vals[position] + "','"
                                        field_keys += key + ','
                                elif isinstance(row_vals[position], float):
                                    val += str(int(row_vals[position])) + "','"
                                    field_keys += key + ','
                                else:
                                    val += row_vals[position] + "','"
                                    field_keys += key + ','
                            field_keys = field_keys[0:-1]
                            val = val[0:-2]
                            if 'nsrmc' not in field_keys:
                                continue
                            if 'rdrq' not in field_keys:
                                field_keys += ',rdrq'
                                val += ",'" + fbrq + "'"
                            if 'fbrq' not in field_keys:
                                field_keys += ',fbrq'
                                val += ",'" + fbrq + "'"
                            if self.db_table:
                                sql = 'insert into test_abnormal (' + field_keys + ') values (' + val + ')'
                            else:
                                sql = 'insert into taxplayer_abnormal (' + field_keys + ') values (' + val + ')'
                            data_nums = self.data_to_mysql(sql, num_repetition, num_info, num_fail)
                            num_sql += 1
                            num_repetition = data_nums[0]
                            num_fail = data_nums[1]
                            if self.db_table:
                                self.log(str(num_info + 1) + sql)
                                break
            except Exception as e:
                if '<html' in str(e.args[0]) or '<!DOCT' in str(e.args[0]):
                    try:
                    # if True:
                        soup = self.get_soup(filepath)
                        tr_list, inner_signal = self.get_tr_list(soup)
                        if tr_list:
                            match_fields = self.get_html_abnormal_field_info(tr_list, fields)
                            new_tr_list = tr_list[1:]
                            if match_fields:
                                self.log(str(num_info+1) + ' ' + str(match_fields))
                                if len(match_fields) <= 1:
                                    continue
                                for j in range(len(new_tr_list)):
                                    td = new_tr_list[j].findAll('td')
                                    field_keys = 'province,region,fbrq,last_update_time,'
                                    rq_fields = 'rdrq, djrq'
                                    val = "'" + self.province + "','" + region + "','" \
                                          + fbrq + "','" + self.last_update_time + "','"
                                    try:
                                        for md in range(len(match_fields)):
                                            key = list(match_fields[md].keys())[0]
                                            position = list(match_fields[md].values())[0]
                                            if key in rq_fields:
                                                val += self.get_formate_date(
                                                    td[position].text.strip()) + "','"
                                                field_keys += key + ','
                                            elif key == 'nsrsbh':
                                                nsrsbh = td[position].text.strip().replace("'", '')
                                                val += nsrsbh + "','"
                                                field_keys += key + ','
                                            elif key == 'nsrmc':
                                                if td[position].text.strip():
                                                    nsrmc = td[position].text.strip().replace("'",
                                                                                                             '')
                                                    val += nsrmc + "','"
                                                    field_keys += key + ','
                                            else:
                                                val += td[position].text.strip() + "','"
                                                field_keys += key + ','
                                    except Exception as e:
                                        self.log(str(num_info+1) + ' ' + str(e))
                                    field_keys = field_keys[0:-1]
                                    val = val[0:-2]
                                    if 'nsrmc' not in field_keys:
                                        continue
                                    if 'rdrq' not in field_keys:
                                        field_keys += ',rdrq'
                                        val += ",'" + fbrq + "'"
                                    if self.db_table:
                                        sql = 'insert into test_abnormal (' + field_keys + ') values (' + val + ')'
                                    else:
                                        sql = 'insert into taxplayer_abnormal (' + field_keys + ') values (' + val + ')'
                                    data_nums = self.data_to_mysql(sql, num_repetition, num_info, num_fail)
                                    num_sql += 1
                                    num_repetition = data_nums[0]
                                    num_fail = data_nums[1]
                                    if self.db_table:
                                        self.log(str(num_info + 1) + sql)
                                        break
                    except Exception as e:
                        self.log(str(num_info + 1) + ',' + 'filepath  ' + filepath)
                        self.log('将非正常户excel文件作为html读取不成功,e:  ' + str(e))
                else:
                    self.log(str(num_info + 1) + ',' + filepath + ',' + '非正常户excel读取失败')
                    self.log('非正常户excel读取失败' + 'e: ' + str(e))
            self.print_chart(num_info,num_sql,num_repetition,num_fail)
            num_sql_all += num_sql
            num_repetition_all += num_repetition
            num_fail_all += num_fail
        self.log_province(self.province,num_sql_all,num_repetition_all,num_fail_all)

    def abnormal_html_reader(self):
        self.log('abnormal_html_reader')
        fields = list()
        sql = "SELECT * from taxplayer_filename where title like '%非正常户%' and (filename like '%.doc%' " \
              "or filename like '%.%htm%') and province = '" + self.province + \
              "' and last_update_time like " + self.today
        self.log(sql)
        info = self.get_province_info(sql)
        num_sql_all, num_repetition_all, num_fail_all = 0, 0, 0
        for num_info in range(0, len(info)):
            num_sql, num_repetition, num_fail = 0, 0, 0
            region = info[num_info][1]
            fbrq = info[num_info][2]
            decode_way = self.get_decode_way(info[num_info][4])
            filepath = self.get_filepath(info[num_info][4], self.path)
            soup = self.get_soup(filepath, decode_way)
            tr_list, inner_signal = self.get_tr_list(soup)
            if tr_list:
                match_fields = self.get_html_abnormal_field_info(tr_list, fields)
                new_tr_list = tr_list[1:]
                if match_fields:
                    self.log(str(num_info+1) + ' ' + str(match_fields))
                    if len(match_fields) <= 1:
                        continue
                    for j in range(len(new_tr_list)):
                        td = new_tr_list[j].findAll('td')
                        field_keys = 'province,region,fbrq,last_update_time,'
                        rq_fields = 'rdrq, djrq'
                        val = "'" + self.province + "','" + region + "','" \
                              + fbrq + "','" + self.last_update_time + "','"
                        try:
                            for md in range(len(match_fields)):
                                key = list(match_fields[md].keys())[0]
                                position = list(match_fields[md].values())[0]
                                if key in rq_fields:
                                    val += self.get_formate_date(td[position].text.strip()) + "','"
                                    field_keys += key + ','
                                elif key == 'nsrsbh':
                                    nsrsbh = td[position].text.strip().replace("'", '')
                                    val += nsrsbh + "','"
                                    field_keys += key + ','
                                elif key == 'nsrmc':
                                    if td[position].text.strip():
                                        nsrmc = td[position].text.strip().replace("'", '')
                                        val += nsrmc + "','"
                                        field_keys += key + ','
                                else:
                                    val += td[position].text.strip() + "','"
                                    field_keys += key + ','
                            field_keys = field_keys[0:-1]
                            val = val[0:-2]
                        except Exception as e:
                            self.log(str(num_info+1) + ' ' + str(e))
                        if 'nsrmc' not in field_keys:
                            continue
                        if 'rdrq' not in field_keys:
                            field_keys += ',rdrq'
                            val += ",'" + fbrq + "'"
                        if self.db_table:
                            sql = 'insert into test_abnormal (' + field_keys + ') values (' + val + ')'
                        else:
                            sql = 'insert into taxplayer_abnormal (' + field_keys + ') values (' + val + ')'
                        data_nums = self.data_to_mysql(sql, num_repetition, num_info, num_fail)
                        num_sql += 1
                        num_repetition = data_nums[0]
                        num_fail = data_nums[1]
                        if self.db_table:
                            self.log(str(num_info + 1) + sql)
                            break
                self.print_chart(num_info,num_sql,num_repetition,num_fail)
                num_sql_all += num_sql
                num_repetition_all += num_repetition
                num_fail_all += num_fail
        self.log_province(self.province,num_sql_all,num_repetition_all,num_fail_all)

    def qsgg_excel_reader(self):
        self.log('qsgg_excel_reader')
        fields = list()
        sql = "SELECT * from taxplayer_filename where (title like '%欠税%' or title like '%缴%')" \
              " and filename like '%.xls%' and province = '" + self.province + \
              "' and last_update_time like " + self.today
        self.log(sql)
        info = self.get_province_info(sql)
        num_sql_all, num_repetition_all, num_fail_all = 0, 0, 0
        for num_info in range(0, len(info)):
            num_sql, num_repetition, num_fail = 0, 0, 0
            region = info[num_info][1]
            fbrq = info[num_info][2]
            if '.xls' in info[num_info][4]:
                filepath = os.path.join(self.path,info[num_info][4])
            else:
                filepath = os.path.join(self.path,info[num_info][4].split('.')[0] + '.xls')
            # if True:
            try:
                excel = self.get_excel(filepath)
                sheets = excel.sheets()
                count = len(sheets)  # sheet数量
                for m in range(count):
                    # try:
                    if True:
                        table = excel.sheets()[m]
                        rows = table.nrows
                        match_fields, wan = self.get_excel_qsgg_field_info(table, rows, fields)
                        print(match_fields,wan)
                        # special = [25, 26, 45, 56, 57]
                        # if n + 1 in special:
                        #     match_fields = [{'nsrmc': 0}, {'nsrsbh': 1}, {'fddbr': 2}, {'jydz': 3}, {'qssz': 4},
                        #                     {'qsje': 6}]
                    # except Exception as e:
                    #     if str(e.args[0]) == 'need more than 0 values to unpack':
                    #         continue
                    #     else:
                    #         self.log(str(num_info+1) + 'filepath  ' + filepath)
                    #         self.log(u"读欠税公告excel字段信息错误（get_excel_qsgg_field_info):e:"  + str(e))
                    #         continue
                    if match_fields:
                        self.log(str(num_info+1) + ' ' + str(match_fields))
                        if len(match_fields) < 3:
                            continue
                        keys = [list(match_field.keys())[0] for match_field in match_fields]
                        cqsje_condition = 'dqsje' in keys and 'cqsje' in keys
                        if 'nsrsbh' not in keys or 'nsrmc' not in keys:
                            self.log('nsrsbh or nsrmc not in keys' + str(num_info+1) + info[num_info][4])
                        if 'qsje' not in keys and not cqsje_condition:
                            self.log(str(num_info + 1) + '  qsje not in keys and not cqsje_condition  ' + info[num_info][4])
                            continue
                        self.log(str(num_info + 1) + '  qsje not in keys and not cqsje_condition  ' + info[num_info][4])
                        start_idx = self.get_excel_start_idx(table, rows)
                        merge_cells = self.get_merge_cells(start_idx, table)
                        int_fields = 'nsrsbh,zjhm,sfzhm'
                        money_fields = 'cqsje,qsje,dqsje'
                        rq_fields = 'xjrq, ssqs, ssqz'
                        keep_fields = 'cqsje,qsje,dqsje,qssz'
                        for j in range(start_idx, rows):
                            row_vals = table.row_values(j)
                            first_position = list(match_fields[0].values())[0]
                            if type(row_vals[first_position]) == float:
                                first_col_val = row_vals[first_position]
                            else:
                                first_col_val = row_vals[first_position].strip()
                            field_keys = 'province,last_update_time,'
                            keys = []
                            val = "'" + self.province + "','" + self.last_update_time + "','"
                            for md in range(len(match_fields)):
                                key = list(match_fields[md].keys())[0]
                                position = list(match_fields[md].values())[0]
                                row_val = row_vals[position]
                                try:
                                    if key not in keep_fields and (merge_cells or not first_col_val):
                                        row_val_new = self.get_field_val(row_val, j, table, merge_cells, position)
                                    else:
                                        row_val_new = row_val
                                except:
                                    row_val_new = row_val
                                # continue
                                # if type(row_val_new) == unicode:
                                #     row_val_new = row_val_new.strip().replace("'", '')
                                if key in int_fields:
                                    val += self.get_int_field(row_val_new) + "','"
                                    field_keys += key + ','
                                    keys.append(key)
                                elif key in money_fields:
                                    qsje_condition = key == 'qsje' and not row_val_new
                                    # if type(row_val_new) == unicode:
                                    #     if (not re.findall(u'\d+', row_val_new) and row_val_new != u'') \
                                    #             or qsje_condition:
                                    #         break
                                    #     else:
                                    #         val += self.get_money_field(row_val_new, wan) + "','"
                                    #         field_keys += key + ','
                                    #         keys.append(key)
                                    # else:
                                    if row_val_new or key != 'qsje' or row_val_new == 0:
                                        val += self.get_money_field(row_val_new, wan) + "','"
                                        field_keys += key + ','
                                        keys.append(key)
                                    else:
                                        break
                                elif key in rq_fields:
                                    val += self.get_date(table, j, position) + "','"
                                    field_keys += key + ','
                                    keys.append(key)
                                elif isinstance(row_val_new, float):
                                    val += str(int(row_val_new)) + "','"
                                    field_keys += key + ','
                                    keys.append(key)
                                elif key == 'qssz':
                                    if u'小计' in row_val_new or u'合计' in row_val_new or u'总和' in row_val_new:
                                        break
                                    else:
                                        val += row_val_new.strip() + "','"
                                        field_keys += key + ','
                                        keys.append(key)
                                else:
                                    val += row_val_new.strip() + "','"
                                    field_keys += key + ','
                                    keys.append(key)

                            field_keys = field_keys[0:-1]
                            val = val[0:-2]
                            # print(val)
                            cqsje_condition = 'dqsje' in keys and 'cqsje' in keys
                            if 'qsje' not in keys and not cqsje_condition:
                                continue
                            if 'fbrq' not in field_keys:
                                field_keys += ',fbrq'
                                val += ",'" + fbrq + "'"

                            if self.db_table:
                                sql = 'insert into test_qsgg (' + field_keys + ') values (' + val + ')'
                            else:
                                sql = 'insert into taxplayer_qsgg (' + field_keys + ') values (' + val + ')'
                            data_nums = self.data_to_mysql(sql, num_repetition, num_info, num_fail)
                            num_sql += 1
                            num_repetition = data_nums[0]
                            num_fail = data_nums[1]
                            if self.db_table:
                                self.log(str(num_info + 1) + sql)
                                break
            except Exception as e:
                print(e)
                if '<html' in str(e.args[0]) or '<!DOCT' in str(e.args[0]):
                    try:
                        soup = self.get_soup(filepath)
                        dw = self.get_money_dw_html(soup)
                        tr_list, inner_signal = self.get_tr_list(soup)
                        if tr_list:
                            match_fields, wan = self.get_html_qsgg_field_info(tr_list, fields)
                            wan = wan or dw
                            new_tr_list = tr_list[1:]
                            if match_fields:
                                self.log(str(num_info+1) + ' ' + str(match_fields) + str(wan))
                                if len(match_fields) < 3:
                                    continue
                                keys = [list(match_field.keys())[0] for match_field in match_fields]
                                cqsje_condition = 'dqsje' in keys and 'cqsje' in keys
                                if 'nsrsbh' not in keys or 'nsrmc' not in keys:
                                    self.log(str(num_info+1) + '  nsrsbh or nsrmc not in keys  ' + info[num_info][4])
                                if not cqsje_condition and 'qsje' not in keys:
                                    self.log(str(num_info + 1) + '  qsje not in keys and mot cqsje_condition  ' + \
                                             info[num_info][4])
                                    continue
                                hb_tr = ''
                                for j in range(len(new_tr_list)):
                                    if inner_signal:
                                        tds = new_tr_list[j].children
                                        td_texts = [td.text.strip() for td in tds if isinstance(td, bs4.element.Tag)]
                                    else:
                                        tds = new_tr_list[j].findAll('td')
                                        td_texts = [td.text.strip() for td in tds]
                                    field_keys = 'province,fbrq,last_update_time,'
                                    money_fields = 'cqsje, qsje, dqsje'
                                    rq_fields = 'xjrq,ssqs,ssqz'
                                    hb_fields = 'nsrsbh, nsrmc, nsrzk, fddbr, zjzl, zjhm, jydz'
                                    vals = "'" + self.province + "','" + fbrq + "','" \
                                           + self.last_update_time + "','"
                                    try:
                                        keys = []
                                        if len(td_texts) >= len(match_fields):
                                            hb_tr = new_tr_list[j]
                                            for md in range(len(match_fields)):
                                                key = list(match_fields[md].keys())[0]
                                                position = list(match_fields[md].values())[0]
                                                if key in money_fields:
                                                    temp_val = td_texts[position]
                                                    qsje_condition = key == 'qsje' and not temp_val
                                                    if (not re.findall(u'\d+', temp_val) and temp_val != u'') \
                                                            or qsje_condition:
                                                        break
                                                    val = temp_val
                                                    val = self.get_money_field(val, wan)
                                                    field_keys += key + ','
                                                    keys.append(key)
                                                elif key in rq_fields:
                                                    val = td_texts[position]
                                                    val = self.get_formate_date(val)
                                                    field_keys += key + ','
                                                    keys.append(key)
                                                else:
                                                    val = self.get_hb_cell_val(j, new_tr_list, position, inner_signal)
                                                    field_keys += key + ','
                                                    keys.append(key)
                                                vals += val + "','"
                                            field_keys = field_keys[:-1]
                                            vals = vals[:-2]
                                        else:
                                            position_new = -1
                                            for md in range(len(match_fields)):
                                                key = list(match_fields[md].keys())[0]
                                                position = list(match_fields[md].values())[0]
                                                if key in hb_fields:
                                                    hb_tds = hb_tr.findAll('td')
                                                    val = hb_tds[position].text.strip()
                                                    field_keys += key + ','
                                                    keys.append(key)
                                                else:
                                                    tds = new_tr_list[j].findAll('td')
                                                    position_new += 1
                                                    val = tds[position_new].text.strip()
                                                    if key in money_fields:
                                                        qsje_condition = key == 'qsje' and not val
                                                        if (not re.findall('\d+', val) and val != '') \
                                                                or qsje_condition:
                                                            break
                                                        val = self.get_money_field(val, wan)
                                                        field_keys += key + ','
                                                        keys.append(key)
                                                    elif key in rq_fields:
                                                        val = self.get_formate_date(val)
                                                        field_keys += key + ','
                                                        keys.append(key)
                                                    else:
                                                        field_keys += key + ','
                                                        keys.append(key)
                                                vals += val + "','"
                                            field_keys = field_keys[:-1]
                                            vals = vals[:-2]
                                    except Exception as e:
                                        field_keys = field_keys[:-1]
                                        vals = vals[:-2]
                                        self.log(str(num_info+1) + str(e))
                                    cqsje_condition = 'dqsje' in keys and 'cqsje' in keys
                                    if not cqsje_condition and 'qsje' not in keys:
                                        continue
                                    if self.db_table:
                                        sql = 'insert into test_qsgg (' + field_keys + ') values (' + vals + ')'
                                    else:
                                        sql = 'insert into taxplayer_qsgg (' + field_keys + ') values (' + vals + ')'
                                    data_nums = self.data_to_mysql(sql, num_repetition, num_info, num_fail)
                                    num_sql += 1
                                    num_repetition = data_nums[0]
                                    num_fail = data_nums[1]
                                    if self.db_table:
                                        self.log(str(num_info + 1) + sql)
                                        break
                    except Exception as e:
                        self.log(str(num_info + 1) + ',' + 'filepath  ' + filepath)
                        self.log('将欠税公告excel文件作为html读取不成功,e:  ' + str(e))
                else:
                    # raise e
                    self.log(str(num_info + 1) + ',' + filepath + ',' + '欠税公告excel读取失败')
                    self.log('欠税公告excel读取失败' + 'e: ' + str(e))
            self.print_chart(num_info,num_sql,num_repetition,num_fail)
            num_sql_all += num_sql
            num_repetition_all += num_repetition
            num_fail_all += num_fail
        self.log_province(self.province,num_sql_all,num_repetition_all,num_fail_all)

    def qsgg_html_reader(self):
        self.log('qsgg_html_reader')
        fields = list()
        sql = "SELECT * from taxplayer_filename where (title like '%欠税%' or title like '%缴%')" \
              " and (filename like '%.doc%' or filename like '%.%htm%') and province = '" \
              + self.province + "' and last_update_time like " + self.today
        self.log(sql)
        info = self.get_province_info(sql)
        num_sql_all, num_repetition_all, num_fail_all = 0, 0, 0
        for num_info in range(0, len(info)):
            num_sql, num_repetition, num_fail = 0, 0, 0
            fbrq = info[num_info][2]
            decode_way = self.get_decode_way(info[num_info][4])
            filepath = self.get_filepath(info[num_info][4], self.path)
            soup = self.get_soup(filepath, decode_way)
            dw = self.get_money_dw_html(soup)
            tr_list, inner_signal = self.get_tr_list(soup)
            if tr_list:
                match_fields, wan = self.get_html_qsgg_field_info(tr_list, fields)
                new_tr_list = tr_list[1:]
                wan = wan or dw
                if match_fields:
                    self.log(str(num_info+1) + ' ' + str(match_fields) + str(wan))
                    if len(match_fields) < 3:
                        continue
                    keys = [list(match_field.keys())[0] for match_field in match_fields]
                    cqsje_condition = 'dqsje' in keys and 'cqsje' in keys
                    if 'nsrsbh' not in keys or 'nsrmc' not in keys:
                        self.log(str(num_info+1) + '  nsrsbh not in keys or nsrmc not in keys  ' + info[num_info][4])
                    if not cqsje_condition and 'qsje' not in keys:
                        self.log(str(num_info+1) + '  cqjse_condition and qsje not in keys  ' + info[num_info][4])
                        continue
                    hb_tr = ''
                    for j in range(len(new_tr_list)):
                        if inner_signal:
                            tds = new_tr_list[j].children
                            td_texts = [td.text.strip() for td in tds if isinstance(td, bs4.element.Tag)]
                        else:
                            tds = new_tr_list[j].findAll('td')
                            td_texts = [td.text.strip() for td in tds]
                        field_keys = 'province,fbrq,last_update_time,'
                        money_fields = 'cqsje, qsje, dqsje'
                        rq_fields = 'xjrq,ssqs,ssqz'
                        hb_fields = 'nsrsbh, nsrmc, nsrzk, fddbr, zjzl, zjhm, jydz'
                        vals = "'" + self.province + "','" + fbrq + "','" \
                               + self.last_update_time + "','"
                        try:
                            keys = []
                            if len(td_texts) >= len(match_fields):
                                hb_tr = new_tr_list[j]
                                for md in range(len(match_fields)):
                                    key = list(match_fields[md].keys())[0]
                                    position = list(match_fields[md].values())[0]
                                    if key in money_fields:
                                        temp_val = td_texts[position]
                                        qsje_condition = key == 'qsje' and not temp_val
                                        if (not re.findall(u'\d+', temp_val) and temp_val != u'') or qsje_condition:
                                            break
                                        val = temp_val
                                        val = self.get_money_field(val, wan)
                                        field_keys += key + ','
                                        keys.append(key)
                                    elif key in rq_fields:
                                        val = td_texts[position]
                                        val = self.get_formate_date(val)
                                        field_keys += key + ','
                                        keys.append(key)
                                    else:
                                        val = self.get_hb_cell_val(j, new_tr_list, position, inner_signal)
                                        field_keys += key + ','
                                        keys.append(key)
                                    vals += val + "','"
                                field_keys = field_keys[:-1]
                                vals = vals[:-2]
                            else:
                                position_new = -1
                                for md in range(len(match_fields)):
                                    key = list(match_fields[md].keys())[0]
                                    position = list(match_fields[md].values())[0]
                                    if key in hb_fields:
                                        hb_tds = hb_tr.findAll('td')
                                        val = hb_tds[position].text.strip()
                                        field_keys += key + ','
                                        keys.append(key)
                                    else:
                                        tds = new_tr_list[j].findAll('td')
                                        position_new += 1
                                        val = tds[position_new].text.strip()
                                        if key in money_fields:
                                            qsje_condition = key == 'qsje' and not val
                                            if (not re.findall('\d+', val) and val != '') or qsje_condition:
                                                break
                                            val = self.get_money_field(val, wan)
                                            field_keys += key + ','
                                            keys.append(key)
                                        elif key in rq_fields:
                                            val = self.get_formate_date(val)
                                            field_keys += key + ','
                                            keys.append(key)
                                        else:
                                            field_keys += key + ','
                                            keys.append(key)
                                    vals += val + "','"
                                field_keys = field_keys[:-1]
                                vals = vals[:-2]
                        except Exception as e:
                            field_keys = field_keys[:-1]
                            vals = vals[:-2]
                            self.log(str(num_info+1) + ' ' + str(e))
                        cqsje_condition = 'dqsje' in keys and 'cqsje' in keys
                        if not cqsje_condition and 'qsje' not in keys:
                            continue
                        if self.db_table:
                            sql = 'insert into test_qsgg (' + field_keys + ') values (' + vals + ')'
                        else:
                            sql = 'insert into taxplayer_qsgg (' + field_keys + ') values (' + vals + ')'
                        data_nums = self.data_to_mysql(sql, num_repetition, num_info, num_fail)
                        num_sql += 1
                        num_repetition = data_nums[0]
                        num_fail = data_nums[1]
                        if self.db_table:
                            self.log(str(num_info + 1) + sql)
                            break
                else:
                    self.log(str(num_info + 1) + u'没有match_fields')
            else:
                self.log(str(num_info + 1) + u'没有tr_list')
            self.print_chart(num_info,num_sql,num_repetition,num_fail)
            num_sql_all += num_sql
            num_repetition_all += num_repetition
            num_fail_all += num_fail
        self.log_province(self.province,num_sql_all,num_repetition_all,num_fail_all)


if __name__ == '__main__':
    reader = HuNanTaxplayerReader()
    # reader.word_to_html()
    # reader.get_abnormal_excel_fieldnames()
    # reader.get_abnormal_html_fieldnames()
    # reader.get_qsgg_excel_fieldnames()
    # reader.get_qsgg_html_fieldnames()
    # reader.abnormal_excel_reader()
    # reader.abnormal_html_reader()
    # reader.qsgg_excel_reader()
    reader.qsgg_html_reader()
