# coding=utf-8

import requests
from bs4 import BeautifulSoup
import sys
import os
from tax.config import TaxConfig
import time,datetime
import mysql.connector


class HuNan(TaxConfig):
    def __init__(self):
        super(HuNan,self).__init__()
        self.province = u"湖南省"
        # print self.conn

    def logger(self,log_name,contents):
        pass


    def main(self):


        headers = {
        'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Host': 'www.hntax.gov.cn',
        'Referer': 'http://www.hntax.gov.cn/zhuanti/qsgg/article_list.jsp?city_id=-1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36',
        # 'Cookie': '_gscu_887772128=43978900x0jor278; _gscbrs_887772128=1; UM_distinctid=1677c505c8c407-0a43ffd4fe983-35667607-1aeaa0-1677c505c8daa6; yfx_c_g_u_id_10003718=_ck18120511015611277569793762083; _gscu_2010918185=43979057f7qhjt53; _gscbrs_2010918185=1; CNZZDATA1273987317=845349678-1543974078-null%7C1544061654; yfx_f_l_v_t_10003718=f_t_1543978916120__r_t_1544065121800__v_t_1544065121800__r_c_1; yfx_mr_10003718=%3A%3Amarket_type_free_search%3A%3A%3A%3Abaidu%3A%3A%3A%3A%3A%3A%3A%3Awww.baidu.com%3A%3A%3A%3Apmf_from_free_search; yfx_mr_f_10003718=%3A%3Amarket_type_free_search%3A%3A%3A%3Abaidu%3A%3A%3A%3A%3A%3A%3A%3Awww.baidu.com%3A%3A%3A%3Apmf_from_free_search; yfx_key_10003718=; JSESSIONID=-PWBcJBTyz2mGbJ3zEiaUdF8DlsN7rNf1jpyT_olRTsRFugqZUG7!-1947031093; _gscs_887772128=t44065121eo2pqs17|pv:2; _gscs_2010918185=t44065131n43mnq53|pv:3'
        }
        for i in range(0,1316):
            print i
            params = {
            'pagenum': i,
            'type': 1,
            'city_id': -1,
            }

            ans = requests.get('http://www.hntax.gov.cn/zhuanti/qsgg/article_list.jsp?city_id=-1',params=params,headers=headers)
            # print ans.content.decode('GBK')
            res = BeautifulSoup(ans.text, 'html5lib')
            # print res
            table = res.find(attrs={'class':'clstbldata'})
            # print table
            trs = table.find_all('tr')
            last_update_time = time.strftime("%Y-%m-%d %H:%M:%S")
            # print last_update_time
            # print len(trs)
            for tr in trs[1:-1]:
                tds = tr.find_all('td')
                nsrmc = tds[0].text.strip()
                nsrsbh = tds[1].text.strip()
                fddbrxm = tds[2].text.strip()
                zjlx = tds[3].text.strip()
                zjhm = tds[4].text.strip()
                jydd = tds[5].text.strip()
                try:
                    qssz = tds[6].text.strip().split('|')[1]
                except:
                    qssz = tds[6].text.strip()
                qsye = tds[7].text.strip()
                dqqsje = tds[8].text.strip()
                fbsj = tds[9].text.strip()
                # print nsrmc

                # sql = "insert into taxplayer_qsgg (province,nsrsbh,nsrmc,fddbr,qssz,qsje,dqsje,zjzl,zjhm,jydz,fbrq,last_update_time) values (" \
                # "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" % self.province.encode('utf8'),nsrsbh.encode('utf8'),nsrmc.encode('utf8'),\
                #       fddbrxm.encode('utf8'),qssz.encode('utf8'),qsye.encode('utf8'),dqqsje.encode('utf8'),zjlx.encode('utf8'),\
                #       zjhm.encode('utf8'),jydd.encode('utf8'),fbsj.encode('utf8'),last_update_time.encode('utf8')
                sql = "insert into taxplayer_qsgg (province,nsrsbh,nsrmc,fddbr,qssz,qsje,dqsje,zjzl,zjhm,jydz,fbrq,last_update_time) values (" \
                      "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (self.province.encode('utf8'),nsrsbh.encode('utf8'),nsrmc.encode('utf8'), \
                      fddbrxm.encode('utf8'),qssz.encode('utf8'),qsye.encode('utf8'),dqqsje.encode('utf8'),zjlx.encode('utf8'), \
                      zjhm.encode('utf8'),jydd.encode('utf8'),fbsj.encode('utf8'),last_update_time.encode('utf8'))
                # print sql
                # self.cursor.execute(sql)
                # self.conn.commit()
                self.save_to_mysql(sql,3,0)
                # print sql

                # print tr
            # print trs
            #     break

    def save_to_mysql(self,sql, num_repeat,num_fail):
        """
        用来将数据插入到mysql数据库，并记录插入异常，另外返回重复次数。
        :param sql: insert 插入语句
        :param num_repeat: 插入语句执行重复条数
        :param nun_fail：执行sql失败条数
        """
        log_name = 'hunan_wrong_sqls.log'
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            data_nums = [num_repeat, num_fail]
            return data_nums
        except Exception as e:
            if e[0] == 2006:
                time.sleep(3)
                data_nums = self.save_to_mysql(sql, num_repeat, num_fail)
                return data_nums
            elif e[0] != 1062:
                num_fail += 1
                print sql, e
                self.logger(log_name, 'hunan')
                self.logger(log_name, str(e[0]))
                self.logger(log_name, sql)
            else:
                num_repeat += 1
            data_nums = [num_repeat, num_fail]
            return data_nums


if __name__ == '__main__':
    hunan = HuNan()
    hunan.main()