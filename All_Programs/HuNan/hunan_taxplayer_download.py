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
import threading
lock = threading.Lock()
# gevent.monkey.patch_all()


class HuNan(TaxConfig):
    def __init__(self):
        super(HuNan,self).__init__()
        self.session = None
        self.province = "湖南省"
        self.log_name = 'HuNan.log'
        self.path = self.get_savefile_directory('HuNan')
        self.regions = {'长沙市':'cs', '株洲市':'zz', '湘潭市':'xt', '岳阳市':'yy', '衡阳市':'hy', '常德市':'cd', '益阳市':'yy',
                      '邵阳市':'sy', '郴州市':'bz', '永州市':'yz', '娄底市':'ld', '张家界市':'zjj', '怀化市':'hh', '湘西自治州':'xx'}
        # self.xzqy_pys = ['cs', 'zz', 'xt', 'yy', 'hy', 'cd', 'yy', 'sy', 'cz', 'yz', 'ld', 'zjj', 'hh', 'xx']


    def log(self,message):
        self.log_base(self.log_name,message)

    #湖南省税务局欠税信息
    def qs_province(self):

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
            print(i)
            params = {
            'pagenum': i,
            'type': 1,
            'city_id': -1,
            }

            ans = requests.get('http://www.hntax.gov.cn/zhuanti/qsgg/article_list.jsp',params=params,headers=headers)
            # print(ans.content.decode('GBK'))
            res = BeautifulSoup(ans.text, 'html.parser')
            # print(res)
            table = res.find(attrs={'class':'clstbldata'})
            # print(table)
            trs = table.find_all('tr')
            last_update_time = time.strftime("%Y-%m-%d %H:%M:%S")
            # print(last_update_time)
            # print(len(trs))
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
                # print(nsrmc)

                sql = "insert into taxplayer_qsgg (province,nsrsbh,nsrmc,fddbr,qssz,qsje,dqsje,zjzl,zjhm,jydz,fbrq,last_update_time) values (" \
                      "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (self.province.encode('utf8'),nsrsbh.encode('utf8'),nsrmc.encode('utf8'), \
                      fddbrxm.encode('utf8'),qssz.encode('utf8'),qsye.encode('utf8'),dqqsje.encode('utf8'),zjlx.encode('utf8'), \
                      zjhm.encode('utf8'),jydd.encode('utf8'),fbsj.encode('utf8'),last_update_time.encode('utf8'))
                # print(sql)
                # self.cursor.execute(sql)
                # self.conn.commit()
                self.save_to_mysql(sql,3,0)

    #地级市欠税公告
    def qs_cities(self):
        for region,pinyin in self.regions.items():
            url_start = 'http://www.%sgtax.gov.cn/%sgtax/article_list_xxgk_fl.jsp?smallclassid=20091203033151' % (pinyin,pinyin)
            url_source = 'http://www.%sgtax.gov.cn/%sgtax/' % (pinyin,pinyin)
            url_host = 'http://www.%sgtax.gov.cn' % pinyin
            tag_list_before = []
            print('region: ',region)
            for p in range(10):
                self.last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')
                print('page: ',p)
                print('time',self.last_update_time)
                params = {
                    'pagenum': p,
                    'smallclassid': 20091203033151
                    }
                tag_list = self.get_tag_list(url_start,params=params)
                if not tag_list or tag_list_before == tag_list:
                    print(u'无详情页列表信息，爬虫结束')
                    break
                tag_list_before = tag_list
                tList = []
                for num,tag in enumerate(tag_list):
                    fbrq = tag.find('em').text.strip()
                    if not fbrq:
                        continue
                    if fbrq <= self.fbrq_stop:
                        self.stop_crawl = True
                        print(u'发布日期爬取到达设定最早日期')
                        break
                    print(tag.text)
                    # parse_tag = self.parse_detail(tag,url_host,url_source,fbrq,region)
                    t = threading.Thread(target=self.parse_detail, args=(tag,url_host,url_source,fbrq,region))
                    tList.append(t)
                for t in tList:
                    t.start()




    #地级市非正常户
    def abnormal_cities(self):

        for region,pinyin in self.regions.items():
            url_start = 'http://www.%sgtax.gov.cn/%sgtax/article_list_xxgk_fl.jsp' % (pinyin,pinyin)
            url_source = 'http://www.%sgtax.gov.cn/%sgtax/' % (pinyin,pinyin)
            url_host = 'http://www.%sgtax.gov.cn' % pinyin
            tag_list_before = []
            print('region: ',region)
            for p in range(10):
                self.last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')
                print('page: ',p)
                print('time',self.last_update_time)
                params = {
                    'pagenum': p,
                    'smallclassid': 20180629130174
                    }
                tag_list = self.get_tag_list(url_start,params=params)
                if not tag_list or tag_list_before == tag_list:
                    print(u'无详情页列表信息，爬虫结束')
                    break
                tag_list_before = tag_list
                taskList = []
                tList = []
                for num,tag in enumerate(tag_list):
                    fbrq = tag.find('em').text.strip()
                    if not fbrq:
                        continue
                    if fbrq <= self.fbrq_stop:
                        self.stop_crawl = True
                        print(u'发布日期爬取到达设定最早日期')
                        break
                    print(tag.text)
                    # parse_tag = self.parse_tag(tag,url_host,url_source,fbrq,region)
                    # taskList.append(gevent.spawn(parse_tag))
                    t = threading.Thread(target=self.parse_detail, args=(tag,url_host,url_source,fbrq,region))
                    tList.append(t)
                for t in tList:
                    t.start()
                    # t.join()
                # 通过协程处理每个详情页信息
                # gevent.joinall(taskList)
                # break
            # break

    #解析详情页
    def parse_detail(self, tag,url_host,url_source,fbrq,region):
        a_tag = tag.find('a')
        href = a_tag.get('href')
        url_detail = url_source + href
        print('url_detail: ',url_detail)
        html_filename = self.get_html_filename(url_detail)
        html_savepath = os.path.join(self.path,html_filename)
        title = a_tag.get('title')
        if '欠' in title or '缴' in title or '非正常户' in title or '失踪' in title:
            # url_detail = 'http://www.hhgtax.gov.cn/hhgtax/article_content_xxgk.jsp?id=20181106283800&smallclassid=20180629130174'

            r_inner = self.get(url_detail)
            if not r_inner:
                return
            charset1 = chardet.detect(r_inner.content)['encoding']
            # print(charset1)
            r_inner.encoding = 'gb18030'
            res_inner = BeautifulSoup(r_inner.text, 'html.parser')
            res_inner_str = str(res_inner)
            # print(res_inner_str)
            a_tag_inners = re.findall(r'<a.*?href=.*?</a>|<A.*?href=.*?</A>', res_inner_str)
            # print(a_tag_inners)
            href_inners = self.get_href(a_tag_inners)
            if href_inners:
                for href_inner in href_inners:
                    download_url = url_host + href_inner
                    # self.log('download_url: ' + download_url)
                    # print('download_url', download_url)
                    # filter_condition = self.check_download_url(download_url)
                    # if filter_condition:
                    filename = self.get_filename(download_url)
                    savepath = os.path.join(self.path, filename)
                    sql = "INSERT into taxplayer_filename VALUES('%s', '%s', '%s', '%s', " \
                          "'%s', '%s', '%s')" % (self.province, region, fbrq, title, filename,
                                                 download_url, self.last_update_time)
                    if os.path.isfile(savepath):
                        self.save_to_mysql(sql,self.log_name,lock=lock)
                    else:
                        # self.log('url_detail: ' + url_detail)
                        # self.log('download_url: ' + download_url)
                        print('download_url', download_url)
                        self.download_file(download_url, filename, savepath)
                        self.save_to_mysql(sql,self.log_name,lock=lock)
            else:
                sql = "INSERT into taxplayer_filename VALUES('%s', '%s', '%s', '%s', '%s', " \
                      "'%s', '%s')" % (self.province, region, fbrq, title, html_filename, url_detail,
                                       self.last_update_time)
                if os.path.isfile(html_savepath):

                    self.save_to_mysql(sql,self.log_name,lock=lock)
                else:
                    # self.log('url_detail_down_html: ' + url_detail)
                    print('url_detail_html ',url_detail)
                    with open(html_savepath, 'w',encoding='utf-8') as f:
                        # print(r_inner.content.decode('gbk'))
                        f.write(r_inner.content.decode(charset1,'ignore'))

                    self.save_to_mysql(sql,self.log_name,lock=lock)

    #解析列表页，返回tag列表
    def get_tag_list(self,url,params=None,headers=None):
        for t in range(5):
            try:
                r = self.get(url,params=params)
                # r1 = requests.get(url,params=params,headers=headers)
                # print(r1.content)
                # print(r.text)
                if not r:
                    return
                if r.status_code == 200:
                    r.encoding = 'gbk'
                    res = BeautifulSoup(r.text, 'html.parser')
                    box = res.find('ul',{'class':'txtcenm overf'})
                    tag_list = box.find_all('li')
                    # for i in tag_list:
                    #     print(i)
                    return tag_list
            except Exception as e:
                print(e)
                return None

    def ts(self):
        r = self.get('http://www.hhgtax.gov.cn/hhgtax/article_content_xxgk.jsp?id=20181106283800&smallclassid=20180629130174')
        print(r.content.decode('gbk'))

if __name__ == '__main__':
    hunan = HuNan()
    # hunan.qs_province()
    hunan.abnormal_cities()
    hunan.qs_cities()
    # hunan.ts()
