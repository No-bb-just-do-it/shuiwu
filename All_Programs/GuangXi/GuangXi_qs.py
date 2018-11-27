# coding=utf-8
import PackageTool
import datetime
import os
import re
import sys
import threading

import MySQLdb
from bs4 import BeautifulSoup

# import MSSQL
from tax.SpiderMan_FYH import SpiderMan


reload(sys)
sys.setdefaultencoding('utf8')


class GuangXiSearcher(SpiderMan):
    # DateBegin = None
    def __init__(self):
        super(GuangXiSearcher, self).__init__(keep_session=True)

    def get_savefile_directory(self, province_py):
        grader_father = os.path.abspath(os.path.dirname(sys.path[0])+os.path.sep+"..")
        # parent_dir = os.path.join(os.path.dirname(__file__), '../All_Files')
        parent_dir = os.path.join(grader_father, './All_Files')
        save_directory = os.path.join(parent_dir, province_py)
        self.get_directory(save_directory)
        return save_directory + '\\'

    def get_directory(self, path):
        if not os.path.exists(path):
            os.makedirs(path)

    def set_config(self):
        self.DateBegin = datetime.datetime.now().strftime("%Y-%m-%d")
        self.start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.province = u'广西壮族自治区'
        province_py = 'Guang_Xi'
        self.path = self.get_savefile_directory(province_py)
        self.conn = MySQLdb.connect(host='172.16.0.76', port=3306, user='guanhuaixuan', passwd='xuanhuaiguan', db='taxplayer',
                         charset='utf8')
        self.cursor = self.conn.cursor()
        # self.logf = open("D:/taxplayer_common/All_Programs/GuangXi/log/%s.txt" % (str(self.DateBegin)), 'a')
        # self.logf = open("E:/workspace/Court_Says/logs/%s.txt" % (str(self.DateBegin)), 'a')
        # self.logf.write(str(self.start_time) +u'开始爬虫'+ '\n')
        nn = 'http://nn.gxds.gov.cn/id_tzgg201506161000254825/page_2/column.shtml'
        gl = 'http://gl.gxds.gov.cn/id_tzgg201506160925227208/page_2/column.shtml'
        bh = 'http://bh.gxds.gov.cn/id_tzgg201506161749430591/page_2/column.shtml'
        qz = 'http://qz.gxds.gov.cn/id_tzgg201506161740052095/page_2/column.shtml'
        lb = 'http://lb.gxds.gov.cn/id_tzgg201506161042429354/page_2/column.shtml'
        cz = 'http://cz.gxds.gov.cn/id_tzgg201506160946282693/page_2/column.shtml'
        m_list = []
        m1 = threading.Thread(target=self.guangxi)
        m_list.append(m1)
        m2 = threading.Thread(target=self.liuzhou)
        m_list.append(m2)
        m3 = threading.Thread(target=self.dishui)
        m_list.append(m3)
        m4 = threading.Thread(target=self.difang,args=(nn,))
        m_list.append(m4)
        m5 = threading.Thread(target=self.difang,args=(gl,))
        m_list.append(m5)
        m6 = threading.Thread(target=self.difang,args=(bh,))
        m_list.append(m6)
        m7 = threading.Thread(target=self.difang,args=(qz,))
        m_list.append(m7)
        m8 = threading.Thread(target=self.difang,args=(lb,))
        m_list.append(m8)
        m9 = threading.Thread(target=self.difang,args=(cz,))
        m_list.append(m9)
        for m in m_list:
            m.setDaemon(True)
            m.start()
        for m in m_list:
            m.join()

    def guangxi(self):
        name = u'广西壮族自治区国税'
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:45.0) Gecko/20100101 Firefox/45.0'}
        params ={
            'appid': 1, 'col': 1, 'columnid': 99646, 'path': '/', 'permissiontype': 0, 'unitid': 106664, 'webid':17,\
            'webname': u'广西壮族自治区国家税务局'
        }
        host = 'http://www.gxgs.gov.cn'
        for p in range(38):
            sr, er = str(p*15+1), str((p+1)*15)
            url = 'http://www.gxgs.gov.cn/module/jslib/jquery/jpage/dataproxy.jsp?startrecord='+sr+'&endrecord='+er+\
                '&perpage=15'
            for i in range(3):
                try:
                    r = self.post(url=url, data=params, headers=headers)
                    if r.status_code == 200:
                        break
                except:
                    continue
            # print r.text
            soup = BeautifulSoup(r.text, 'html5lib')
            tr_list = soup.find_all('table')
            for i,item in enumerate(tr_list):
                # print item
                # title = item.find('a', target='_blank').get('title').strip().replace('\n','').replace(' ','')
                title = item.find('a', target='_blank').text.strip().replace('\n','').replace(' ','')
                fbrq = item.find_all('td')[-1].text.strip().split('[')[1].split(']')[0]
                print u'第%d页%d条'%(p+1, i), title, fbrq
                if u'欠税' in title  or u'非正常户' in title:
                    # html_herf = item.a['href'].split("\'")[1].split("\\")[0]
                    # print item.find('a', target='_blank').get('href')
                    html_herf = item.find('a', target='_blank').get('href').split("\\'")[1]
                    fbrq = item.find_all('td')[-1].text.split('[')[1].split(']')[0]
                    html_id = os.path.split(html_herf)[1]
                    print u'第%d页%d条'%(p+1, i), 'html_id:', html_id
                    link = host + html_herf
                    # print u'第%d页%d条'%(p+1, i), 'html_herf:', html_herf
                    # print u'第%d页%d条'%(p+1, i), 'link',link,html_id
                    r = self.get(url=link, headers=headers)
                    r.encoding = 'utf8'
                    soup = BeautifulSoup(r.text, 'html5lib')
                    for i in range(3):
                        try:
                            link_1 = soup.find('div', id='info_id').find('a')['href']
                            i_d = re.findall(r'\d{9,}',link_1)[0]
                            if link_1:
                                break
                        except:
                            continue
                    # print '_link1_',link_1
                    if '.xls' in r.text or '.doc' in r.text:
                        # xls_path = os.path.join(sys.path[0], './File/' + i_d + '.xls')
                        # word_path = os.path.join(sys.path[0], './File/' + i_d + '.doc')
                        xls_path = self.path + i_d + '.xls'
                        word_path = self.path + i_d + '.doc'
                        print 'xls_path',xls_path
                        xls_name = i_d + '.xls'
                        xls_url = host+link_1
                        # print 'xls_url',xls_url
                        for i in range(3):
                            try:
                                r_2 = self.get(url=xls_url, headers=headers)
                                if r.status_code == 200:
                                    break
                            except:
                                continue
                        # print 'r_2.content', r_2.content
                        if 'xls' in link_1:
                            with open(xls_path, 'wb') as f:
                                f.write(r_2.content)
                                self.logf.write(u'第%d页%d条'%(p, i)+xls_path+ '\n')
                        elif 'doc' in link_1:
                            with open(word_path, 'wb') as f:
                                f.write(r_2.content)
                                self.logf.write(u'第%d页%d条'%(p, i)+word_path+ '\n')
                        sql = "insert into taxplayer_filename VALUES('%s','%s','%s','%s','%s','%s','%s')"\
                             %(self.province,name,fbrq,title,xls_name,xls_url,self.start_time)
                        try:
                            self.cursor.execute(sql)
                            self.conn.commit()
                        except:
                            print u'数据库已有该数据'
                    elif u'法定代表人' in r.text or u'纳税人识别号' in r.text:
                        # path = os.path.join(sys.path[0], './File/' + html_id)
                        path = self.path + html_id
                        # path = 'E:\workspace\Court_Says\ShuiWu\File'
                        print 'path',path
                        with open(path, 'wb') as f:
                            f.write(r.content)
                            self.logf.write(u'第%d页%d条'%(p, i)+path+ '\n')
                        sql = "insert into taxplayer_filename VALUES('%s','%s','%s','%s','%s','%s','%s')"\
                                     %(self.province,name,fbrq,title,html_id,link,self.start_time)
                        try:
                            self.cursor.execute(sql)
                            self.conn.commit()
                        except:
                            print u'数据库已有该数据'


    def liuzhou(self):
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:45.0) Gecko/20100101 Firefox/45.0'}
        params ={
            'appid': 1, 'col': 1, 'columnid': 101769, 'path': '/', 'permissiontype': 0, 'unitid': 119051, 'webid':24,\
            'webname': u'广西壮族自治区柳州市国家税务局'
        }
        host = 'http://lz.gxgs.gov.cn'
        name = u'柳州市国家税务局'
        for p in range(1,25):
            sr, er = str(p*15+1), str((p+1)*15)
            url = 'http://lz.gxgs.gov.cn/module/jslib/jquery/jpage/dataproxy.jsp?startrecord='+sr+'&endrecord='+er+\
                '&perpage=15'
            # print 'url',url
            for i in range(3):
                try:
                    r = self.post(url=url, data=params, headers=headers)
                    if r.status_code == 200:
                        break
                except:
                    continue
            # print r.text
            soup = BeautifulSoup(r.text, 'html5lib')
            tr_list = soup.find_all('table')
            for i,item in enumerate(tr_list):
                title = item.find('a', class_='bt_link').get('title').strip().replace('\n','').replace(' ','')
                fbrq = item.find_all('td')[-1].text.split('[')[1].split(']')[0]
                # title = item.text.strip().replace('\n','').replace(' ','')
                # href = item.get('href').strip()
                # print 'title',title, fbrq
                if u'欠税' in title  or u'非正常户' in title:
                    html = item.find('a', class_='bt_link')
                    html_herf = html.get('href').split("\'")[1].split("\\")[0]
                    # i_d = 'art_'+html_herf.split("art_")[1].split(".html")[0]
                    link = host + html_herf
                    print u'第%d页%d条'%(p, i), title, link
                    r = self.get(url=link, headers=headers)
                    r.encoding = 'utf8'
                    soup = BeautifulSoup(r.text, 'html5lib')
                    for i in range(3):
                        try:
                            detial_info = soup.find('table', align='center')
                            # i_d = re.findall(r'\d{9,}',link_1)[0]
                            if detial_info:
                                break
                        except:
                            continue
                    # print 'detial_info',detial_info
                    if '.xls' in r.text or '.doc' in r.text:
                        url_list = detial_info.find_all('a')
                        for item in url_list:
                            # print 'item', item, item.get('href'), item.text.strip()
                            if 'download' in item.get('href') or u'附件下载' in item.text.strip():
                                url = item['href']
                                a_d = os.path.split(url)[1]
                                # print 'url', url,a_d
                                i_d = a_d.split('=')[1]
                                if '.xls' in a_d:
                                    # xls_path = os.path.join(sys.path[0], './File/' + i_d)
                                    xls_path = self.path + i_d
                                elif '.doc' in a_d:
                                    # word_path = os.path.join(sys.path[0], './File/' + i_d)
                                    word_path = self.path+ i_d
                                # print 'xls_path',xls_path
                                xls_name,word_name = i_d, i_d
                                xls_url = host+url
                                # print 'xls_url',xls_url
                                for i in range(3):
                                    try:
                                        r_2 = self.get(url=xls_url, headers=headers)
                                        if r.status_code == 200:
                                            break
                                    except:
                                        continue
                                # print 'r_2.content', r_2.content
                                if 'xls' in a_d:
                                    with open(xls_path, 'wb') as f:
                                        f.write(r_2.content)
                                        self.logf.write(u'第%d页%d条'%(p, i)+xls_path+ '\n')
                                        sql = "insert into taxplayer_filename VALUES('%s','%s','%s','%s','%s','%s','%s')"\
                                        %(self.province,name,fbrq,title,xls_name,xls_url,self.start_time)
                                        print u'下载xls', sql
                                elif 'doc' in a_d:
                                    with open(word_path, 'wb') as f:
                                        f.write(r_2.content)
                                        self.logf.write(u'第%d页%d条'%(p, i)+word_path+ '\n')
                                        sql = "insert into taxplayer_filename VALUES('%s','%s','%s','%s','%s','%s','%s')"\
                                                %(self.province,name,fbrq,title,xls_name,xls_url,self.start_time)
                                        print u'下载word', sql
                                try:
                                    self.cursor.execute(sql)
                                    self.conn.commit()
                                except:
                                    print u'数据库已有该数据'
                    elif u'法定代表人' in r.text or u'纳税人识别号' in r.text:
                        # path = os.path.join(sys.path[0], './File/' + html_herf)
                        path = self.path + html_herf
                        # path = os.path.join(sys.path[0], './File/' + html_herf)
                        with open(path, 'wb') as f:
                            f.write(r.content)
                            self.logf.write(u'第%d页%d条'%(p, i)+path+ '\n')
                        sql = "insert into taxplayer_filename VALUES('%s','%s','%s','%s','%s','%s','%s')"\
                              %(self.province,name,fbrq,title,html_herf,link,self.start_time)
                        try:
                            self.cursor.execute(sql)
                            self.conn.commit()
                        except:
                            print u'数据库已有该数据'

    def dishui(self):
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:45.0) Gecko/20100101 Firefox/45.0'}
        # params ={
        #     'appid': 1, 'col': 1, 'columnid': 101769, 'path': '/', 'permissiontype': 0, 'unitid': 119051, 'webid':24,\
        #     'webname': u'广西壮族自治区柳州市国家税务局'
        # }
        name = u'广西壮族自治区地税'
        host = 'http://www.gxds.gov.cn'
        for p in range(0, 14):
            url = 'http://www.gxds.gov.cn/id_tzgg201503160950014158/page_'+str(p)+'/column.shtml'
            # print 'url',url
            for i in range(3):
                try:
                    r = self.get(url=url, headers=headers)
                    if r.status_code == 200:
                        break
                except:
                    continue
            # print r.text
            soup = BeautifulSoup(r.text, 'html5lib')
            href_list = soup.find('ul', class_='news-list').find_all('li')
            for i,item in enumerate(href_list):
                # print 'item',item
                title = item.find('a').text.strip().replace('\n','').replace(' ','').replace(' ','')
                fbrq = item.find('span').text.strip()
                # print 'title',title,
                if u'欠税' in title or u'非正常户' in title:
                    html_herf = item.a['href'].replace('/','',1)
                    print 'html_href',html_herf
                    i_d = html_herf.split("/")[0]
                    link = host + html_herf
                    print u'第%d页%d条'%(p, i), title, link
                    r = self.get(url=link, headers=headers)
                    r.encoding = 'utf8'
                    soup = BeautifulSoup(r.text, 'html5lib')
                    for i in range(3):
                        try:
                            detial_info = soup.find('div', class_='news-contents')
                            # i_d = re.findall(r'\d{9,}',link_1)[0]
                            if detial_info:
                                break
                        except:
                            continue
                    # print '_link1_',link_1
                    if '.xls' in r.text or '.doc' in r.text:
                        url = detial_info.find('a', target='_blank').get('href')
                        # print 'url_111',url
                        # xls_path = os.path.join(sys.path[0], './guang_xi/' + i_d + '.xls')
                        # word_path = os.path.join(sys.path[0], './guang_xi/' + i_d + '.doc')
                        # xls_path = os.path.join(sys.path[0], './File/' + i_d + '.xls')
                        # word_path = os.path.join(sys.path[0], './File/' + i_d + '.doc')
                        xls_path = self.path + i_d + '.xls'
                        word_path = self.path + i_d + '.doc'
                        xls_name,word_name = i_d + '.xls', i_d + '.doc'
                        print 'xls_path', xls_path
                        xls_url = host + url
                        # print 'xls_url',xls_url
                        for i in range(3):
                            try:
                                r_2 = self.get(url=xls_url, headers=headers)
                                if r.status_code == 200:
                                    break
                            except:
                                continue
                        # print 'r_2.content', r_2.content
                        if 'xls' in url:
                            with open(xls_path, 'wb') as f:
                                f.write(r_2.content)
                                self.logf.write(u'第%d页%d条'%(p, i)+xls_path+ '\n')
                                sql = "insert into taxplayer_filename VALUES('%s','%s','%s','%s','%s','%s','%s')"\
                                      %(self.province,name,fbrq,title,xls_name,xls_url,self.start_time)
                                print u'下载xls',sql
                        elif 'doc' in url:
                            with open(word_path, 'wb') as f:
                                f.write(r_2.content)
                                self.logf.write(u'第%d页%d条'%(p, i)+word_path+ '\n')
                                sql = "insert into taxplayer_filename VALUES('%s','%s','%s','%s','%s','%s','%s')"\
                                      %(self.province,name,fbrq,title,word_name,xls_url,self.start_time)
                                print u'下载xls',sql
                        try:
                            self.cursor.execute(sql)
                            self.conn.commit()
                        except:
                            print u'数据库已有该数据'
                    elif u'法定代表人' in r.text or u'纳税人识别号' in r.text:
                        # path = os.path.join(sys.path[0], './guang_xi/' + html_herf)
                        # path = os.path.join(sys.path[0], './File/' + html_herf)
                        path = self.path + html_herf
                        print 'path',path
                        with open(path, 'wb') as f:
                            f.write(r.content)
                            self.logf.write(u'第%d页%d条'%(p, i)+path+ '\n')
                        sql = "insert into taxplayer_filename VALUES('%s','%s','%s','%s','%s','%s','%s')"\
                              %(self.province,name,fbrq,title,html_herf,link,self.start_time)
                        try:
                            self.cursor.execute(sql)
                            self.conn.commit()
                        except:
                            print u'数据库已有该数据'

    def difang(self, url):
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:45.0) Gecko/20100101 Firefox/45.0'}
        ad = url
        conn = MySQLdb.connect(host='172.16.0.76', port=3306, user='guanhuaixuan', passwd='xuanhuaiguan', db='taxplayer',
                         charset='utf8')
        cursor = conn.cursor()
        name_dic = {'nn': u'南宁市地方税务局','gl': u'桂林市地方税务局','bh': u'北海市地方税务局',
                     'qz': u'钦州市地方税务局','lb': u'来宾市地方税务局','cz': u'崇左市地方税务局',}
        name_1 = ad.split("http://")[1].split(".gxds")[0]
        name = name_dic[name_1]
        host = ad.split('/id')[0]
        detail_host = url.split('.cn/')[0]+'.cn'
        # xls_host = ad.split('cn/')[0]+'cn'
        url_new = ad.split('page_')[0]+'page_'+str(1)+'/column.shtml'
        print 'url_new',url_new
        for i in range(3):
            try:
                r = self.get(url=url_new, headers=headers)
                if r.status_code == 200:
                    break
            except:
                continue
            # print r.text
        r.encoding = 'utf8'
        soup = BeautifulSoup(r.text, 'html5lib')
        # print soup.find('div',class_='right_info').find('div',class_='strong-ui-pager')
        page_1 = soup.find('div',class_='right_info').find('div',class_='strong-ui-pager').find('span').text.strip()
        page = page_1.split('/')[1]
        # print 'page',page,type(page),type(int(page))
        for p in range(int(page)):
        # for p in range(4,5):
            url_new = ad.split('page_')[0]+'page_'+str(p+1)+'/column.shtml'
            print 'url_new',url_new
            for i in range(3):
                try:
                    r = self.get(url=url_new, headers=headers)
                    if r.status_code == 200:
                        break
                except:
                    continue
                # print r.text
            r.encoding = 'utf8'
            soup = BeautifulSoup(r.text, 'html5lib')
            href_list = soup.find('div', class_='right_info').find_all('li')
            for i,item in enumerate(href_list):
                title = item.find('a').text.strip().replace('\n','').replace(' ','').replace(' ','')
                href = item.a['href']
                fbrq = item.find('p',class_='right_list_time').text.strip().replace('\n','').replace(' ','')
                html_herf = href.split('/')[1]
                # print u'第%d页%d条'%(p+1, i), title, href,fbrq,name
                if u'欠税' in title or u'缴税' in title or u'非正常户' in title:
                    link = detail_host+href
                    print u'aaaa', title, link
                    r = self.get(url=link, headers=headers)
                    r.encoding = 'utf8'
                    soup = BeautifulSoup(r.text, 'html5lib')
                    # if '.xls' in r.text or '.doc' in r.text:
                    for i in range(3):
                        try:
                            detial_info = soup.find('div', class_='right_detial_news')
                            # i_d = re.findall(r'\d{9,}',link_1)[0]
                            if detial_info:
                                break
                        except:
                            continue
                    # print '_link1_',link_1
                    if '.xls' in r.text or '.doc' in r.text:
                        url_list = detial_info.find_all('a', target='_blank')
                        for item in url_list:
                            url = item['href']
                            a_d = os.path.split(url)[1]
                            i_d = ''
                            if '.xls' in a_d:
                                i_d = a_d.split('.xls')[0]
                            elif '.doc' in a_d:
                                i_d = a_d.split('.doc')[0]
                            # xls_path = os.path.join(sys.path[0], './guang_xi/' + i_d + '.xls')
                            # word_path = os.path.join(sys.path[0], './guang_xi/' + i_d + '.doc')
                            # xls_path = os.path.join(sys.path[0], './File/' + i_d + '.xls')
                            # word_path = os.path.join(sys.path[0], './File/' + i_d + '.doc')
                            xls_path = self.path+ i_d + '.xls'
                            word_path = self.path + i_d + '.doc'
                            # print 'xls_path',xls_path, '\n', word_path
                            xls_name, word_name = i_d + '.xls', i_d + '.doc'
                            xls_url = host+url
                            print 'xls_url',xls_url
                            for i in range(3):
                                try:
                                    r_2 = self.get(url=xls_url, headers=headers)
                                    if r.status_code == 200:
                                        break
                                except:
                                    continue
                            # print 'r_2.content', r_2.content
                            sql = ''
                            if 'xls' in a_d:
                                with open(xls_path, 'wb') as f:
                                    f.write(r_2.content)
                                    self.logf.write(u'第%d页%d条'%(p, i)+xls_path+ '\n')
                                    sql = "insert into taxplayer_filename VALUES('%s','%s','%s','%s','%s','%s','%s')"\
                                  %(self.province,name,fbrq,title,xls_name,xls_url,self.start_time)
                            elif 'doc' in a_d:
                                with open(word_path, 'wb') as f:
                                    f.write(r_2.content)
                                    self.logf.write(u'第%d页%d条'%(p, i)+word_path+ '\n')
                                    sql = "insert into taxplayer_filename VALUES('%s','%s','%s','%s','%s','%s','%s')"\
                                          %(self.province,name,fbrq,title,word_name,xls_url,self.start_time)
                            print u'xls下载', sql
                            try:
                                cursor.execute(sql)
                                conn.commit()
                            except:
                                print u'数据库已有该数据'
                    elif u'法定代表人' in r.text or u'纳税人识别号' in r.text:
                        # path = os.path.join(sys.path[0], './File/' + html_herf+'.html')
                        path = self.path + html_herf+'.html'
                        html_name = html_herf+'.html'
                        # print 'path',path
                        with open(path, 'wb') as f:
                            f.write(r.content)
                            self.logf.write(u'第%d页%d条'%(p, i)+path+ '\n')
                        sql = "insert into taxplayer_filename VALUES('%s','%s','%s','%s','%s','%s','%s')"\
                         %(self.province,name,fbrq,title,html_name,link,self.start_time)
                        print u'页面下载', sql
                        try:
                            cursor.execute(sql)
                            conn.commit()
                        except:
                            print u'数据库已有该数据'
if __name__ == '__main__':
    searcher = GuangXiSearcher()
    searcher.set_config()
    # d1 = datetime.datetime.now()
    # # searcher.run(1, 2)
    # MSSQL.execute_sop('hebei')
    # d2 = datetime.datetime.now()
    # dd = d2-d1
    # print dd, u'一共运行%s秒'%dd
