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

# reload(sys)
# sys.setdefaultencoding('utf8')


class HeNanSearcher(SpiderMan):
    # DateBegin = None
    def __init__(self):
        super(HeNanSearcher, self).__init__(keep_session=True)

    def get_savefile_directory(self, province_py):
        grader_father = os.path.abspath(os.path.dirname(sys.path[0])+os.path.sep+"..")
        # print 'grader_father',grader_father
        # parent_dir = os.path.join(os.path.dirname(__file__), '../All_Files')
        parent_dir = os.path.join(grader_father, './All_Files')
        # print 'parent_dir',parent_dir
        save_directory = os.path.join(parent_dir, province_py)
        self.get_directory(save_directory)
        return save_directory + '\\'

    def get_directory(self, path):
        if not os.path.exists(path):
            os.makedirs(path)

    def set_config(self):
        self.DateBegin = datetime.datetime.now().strftime('%Y-%m-%d')
        self.start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.province = u'河南省'
        province_py = 'He_Nan'
        self.path = self.get_savefile_directory(province_py)
        self.conn = MySQLdb.connect(host='172.16.0.76', port=3306, user='guanhuaixuan', passwd='xuanhuaiguan', db='taxplayer',
                         charset='utf8')
        self.cursor = self.conn.cursor()
        # sql_1 = "UPDATE ktgg_job set start_time= '%s' , updatetime= '%s' where name='hebei' " % (
        # start_time, self.DateBegin)
        smx = 'http://smx.12366.ha.cn/012/bsfw_01212/tzgg_0121201/0121201_list_0.html?LM_ID=0121201'
        jz = 'http://jz.12366.ha.cn/008/bsfw_00812/tzgg_0081201/0081201_list_0.html?LM_ID=0081201'
        xc = 'http://xc.12366.ha.cn/010/bsfw_01012/tzgg_0101201/0101201_list_0.html?LM_ID=0101201'
        zmd ='http://zmd.12366.ha.cn/016/bsfw_01612/tzgg_0161201/0161201_list_0.html?NVG=6&LM_ID=0161201'
        jy ='http://jy.12366.ha.cn/018/bsfw_01812/tzgg_0181201/0181201_list_0.html?NVG=6&LM_ID=0181201'
        zz ='http://zz.ha-l-tax.gov.cn/viewCmsCac.do?cacId=ff8080815c1fb0ea015c7b62e6717673&offset=0&'
        xy ='http://xy.12366.ha.cn/017/bsfw_01712/tzgg_0171201/0171201_list_0.html?NVG=6&LM_ID=0171201'
        # try:
        #     self.cursor_2.execute(sql_1)
        #     self.conn_2.commit()
        # except:
        #     print u'更改监控开始时间报错'
        # self.logf = open("D:/taxplayer_common/All_Programs/HeNan/log/%s.txt" % (str(self.DateBegin)), 'a')
        # self.logf.write(str(self.start_time) +u'开始爬虫'+ '\n')
        m_list = []
        m1 = threading.Thread(target=self.henan)
        m_list.append(m1)
        m2 = threading.Thread(target=self.qiansui,args=(smx,))
        m_list.append(m2)
        m3 = threading.Thread(target=self.qiansui,args=(jz,))
        m_list.append(m3)
        m4 = threading.Thread(target=self.qiansui,args=(xc,))
        m_list.append(m4)
        m5 = threading.Thread(target=self.qiansui,args=(zmd,))
        m_list.append(m5)
        m6 = threading.Thread(target=self.qiansui,args=(jy,))
        m_list.append(m6)
        m7 = threading.Thread(target=self.qiansui,args=(xy,))
        m_list.append(m7)
        m8 = threading.Thread(target=self.zhengzhou,args=(zz,))
        m_list.append(m8)
        for m in m_list:
            m.setDaemon(True)
            m.start()
        for m in m_list:
            m.join()

    def henan(self):
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:45.0) Gecko/20100101 Firefox/45.0'}
        url = 'http://www.12366.ha.cn/003/xxgk_301/qsgg_30117/30117_list_0.html?NVG=1&LM_ID=30117'
        host = 'http://www.12366.ha.cn/003/xxgk_301/qsgg_30117/'
        name = u'河南省国家税务局'
        for i in range(2):
            try:
                r = self.get(url=url, headers=headers)
                if r.status_code == 200:
                    break
            except:
                continue
        # print r.text
        r.encoding='utf8'
        soup = BeautifulSoup(r.text, 'html5lib')
        href_list = soup.find('table', class_='title_list_table').find_all('tr')
        for i,item in enumerate(href_list):
            title = item.find('td').text.strip().replace('\n','').replace(' ','')
            href = host+item.find('td').a['href']
            fbrq = item.find_all('td')[-1].text.split('[')[1].split(']')[0]
            print u'第%d条'%(i),title, href, fbrq
            for i in range(3):
                try:
                    r = self.get(url=href, headers=headers)
                    if r.status_code == 200:
                        break
                except:
                    continue
            soup= BeautifulSoup(r.text, 'html5lib')
            link_1 = soup.find('div', id='info_id').find('a')['href']
            i_d = re.findall(r'\d{9,}',link_1)[0]
            link = 'http://xz.12366.ha.cn/xz'+link_1
            # print 'link',link
            for i in range(3):
                try:
                    r = self.get(url=link, headers=headers)
                    if r.status_code == 200:
                        break
                except:
                    continue
            # grader_father = os.path.abspath(os.path.dirname(sys.path[0])+os.path.sep+"..")
            # print 'grader_father', grader_father \All_Files\He_Nan
            # xls_path = 'D:/taxplayer_common/All_Files/He_Nan/'+i_d + '.xls'
            xls_path = self.path + i_d + '.xls'
            # xls_path = os.path.join(grader_father, './All_Files/He_Nan/'+ i_d + '.xls')
            # xls_path = os.path.join(grader_father, './He_Nan/'+i_d + '.xls')
            # xls_path = os.path.join(father_path, './He_Nan/'+i_d + '.xls')
            xls_name = i_d + '.xls'
            print 'xls_path',xls_path
            with open(xls_path, 'wb') as f:
                f.write(r.content)
                f.close()
            # self.logf.write(u'第%d条'%(i)+u'河南省国税'+xls_path+ '\n')
            sql = "insert into taxplayer_filename VALUES('%s','%s','%s','%s','%s','%s','%s')"\
                   %(self.province,name,fbrq,title,xls_name,link,self.start_time)
            try:
                self.cursor.execute(sql)
                self.conn.commit()
            except:
                print u'数据库已有该数据'

    def qiansui(self,url):
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:45.0) Gecko/20100101 Firefox/45.0'}
        conn = MySQLdb.connect(host='172.16.0.76', port=3306, user='guanhuaixuan', passwd='xuanhuaiguan', db='taxplayer',
                         charset='utf8')
        cursor = conn.cursor()
        ad = url.split('list')[0]+'list_'
        af = '.html'+url.split('.html')[1]
        name_dic = {'smx': u'三门峡市国家税务局','jz': u'焦作市国家税务局','xc': u'许昌市国家税务局',
                     'zmd': u'驻马店市国家税务局','jy': u'济源市国家税务局','xy': u'郑州市国家税务局',}
        name_1 = url.split("://")[1].split('.')[0]
        name = name_dic[name_1]
        detail_host = os.path.split(ad)[0]
        xls_host = ad.split('cn/')[0]+'cn'
        for p in range(10):
            url_new = ad+str(p)+af
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
            href_list = soup.find('div', class_='list cf').find('ul').find_all('li')
            for i,item in enumerate(href_list):
                title = item.find('div',class_='list_contain').find('h2').text.strip()
                href = item.find('div',class_='list_contain').find('h2').a['href']
                html_herf = href.split('.html?')[1]
                # print u'第%d页%d条'%(p, i), title, href
                if u'欠税' in title or u'缴税' in title or u'非正常户' in title:
                    link = detail_host+'/'+href
                    print u'aaaa', title, link
                    r = self.get(url=link, headers=headers)
                    r.encoding = 'utf8'
                    soup = BeautifulSoup(r.text, 'html5lib')
                    # if '.xls' in r.text or '.doc' in r.text:
                    for i in range(3):
                        try:
                            link_1 = soup.find('div', id='info_id').find('a')['href']
                            i_d = re.findall(r'\d{9,}',link_1)[0]
                            if link_1:
                                break
                        except:
                            continue
                    fb_time = soup.find('div', id='PrintContent').find('span', class_='zw-fb-time').text.strip()
                    fbrq = fb_time.split(u'发布日期：')[1].split(u'消息')[0]
                    if '.xls' in r.text or '.doc' in r.text:
                        # xls_path = os.path.join(sys.path[0], './he_nan/' + i_d + '.xls')
                        # word_path = os.path.join(sys.path[0], './he_nan/' + i_d + '.doc')
                        xls_path = self.path + i_d + '.xls'
                        word_path = self.path + i_d + '.doc'
                        # xls_path = os.path.join(sys.path[0], './File/' + i_d + '.xls')
                        # word_path = os.path.join(sys.path[0], './File/' + i_d + '.doc')
                        print 'xls_path',xls_path
                        xls_name, word_name = i_d + '.xls', i_d + '.doc'
                        xls_url = xls_host+link_1
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
                                sql = "insert into taxplayer_filename VALUES('%s','%s','%s','%s','%s','%s','%s')"\
                                  %(self.province,name,fbrq,title,xls_name,xls_url,self.start_time)
                                try:
                                    cursor.execute(sql)
                                    conn.commit()
                                except:
                                    print u'数据库已有该数据'
                        elif 'doc' in link_1:
                            with open(word_path, 'wb') as f:
                                f.write(r_2.content)
                                self.logf.write(u'第%d页%d条'%(p, i)+word_path+ '\n')
                                sql = "insert into taxplayer_filename VALUES('%s','%s','%s','%s','%s','%s','%s')"\
                                  %(self.province,name,fbrq,title,word_name,xls_url,self.start_time)
                                try:
                                    cursor.execute(sql)
                                    conn.commit()
                                except:
                                    print u'数据库已有该数据'
                    elif u'法定代表人' in r.text or u'纳税人识别号' in r.text:
                        # path = os.path.join(sys.path[0], './he_nan/' + html_herf+'.html')
                        path = self.path + html_herf+'.html'
                        html_name = html_herf+'.html'
                        print 'path',path
                        with open(path, 'wb') as f:
                            f.write(r.content)
                            self.logf.write(u'第%d页%d条'%(p, i)+path+ '\n')
                            sql = "insert into taxplayer_filename VALUES('%s','%s','%s','%s','%s','%s','%s')"\
                              %(self.province,name,fbrq,title,html_name,link,self.start_time)
                            try:
                                cursor.execute(sql)
                                conn.commit()
                            except:
                                print u'数据库已有该数据'

    def zhengzhou(self,url):
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:45.0) Gecko/20100101 Firefox/45.0'}
        xls_host = url.split('cn/')[0]+'cn'
        name = u'郑州市地方税务局'
        ad = url.split('set=')[0]+'set='
        for p in range(12):
            url_new = ad+str(p*12)+'&'
            print 'url_new',url_new
            for i in range(3):
                try:
                    r = self.get(url=url_new, headers=headers)
                    if r.status_code == 200:
                        break
                except:
                    continue
                # print r.text
            r.encoding='utf8'
            soup = BeautifulSoup(r.text, 'html5lib')
            href_list = soup.find('div', class_='ej_rightbox').find('td',align='center').find_all('td',align='left')
            fb_rq = soup.find('div', class_='ej_rightbox').find('td',align='center').find_all('td',align='right')
            # tr_list = soup.find('div', class_='ej_rightbox').find('td',align='center').find_all('table')[1].find_all()
            for i,item in enumerate(href_list[1:]):
                # print 'item', item
                title = item.text.strip()
                href = item.a['href']
                fbrq = fb_rq[i].text.split('[')[1].split(']')[0]
                print u'第%d页%d条'%(p, i), title, href, fbrq
                if u'欠税' in title or u'缴税' in title :
                    link = xls_host+'/'+href
                    # print u'aaaa', title, link
                    r = self.get(url=link, headers=headers)
                    r.encoding = 'utf8'
                    # print r.text
                    soup = BeautifulSoup(r.text, 'html5lib')
                    try:
                        link_1 = soup.find_all('div', class_='ds_content')[1].find('a',target="_blank")
                        # link = link_1.split('href="')[1].split('"')[0]
                        link = link_1.get('href')
                        print 'link',link
                        i_d = re.findall(r'\d{9,}',link)[0]
                        # xls_path = os.path.join(sys.path[0], './he_nan/' + i_d + '.xls')
                        # word_path = os.path.join(sys.path[0], './he_nan/' + i_d + '.doc')
                        xls_path = self.path + i_d + '.xls'
                        word_path = self.path + i_d + '.doc'
                        # xls_path = os.path.join(sys.path[0], './File/' + i_d + '.xls')
                        # word_path = os.path.join(sys.path[0], './File/' + i_d + '.doc')
                        # print 'xls_path',xls_path
                        xls_name, word_name = i_d + '.xls', i_d + '.doc'
                        xls_url = 'http://zz.ha-l-tax.gov.cn'+link
                        # print 'xls_url',xls_url
                        for i in range(3):
                            try:
                                r_2 = self.get(url=xls_url, headers=headers)
                                if r.status_code == 200:
                                    break
                            except:
                                continue
                        # print 'r_2.content', r_2.content
                        if 'xls' in link:
                            with open(xls_path, 'wb') as f:
                                f.write(r_2.content)
                                self.logf.write(u'第%d页%d条'%(p, i)+xls_path+ '\n')
                                sql = "insert into taxplayer_filename VALUES('%s','%s','%s','%s','%s','%s','%s')"\
                                  %(self.province,name,fbrq,title,xls_name,xls_url,self.start_time)
                                print u'下载xls', sql
                                try:
                                    self.cursor.execute(sql)
                                    self.conn.commit()
                                except:
                                    print u'数据库已有该数据'
                        elif 'doc' in link:
                            with open(word_path, 'wb') as f:
                                f.write(r_2.content)
                                self.logf.write(u'第%d页%d条'%(p, i)+word_path+ '\n')
                                sql = "insert into taxplayer_filename VALUES('%s','%s','%s','%s','%s','%s','%s')"\
                                  %(self.province,name,fbrq,title,word_name,xls_url,self.start_time)
                                try:
                                    self.cursor.execute(sql)
                                    self.conn.commit()
                                except:
                                    print u'数据库已有该数据'
                    except:
                        pass


if __name__ == '__main__':
    searcher = HeNanSearcher()
    searcher.set_config()
    # searcher.run(1, 2)