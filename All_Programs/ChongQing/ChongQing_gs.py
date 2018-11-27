# coding=utf-8
import PackageTool
import datetime
import os
import sys
import MySQLdb
from bs4 import BeautifulSoup
from tax.SpiderMan_FYH import SpiderMan
# from tax.SpiderMan import SpiderMan
from tax.Mysql_Config_Fyh import logger

reload(sys)
sys.setdefaultencoding('utf8')


class ChongQingSearcher(SpiderMan):
    def __init__(self):
        super(ChongQingSearcher, self).__init__(keep_session=True)

    def log(self, message):
        log_name = 'chong_qing_tax_gs.log'
        logger(log_name, message)

    def get_savefile_directory(self, province_py):
        grader_father = os.path.abspath(os.path.dirname(sys.path[0])+os.path.sep+"..")
        parent_dir = os.path.join(grader_father, './All_Files')
        save_directory = os.path.join(parent_dir, province_py)
        self.get_directory(save_directory)
        return save_directory + '\\'

    def get_directory(self, path):
        if not os.path.exists(path):
            os.makedirs(path)

    def set_config(self):
        self.DateBegin = datetime.datetime.now().strftime('%Y-%m-%d')
        self.start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.province = u'重庆市'
        province_py = 'Chong_Qing'
        self.path = self.get_savefile_directory(province_py)
        self.log(self.province+ '税务局')
        self.run()

    def run(self):
        host = 'http://www.cqsw.gov.cn/Xbb_gsgw/Xbb_xxgk/Xbb_xxgkTzgg'
        url_1 = 'http://www.cqsw.gov.cn/Xbb_gsgw/Xbb_xxgk/Xbb_xxgkTzgg/index.html'
        self.parse(url_1)
        for p in range(2, 21):
            url = host+'/index_'+str(p-1)+'.html'
            self.log('url_list:'+url)
            self.parse(url)

    def parse(self, url):
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:45.0) Gecko/20100101 Firefox/45.0'}
        host = 'http://www.cqsw.gov.cn/Xbb_gsgw/Xbb_xxgk/Xbb_xxgkTzgg'
        r = self.get(url=url, headers=headers)
        r.encoding = 'utf8'
        soup = BeautifulSoup(r.text, 'html5lib')
        item_list = soup.find('div', class_='mr_con same1').find_all('li')
        for i,item in enumerate(item_list):
            self.i = i
            self.title = item.find('a', target='_blank').text.strip().replace(' ', '')
            self.fbrq = item.find('span').text.strip()
            # print u'第%d条'%(i), self.title,self.fbrq
            if u'欠税' in self.title or u'缴税' in self.title or u'非正常户' in self.title:
                href = item.find('a', target='_blank').get('href')
                link = host+href
                self.parse_detail(link)

    def parse_detail(self, link):
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:45.0) Gecko/20100101 Firefox/45.0'}
        r = self.get(url=link, headers=headers)
        name = u'重庆市国家税务局'
        host = 'http://www.cqsw.gov.cn/Xbb_gsgw/Xbb_xxgk/Xbb_xxgkTzgg/'
        r.encoding = 'utf8'
        soup = BeautifulSoup(r.text, 'html5lib')
        if '.xls' in r.text or '.doc' in r.text:
            item_list = soup.find('div', class_='TRS_Editor').find_all('p')
            for item in item_list:
                if item.text.strip():
                    # print item
                    href_1 = item.a['href']
                    print 'href_1', href_1
                    href = os.path.split(href_1)[1]
                    sj = href[:8][2:]
                    url = host+sj+'/'+href
                    print 'url',url
                    self.log(url)
                    r_2 = self.get(url=url, headers=headers)
                    xls_path = self.path+ href
                    word_path = self.path + href
                    print 'xls_path',xls_path
                    xls_name, word_name = href, href
                    # print 'r_2.content', r_2.content
                    if 'xls' in href:
                        with open(xls_path, 'wb') as f:
                            f.write(r_2.content)
                            sql = "insert into taxplayer_filename VALUES('%s','%s','%s','%s','%s','%s','%s')"\
                              %(self.province,name,self.fbrq,self.title,xls_name,url,self.start_time)
                            self.save(sql)
                    elif 'doc' in href:
                            sql = "insert into taxplayer_filename VALUES('%s','%s','%s','%s','%s','%s','%s')"\
                              %(self.province,name,self.fbrq,self.title,word_name,url,self.start_time)
                            self.save(sql)

                #     elif u'法定代表人' in r.text or u'纳税人识别号' in r.text:
                #         path = os.path.join(sys.path[0], './he_nan/' + html_herf+'.html')
                #         html_name = html_herf+'.html'
                #         print 'path',path
                #         with open(path, 'wb') as f:
                #             f.write(r.content)
                #             self.logf.write(u'第%d页%d条'%(p, i)+path+ '\n')
                #             sql = "insert into taxplayer_filename VALUES('%s','%s','%s','%s','%s','%s','%s')"\
                #               %(self.province,name,fbrq,title,html_name,link,self.start_time)
                #             try:
                #                 cursor.execute(sql)
                #                 conn.commit()
                #             except:
                #                 print u'数据库已有该数据'

    def save(self, sql):
        conn = MySQLdb.connect(host='172.16.0.76', port=3306, user='guanhuaixuan', passwd='xuanhuaiguan', db='taxplayer',
                         charset='utf8')
        cursor = conn.cursor()
        try:
            print sql
            cursor.execute(sql)
            conn.commit()
        except:
            print u'数据库已有该数据'

if __name__ == '__main__':
    searcher = ChongQingSearcher()
    searcher.set_config()