# coding=utf-8
from SpiderMan_FYH import SpiderMan
from bs4 import BeautifulSoup
import os
import re
from Mysql_Config_Fyh import logger


class TaxplayerDownload(SpiderMan):
    def __init__(self):
        super(TaxplayerDownload, self).__init__(keep_session=True)
        self.province = u''
        self.path = ''
        self.last_update_time = ''
        self.fbrq_stop = '2018-01-01'
        self.stop_crawl = False

    def get_directory(self, path):
        if not os.path.exists(path):
            os.makedirs(path)

    def get_savefile_directory(self, province_py):
        parent_dir = os.path.join(os.path.dirname(__file__), '../All_Files')
        save_directory = os.path.join(parent_dir, province_py)
        self.get_directory(save_directory)
        return save_directory

    def get_tag_list(self, **kwargs):
        pass
    #判断链接列表中需要下载的文件的链接，返回链接列表
    def get_href(self, a_tag_list):
        href_list = []
        for a_tag in a_tag_list:
            soup = BeautifulSoup(a_tag, "html.parser")
            href = soup.find('a').get('href', ' ').encode('utf-8')
            file_formats = ['.doc', '.xls', '.pdf', '.rar', '.DOC']
            file_condition = True in [file_format in href for file_format in file_formats]
            if file_condition and 'javascript' not in href:
                href_list.append(href)
        return href_list

    #判断链接列表中需要下载的文件的链接，返回链接列表
    def get_href_new(self,a_href_list):
        href_list = []
        for a_href in a_href_list:
            file_formats = ['.doc', '.xls', '.pdf', '.rar', '.DOC']
            file_condition = True in [file_format in a_href for file_format in file_formats]
            if file_condition and 'javascript' not in a_href:
                href_list.append(a_href)
        return href_list
    #获得需要保存的html文件名
    def get_html_filename(self, url_inner):
        html_filename = url_inner.split('/')[-1]
        if '=' in url_inner:
            if '.htm' not in url_inner:
                html_filename = html_filename.split('=')[-1] + '.html'
            else:
                html_filename = html_filename.split('=')[-1]
        return html_filename

    def get_filename(self, url):
        filename = url.split('/')[-1]
        if '=' in filename:
            filename = filename.split('=')[-1]
        return filename

    def check_download_url(self, download_url):
        """
        核查下载文件的地址，如果匹配正则表达式，表示不符合规范，反之，表示符合规范。
        :param download_url:
        :return:
        """
        pattern = 'http://www.*?//.*|http://www\.*?http://.*|http://www.*?\./.|http://www.*?\.cn/\d{6}\w.*'
        if re.match(pattern, download_url):
            return True
        else:
            return False

    # 下载文件
    def download_file(self, download_url, filename, savepath):
        # print 'filename ',filename
        # print 'savepath ',savepath
        for k in range(5):
            try:
                fs = self.get(download_url, timeout=15)
                if fs.status_code == 200:
                    pattern = 'http://.*?' + filename
                    download_url_news = re.findall(pattern, fs.content)
                    if download_url_news:
                        fs_new = self.get(download_url_news[0], timeout=15)
                        if fs_new.status_code == 200:
                            download_url_content = fs_new.content
                        else:
                            download_url_content = ''
                    else:
                        download_url_content = fs.content
                    with open(savepath, 'wb') as f:
                        f.write(download_url_content)
                    break
            except Exception as e:
                print e
                if k == 4:
                    print u'下载失败:', download_url
                    # logger('download_url','下载失败')

                else:
                    print u'第' + str(k) + u'次下载请求'

    def download_htmlfile(self, r_inner, html_savepath):
        with open(html_savepath, 'wb') as f:
            f.write(r_inner.content)
