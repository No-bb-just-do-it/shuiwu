# coding=utf-8

import requests
import json
import time
from tax.MyException import StatusCodeException

class SpiderMan(object):

    session = None
    manager_host = ''
    manager_port = 123123
    order = None

    def __init__(self, order='5fe6cf97-5592-11e7-be16-f45c89a63279', keep_session=True, keep_ip=False, max_try_times=5,headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:45.0) Gecko/20100101 Firefox/45.0'}):
        self.order = order
        self.headers=headers
        self.keep_ip = keep_ip
        if self.keep_ip:
            self.expected_ip = ''
        if keep_session:
            self.session = requests.session()
        if max_try_times:
            self.max_try_times = max_try_times

    def reset_session(self):
        self.session = requests.session()

    def get(self, url, **kwargs):
        # print('session,',self.session)
        for t in range(self.max_try_times):
            # print('try_num:',t)
            proxy_config = self.get_proxy()

            kwargs['proxies'] = {'http': 'http://%(user)d:%(pwd)s@%(proxy)s' % proxy_config,
                                 'https': 'https://%(user)d:%(pwd)s@%(proxy)s' % proxy_config}
            kwargs['timeout'] = proxy_config['timeout']

            try:
                if self.session:
                    r = self.session.get(url=url, headers=self.headers,**kwargs)
                    # print(r)
                    if r.status_code != 200:
                        raise StatusCodeException(str(r.status_code))
                    return r
                else:
                    r = requests.get(url=url, headers=self.headers,**kwargs)
                    # print(r.content)
                    if r.status_code != 200:
                        raise StatusCodeException(str(r.status_code))
                    return r
            except (requests.exceptions.RequestException,StatusCodeException) as e:
            # except Exception as e:
                # time.sleep(2)
                if t == self.max_try_times - 1:
                    print(e)
        return None

                    # raise e

    def post(self, url, **kwargs):
        for t in range(self.max_try_times):
            proxy_config = self.get_proxy()
            kwargs['proxies'] = {'http': 'http://%(user)d:%(pwd)s@%(proxy)s' % proxy_config,
                                 'https': 'https://%(user)d:%(pwd)s@%(proxy)s' % proxy_config}
            kwargs['timeout'] = proxy_config['timeout']
            # if 'headers' in kwargs:
            #     kwargs['headers']['Proxy-Authentication'] = proxy_config['secret_key']
            # else:
            #     kwargs['headers'] = {'Proxy-Authentication': proxy_config['secret_key']}
            try:
                if self.session:
                    r = self.session.post(url=url, **kwargs)
                    if r.status_code != 200:
                        raise StatusCodeException(str(r.status_code))
                else:
                    r = requests.post(url=url, **kwargs)
                    if r.status_code != 200:
                        raise StatusCodeException(str(r.status_code))
                return r
            except (requests.exceptions.RequestException,StatusCodeException) as e:
                if t == self.max_try_times - 1:
                    raise e

    def reset_ip(self):
        self.expected_ip = ''

    def get_proxy(self, domain=None):
        while True:
            url = 'http://%s:%d/get-proxy-api' % (self.manager_host, self.manager_port)
            params = {'order': self.order}
            if self.keep_ip:
                params['expected_ip'] = self.expected_ip
            if domain is not None:
                params['domain'] = domain
            res = requests.get(url, params=params)
            # print(res.text)
            if res.status_code == 200 and res.text != '{}':
                json_obj = json.loads(res.text)
                if self.keep_ip:
                    self.expected_ip = json_obj['proxy'].split(':')[0]
                return json_obj
            else:
                time.sleep(1)
                print(u'暂无可用代理')

    def add_to_black_list(self, domain, proxy_ip):
        params = {'domain': domain, 'proxy_ip': proxy_ip, 'order': self.order}
        print(u'黑名单', params)
        url = 'http://%s:%d/add-proxy-to-blacklist-api' % (self.manager_host, self.manager_port)
        r = requests.post(url, params=params)
        # print(r.status_code)

if __name__ == '__main__':
    order_nbr = '5fe6cf97-5592-11e7-be16-f45c89a63279'
    crawler = SpiderMan(order_nbr, keep_session=True)

    r = crawler.get('http://1212.ip138.com/ic.asp',
                    headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:45.0) Gecko/20100101 Firefox/45.0'}
                    )
    r.encoding = 'gbk'
    print(r.text)
