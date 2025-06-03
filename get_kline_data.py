#!/#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import json
import requests
import re

import time
import datetime

from comm_selenium import *
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
import time, datetime
import pandas as pd
import os
import re

import json


debug=0
debug=0


import random
def get_headers():
    '''
    随机获取一个headers
    '''
    user_agents =  ['Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1',\
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',\
            'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11',\
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",\
        "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",\
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",\
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",\
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",\
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",\
        "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",\
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",\
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",\
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",\
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",\
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",\
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3"]
    

    user_agents = [
    "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:38.0) Gecko/20100101 Firefox/38.0",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; .NET4.0C; .NET4.0E; .NET CLR 2.0.50727; .NET CLR 3.0.30729; .NET CLR 3.5.30729; InfoPath.3; rv:11.0) like Gecko",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)",
    "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11",
    "Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Maxthon 2.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; TencentTraveler 4.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; The World)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SE 2.X MetaSr 1.0; SE 2.X MetaSr 1.0; .NET CLR 2.0.50727; SE 2.X MetaSr 1.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Avant Browser)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
    "Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
    "Mozilla/5.0 (iPod; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
    "Mozilla/5.0 (iPad; U; CPU OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
    "Mozilla/5.0 (Linux; U; Android 2.3.7; en-us; Nexus One Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    "MQQBrowser/26 Mozilla/5.0 (Linux; U; Android 2.3.7; zh-cn; MB200 Build/GRJ22; CyanogenMod-7) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    "Opera/9.80 (Android 2.3.4; Linux; Opera Mobi/build-1107180945; U; en-GB) Presto/2.8.149 Version/11.10",
    "Mozilla/5.0 (Linux; U; Android 3.0; en-us; Xoom Build/HRI39) AppleWebKit/534.13 (KHTML, like Gecko) Version/4.0 Safari/534.13",
    "Mozilla/5.0 (BlackBerry; U; BlackBerry 9800; en) AppleWebKit/534.1+ (KHTML, like Gecko) Version/6.0.0.337 Mobile Safari/534.1+",
    "Mozilla/5.0 (hp-tablet; Linux; hpwOS/3.0.0; U; en-US) AppleWebKit/534.6 (KHTML, like Gecko) wOSBrowser/233.70 Safari/534.6 TouchPad/1.0",
    "Mozilla/5.0 (SymbianOS/9.4; Series60/5.0 NokiaN97-1/20.0.019; Profile/MIDP-2.1 Configuration/CLDC-1.1) AppleWebKit/525 (KHTML, like Gecko) BrowserNG/7.1.18124",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows Phone OS 7.5; Trident/5.0; IEMobile/9.0; HTC; Titan)",
    "UCWEB7.0.2.37/28/999",
    "NOKIA5700/ UCWEB7.0.2.37/28/999",
    "Openwave/ UCWEB7.0.2.37/28/999",
    "Mozilla/4.0 (compatible; MSIE 6.0; ) Opera/UCWEB7.0.2.37/28/999",
    # iPhone 6：
    "Mozilla/6.0 (iPhone; CPU iPhone OS 8_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/8.0 Mobile/10A5376e Safari/8536.25",

    ]

    headers = {'User-Agent':random.choice(user_agents)}
    return headers


def get_kline_data(code=None, count=None, period=None):
    
    #1:sh  0:sz
    stock_type = '1.'
    data_df = pd.DataFrame()

    timestamp=str(round(time.time() * 1000))

    if code == None:
        return data_df
    
    if count == None:
        count = 700
    
    if period == None:
        period = 101

    if code[0] == '6':
        stock_type = '1.'
    else:
        stock_type = '0.'

    url= 'http://50.push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery1124032193835566918194_1624510339003&secid=0.300582&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&klt=101&fqt=0&end=20500101&lmt=5&_=1624510339048'

    url= 'http://50.push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery1124032193835566918194_'\
        + timestamp\
        + '&secid='\
        + stock_type\
        + code\
        + '&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&'\
        + 'fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&klt='\
        + str(period)\
        + '&fqt=0&end=20500101&lmt='\
        + str(count)\
        + '&_='\
        + timestamp

    my_dbg(url)
    tmp_header = get_headers()
    my_dbg(tmp_header)
    response = requests.get(url, headers=tmp_header)
    my_dbg(response)


    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, response.text)
    api_param = json.loads(response_array[0])
    name = api_param['data']['name']

    rawdata = api_param['data']['klines']
    tmp_column = ['f1', 'zxj', 'zdf', 'zde', 'turnoverrate', 'code', 'f13',
    'name', 'mkt', 'up_num', 'down_num', 'lzgp', 'lzgp_code', 'f141',
    'lz_zdf' ]


    data_df = pd.DataFrame(rawdata)

    #data_df.columns=tmp_column
    #data_df = data_df.loc[:, new_column]

   
    if debug:
        my_dbg(data_df.head(5))


        
    return data_df, api_param




'''
#https://quote.eastmoney.com/sz002224.html?jump_to_web=true#fullScreenChart
#search kline
peroid:
30 30minutes
60 60minutes
101 day
102 week
103 season
104 year
'''

def get_kline_data2(code=None, count=None, period=None):
    
    #1:sh  0:sz
    stock_type = '1.'
    data_df = pd.DataFrame()

    timestamp=str(round(time.time() * 1000))

    if code == None:
        return data_df
    
    if count == None:
        count = 700
    
    if period == None:
        period = 101

    if code[0] == '6':
        stock_type = '1.'
    else:
        stock_type = '0.'

    url= 'http://50.push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery1124032193835566918194_1624510339003&secid=0.300582&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&klt=101&fqt=0&end=20500101&lmt=5&_=1624510339048'

    url= 'http://50.push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery1124032193835566918194_'\
        + timestamp\
        + '&secid='\
        + stock_type\
        + code\
        + '&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&'\
        + 'fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&klt='\
        + str(period)\
        + '&fqt=0&end=20500101&lmt='\
        + str(count)\
        + '&_='\
        + timestamp

    my_dbg(url)


    browser = get_browser()

    html = ''
    try:
        browser.get(url)
        browser.implicitly_wait(5)
        html = browser.page_source
    except Exception as e:
        my_dbg(e)
        browser.close()
        browser.quit()
    finally:
        browser.close()
        browser.quit()


    if debug:
        my_dbg(html)

    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, html)
    try:
        api_param = json.loads(response_array[0])
    except Exception as e:
        my_dbg(e)
    finally:
        pass

    name = api_param['data']['name']

    rawdata = api_param['data']['klines']
    tmp_column = [ 'record_date', 'open', 'close', 'high', 'low', 'volume', 'amount', \
            'amplitude', 'percent', 'chg', 'turnoverrate' ]


    data_df = pd.DataFrame(rawdata)


    if len(data_df):
        data_df = data_df[0].str.split(',', expand=True)
        data_df.columns=tmp_column
        data_df.insert(1, 'stock_name', name, allow_duplicates=False)
        data_df.insert(1, 'stock_code', code, allow_duplicates=False)
        
        new_column = ['record_date', 'stock_code', 'stock_name', 'open', 'close', 'high', 'low',\
                        'volume', 'amount', 'amplitude', 'percent', 'chg', 'turnoverrate']

        data_df = data_df.loc[:, new_column]

    if debug:
        my_dbg(data_df)
        
    return data_df, api_param


#bankuai
'''
    N     zxj  percent chg turnoverrate code    N     name      mkt_cap      up_num   down_num   lz_name   lz_code    N    lz_pct  N  last_name last_code    N  last_pct
    f1    f2   f3      f4     f8        f12    f13    f14         f20         f104     f105       f128      f140     f141   f136  f152   f207    f208      f209  f222
    2   411186  110    4492   29      BK0475   90     银行  14185163776000    39       3         中信银行   601998      1   365     2   上海银行  601229     1     -9
'''
def get_bk_data():
    
    data_df = pd.DataFrame()

    timestamp=str(round(time.time() * 1000))

    #https://data.eastmoney.com/bkzj/hy.html
    #https://push2.eastmoney.com/api/qt/clist/get?np=1&fltt=1&invt=2&cb=jQuery37102897695379394136_1747924930196&fs=m%3A90%2Bt%3A2%2Bf%3A!50&fields=f12%2Cf13%2Cf14%2Cf1%2Cf2%2Cf4%2Cf3%2Cf152%2Cf20%2Cf8%2Cf104%2Cf105%2Cf128%2Cf140%2Cf141%2Cf207%2Cf208%2Cf209%2Cf136%2Cf222&fid=f3&pn=1&pz=100&po=1
    url = 'https://push2.eastmoney.com/api/qt/clist/get?np=1&fltt=1&invt=2&cb=jQuery37102897695379394136_'\
        + timestamp\
        + '&fs=m%3A90%2Bt%3A2%2Bf%3A!50&fields=f12%2Cf13%2Cf14%2Cf1%2Cf2%2Cf4%2Cf3%2Cf152%2Cf20%2Cf8%2Cf104%2Cf105%2Cf128%2Cf140%2Cf141%2Cf207%2Cf208%2Cf209%2Cf136%2Cf222&'\
        + 'fid=f3&pn=1&pz=100&po=1'
        
    my_dbg(url)

    browser = get_browser()

    html = ''
    try:
        browser.get(url)
        browser.implicitly_wait(5)
        html = browser.page_source
    except Exception as e:
        my_dbg(e)
        browser.close()
        browser.quit()
    finally:
        browser.close()
        browser.quit()


    if debug:
        my_dbg(html)

    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, html)
    try:
        api_param = json.loads(response_array[0])
        name = api_param['data']['diff']
        data_df = pd.DataFrame(name)
    except Exception as e:
        my_dbg(e)
        return data_df
    finally:
        pass


    if debug:
        my_dbg(data_df)

    if 'f1' in data_df.columns:
        del data_df['f1']

    if 'f13' in data_df.columns:
        del data_df['f13']

    if 'f141' in data_df.columns:
        del data_df['f141']

    if 'f152' in data_df.columns:
        del data_df['f152']

    if 'f209' in data_df.columns:
        del data_df['f209']

    cols = ['zxj', 'percent', 'chg', 'turnoverrate', 'stock_code', 'stock_name', 'mkt_cap',\
           'up_num', 'down_num', 'lz_name', 'lz_code', 'lz_percent', 'last_name', 'last_code', 'last_percent']

    my_dbg(data_df)

    data_df.columns = cols

    record_date = time.strftime("%Y-%m-%d", time.localtime())
    
    data_df.insert(0, 'record_date' , record_date, allow_duplicates=False)


    data_df['zxj'] = data_df['zxj'].apply(lambda i: i/100)
    data_df['percent'] = data_df['percent'].apply(lambda i: i/100)
    data_df['chg'] = data_df['chg'].apply(lambda i: i/100)
    data_df['turnoverrate'] = data_df['turnoverrate'].apply(lambda i: i/100)
    data_df['lz_percent'] = data_df['lz_percent'].apply(lambda i: i/100)
    data_df['last_percent'] = data_df['last_percent'].apply(lambda i: i/100)


    #add zig
    data_df['zig'] = 0

    return data_df



if __name__ == '__main__':

    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    df, api_param = get_kline_data('000977', 10)
    
    df, api_param = get_kline_data2('000977', 10)

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    my_dbg("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    my_dbg("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
