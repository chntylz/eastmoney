#!/#!/usr/bin/env python
# -*- coding: utf-8 -*-


import pandas as pd
import json
import requests
import re

import time
import datetime

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
import time, datetime
import pandas as pd
import os


import bs4
import json


debug=1
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

def get_realtime_data():
    
    nowdate=datetime.datetime.now().date()
    if debug:
        print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d")))
    
    timestamp=str(round(time.time() * 1000))
    url='https://61.push2.eastmoney.com/api/qt/clist/get?cb'\
            + '=jQuery1124044204950317612046_'\
            + timestamp\
            + '&pn=1&pz=10000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&'\
            + 'fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23&'\
            + 'fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,'\
            + 'f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152&_='\
            + timestamp

    print(url)
    tmp_header = get_headers()
    print(tmp_header)
    response = requests.get(url, headers=tmp_header)
    print(response)

    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, response.text)
    api_param = json.loads(response_array[0])
    rawdata = api_param['data']['diff']
    data_df = pd.DataFrame(rawdata)
    tmp_column = ['close', 'percent', 'chg', 'volume', 'amount', 'amplitude', 'turnoverrate', \
            'pe', 'stock_code', 'stock_name', 'high', 'low', 'open', 'pre_close', \
            'mkt_cap', 'circulation_mkt','pb','zlje']


    if len(data_df):
        del data_df['f1']
        del data_df['f10']
        del data_df['f11']
        del data_df['f13']
        del data_df['f22']
        del data_df['f24']
        del data_df['f25']
        del data_df['f115']
        del data_df['f128']
        del data_df['f140']
        del data_df['f141']
        del data_df['f136']
        del data_df['f152']
        data_df = data_df.replace('-',0)
        data_df.columns = tmp_column	
        data_df.insert(1, 'record_date', nowdate.strftime("%Y-%m-%d"), allow_duplicates=False)

        new_column = ['record_date', 'stock_code', 'stock_name', 'open', 'close', 'high', 'low',\
            'volume', 'amount', 'amplitude', 'percent', 'chg', 'turnoverrate',\
            'pre_close', 'pe', 'pb', 'mkt_cap', 'circulation_mkt', 'zlje' ]

        data_df = data_df.loc[:, new_column]

        #data_df.to_csv('./csv/real-' + nowdate.strftime("%Y-%m-%d")+ '.csv', encoding='gbk')

        if debug:
            print(data_df)

        data_df = data_df.sort_values('stock_code', ascending=1)
        data_df = data_df.reset_index(drop=True)

    return data_df, api_param





def get_realtime_data2():
    
    # 添加无头headlesss
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    browser = webdriver.Chrome(chrome_options=chrome_options)

    # browser = webdriver.PhantomJS() # 会报警高提示不建议使用phantomjs，建议chrome添加无头
    browser.maximize_window()  # 最大化窗口
    wait = WebDriverWait(browser, 10)


    nowdate=datetime.datetime.now().date()
    if debug:
        print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d")))
    
    timestamp=str(round(time.time() * 1000))
    url='https://61.push2.eastmoney.com/api/qt/clist/get?cb'\
            + '=jQuery1124044204950317612046_'\
            + timestamp\
            + '&pn=1&pz=10000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&'\
            + 'fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23&'\
            + 'fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,'\
            + 'f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152&_='\
            + timestamp

    print(url)

    html = ''
    try:
        browser.get(url)
        time.sleep(3)
        html = browser.page_source
    except:
        browser.close()
        browser.quit()
    finally:
        browser.close()
        browser.quit()


    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, html)
    api_param = json.loads(response_array[0])
    rawdata = api_param['data']['diff']
    data_df = pd.DataFrame(rawdata)


    tmp_column = ['close', 'percent', 'chg', 'volume', 'amount', 'amplitude', 'turnoverrate', \
            'pe', 'stock_code', 'stock_name', 'high', 'low', 'open', 'pre_close', \
            'mkt_cap', 'circulation_mkt','pb','zlje']


    if len(data_df):
        del data_df['f1']
        del data_df['f10']
        del data_df['f11']
        del data_df['f13']
        del data_df['f22']
        del data_df['f24']
        del data_df['f25']
        del data_df['f115']
        del data_df['f128']
        del data_df['f140']
        del data_df['f141']
        del data_df['f136']
        del data_df['f152']
        data_df = data_df.replace('-',0)
        data_df.columns = tmp_column	
        data_df.insert(1, 'record_date', nowdate.strftime("%Y-%m-%d"), allow_duplicates=False)

        new_column = ['record_date', 'stock_code', 'stock_name', 'open', 'close', 'high', 'low',\
            'volume', 'amount', 'amplitude', 'percent', 'chg', 'turnoverrate',\
            'pre_close', 'pe', 'pb', 'mkt_cap', 'circulation_mkt', 'zlje' ]

        data_df = data_df.loc[:, new_column]

        #data_df.to_csv('./csv/real-' + nowdate.strftime("%Y-%m-%d")+ '.csv', encoding='gbk')

        if debug:
            print(data_df)

        #all
        data_df = data_df.sort_values('stock_code', ascending=1)
        data_df = data_df.reset_index(drop=True)

        #stopped
        stop_df = data_df[data_df['close'].isin([0])] 
        stop_df = stop_df.reset_index(drop=True)

        #worked
        work_df = data_df[~data_df['close'].isin([0])] 
        work_df = work_df.reset_index(drop=True)

    return data_df, work_df, stop_df, api_param






if __name__ == '__main__':

    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    data_df, work_df, stop_df, api_param = get_realtime_data2()

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))


