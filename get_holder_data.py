#!/#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import json
import requests
import re

import time
import datetime

import random

'''
一次获取最新全部户数
https://data.eastmoney.com/gdhs/
https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery11230745007796091407_1626758636178&sortColumns=HOLD_NOTICE_DATE%2CSECURITY_CODE&sortTypes=-1%2C-1&pageSize=50&pageNumber=1&reportName=RPT_HOLDERNUMLATEST&columns=SECURITY_CODE%2CSECURITY_NAME_ABBR%2CEND_DATE%2CINTERVAL_CHRATE%2CAVG_MARKET_CAP%2CAVG_HOLD_NUM%2CTOTAL_MARKET_CAP%2CTOTAL_A_SHARES%2CHOLD_NOTICE_DATE%2CHOLDER_NUM%2CPRE_HOLDER_NUM%2CHOLDER_NUM_CHANGE%2CHOLDER_NUM_RATIO%2CEND_DATE%2CPRE_END_DATE&quoteColumns=f2%2Cf3&source=WEB&client=WEB

一次获取一只股票历史户数
https://data.eastmoney.com/gdhs/detail/600212.html
'''

debug=0
debug=0

def get_headers():
    '''
    随机获取一个headers
    '''
    user_agents =  ['Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1',\
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',\
            'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11']

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


def get_holder_data(is_all=1, pagesize=500, pagenumber=1):
    timestamp=str(round(time.time() * 1000))
    df = pd.DataFrame()
    api_param=''

    url_all = 'https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery112305269055184325129_'\
            + timestamp \
            + '&reportName=RPT_HOLDERNUM_DET'\
            + '&columns=SECURITY_CODE%2CSECURITY_NAME_ABBR%2C'\
            + 'CHANGE_SHARES%2CCHANGE_REASON%2C'\
            + 'END_DATE%2CINTERVAL_CHRATE%2C'\
            + 'AVG_MARKET_CAP%2CAVG_HOLD_NUM%2C'\
            + 'TOTAL_MARKET_CAP%2CTOTAL_A_SHARES%2CHOLD_NOTICE_DATE%2C'\
            + 'HOLDER_NUM%2C'\
            + 'PRE_HOLDER_NUM%2CHOLDER_NUM_CHANGE%2CHOLDER_NUM_RATIO%2CEND_DATE%2CPRE_END_DATE'\
            + '&pageSize='\
            + str(pagesize) \
            + '&pageNumber='\
            + str(pagenumber) 

    url_latest = 'https://datacenter-web.eastmoney.com/api/data/v1/get?callback'\
            + '=jQuery11230745007796091407_'\
            + timestamp \
            + '&sortColumns=HOLD_NOTICE_DATE%2CSECURITY_CODE&sortTypes=-1%2C-1&pageSize='\
            + str(pagesize) \
            + '&pageNumber='\
            + str(pagenumber) \
            + '&reportName=RPT_HOLDERNUMLATEST'\
            + '&columns=SECURITY_CODE%2CSECURITY_NAME_ABBR%2C'\
            + 'CHANGE_SHARES%2CCHANGE_REASON%2C'\
            + 'END_DATE%2CINTERVAL_CHRATE%2C'\
            + 'AVG_MARKET_CAP%2CAVG_HOLD_NUM%2C'\
            + 'TOTAL_MARKET_CAP%2CTOTAL_A_SHARES%2CHOLD_NOTICE_DATE%2C'\
            + 'HOLDER_NUM%2C'\
            + 'PRE_HOLDER_NUM%2CHOLDER_NUM_CHANGE%2CHOLDER_NUM_RATIO%2CEND_DATE%2CPRE_END_DATE'
    
    
    if is_all == 1:
        url = url_all
    else:
        url = url_latest

    if debug:
        print('url= %s ' % url)

    tmp_header = get_headers()
    response = requests.get(url, headers=tmp_header)

    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, response.text)

    api_param = json.loads(response_array[0])
    rawdata = api_param['result']['data']
    df = pd.DataFrame(rawdata)

    return df, api_param


if __name__ == '__main__':

    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    df, api_param = get_holder_data(is_all=0, pagesize=500, pagenumber=9)
    print(df.columns)
    print(df)

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))

