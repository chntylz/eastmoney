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

