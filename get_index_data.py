#!/#!/usr/bin/env python
# -*- coding: utf-8 -*-


import pandas as pd
import json
import requests
import re

import time
import datetime


debug=1
debug=0

import random
def get_headers():
    '''
    随机获取一个headers
    '''
    user_agents =  ['Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1',\
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',\
            'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11']
    headers = {'User-Agent':random.choice(user_agents)}
    return headers


def get_index_data():
    
    nowdate=datetime.datetime.now().date()
    if debug:
        print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d")))
    
    timestamp=str(round(time.time() * 1000))

    url='http://52.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112408473004545667344_'\
    + timestamp\
        + '&pn=1&pz=500&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281'\
        + '&fltt=2&invt=2&fid=&fs=b:MK0010'\
        + '&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,'\
        + 'f20,f21,f23,f24,f25,f26,f22,f11,f62,f128,f136,f115,f152'

    
    tmp_header = get_headers()
    response = requests.get(url, headers=tmp_header)

    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, response.text)
    api_param = json.loads(response_array[0])
    rawdata = api_param['data']['diff']
    df = pd.DataFrame(rawdata)
    
    tmp_column=[ 'close', 'percent', 'chg', 'volume', 'amount', \
            'code', 'name', 'high', 'low', 'open', 'pre_close']

    if len(df):
        del df['f1']
        del df['f7']
        del df['f8']
        del df['f9']
        del df['f10']
        del df['f11']
        del df['f13']
        del df['f20']
        del df['f21']
        del df['f22']
        del df['f23']
        del df['f24']
        del df['f25']
        del df['f26']
        del df['f62']


        del df['f115']
        del df['f128']
        del df['f140']
        del df['f141']
        del df['f136']
        del df['f152']

        df = df.replace('-',0)
        #df rename
        df.columns = tmp_column	
        df.insert(1, 'record_date', nowdate.strftime("%Y-%m-%d"), allow_duplicates=False)

        new_column = ['record_date', 'code', 'name', 'open', 'close', 'high', 'low',\
            'volume', 'amount', 'percent', 'chg', 'pre_close', ]

        df = df.loc[:, new_column]

        #df.to_csv('./csv/real-' + nowdate.strftime("%Y-%m-%d")+ '.csv', encoding='gbk')

        if debug:
            print(df)

    return df, api_param






if __name__ == '__main__':

    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    df, api_param = get_index_data()

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))


