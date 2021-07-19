#!/#!/usr/bin/env python
# -*- coding: utf-8 -*-


import pandas as pd
import json
import requests
import re

import time
import datetime



'''
http://data.eastmoney.com/zjlx/list.html
To get industry info
https://push2.eastmoney.com/api/qt/clist/get?cb=jQuery1123022981019595514018_1626678575964&fid=f184&po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&fields=f2%2Cf3%2Cf12%2Cf13%2Cf14%2Cf62%2Cf184%2Cf225%2Cf165%2Cf263%2Cf109%2Cf175%2Cf264%2Cf160%2Cf100%2Cf124%2Cf265%2Cf1&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2
'''


debug=1
debug=1

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


def get_zlpm_data():
    
    nowdate=datetime.datetime.now().date()
    if debug:
        print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d")))
    
    timestamp=str(round(time.time() * 1000))

    url = 'https://push2.eastmoney.com/api/qt/clist/get?cb=jQuery1123022981019595514018_'\
            + timestamp \
            + '&fid=f184&po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&'\
            + 'fields=f2%2Cf3%2Cf12%2Cf13%2Cf14%2Cf62%2Cf184%2Cf225%2Cf165%2Cf263%2Cf109%2Cf175%2Cf264%2Cf160%2Cf100%2Cf124%2Cf265%2Cf1&'\
            + 'ut=b2884a393a59ad64002292a3e90d46a5&'\
            + 'fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2'

    tmp_header = get_headers()
    response = requests.get(url, headers=tmp_header)

    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, response.text)
    api_param = json.loads(response_array[0])
    rawdata = api_param['data']['diff']
    data_df = pd.DataFrame(rawdata)
    tmp_column = ['close', 'percent', 'stock_code', 'stock_name', 'industry', 'percent_5day', 'percent_10day', \
            'zljzb_5day', 'zljzb_10day', 'zljzb', 'zljzb_pm', 'zljzb_pm_5day', 'zljzb_pm_10day' ]


    if len(data_df):
        del data_df['f1']
        del data_df['f13']
        del data_df['f62']
        del data_df['f124']
        del data_df['f265']
        data_df = data_df.replace('-',0)
        data_df.columns = tmp_column	
        data_df.insert(1, 'record_date', nowdate.strftime("%Y-%m-%d"), allow_duplicates=False)

        new_column = ['stock_code', 'stock_name', 'close', 'percent',  'industry', 'percent_5day', 'percent_10day', \
            'zljzb_5day', 'zljzb_10day', 'zljzb', 'zljzb_pm', 'zljzb_pm_5day', 'zljzb_pm_10day' ]


        data_df = data_df.loc[:, new_column]

        #data_df.to_csv('./csv/real-' + nowdate.strftime("%Y-%m-%d")+ '.csv', encoding='gbk')

        if debug:
            print(data_df)

        data_df = data_df.sort_values('stock_code', ascending=1)
        data_df = data_df.reset_index(drop=True)

    return data_df, api_param






if __name__ == '__main__':

    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    df, api_param = get_zlpm_data()

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))


