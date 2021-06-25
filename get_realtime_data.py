#!/#!/usr/bin/env python
# -*- coding: utf-8 -*-


import pandas as pd
import json
import requests
import re

import time
import datetime

debug=1

def get_realtime_data():
    
    timestamp=str(round(time.time() * 1000))
    url='https://61.push2.eastmoney.com/api/qt/clist/get?cb'\
            + '=jQuery1124044204950317612046_1624510179891&'\
            + 'pn=1&pz=10000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&'\
            + 'fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23&'\
            + 'fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,'\
            + 'f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152&_='\
            + timestamp

    response = requests.get(url)
    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, response.text)
    api_param = json.loads(response_array[0])
    rawdata = api_param['data']['diff']
    data_df = pd.DataFrame(rawdata)
    tmp_column = ['close', 'percent', 'change', 'volume', 'amount', 'amplitude', 'touroverrate', \
            'pe', 'stock_code', 'stock_city', 'stock_name', 'high', 'low', 'open', 'pre_clolse', \
            'mkt_cap', 'circulation_mkt','pb','zlje']


    if len(data_df):
        del data_df['f1']
        del data_df['f10']
        del data_df['f11']
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
        data_df.to_csv('real-6-25.csv', encoding='gbk')
        if debug:
            print(data_df)

    return data_df






if __name__ == '__main__':

    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    df = get_realtime_data()

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))


