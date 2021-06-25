#!/#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import json
import requests
import re

import time
import datetime

debug=1

def get_kline_data(code=None, count=None):
    
    #1:sh  0:sz
    stock_type = '1.'
    data_df = pd.DataFrame()

    timestamp=str(round(time.time() * 1000))

    if code == None:
        return data_df
    
    if count == None:
        count = 700
    

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
        + 'fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&klt=101&fqt=0&end=20500101&lmt='\
        + str(count)\
        + '&_='\
        + timestamp

    response = requests.get(url)
    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, response.text)
    api_param = json.loads(response_array[0])
    rawdata = api_param['data']['klines']
    tmp_column = [ 'record_date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'amplitude', 'percent', 'change', 'turnoverrate' ]


    data_df = pd.DataFrame(rawdata)


    if len(data_df):
        data_df = data_df[0].str.split(',', expand=True)
        data_df.columns=tmp_column

    if debug:
        print(data_df)
        
    return data_df



if __name__ == '__main__':

    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    df = get_kline_data('000977', 700)
    
    df = get_kline_data('603027', 700)

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
