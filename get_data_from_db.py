#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import  datetime
import time 

import pandas as pd
import numpy as np

import sys
import os


import psycopg2 #使用的是PostgreSQL数据库
from HData_eastmoney_day import *
from HData_eastmoney_holder import *
from HData_eastmoney_zlpm import *

from HData_eastmoney_zlje import *
from HData_eastmoney_zlje_3 import *
from HData_eastmoney_zlje_5 import *
from HData_eastmoney_zlje_10 import *

from HData_xq_fina import *

debug=0


hdata_day    = HData_eastmoney_day("usr","usr")
hdata_zlpm   = HData_eastmoney_zlpm("usr","usr")
hdata_holder = HData_eastmoney_holder("usr","usr")

hdata_zlje    = HData_eastmoney_zlje('usr', 'usr')
hdata_zlje_3  = HData_eastmoney_zlje_3('usr', 'usr')
hdata_zlje_5  = HData_eastmoney_zlje_5('usr', 'usr')
hdata_zlje_10 = HData_eastmoney_zlje_10('usr', 'usr')

hdata_fina = HData_xq_fina('usr', 'usr')



'''
'''

def get_zlje(df, stock_code, url=None, curr_date=None):
    zlje =  0

    #zlje_df = get_zlje_data_from_db(url, curr_date)
    zlje_df = df

    tmp_zlje_df = zlje_df[zlje_df['stock_code'] == stock_code]
    tmp_zlje_df = tmp_zlje_df.reset_index(drop=True)
    if debug:
            print(new_code, len(tmp_zlje_df))

    if len(tmp_zlje_df):
        zlje = tmp_zlje_df['zlje'][0]
        zdf  = tmp_zlje_df['zdf'][0]
        zlje = str(zlje) + '<br>' + str(zdf) + '</br>'

    return zlje




def zlpm_data(stock_code, start_date, end_date, limit):
    df = hdata_zlpm.get_data_from_hdata(stock_code, start_date, end_date, limit)
    return df

def kline_data(stock_code, start_date, end_date, limit):
    df = hdata_day.get_data_from_hdata(stock_code, start_date, end_date, limit)
    return df
    


def get_zlje_data_from_db(url=None, curr_date=None):

    hdata_db = hdata_zlje

    if curr_date is None:
        nowdate=datetime.datetime.now().date()
        curr_date = nowdate.strftime('%Y-%m-%d') 
    
    if url == 'url_3':
        hdata_db = hdata_zlje_3
    elif url == 'url_5':
        hdata_db = hdata_zlje_5
    elif url == 'url_10':
        hdata_db = hdata_zlje_10
    else:
        hdata_db = hdata_zlje

    df = hdata_db.get_data_from_hdata(start_date=curr_date, end_date=curr_date)
    
    return df



if __name__ == '__main__':

    
    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    nowdate=datetime.datetime.now().date()
    print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d")))

       

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
