#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
from HData_eastmoney_day import *
import  datetime
import time 

import pandas as pd
import numpy as np

import sys
import os

from file_interface import *
from get_kline_data import *
from get_realtime_data import *


debug=0
#debug=1

para1 = 0

hdata_day=HData_eastmoney_day("usr","usr")


'''
'''


#sys.exit()

def handle_raw_df(df):
    
    if len(df) == 0:
        return df

    if 'pre_close' not in df.columns:
        df['pre_close'] = 0
    if 'pe' not in df.columns:
        df['pe'] = 0
    if 'pb' not in df.columns:
        df['pb'] = 0
    if 'mkt_cap' not in df.columns:
        df['mkt_cap'] = 0
    if 'circulation_mkt' not in df.columns:
        df['circulation_mkt'] = 0
    if 'zlje' not in df.columns:
        df['zlje'] = 0

    #add is_peach is_zig is_quad
    if 'is_peach' not in df.columns:
        df['is_peach'] = 0
    if 'is_zig' not in df.columns:
        df['is_zig'] = 0
    if 'is_quad' not in df.columns:
        df['is_quad'] = 0
    if 'is_macd' not in df.columns:
        df['is_macd'] = 0
    if 'is_2d3pct' not in df.columns:
        df['is_2d3pct'] = 0
    if 'is_up_days' not in df.columns:
        df['is_up_days'] = 0
    if 'is_cup_tea' not in df.columns:
        df['is_cup_tea'] = 0
    if 'is_duck_head' not in df.columns:
        df['is_duck_head'] = 0
    if 'is_cross3line' not in df.columns:
        df['is_cross3line'] = 0

    df=df.fillna(0)
   
    #rename column
    new_cols = ['record_date', 'stock_code', 'stock_name', 'open', 'close', 'high', 'low',\
        'volume', 'amount', 'amplitude', 'percent', 'chg', 'turnoverrate',\
        'pre_close', 'pe', 'pb', 'mkt_cap', 'circulation_mkt', 'zlje',\
        'is_peach' , 'is_zig' , 'is_quad' ,\
        'is_macd', 'is_2d3pct', 'is_up_days', 'is_cup_tea', 'is_duck_head', 'is_cross3line' ]
        
    #resort conlums
    df = df[new_cols]
   
    return df
    

def check_table():
    table_exist = hdata_day.table_is_exist() 
    print('table_exist=%d' % table_exist)
    if table_exist:
        if int(para1):
            #hdata_day.db_hdata_eastmoney_create()
            print('table already exist, recreate')
    else:
        hdata_day.db_hdata_eastmoney_create()
        print('table not exist, create')



if __name__ == '__main__':

    cript_name, para1 = check_input_parameter()

    if int(para1) == 0:
        f_day = get_file_modify_day('r_df_today.csv')
        if f_day != 0:
            print(' exit... ')
            sys.exit(0)
    
    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    nowdate=datetime.datetime.now().date()
    nowdate=nowdate-datetime.timedelta(int(para1))
    print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d")))

    #check table exist
    check_table()

    r_df, api_param = get_realtime_data2()
    r_df = handle_raw_df(r_df)

    if int(para1):
        print('get kline data')
        r_len = len(r_df)
        if r_len:
            for i in range(0, r_len):
                code = r_df['stock_code'][i]
                k_df, api_param = get_kline_data2(code, 700)
                k_df = handle_raw_df(k_df)
                if k_df is None:
                    continue

                if len(k_df):
                    k_df.to_csv('./csv/k_df_' + code + '.csv', encoding='gbk')
                    hdata_day.copy_from_stringio(k_df)
    else:
        print('today data')
        r_df.to_csv('./csv/r_df_today.csv', encoding='gbk')
        hdata_day.delete_data_from_hdata(
                start_date=datetime.datetime.now().date().strftime("%Y-%m-%d"),
                end_date=datetime.datetime.now().date().strftime("%Y-%m-%d")
                )
        hdata_day.copy_from_stringio(r_df)
        #hdata_day.insert_all_stock_data_3(r_df)
        

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
