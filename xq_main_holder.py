#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
from HData_xq_holder import *

import time 
import datetime

import pandas as pd
import numpy as np

import sys
import os

from xq_get_basic_data import * 
from file_interface import * 


debug=0
#debug=1

hdata_holder=HData_xq_holder("usr","usr")

def get_original_data():

    df = pd.DataFrame()
    codestock_local=xq_get_stock_list()
    length=len(codestock_local)
    tt_1 = time.time()
    t_1 = t_2 = 0
    mod = 1000
    for i in range(0,length):
        

        if i % mod == 0:
            t_1 = time.time()
        nowcode=codestock_local[i][1]
        if nowcode[0:1] == '6':
            stock_code_new= 'SH' + nowcode
        else:
            stock_code_new= 'SZ' + nowcode
        tmp_df = xq_get_holder_data(stock_code_new)

        if debug:
            print(i, stock_code_new)
            print(tmp_df)
        #add stock_code
        #tmp_df['symbol'] = stock_code_new
        if len(tmp_df):
            tmp_df.insert(1, 'symbol' , stock_code_new[2:], allow_duplicates=False)

            tmp_df = round(tmp_df, 2)

            if 'timestamp' in tmp_df.columns:
                tmp_df['timestamp'] = tmp_df['timestamp'].apply(lambda x: get_date_from_timestamp(int(x)))
                tmp_df = tmp_df.sort_values('timestamp', ascending=False)


            if 'price' in tmp_df.columns:
                tmp_df['tmp_price'] = tmp_df['price'].shift(-1)
                tmp_df['price_ratio'] = round((tmp_df['price'] - tmp_df['tmp_price'] ) * 100 / tmp_df['tmp_price'] , 2)
                del tmp_df['tmp_price']

                tmp_df = tmp_df.fillna(0)
                hdata_holder.copy_from_stringio(tmp_df)
            df = pd.concat([df, tmp_df])

        #debug
        if( 0 ):
            if i > 1:
                return df

        if i % (mod-1) == 0:
            t_2 = time.time()
            d_t = t_2 - t_1
            if debug:
                print(t_1, t_2)
                print('get_holder() i=%d, stock_code_new =%s ,d_t=%f, len(tmp_df)=%d' % \
                        (i, stock_code_new, d_t, len(tmp_df)))

    tt_2 = time.time()
    delta_t = tt_2 - tt_1
    if debug:
        print('get_holder() delta_t=%f' % delta_t)
        print('len(list(df))=%d' % len(list(df)))
        print('list(df)=%s' % list(df))

    return df

def get_holder():

    df = get_original_data()
    df = df.reset_index(drop=True)

    if debug:
        print('len(df)=%d' % len(df))
        print(df.head(10))

    return df


def check_table():
    table_exist = hdata_holder.table_is_exist() 
    print('table_exist=%d' % table_exist)
    if table_exist:
        hdata_holder.db_hdata_xq_create()
        print('table already exist')
    else:
        hdata_holder.db_hdata_xq_create()
        print('table not exist, create')


def update_database_holder():
    print('holder')
    hdata_holder.db_hdata_xq_create()
    df_holder = get_holder()
    df_holder.to_csv('./csv/test_holder.csv', encoding='gbk')



if __name__ == '__main__':
    

    
    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    nowdate=datetime.datetime.now().date()
    print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d")))

    update_database_holder()
    
    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
