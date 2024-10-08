#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
from HData_eastmoney_holder import *
import  datetime
import time 

import pandas as pd
import numpy as np

import sys
import os

from file_interface import *
from get_holder_data import *


debug=0
debug=0

para1 = 0

hdata_holder=HData_eastmoney_holder("usr","usr")


'''
'''


#sys.exit()

def check_table():
    table_exist = hdata_holder.table_is_exist() 
    print('table_exist=%d' % table_exist)
    if table_exist:
        hdata_holder.db_hdata_eastmoney_create()
        print('table already exist')
    else:
        hdata_holder.db_hdata_eastmoney_create()
        print('table not exist, create')

def get_date_from_str(x):
    if x != 0:
        x=x[0:10]
    else:
        x='1900-01-01'
    return x
        

if __name__ == '__main__':

    cript_name, para1 = check_input_parameter()
    
    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    nowdate=datetime.datetime.now().date()
    nowdate=nowdate-datetime.timedelta(int(para1))
    print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d")))

    #check table exist
    check_table()

    
    if int(para1):
        print('get history holder data')
        get_all = 1
    else:
        get_all = 0

    df = pd.DataFrame()
    i = 1
    #每次最多只能得到500条数据
    while (1):
        try:
            h_df, api_param = get_holder_data2(is_all=get_all, pagesize=500, pagenumber=i)
            if len(h_df):
                h_df = h_df.fillna(0)

                #if 'SECURITY_CODE' in h_df.columns:
                #    h_df['SECURITY_CODE'] = h_df['SECURITY_CODE'].apply(lambda x: str(x))

                if 'END_DATE' in h_df.columns:
                    h_df['END_DATE'] = h_df['END_DATE'].apply(lambda x: get_date_from_str(x))

                if 'HOLD_NOTICE_DATE' in h_df.columns:
                    h_df['HOLD_NOTICE_DATE'] = h_df['HOLD_NOTICE_DATE'].apply(lambda x: get_date_from_str(x))

                if 'PRE_END_DATE' in h_df.columns:
                    h_df['PRE_END_DATE'] = h_df['PRE_END_DATE'].apply(lambda x: get_date_from_str(x))

                if 'CHANGE_REASON' in h_df.columns:
                    h_df['CHANGE_REASON'] = h_df['CHANGE_REASON'].apply(lambda x: str(x).replace(',', '_') )

                if debug:
                    print(h_df)
                    print(h_df.columns)
                    h_df.to_csv('./csv/h_df_' + str(i) + '.csv', encoding='gbk')

                df = pd.concat([df, h_df])

            
        except Exception as e:
            print(e)
            break
        else:
            i = i + 1

            if i>300:
                break

    df.to_csv('./csv/'+ nowdate.strftime("%Y-%m-%d")+ '_holder_all_df_' + str(i) + '.csv', encoding='gbk')
    df = df.drop_duplicates(subset=['SECURITY_CODE', 'END_DATE'], keep='first')
    
    if get_all == 1:
        hdata_holder.copy_from_stringio(df)
    else:
        #PostgreSQL数据库如果不存在则插入，存在则更新
        hdata_holder.insert_all_stock_data_2(df)


    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
