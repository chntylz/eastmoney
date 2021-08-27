#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
from HData_eastmoney_fina import *
import  datetime
import time 

import pandas as pd
import numpy as np

import sys
import os

from file_interface import *
from get_fina_data import *


debug=0
debug=1

para1 = 0

hdata_fina=HData_eastmoney_fina("usr","usr")


'''
'''


#sys.exit()

def check_table():
    table_exist = hdata_fina.table_is_exist() 
    print('table_exist=%d' % table_exist)
    if table_exist:
        #hdata_fina.db_hdata_eastmoney_create()
        print('table already exist')
    else:
        hdata_fina.db_hdata_eastmoney_create()
        print('table not exist, create')

       

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
        print('get history fina data')
        get_all = 1
    else:
        get_all = 0

    df = pd.DataFrame()
    i = 1
    #每次最多只能得到500条数据
    while (1):
        try:
            f_df, api_param = get_fina_data(i)
            time.sleep(10)
            if len(f_df):

                if debug:
                    print(f_df)
                    print(f_df.columns)
                    f_df.to_csv('./csv/f_df_' + str(i) + '.csv', encoding='gbk')

                df = pd.concat([df, f_df])

            
        except Exception as e:
            print(e)
            break
        else:
            i = i + 1

    df.to_csv('./csv/fina_all_df_' + str(i) + '.csv', encoding='gbk')
    df = df.drop_duplicates(subset=['security_code', 'reportdate'], keep='first')
   
    if get_all == 1:
        hdata_fina.copy_from_stringio(df)
    else:
        #PostgreSQL数据库如果不存在则插入，存在则更新
        hdata_fina.insert_all_stock_data_2(df)

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
