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
debug=0

pagesize=600

para1 = 0

hdata_fina=HData_eastmoney_fina("usr","usr")


'''
'''


#sys.exit()

def check_table():
    table_exist = hdata_fina.table_is_exist() 
    my_dbg('table_exist=%d' % table_exist)
    if table_exist:
        hdata_fina.db_hdata_eastmoney_create()
        my_dbg('table already exist')
    else:
        hdata_fina.db_hdata_eastmoney_create()
        my_dbg('table not exist, create')

       

if __name__ == '__main__':

    script_name, para1 = check_input_parameter()
    
    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    nowdate=datetime.datetime.now().date()
    nowdate=nowdate-datetime.timedelta(int(para1))
    my_dbg("nowdate is %s"%(nowdate.strftime("%Y-%m-%d")))

    
    if int(para1):
        my_dbg('get history fina data')
        get_all = 1
    else:
        get_all = 0

    df = pd.DataFrame()
    i = 1
    #每次最多只能得到500条数据
    while (1):
        try:
            f_df, api_param = get_fina_data3('cpd', i)
            if len(f_df):

                if debug:
                    my_dbg(f_df)
                    my_dbg(f_df.columns)
                    f_df.to_csv('./csv/f_df_' + str(i) + '.csv', encoding='gbk')

                df = pd.concat([df, f_df])

            
        except Exception as e:
            my_dbg(e)
            break
        else:
            i = i + 1
            if i>pagesize:
                break

    df.to_csv('./csv/' + nowdate.strftime("%Y-%m-%d") + '_fina_all_df_' + str(i) + '.csv', encoding='gbk')
    try :
        df = df.drop_duplicates(subset=['security_code', 'reportdate'], keep='first')
    except Exception as e:
        my_dbg(e)
    
    try :
        df = df.drop_duplicates(subset=['security_code', 'report_date'], keep='first')
    except Exception as e:
        my_dbg(e)


    #delete new add columns, 2025-3-30
    need_del = ['board_name', 'ori_board_code', 'board_code']
    for i, cols in enumerate(need_del):
        my_dbg(cols)
        if cols in df.columns:
            del df[cols]

    if len(df) > 0:
        #check table exist
        check_table()
       
        if get_all == 1:
            hdata_fina.copy_from_stringio(df)
        else:
            #PostgreSQL数据库如果不存在则插入，存在则更新
            hdata_fina.insert_all_stock_data_2(df)
    else:
        my_dbg('fina dataframe is null')


    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    my_dbg("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    my_dbg("%s t1:%s, t2:%s, delta=%s"%(script_name, t1, t2, t2-t1))


#read csv data, then import to database
'''
df = pd.read_csv('./csv/fina_all_df_601.csv',encoding='gbk', index_col=[0], dtype={'security_code':str})
df = df.drop_duplicates(subset=['security_code', 'reportdate'], keep='first')
df = df.reset_index(drop=True)
hdata_fina.copy_from_stringio(df)
'''
