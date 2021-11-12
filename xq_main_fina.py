#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
from HData_xq_fina import *
from HData_xq_income import *
from HData_xq_balance import *
from HData_xq_cashflow import *

import time 
import datetime

import pandas as pd
import numpy as np

import sys
import os

from xq_get_basic_data import * 
from file_interface import * 


debug=0
debug=0


hdata_fina=HData_xq_fina("usr","usr")
hdata_income=HData_xq_income("usr","usr")
hdata_balance=HData_xq_balance("usr","usr")
hdata_cashflow=HData_xq_cashflow("usr","usr")


def xq_handle_raw_df(df):

    df=df.fillna(0)
   
    #timestamp -> date
    if 'report_date' in df.columns:
        df['report_date'] = df['report_date'].apply(lambda x: get_date_from_timestamp(x))

    if 'ctime' in df.columns:
        df['ctime'] = df['ctime'].apply(lambda x: get_date_from_timestamp(x))

    if 'report_date' in df.columns:
        df = df[df['report_date'] != '1970-01-01']
    
    return df
    

def xq_get_original_data(datatype=None):

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
        tmp_df = xq_get_fina_data(stock_code_new, datatype, def_cnt=20)
        
        if len(tmp_df):
            pass
        else:
            print('stock_code_new=%s, len(df)=0, #error# abnormal'\
                    % stock_code_new)
            continue

        if debug:
            print(i, stock_code_new)
            print(tmp_df)
        #add stock_code
        #tmp_df['symbol'] = stock_code_new
        tmp_df.insert(1, 'symbol' , stock_code_new, allow_duplicates=False)
        df = pd.concat([df, tmp_df])

        #debug
        if( 0 ):
            if i > 5:
                return df

        if i % (mod-1) == 0:
            t_2 = time.time()
            d_t = t_2 - t_1
            if debug:
                print(t_1, t_2)
                print('xq_get_fina() i=%d, stock_code_new =%s ,d_t=%f, len(tmp_df)=%d' % \
                        (i, stock_code_new, d_t, len(tmp_df)))

    tt_2 = time.time()
    delta_t = tt_2 - tt_1
    if debug:
        print('xq_get_fina() delta_t=%f' % delta_t)
        print('len(list(df))=%d' % len(list(df)))
        print('list(df)=%s' % list(df))

    return df

def xq_get_fina(datatype=None):

    df = xq_get_original_data(datatype)

    df = xq_handle_raw_df(df)
    df = df.reset_index(drop=True)

    if debug:
        print('len(df)=%d' % len(df))
        print(df.head(10))

    return df


def check_table():
    table_exist = hdata_fina.table_is_exist() 
    print('table_exist=%d' % table_exist)
    if table_exist:
        hdata_fina.db_hdata_xq_create()
        print('table already exist')
    else:
        hdata_fina.db_hdata_xq_create()
        print('table not exist, create')


def update_database_indicator():
    print('#indicator zhuyao caiwu zhibiao')
    df_indicator = xq_get_fina()
    df_indicator.to_csv('./csv/test_indicator.csv', encoding='gbk')
    df_indicator = df_indicator.drop_duplicates(subset=['report_date', 'symbol'], keep='first')
    if len(df_indicator):
        hdata_fina.db_hdata_xq_create()
        hdata_fina.copy_from_stringio(df_indicator)
    pass


def spilt_df(df, key_word):
    df_colunms = df.columns
    index = len(df_colunms) - 1
    try:
        index = df_colunms.get_loc(key_word)
    except:
        print('error: %s not found' % key_word)
    else:
        pass

    #cut 0-[index+1] column
    df_tmp = df.iloc[:, :index+1]

    return df_tmp

def update_database_income():
    print('#income  net profit')
    df_income = xq_get_fina(datatype='income')
    df_income.to_csv('./csv/test_income.csv', encoding='gbk')
    df_income = df_income.drop_duplicates(subset=['report_date', 'symbol'], keep='first')
    df_income = spilt_df(df_income,'operating_total_cost_si_new')
    if len(df_income):
        hdata_income.db_hdata_xq_create()
        hdata_income.copy_from_stringio(df_income)
    pass

def update_database_balance():
    print('#balance zichan fuzhai biao')
    df_balance = xq_get_fina(datatype='balance')
    df_balance.to_csv('./csv/test_balance.csv', encoding='gbk')
    df_balance = df_balance.drop_duplicates(subset=['report_date', 'symbol'], keep='first')
    df_balance = spilt_df(df_balance,'lt_staff_salary_payable_new')
    if len(df_balance):
        hdata_balance.db_hdata_xq_create()
        hdata_balance.copy_from_stringio(df_balance)
    pass
    
def update_database_cashflow():
    print('#cashflow xianjinliuliang biao')
    df_cashflow = xq_get_fina(datatype='cashflow')
    df_cashflow.to_csv('./csv/test_cashflow.csv', encoding='gbk')
    df_cashflow = df_cashflow.drop_duplicates(subset=['report_date', 'symbol'], keep='first')
    df_cashflow = spilt_df(df_cashflow,'net_increase_in_pledge_loans_new')
    if len(df_cashflow):
        hdata_cashflow.db_hdata_xq_create()
        hdata_cashflow.copy_from_stringio(df_cashflow)
    pass



if __name__ == '__main__':
    

    cript_name, para1 = check_input_parameter()
    
    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    nowdate=datetime.datetime.now().date()
    nowdate=nowdate-datetime.timedelta(int(para1))
    print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d")))

    update_database_indicator()
    update_database_income()
    update_database_balance()
    update_database_cashflow()

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
