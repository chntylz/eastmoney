#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
from HData_xq_fund import *

import time 
import datetime

import pandas as pd
import numpy as np

import sys
import os


import get_xq_data
from xq_get_basic_data import * 
from file_interface import * 

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait


debug=0
debug=0

hdata_fund=HData_xq_fund("usr","usr")

def get_fund_rawdata(report_date):

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
        tmp_df = xq_get_fund(stock_code_new, report_date)

        if debug:
            print(i, stock_code_new)
            print(tmp_df)
        #add stock_code
        #tmp_df['symbol'] = stock_code_new
        if len(tmp_df):
            tmp_df = tmp_df[tmp_df['org_name_or_fund_name'] == '全部合计']
            tmp_df.insert(0, 'record_date' , report_date, allow_duplicates=False)
            tmp_df.insert(0, 'stock_code'  , stock_code_new[2:], allow_duplicates=False)
            tmp_df = round(tmp_df, 2)
            tmp_df = tmp_df.fillna(0)

            #hdata_fund.copy_from_stringio(tmp_df)

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
                print(' get_fund_rawdata() i=%d, stock_code_new =%s ,d_t=%f, len(tmp_df)=%d' % \
                        (i, stock_code_new, d_t, len(tmp_df)))


    tt_2 = time.time()
    delta_t = tt_2 - tt_1
    if debug:
        print(' get_fund_rawdata() delta_t=%f' % delta_t)
        print('len(list(df))=%d' % len(list(df)))
        print('list(df)=%s' % list(df))

    return df

def get_fund(report_date):

    df = get_fund_rawdata(report_date)
    df = df.reset_index(drop=True)

    if debug:
        print('len(df)=%d' % len(df))
        print(df.head(10))

    return df


def check_table():
    table_exist = hdata_fund.table_is_exist() 
    print('table_exist=%d' % table_exist)
    if table_exist:
        hdata_fund.db_hdata_xq_create()
        print('table already exist')
    else:
        hdata_fund.db_hdata_xq_create()
        print('table not exist, create')


def update_database_fund(report_date):
    print('update_database_fund() %s' % report_date)
    df_fund = get_fund(report_date)
    hdata_fund.copy_from_stringio(df_fund)
    df_fund.to_csv('./csv/test_xq_fund_' + report_date +'.csv', encoding='gbk')



if __name__ == '__main__':
    
    get_xq_data._init()

# 添加无头headlesss
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    browser = webdriver.Chrome(chrome_options=chrome_options)

#browser = webdriver.PhantomJS() # 会报警高提示不建议使用phantomjs，建议chrome添加无头
    browser.maximize_window()  # 最大化窗口
    wait = WebDriverWait(browser, 10)


    get_xq_data.set_browser(browser)
    get_xq_data.xq_login2(browser)


    
    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    nowdate=datetime.datetime.now().date()
    print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d")))

    check_table()

    update_database_fund('2021-12-31')
    update_database_fund('2022-09-30')
    update_database_fund('2022-06-30')
    update_database_fund('2022-03-31')
    
    
    browser.close()
    browser.quit()
    
    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
