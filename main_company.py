#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
from HData_company_info import *
from file_interface import *
import  datetime
import time 

import pandas as pd
import numpy as np

import sys
import os

import csv

debug=0
#debug=1


company_info=HData_company_info("usr","usr")



def get_company_info():

    company_cols = [ 'p_index','stock_code', 'stock_name', 'company_name','area', 'city', 'op_income', \
                'net_income', 'employee', 'issue_date', 'stock_book', 'finiance', \
                'industry_type', 'product_type', 'op_bussiness' ]

    all_df = pd.DataFrame()
    for i in range(1,264):  # 爬取全部264页数据
    #for i in range(1,8):  # 爬取全部264页数据
        url = 'http://s.askci.com/stock/a/?reportTime=2017-12-31&pageNum=%s' % (str(i))
        #https://s.askci.com/stock/a/0-0?pageNum=264
        url = 'https://s.askci.com/stock/a/0-0?pageNum=%s' % (str(i))
        tb = pd.read_html(url)[3] #经观察发现所需表格是网页中第4个表格，故为[3]
        #tb.to_csv(r'1.csv', mode='a', encoding='utf_8_sig', header=1, index=0)
        print('第'+str(i)+'页抓取完成')
        df = pd.DataFrame(tb)

        try:
            df.columns = company_cols
            if 'p_index' in df.columns:
                del df['p_index']

            df = df.replace('--',0)
            df=df.fillna(0)
            df['stock_code']  = df['stock_code'].apply(lambda x: stock_code_format(x))
            df['op_income']  = df['op_income'].apply(lambda x: money_unit_transfer(x))
            df['net_income'] = df['net_income'].apply(lambda x: money_unit_transfer(x))

            df['product_type']  = df['product_type'].apply(lambda x: x.replace(',', '、'))
            df['op_bussiness']  = df['op_bussiness'].apply(lambda x: x.replace(',', '、'))

            all_df = pd.concat([all_df, df])
        except Exception as e:
            print('#error: %s' % e)
            print(df)
        finally:
            pass


    return all_df

   

def check_table():
    table_exist = company_info.table_is_exist() 
    print('table_exist=%d' % table_exist)
    if table_exist:
        company_info.db_hdata_company_create()
        print('table already exist, recreate')
    else:
        company_info.db_hdata_company_create()
        print('table not exist, create')



if __name__ == '__main__':

    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    nowdate=datetime.datetime.now().date()
    print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d")))

    df = get_company_info()
    df.to_csv(r'./csv/company_info.csv', mode='w', encoding='utf_8_sig', header=1, index=0)
    if (len(df) > 100):
        #check table exist
        check_table()
        company_info.copy_from_stringio(df)

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("main_company.py t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
