#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
from HData_eastmoney_zlpm import *
import  datetime
import time 

import pandas as pd
import numpy as np

import sys
import os

from get_zlpm_data import *


debug=0
debug=0


hdata_zlpm=HData_eastmoney_zlpm("usr","usr")


'''
'''



def check_table():
    table_exist = hdata_zlpm.table_is_exist() 
    print('table_exist=%d' % table_exist)
    if table_exist:
        hdata_zlpm.db_hdata_eastmoney_create()
        print('table already exist')
    else:
        hdata_zlpm.db_hdata_eastmoney_create()
        print('table not exist, create')

if __name__ == '__main__':

    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    #check table exist
    check_table()
    df, api_param = get_zlpm_data()
    
    df.to_csv('./csv/zlpm_df.csv', encoding='gbk')
    
    hdata_zlpm.copy_from_stringio(df)


    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
