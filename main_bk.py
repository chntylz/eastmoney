#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
from HData_eastmoney_bk import *
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

hdata_bk=HData_eastmoney_bk("usr","usr")


'''
'''



def check_table():
    table_exist = hdata_bk.table_is_exist() 
    my_dbg('table_exist=%d' % table_exist)
    if table_exist:
        if int(para1):
            #hdata_bk.db_hdata_eastmoney_create()
            my_dbg('table already exist, recreate')
    else:
        hdata_bk.db_hdata_eastmoney_create()
        my_dbg('table not exist, create')



if __name__ == '__main__':

    script_name, para1 = check_input_parameter()

    if int(para1) == 0:
        f_day = get_file_modify_day('csv/r_df_today.csv')
        my_dbg('f_day=%d' % f_day)
        if f_day == 0:
            my_dbg(' exit... ')
            sys.exit(0)
    
    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    nowdate=datetime.datetime.now().date()
    nowdate=nowdate-datetime.timedelta(int(para1))
    my_dbg("nowdate is %s"%(nowdate.strftime("%Y-%m-%d")))

    #check table exist
    check_table()

    bk_df = get_bk_data()
    bk_df = bk_df.drop_duplicates(subset=['stock_code'], keep='first')
    bk_df.to_csv('./csv/'+ nowdate.strftime("%Y-%m-%d")+ '_bankuai.csv', encoding='gbk')

    #bankuai save to db
    hdata_bk.copy_from_stringio(bk_df)



    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    my_dbg("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    my_dbg("%s t1:%s, t2:%s, delta=%s"%(script_name, t1, t2, t2-t1))
