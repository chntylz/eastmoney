#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import datetime
import time

import sys 

from xq_get_basic_data import *

debug = 0
debug = 1
debug = 0

from HData_xq_simple_day import *
from HData_xq_kday import *

hdata_s_day=HData_xq_simple_day("usr","usr")
hdata_kday=HData_xq_kday("usr","usr")

def check_kday_table():
    table_exist = hdata_kday.table_is_exist() 
    my_dbg('table_exist=%d' % table_exist)
    if table_exist:
        #hdata_kday.db_hdata_xq_kday_create()
        my_dbg('kday table already exist')
    else:
        hdata_kday.db_hdata_xq_kday_create()
        my_dbg('kday table not exist, create')

def check_day_table():
    table_exist = hdata_s_day.table_is_exist() 
    my_dbg('table_exist=%d' % table_exist)
    if table_exist:
        #hdata_s_day.db_hdata_xq_simple_create()
        my_dbg('kday table already exist')
    else:
        hdata_s_day.db_hdata_xq_simple_create()
        my_dbg('kday table not exist, create')



if __name__ == '__main__':
    
    my_dbg(time.localtime(time.time()))
    nowdate=datetime.datetime.now().date()
    t1 = time.time()
    df = xq_get_realtime_data()
    if len(df):
        my_dbg(f'df columns: {df.columns}')
        df=df.fillna(0)
        df = df.drop_duplicates(subset=['symbol'], keep='first')
        df.insert(0, 'record_date', nowdate.strftime("%Y-%m-%d"), allow_duplicates=False)
        df.to_csv('./csv/'+ datetime.datetime.now().strftime('%Y-%m-%d-%H-%M') + '_df_xq_simple'  +'.csv', encoding='gbk')
        check_day_table()
        hdata_s_day.delete_data_from_hdata(
                start_date=nowdate.strftime("%Y-%m-%d"),
                end_date=nowdate.strftime("%Y-%m-%d")
                )
        try:
            hdata_s_day.copy_from_stringio(df)
        except Exception as e:
            my_dbg(f"error:{e}")
    else:
        my_dbg('#ERROR df is null')



    kday_df = xq_get_kday_data()
    if len(kday_df):
        my_dbg(f'kday_df columns: {kday_df.columns}')
        kday_df = kday_df.drop_duplicates(subset=['symbol'], keep='first')
        kday_df.to_csv('./csv/'+ datetime.datetime.now().strftime('%Y-%m-%d-%H-%M') + '_df_xq_kday'  +'.csv', encoding='gbk')
        check_kday_table()
        hdata_kday.delete_data_from_hdata(
                start_date=nowdate.strftime("%Y-%m-%d"),
                end_date=nowdate.strftime("%Y-%m-%d")
                )
        try:
            hdata_kday.copy_from_stringio(kday_df)
        except Exception as e:
            my_dbg(f"error:{e}")
            
        
    else:
        my_dbg('#ERROR kday_df is null')


    t2 = time.time()
    my_dbg("t1:%s, t2:%s, delta time=%s"%(t1, t2, t2-t1))
    my_dbg(time.localtime(time.time()))
