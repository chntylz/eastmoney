#!/usr/bin/env python  
# -*- coding: utf-8 -*-

from HData_eastmoney_day import *
import datetime
import time

import sys 




import numpy as np
import pandas as pd

from get_xq_data import *

debug = 0
#debug = 1
hdata_day=HData_eastmoney_day("usr","usr")

def xq_get_stock_list():
    codestock_local=hdata_day.get_latest_data_from_hdata()
    return codestock_local


def xq_get_fina_data(stock_code, datatype=None, is_annuals=0, def_cnt=10):

    df      = pd.DataFrame() 
    new_df  = pd.DataFrame() 

    fina_data = xq_get_raw_data2(stock_code, datatype, is_annuals, def_cnt)

    try:
        fina_data = fina_data['data']['list']
    except Exception as e:
        print(e)
        print(stock_code)
        return df


    if 0: 
        #first think, drop later
        s=str(fina_data)
        s=s.replace('[', '\'')
        s=s.replace(']', '\'')
        s=s.replace('None', '0')
        s1=s[1:len(s)-1]
        d=eval(s1)
        if debug:
            print(d)
        df = pd.DataFrame(d) 
    else:
        df = pd.DataFrame(fina_data) 

    if len(df):
        pass
    else:
        print('stock_code=%s, len(df)=0, #error# abnormal' \
                % stock_code)
        return df

    if debug:
        print(df.loc[len(df)-1])        #series
        print(df[len(df)-2:len(df)-1])  #dataframe


    len_cols = len(list(df)) 
    i = 0
    for i in range(3, len_cols):
        #split
        try:
            tmp_df = pd.DataFrame(data=[x[i] for x in df.values])
        except Exception as e:
            print(e)
            print('try stock_code=%s, len(df)=%d, i=%d, #error# abnormal' \
                    % (stock_code, len(df), i))
            return new_df
        else:
            pass

        col_name = list(df)[i]
        tmp_df.rename(columns={0: col_name},inplace=True)
        tmp_df.rename(columns={1: col_name+'_new'},inplace=True)
        tmp_df.fillna(0, inplace=True)
        tmp_df = round(tmp_df, 4)
        if debug:
            print('col_name=%s'% col_name)
            print('tmp_df=%s\r'% tmp_df)

        if i == 3:
            new_df = tmp_df
        else:
            #new_df = pd.concat([new_df, tmp_df], axis=1, join_axes=[new_df.index])
            new_df = pd.concat([new_df, tmp_df], axis=1)


    if debug:
        print(df.head(1))
        print(list(df))
        print(new_df.head(1))
    
    #保留前3列，连接拆分出来的新df
    new_cols = ['report_date', 'report_name', 'ctime']
    df = df[new_cols]
    df = pd.concat([df, new_df], axis=1)

    return df


if __name__ == '__main__':
    

    print(time.localtime(time.time()))
    t1 = time.time()
    t2 = time.time()
    print("t1:%s, t2:%s, delta time=%s"%(t1, t2, t2-t1))
    print(time.localtime(time.time()))
