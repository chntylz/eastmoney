#!/usr/bin/env python  
# -*- coding: utf-8 -*-

from HData_eastmoney_day import *
import datetime
import time

import sys 




import numpy as np
import pandas as pd

from get_xq_data import *

import multiprocessing
from multiprocessing import Pool, Manager


import get_xq_data
from file_interface import * 

from comm_selenium import *
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait




debug = 0
debug = 1


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
        my_dbg(e)
        my_dbg(stock_code)
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
            my_dbg(d)
        df = pd.DataFrame(d) 
    else:
        df = pd.DataFrame(fina_data) 

    if len(df):
        pass
    else:
        my_dbg('stock_code=%s, len(df)=0, #error# abnormal' \
                % stock_code)
        return df

    if debug:
        my_dbg(df.loc[len(df)-1])        #series
        my_dbg(df[len(df)-2:len(df)-1])  #dataframe


    len_cols = len(list(df)) 
    i = 0
    for i in range(3, len_cols):
        #split
        try:
            tmp_df = pd.DataFrame(data=[x[i] for x in df.values])
        except Exception as e:
            my_dbg(e)
            my_dbg('try stock_code=%s, len(df)=%d, i=%d, #error# abnormal' \
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
            my_dbg('col_name=%s'% col_name)
            my_dbg('tmp_df=%s\r'% tmp_df)

        if i == 3:
            new_df = tmp_df
        else:
            #new_df = pd.concat([new_df, tmp_df], axis=1, join_axes=[new_df.index])
            new_df = pd.concat([new_df, tmp_df], axis=1)


    if debug:
        my_dbg(df.head(1))
        my_dbg(list(df))
        my_dbg(new_df.head(1))
    
    #保留前3列，连接拆分出来的新df
    new_cols = ['report_date', 'report_name', 'ctime']
    df = df[new_cols]
    df = pd.concat([df, new_df], axis=1)

    return df


def xq_start_get_realtime_data(browser, url):
    data_df = pd.DataFrame()

    html = ''
    try: 
        browser.get(url)
        browser.implicitly_wait(10)
        html = browser.page_source
    except:
        browser.close()
        browser.quit()
    finally:
        browser.close()
        browser.quit()


    #my_dbg(html)
   
    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, html)
    api_param = json.loads(response_array[0])
    rawdata = api_param['data']
    data_df = pd.DataFrame(rawdata)

    if debug:
        my_dbg('%s %s ' %( url, data_df))
    
    return data_df




def worker(sem, name):
    with sem:  # 自动acquire/release
        print(f"Task {name} acquired semaphore")
        time.sleep(2)

        if debug:
            my_dbg("Worker %s %s started" % (name[0], name[1]))
        
        page = name[0]
        browser = name[1]

        url = 'https://xueqiu.com/service/v5/stock/screener/quote/list?page='\
            + str(page) \
            +'&size=100&order=asc&orderby=percent&order_by=percent&market=CN&type=sh_sz'

        if debug:
            my_dbg(url)

        #xq_start_get_realtime_data(browser, url)

        print(f"Task {name} released semaphore")
        return

def xq_get_realtime_data():
 
    '''
    get_xq_data._init()
    browser = get_browser()
    get_xq_data.set_browser(browser)
    get_xq_data.xq_login2(browser)
    start_slider_login(browser)

    '''
    browser = 'test'

    data_list = []
    for idx in range(1, 3):
        data_list.append([idx, browser])

    if debug:
        my_dbg(data_list)



    manager =  Manager()
    sem = manager.Semaphore(1)  # 允许2个进程同时访问

    processes = multiprocessing.cpu_count()
    number = len(data_list)
    mplist = []
    with multiprocessing.Pool(processes) as pool:
       mplist.append(
           pool.starmap(worker, [(sem, tid) for tid in data_list]))
 

if __name__ == '__main__':
    

    my_dbg(time.localtime(time.time()))
    t1 = time.time()
    t2 = time.time()
    xq_get_realtime_data()
    my_dbg("t1:%s, t2:%s, delta time=%s"%(t1, t2, t2-t1))
    my_dbg(time.localtime(time.time()))
