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

from bs4 import BeautifulSoup
import json
import random

from HData_xq_simple_day import *
pd.set_option('future.no_silent_downcasting', True)

debug = 0
debug = 1


hdata_day=HData_xq_simple_day("usr","usr")



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

def extract_json_from_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    pre_tag = soup.find('pre')
    if pre_tag:
        try:
            json_data = json.loads(pre_tag.text)
            return json_data
        except json.JSONDecodeError as e:
            print(f"JSON解析错误: {e}")
            return None
    return None


def xq_start_get_realtime_data(browser, url):
    data_df = pd.DataFrame()

    html = ''
    try: 
        browser.get(url)
        browser.implicitly_wait(10)
        html = browser.page_source
    except:
        #browser.close()
        #browser.quit()
        pass
    finally:
        #browser.close()
        #browser.quit()
        pass

    json_data = extract_json_from_html(html)

    #my_dbg(html)
   
    rawdata = json_data['data']['list']
    data_df = pd.DataFrame(rawdata)

    if debug:
        my_dbg('%s %s ' %( url, data_df))
    
    return data_df


def xq_get_realtime_data():
    get_xq_data._init()
    browser = get_browser()
    get_xq_data.set_browser(browser)
    get_xq_data.xq_login2(browser)
    start_slider_login(browser)

    df = pd.DataFrame()
    page = 1
    while True:
        url = 'https://xueqiu.com/service/v5/stock/screener/quote/list?page='\
            + str(page) \
            +'&size=100&order=asc&orderby=percent&order_by=percent&market=CN&type=sh_sz'

        if debug:
            my_dbg(url)

        tmp_df = xq_start_get_realtime_data(browser, url)
        if len(tmp_df):
            page = page + 1
            df = pd.concat([df, tmp_df])
            #time.sleep(random.randint(1, 2))
        else:
            break;


    browser.close()
    browser.quit()

    my_dbg(df)

    if 'mapping_quote_current' in df.columns :
        del df['mapping_quote_current']
    
    if 'dual_counter_mapping_symbol' in df.columns :
        del df['dual_counter_mapping_symbol']


    return df


def xq_get_kday_data():

    data_df = hdata_day.get_latest_data_from_hdata()
        
    get_xq_data._init()
    browser = get_browser()
    get_xq_data.set_browser(browser)
    get_xq_data.xq_login2(browser)
    start_slider_login(browser)


    # 定义基础URL和股票代码列表
    multi_url = 'https://stock.xueqiu.com/v5/stock/realtime/quotec.json?symbol='
    symbols = data_df.stock_code.to_list()

    # 初始化空DataFrame
    df = pd.DataFrame()

    # 使用列表收集股票代码，每300个构建一次URL并发送请求
    codes = []
    for idx, symbol in enumerate(symbols):
        codes.append(symbol)
        if (idx + 1) % 200 == 0 or idx == len(symbols) - 1:
            code_str = ','.join(codes)
            url = multi_url + code_str
            my_dbg(url)
            
            try:
                browser.get(url)
                time.sleep(1)
                html = browser.page_source
                json_data = extract_json_from_html(html)  # 确保此函数能正确提取JSON数据
                rawdata = json_data['data']
                tmp_df = pd.DataFrame(rawdata)
                df = pd.concat([df, tmp_df])
            except Exception as e:
                my_dbg("Error fetching data for URL {url}: {e}")
            
            # 重置股票代码列表
            codes = []

    # 关闭浏览器（确保所有资源被释放）
    browser.quit()

    df=df.fillna(0)
    df = df.reset_index(drop=True)
    df['timestamp'] = df['timestamp'].apply(lambda x: get_date_from_timestamp(x))

    return df





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

def xq_get_realtime_data_multi():
 
    get_xq_data._init()
    browser = get_browser()
    get_xq_data.set_browser(browser)
    get_xq_data.xq_login2(browser)
    start_slider_login(browser)

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
    nowdate=datetime.datetime.now().date()
    t1 = time.time()
    df = xq_get_realtime_data()
    if len(df):
        df=df.fillna(0)
        df.insert(0, 'record_date', nowdate.strftime("%Y-%m-%d"), allow_duplicates=False)
        df.to_csv('./csv/'+ datetime.datetime.now().strftime('%Y-%m-%d-%H-%M') + '_df_xq_simple'  +'.csv', encoding='gbk')
        hdata_day.delete_data_from_hdata(
                start_date=nowdate.strftime("%Y-%m-%d"),
                end_date=nowdate.strftime("%Y-%m-%d")
                )
        hdata_day.copy_from_stringio(df)
    else:
        my_dbg('#ERROR df is null')

    t2 = time.time()
    my_dbg("t1:%s, t2:%s, delta time=%s"%(t1, t2, t2-t1))
    my_dbg(time.localtime(time.time()))
