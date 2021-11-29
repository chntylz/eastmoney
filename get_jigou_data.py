#!/#!/usr/bin/env python
# -*- coding: utf-8 -*-

#https://data.eastmoney.com/zlsj/detail/2021-09-30-1-600519.html
#url = 'https://datacenter-web.eastmoney.com/api/data/v1/get?reportName=RPT_MAIN_ORGHOLD&columns=ALL&quoteColumns=&filter=(SECURITY_CODE%3D%22300871%22)(REPORT_DATE%3D%272021-09-30%27)&pageNumber=1&pageSize=8&sortTypes=&sortColumns=&source=WEB&client=WEB&callback=jQuery112307224581864083242&'

import pandas as pd
import json
import requests
import re

import time
import datetime

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
import time, datetime
import pandas as pd
import os
import re

from get_realtime_data import *

from HData_eastmoney_jigou import *

hdata_jigou=HData_eastmoney_jigou("usr","usr")

debug = 1
debug = 0

def get_jigou_data(stock_code, record_date):
    df = pd.DataFrame()

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    browser = webdriver.Chrome(options=chrome_options)

    # browser = webdriver.PhantomJS() # 会报警高提示不建议使用phantomjs，建议chrome添加无头
    browser.maximize_window()  # 最大化窗口
    wait = WebDriverWait(browser, 10)

    url = 'https://datacenter-web.eastmoney.com/api/data/v1/get?'\
            + 'reportName=RPT_MAIN_ORGHOLD&columns=ALL&quoteColumns=&filter='\
            + '(SECURITY_CODE%3D%22'\
            + stock_code \
            + '%22)(REPORT_DATE%3D%27'\
            + record_date \
            + '%27)&pageNumber=1&pageSize=8&sortTypes=&sortColumns=&source=WEB&client=WEB&'\
            + 'callback=jQuery112307224581864083242&'

    html = ''
    try:
        browser.get(url)
        browser.implicitly_wait(5)
        html = browser.page_source
    except:
        browser.close()
        browser.quit()
    finally:
        browser.close()
        browser.quit()

    if debug:
        print(url)

    #if debug:
    #    print(html)

    s=html

    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, s)
    api_param = json.loads(response_array[0])
    rawdata = ''
    try:
        rawdata = api_param['result']['data']
    except Exception as e:
        print(e)
        print('get_jigou_data rawdata is none %s %s' % (stock_code, record_date))
        return df

    data_df = pd.DataFrame(rawdata)
    data_df = data_df.fillna(0)

    data_df.columns = data_df.columns.map(lambda x:x.lower())
    data_df['report_date'] = data_df['report_date'].apply(lambda x: x[:10])
    data_df = data_df[data_df['org_type_name'] == '机构汇总']
    #data_df.to_csv('./csv/jigou_'+ stock_code + '_' + record_date + '.csv', encoding='gbk')

    '''
    机构属性	持股家数(家)	持股总数(万股)	持股市值(亿元)	占总股本比例(%)	占流通股
    比例(%)
    基金	7	773.15	2.50	4.65	9.82
    QFII	-	0.00	-	-	-
    社保	-	0.00	-	-	-
    保险	-	0.00	-	-	-
    券商	-	0.00	-	-	-
    信托	-	0.00	-	-	-
    其他	4	2389.62	7.73	14.37	30.34
    机构汇总	11	3162.77	10.23	19.02	40.16
    '''

    df['stock_code'] = data_df['security_code']
    df['stock_name'] = data_df['security_name_abbr']
    df['record_date'] = data_df['report_date']
    df['hould_num'] = data_df['hould_num']

    df['total_shares'] = data_df['total_shares']
    df['total_shares'] = df['total_shares'].apply(lambda x: x/10000)
    df['hold_value'] = data_df['hold_value']
    df['hold_value'] = df['hold_value'].apply(lambda x: x/10000/10000)
    df['totalshares_ratio'] = data_df['totalshares_ratio']
    df['freeshares_ratio'] = data_df['freeshares_ratio']
    df = round(df, 2)

    return df

def get_jigou():
    
    date_list = ['2021-09-30', '2021-06-30', '2021-03-31']

    df = tmp_df = pd.DataFrame()
    r_df, work_df, stop_df, api_param = get_realtime_data2()

    r_len = len(r_df)

    for my_date in date_list:

        record_date = my_date

        for i in range(0, r_len):
            stock_code = r_df['stock_code'][i]

            #record_date = '2021-09-30'
            tmp_df = get_jigou_data(stock_code, record_date)
            if debug:
                print(tmp_df)

            df = pd.concat([df, tmp_df])

    return df


def check_table():
    table_exist = hdata_jigou.table_is_exist()
    print('table_exist=%d' % table_exist)
    if table_exist:
        #hdata_jigou.db_hdata_xq_create()
        print('table already exist')
    else:
        hdata_jigou.db_hdata_xq_create()
        print('table not exist, create')
    pass

if __name__ == '__main__':
    
    nowdate=datetime.datetime.now().date()
    date_string = nowdate.strftime('%Y-%m-%d')
    
    df  = get_jigou()
    df.to_csv('./csv/jigou_'+ date_string + '.csv', encoding='gbk')

    if len(df):
        check_table()
        hdata_jigou.copy_from_stringio(df)

