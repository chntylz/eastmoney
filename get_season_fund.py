#!/#!/usr/bin/env python  
# -*- coding: utf-8 -*-

#https://data.eastmoney.com/zlsj/
#https://data.eastmoney.com/zlsj/2021-03-31-1-2.html

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



from HData_eastmoney_fund import *

debug = 0
debug = 0


hdata_fund = HData_eastmoney_fund('usr', 'usr')

date_list = ['03-31', '06-30', '09-30', '12-31']


def get_season_fund(date=None, pagenumber=1, pagesize=500):

    data_df = pd.DataFrame()
    nowdate=datetime.datetime.now().date()

    if date is None:
        date = nowdate.strftime('%Y-%m-%d')

    if date[5:] in date_list:

        #https://data.eastmoney.com/dataapi/zlsj/list?date=2021-06-30&type=1&zjc=0&sortField=HOULD_NUM&sortDirec=1&pageNum=1&pageSize=10000
        url = 'https://data.eastmoney.com/dataapi/zlsj/list?date='\
              + date \
              + '&type=1&zjc=0&sortField=HOULD_NUM&sortDirec=1'\
              + '&pageNum=' + str(pagenumber)\
              + '&pageSize=' + str(pagesize)

        #if debug:
        #    print(url)

        response = requests.get(url)
        api_param = json.loads(response.text)
        rawdata = api_param['data']
        data_df = pd.DataFrame(rawdata)
        data_df.columns = data_df.columns.map(lambda x:x.lower())
        #replace '' with nan
        data_df.replace(to_replace=r'^\s*$',value=np.nan,regex=True,inplace=True)
        #replace nan with 0
        data_df = data_df.fillna(0)


    return data_df, api_param


def get_season_fund2(date=None, pagenumber=1, pagesize=500):

    data_df = pd.DataFrame()
    nowdate=datetime.datetime.now().date()

    if date is None:
        date = nowdate.strftime('%Y-%m-%d')

    if date[5:] in date_list:

        #https://data.eastmoney.com/dataapi/zlsj/list?date=2021-06-30&type=1&zjc=0&sortField=HOULD_NUM&sortDirec=1&pageNum=1&pageSize=10000

        url = 'https://data.eastmoney.com/dataapi/zlsj/list?date='\
              + date \
              + '&type=1&zjc=0&sortField=HOULD_NUM&sortDirec=1'\
              + '&pageNum=' + str(pagenumber)\
              + '&pageSize=' + str(pagesize)

        #if debug:
        #    print(url)
        
        # 添加无头headlesss
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        browser = webdriver.Chrome(chrome_options=chrome_options)

        # browser = webdriver.PhantomJS() # 会报警高提示不建议使用phantomjs，建议chrome添加无头
        browser.maximize_window()  # 最大化窗口
        wait = WebDriverWait(browser, 10)

        html = ''
        try: 
            browser.get(url)
            html = browser.page_source
        except:
            browser.close()
            browser.quit()
        finally:
            browser.close()
            browser.quit()


        #if debug:
        #    print(html)	

        s=html
        f1 = s.find('{')
        s = s[:f1] + '(' + s[f1 : ]  #add '(' before first '{'
        f2 = s.rfind('}')              #add ')' after last  '}'
        s = s[:f2+1] + ')' + s[f2+1 : ]
        html=s
        p1 = re.compile(r'[(](.*?)[)]', re.S)
        response_array = re.findall(p1, s)
        api_param = json.loads(response_array[0])
        rawdata = api_param['data']
        data_df = pd.DataFrame(rawdata)

        data_df.columns = data_df.columns.map(lambda x:x.lower())
        #replace '' with nan
        data_df.replace(to_replace=r'^\s*$',value=np.nan,regex=True,inplace=True)
        #replace nan with 0
        data_df = data_df.fillna(0)

        if debug:
            print(data_df)

    return data_df, api_param

def get_all_season_fund(date=None):

    df = pd.DataFrame()
    i = 1
    #每次最多只能得到500条数据
    while (1):
        try:
            df_fund, api_param = get_season_fund2(date=date, pagenumber=i, pagesize=500 )
            time.sleep(10)
            if len(df_fund):
                df_fund = df_fund.fillna(0)

                if debug:
                    print(df_fund)
                    print(df_fund.columns)
                    df_fund.to_csv('./csv/df_fund_' + str(i) + '.csv', encoding='gbk')

                df = pd.concat([df, df_fund])
            
        except Exception as e:
            print(e)
            break
        else:
            i = i + 1

    df['report_date'] = df['report_date'].apply(lambda x: x[:10])
    df = df.drop_duplicates(subset=['report_date', 'secucode'], keep='first')
    df = df.reset_index(drop=True)

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
    pass

if __name__ == '__main__':
    
    nowdate=datetime.datetime.now().date()
    date_string = nowdate.strftime('%Y-%m-%d')

    df  = get_all_season_fund('2019-12-31')
    df2 = get_all_season_fund('2020-03-31')
    df3 = get_all_season_fund('2020-06-30')
    df4 = get_all_season_fund('2020-09-30')
    df5 = get_all_season_fund('2020-12-31')
    df6 = get_all_season_fund('2021-03-31')

    df = pd.concat([df, df2])
    df = pd.concat([df, df3])
    df = pd.concat([df, df4])
    df = pd.concat([df, df5])
    df = pd.concat([df, df6])
    
    df.to_csv('./csv/test_fund.csv', encoding='gbk')
    if len(df):
        check_table()
        hdata_fund.copy_from_stringio(df)


