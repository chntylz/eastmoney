#!/#!/usr/bin/env python
# -*- coding: utf-8 -*-

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
import time, datetime
import pandas as pd
import os
import re

import json

import psycopg2 #使用的是PostgreSQL数据库
from HData_xq_simple_day import *

debug=0

hdata_day=HData_xq_simple_day("usr","usr")

def check_table():
    table_exist = hdata_day.table_is_exist() 
    print('table_exist=%d' % table_exist)
    if table_exist:
        #hdata_day.db_hdata_xq_simple_create()
        print('table already exist')
    else:
        hdata_day.db_hdata_xq_simple_create()
        print('table not exist, create')


def handle_data():
    '''
    element = browser.find_element_by_css_selector('#app > div.container-lg.clearfix > div.container-sm.float-left.stock__main > div.quote-container > table')

    td_content = element.find_elements_by_tag_name("td")
    lst = []  # 存储为list
    for td in td_content:
        lst.append(td.text)

    df=pd.DataFrame(lst)
    df=df[0].str.split('：', expand=True)
    print(df)
    '''

    info1 = browser.find_element_by_class_name('quote-container')
    s1=info1.text
    #print(s1)

    #close=browser.find_element_by_class_name('stock-current').text
    #print(close)

def get_stock_info(file_name):
    stock_list = []
    with open(file_name) as f:
        for line in f:
            if debug:
                print (line, len(line))
            if len(line) < 6 or '#' in line:
                if debug:
                    print('unvalid line data, skip!')
                continue
            space_pos = line.rfind(' ')
            stock_list.append([line[0:space_pos], line[space_pos+1: -1]])

    return stock_list



def get_data():
    file_name = 'my_optional.txt'
    my_list = get_stock_info(file_name)
    if debug:
        print(my_list)
    length=len(my_list)

    for i in range(length):
        stock_code_new  = my_list[i][0]
        new_code        = stock_code_new[2:]
        new_name        = my_list[i][1]
        url='https://xueqiu.com/S/' + stock_code_new
        if debug:
            print("i=%d,  new_code:%s, " %(i, new_code))
            print(url)

        browser.get(url)
        try:
            handle_data()
        except:
            print('###' + url)
            browser.close()



#order=desc
def get_data2(browser):

#url='https://xueqiu.com/service/v5/stock/screener/quote/list'\
#        + '?page=1&size=10000&order=desc&orderby=percent&order_by=percent&market=CN&type=sh_sz'

    #10000 is too big now, change to 5000
    url='https://xueqiu.com/service/v5/stock/screener/quote/list'\
        + '?page=1&size=5000&order=desc&orderby=percent&order_by=percent&market=CN&type=sh_sz'

    browser.get(url)
    browser.implicitly_wait(5)
    print(url)
    html = browser.page_source
    s=html
    f1 = s.find('{')
    s = s[:f1] + '(' + s[f1 : ]  #add '(' before first '{' 
    f2 = s.rfind('}')              #add ')' after last  '}' 
    s = s[:f2+1] + ')' + s[f2+1 : ]
    html=s

    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, html)

    api_param = json.loads(response_array[0])
    rawdata = api_param['data']['list']
    df = pd.DataFrame(rawdata)
    del df['mapping_quote_current']
    del df['dual_counter_mapping_symbol']
    
    return df


#order=asc
def get_data3(browser):

#url='https://xueqiu.com/service/v5/stock/screener/quote/list'\
#        + '?page=1&size=10000&order=asc&orderby=percent&order_by=percent&market=CN&type=sh_sz'

    #10000 is too big now, change to 5000
    url='https://xueqiu.com/service/v5/stock/screener/quote/list'\
        + '?page=1&size=5000&order=asc&orderby=percent&order_by=percent&market=CN&type=sh_sz'

    browser.get(url)
    browser.implicitly_wait(5)
    print(url)
    html = browser.page_source
    s=html
    f1 = s.find('{')
    s = s[:f1] + '(' + s[f1 : ]  #add '(' before first '{' 
    f2 = s.rfind('}')              #add ')' after last  '}' 
    s = s[:f2+1] + ')' + s[f2+1 : ]
    html=s

    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, html)

    api_param = json.loads(response_array[0])
    rawdata = api_param['data']['list']
    df = pd.DataFrame(rawdata)
    del df['mapping_quote_current']
    del df['dual_counter_mapping_symbol']
    
    return df


if __name__ == '__main__':
    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    nowdate=datetime.datetime.now().date()
    print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d")))

    df = pd.DataFrame()
    #check table exist
    check_table()


    # 先chrome，后phantomjs
    # browser = webdriver.Chrome()

    # 添加无头headlesss
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    browser = webdriver.Chrome(options=chrome_options)

    # browser = webdriver.PhantomJS() # 会报警高提示不建议使用phantomjs，建议chrome添加无头
    browser.maximize_window()  # 最大化窗口
    wait = WebDriverWait(browser, 10)

    try:
       df = df2 = get_data2(browser)
    except:
        print('#ERROR get_data2()')
        browser.close()
        browser.quit()
    finally:
        #browser.close()
        #browser.quit()
        pass

#'''
    time.sleep(5)

    try:
        df3 = get_data3(browser)
    except:
        print('#ERROR get_data3()')
        browser.close()
        browser.quit()
    finally:
        browser.close()
        browser.quit()

    df = pd.concat([df2, df3])
    df = df.drop_duplicates(subset=['symbol'], keep='first')
    df = df.reset_index(drop=True)

#'''

    if len(df):
        df=df.fillna(0)
        df.insert(0, 'record_date', nowdate.strftime("%Y-%m-%d"), allow_duplicates=False)
        df.to_csv('./csv/df_xq_simple_'+ datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')  +'.csv', encoding='gbk')
        hdata_day.delete_data_from_hdata(
                start_date=nowdate.strftime("%Y-%m-%d"),
                end_date=nowdate.strftime("%Y-%m-%d")
                )
        hdata_day.copy_from_stringio(df)
    else:
        print('#ERROR df is null')

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))

