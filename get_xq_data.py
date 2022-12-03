#!/#!/usr/bin/env python  
# -*- coding: utf-8 -*-


import pandas as pd
import json
import requests
import re

import time
import datetime

from file_interface import *

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
import time, datetime
import pandas as pd
import os
import re

debug = 0

def _init():
    global _global_browser
    _global_browser = {}

def set_browser(key):
    _global_browser[0] = key

def get_browser():
    return  _global_browser[0]

def xq_login(driver):
    print('xq_login')
    time.sleep(1)
    driver.get('https://xueqiu.com/user/login')
    time.sleep(2)
    driver.find_element_by_name("username").clear()
    driver.find_element_by_name("username").send_keys('chntylz@gmail.com')
    driver.find_element_by_name("password").clear()
    driver.find_element_by_name("password").send_keys('820820')
    driver.find_element_by_class_name("button").click()
    time.sleep(1)


def xq_login2(driver):
    #selenium example
    #https://vimsky.com/examples/detail/python-ex-selenium.webdriver.support.ui-WebDriverWait-send_keys-method.html
    #https://www.cnblogs.com/denise1108/p/10551019.html
    print('xq_login')
    driver.get('https://xueqiu.com/user/login')
    elem = WebDriverWait(driver, 30).until(EC.visibility_of_element_located((By.NAME, 'username')))
    elem.send_keys('chntylz@gmail.com')
    elem = WebDriverWait(driver, 30).until(EC.visibility_of_element_located((By.NAME, 'password')))
    elem.send_keys('820820')
    driver.find_element_by_class_name("button").click()


def xq_get_raw_data2(symbol, datatype=None, is_annuals=0, count=10):

    # finance
    finance_cash_flow_url = "https://stock.xueqiu.com/v5/stock/finance/cn/cash_flow.json?symbol="
    finance_indicator_url = "https://stock.xueqiu.com/v5/stock/finance/cn/indicator.json?symbol="
    finance_balance_url = "https://stock.xueqiu.com/v5/stock/finance/cn/balance.json?symbol="
    finance_income_url = "https://stock.xueqiu.com/v5/stock/finance/cn/income.json?symbol="
    finance_business_url = "https://stock.xueqiu.com/v5/stock/finance/cn/business.json?symbol="
       
    fina_data = None
    
    if datatype == 'income':
        url = finance_income_url+symbol
    elif datatype == 'balance':
        url = finance_balance_url+symbol
    elif datatype == 'cashflow':
        url = finance_cash_flow_url+symbol
    else :
        url = finance_indicator_url+symbol

    if is_annuals == 1:
        url += '&type=Q4&count='
    else:
        url += '&type=all&is_detail=true&count='
    
    url += str(count)

    print(url)
    '''
    # 添加无头headlesss
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    browser = webdriver.Chrome(chrome_options=chrome_options)

    # browser = webdriver.PhantomJS() # 会报警高提示不建议使用phantomjs，建议chrome添加无头
    browser.maximize_window()  # 最大化窗口
    wait = WebDriverWait(browser, 10)
    '''
    _global_browser = get_browser()
    try:
        pass
        # xq_login2(_global_browser)
    except Exception as e:
        print(e)
        print('alread login in')
    finally:
        pass

    html = ''
    try: 
        _global_browser.get(url)
        _global_browser.implicitly_wait(5)
        html = _global_browser.page_source
    except Exception as e:
        print(e)
        #_global_browser.close()
        #_global_browser.quit()
    finally:
        #_global_browser.close() 
        ##_global_browser.quit()
        pass
     
    if debug:
        print(html)
    
    s=html
    f1 = s.find('{')
    s = s[:f1] + '(' + s[f1 : ]  #add '(' before first '{'
    f2 = s.rfind('}')              #add ')' after last  '}'
    s = s[:f2+1] + ')' + s[f2+1 : ]
    html=s
       
    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, html)
    try:
        fina_data = json.loads(response_array[0])
    except Exception as e:
        xq_login2(_global_browser)
        print(e)
        print(url)
        print(html)
    finally:
        pass
 
    return fina_data



def xq_get_holder_data(symbol, page=1, size=10):
    # holder
    #https://stock.xueqiu.com/v5/stock/f10/cn/holders.json?symbol=SZ300859&extend=true&page=1&size=10
    url = 'https://stock.xueqiu.com/v5/stock/f10/cn/holders.json?'\
        + 'symbol=' + symbol \
        + '&extend=true&page=' + str(page) + '&size=' + str(size)      

    data_df = pd.DataFrame()
    print(url)

    # 添加无头headlesss
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    browser = webdriver.Chrome(chrome_options=chrome_options)

    # browser = webdriver.PhantomJS() # 会报警高提示不建议使用phantomjs，建议chrome添加无头
    browser.maximize_window()  # 最大化窗口
    wait = WebDriverWait(browser, 10)

    try:
        xq_login2(browser)
    except Exception as e:
        print(e)
        print('alread login in')
    finally:
        pass


    html = ''
    try: 
        browser.get(url)
        browser.implicitly_wait(5)
        html = browser.page_source
    except Exception as e:
        print(e)
        browser.close()
        browser.quit()
    finally:
        browser.close()
        browser.quit()

    if debug:
        print(html)
    
    s=html
    f1 = s.find('{')
    s = s[:f1] + '(' + s[f1 : ]    #add '(' before first '{'
    f2 = s.rfind('}')              #add ')' after last  '}'
    s = s[:f2+1] + ')' + s[f2+1 : ]
       
    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, s)
    try:
        api_param = json.loads(response_array[0])
        rawdata = api_param['data']['items']
        data_df = pd.DataFrame(rawdata)
    except Exception as e:
        print(e)
        print(url)
        print(html)
    finally:
        pass
 
    return data_df


def xq_get_fund(browser, stock_code, report_date):

    data_df = pd.DataFrame()

    fund_report_date = str(int(string2timestamp(report_date)*1000))

    url='https://stock.xueqiu.com/v5/stock/f10/cn/org_holding/detail.json?'\
            + 'symbol=' + stock_code + '&timestamp=' + fund_report_date + '&extend=true'

    html = ''
    try:
        browser.implicitly_wait(15)
        #time.sleep(5)
        browser.get(url)
        html = browser.page_source
    except Exception as e:
        print(e)
    finally:
        pass

    s=html
    s=s.replace('(', '_')   # replace '(' by '_'
    s=s.replace(')', '')     # replace ')' by space
    f1 = s.find('{')
    s = s[:f1] + '(' + s[f1 : ]  #add '(' before first '{'
    f2 = s.rfind('}')              #add ')' after last  '}'
    s = s[:f2+1] + ')' + s[f2+1 : ]


    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, s)
    data = json.loads(response_array[0])


    try:
        data = json.loads(response_array[0])
        data = data['data']['fund_items']
    except Exception as e:
        print(e)
        print(url)
        print(html)
    finally:
        pass


    data_df = pd.DataFrame(data)

    return data_df



if __name__ == '__main__':
    fina_data = xq_get_raw_data2('SZ000977')
    fina_data = fina_data['data']['list']

    df = pd.DataFrame(fina_data) 

    print(df)
 
