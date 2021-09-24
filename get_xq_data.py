#!/#!/usr/bin/env python  
# -*- coding: utf-8 -*-


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

debug = 0

def xq_login(driver):
    driver.get('https://xueqiu.com/user/login')
    driver.find_element_by_name("username").clear()
    driver.find_element_by_name("username").send_keys('chntylz@gmail.com')
    driver.find_element_by_name("password").clear()
    driver.find_element_by_name("password").send_keys('820820')
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

    # 添加无头headlesss
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    browser = webdriver.Chrome(chrome_options=chrome_options)

    # browser = webdriver.PhantomJS() # 会报警高提示不建议使用phantomjs，建议chrome添加无头
    browser.maximize_window()  # 最大化窗口
    wait = WebDriverWait(browser, 10)

    xq_login(browser)

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
    fina_data = json.loads(response_array[0])
 
    return fina_data




if __name__ == '__main__':
    fina_data = xq_get_raw_data2('SZ000977')
    fina_data = fina_data['data']['list']

    df = pd.DataFrame(fina_data) 

    print(df)
 
