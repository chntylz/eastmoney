#!/#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import json
import requests
import re

import time, datetime
import os
import random

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from get_daily_zlje import *

import multiprocessing

debug = 0
debug = 0


def open_broswer():
    # 添加无头headlesss
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument(
            'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36')

    chrome_options.add_argument("--disable-blink-features=AutomationControlled")

    chrome_options.add_argument("blink-settings=imagesEnabled=false")
    chrome_options.add_argument("disable-infobars");
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    browser = webdriver.Chrome(options=chrome_options)
    browser.maximize_window()  # 最大化窗口
    wait = WebDriverWait(browser, 10)
    with open('./stealth.min.js') as f:
        js = f.read()

    browser.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": js})

    return browser


def close_broser(browser):
    browser.close()
    browser.quit()
    

def worker(data):
    if debug:
        print(data[2])

    df = get_sina_fina()

    #return data[2]
    return df

    
def get_sina_fina():
    browser = open_broswer()


    data = []

    url='https://money.finance.sina.com.cn/corp/go.php/vFD_BalanceSheet/stockid/600660/ctrl/2023/displaytype/4.phtml'
    browser.get(url)
    table = browser.find_element('xpath', '//*[@id="BalanceSheetNewTable0"]')
    rows = table.find_elements(By.TAG_NAME, "tr")
    for row in rows:
        cols = row.find_elements(By.TAG_NAME, "td")
        cols_data = [col.text for col in cols] # 提取每一列的文本数据
        data.append(cols_data) # 将每一行的数据添加到数据列表中

    close_broser(browser)
    if debug:
        print(data)

    df = pd.DataFrame(data)
    df = df.T

    return df

if __name__ == '__main__':

    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    stock_df=get_daily_zlje2()
    stock_df=stock_df.head(40)
    data_list = np.array(stock_df)
    data_list = data_list.tolist()


    processes = 4
    number = len(stock_df)
    mplist = []
    with multiprocessing.Pool(int(processes)) as pool:
       mplist.append(
                      pool.map(worker, data_list))

    print(mplist)



    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))

'''
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

# 添加无头headlesss
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument(
        'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36')

chrome_options.add_argument("--disable-blink-features=AutomationControlled")

chrome_options.add_argument("blink-settings=imagesEnabled=false")
chrome_options.add_argument("disable-infobars");
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
browser = webdriver.Chrome(options=chrome_options)
browser.maximize_window()  # 最大化窗口
wait = WebDriverWait(browser, 10)
with open('./stealth.min.js') as f:
    js = f.read()

browser.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": js})


data = []

url='https://money.finance.sina.com.cn/corp/go.php/vFD_BalanceSheet/stockid/600660/ctrl/2023/displaytype/4.phtml'
browser.get(url)



table = browser.find_element('xpath', '//*[@id="BalanceSheetNewTable0"]')

main_table = driver.find_element(By.XPATH, '//*[@id="BalanceSheetNewTable0"]/tr/td')
for x in main_table:
   print(x.text)

rows = table.find_elements(By.TAG_NAME, "tr")
for row in rows:
    cols = row.find_elements(By.TAG_NAME, "td")
    cols_data = [col.text for col in cols] # 提取每一列的文本数据
    data.append(cols_data) # 将每一行的数据添加到数据列表中

#print(data)




import pandas as pd
df = pd.DataFrame(data)
df = df.T



main_table = browser.find_element('xpath', '//*[@id="BalanceSheetNewTable0"]').get_attribute("outerHtml")
df_new=pd.read_html(main_table)[0]
print(df_new)

browser.close()
browser.quit()


'''
