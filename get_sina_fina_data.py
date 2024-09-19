#!/#!/usr/bin/env python
# -*- coding: utf-8 -*-

#balance
#https://money.finance.sina.com.cn/corp/go.php/vFD_BalanceSheet/stockid/600660/ctrl/part/displaytype/4.phtml
#https://money.finance.sina.com.cn/corp/go.php/vFD_BalanceSheet/stockid/600660/ctrl/2024/displaytype/4.phtml


#income
#https://money.finance.sina.com.cn/corp/go.php/vFD_ProfitStatement/stockid/600660/ctrl/2024/displaytype/4.phtml

#cashflow
#https://money.finance.sina.com.cn/corp/go.php/vFD_CashFlow/stockid/600660/ctrl/2024/displaytype/4.phtml


#fina
#https://vip.stock.finance.sina.com.cn/corp/go.php/vFD_FinancialGuideLine/stockid/600660/displaytype/4.phtml
#https://money.finance.sina.com.cn/corp/go.php/vFD_FinancialGuideLine/stockid/600660/ctrl/2024/displaytype/4.phtml


# soup tbody handle refer to follow link
#https://codenews.cc/view/146129
#https://blog.csdn.net/qq_16912257/article/details/53332474

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

from bs4 import BeautifulSoup

debug = 0
debug = 1


def open_browser():
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
        print(data)
    stock_code = data[2]
    stock_name = data[3]

    get_sina_fina_by_soup(stock_code, stock_name)

    return


def get_sina_comm_data(browser, url):

    print(url)
    if debug:
        print(url)

    data = []
    browser.get(url)
    html_doc=browser.page_source

    soup = BeautifulSoup(html_doc, 'html.parser')


    ####################################################
    tbodys = soup.find_all('tbody')
    i=0
    for i in range(len(tbodys)):
        if '报表日期' in tbodys[i].text or '报告日期' in tbodys[i].text: #check where valid data is located tbodys 
            print(" right tbody is found")
            print(i)
            break
        else:
            if debug:
                print(i)
            pass

    tbody = tbodys[i]
    
    for idx, tr in enumerate(tbody.find_all('tr')):
        
        tds = tr.find_all('td')
        length = len(tds)

        if False:
            print('--------------------------------')
            print(tds)

        if length == 2:
            data.append([tds[0].contents[0].text.replace(',',''), 
                tds[1].contents[0].replace(',','').replace('--','0')]) 
        elif length == 3:
            data.append([tds[0].contents[0].text.replace(',','').replace('--','0'), 
                tds[1].contents[0].replace(',','').replace('--','0'), 
                tds[2].contents[0].replace(',','').replace('--','0')])
        elif length == 4:
            data.append([tds[0].contents[0].text.replace(',','').replace('--','0'), 
                tds[1].contents[0].replace(',','').replace('--','0'), 
                tds[2].contents[0].replace(',','').replace('--','0'),
                tds[3].contents[0].replace(',','').replace('--','0')])
        elif length == 5:
            data.append([tds[0].contents[0].text.replace(',','').replace('--','0'), 
                tds[1].contents[0].replace(',','').replace('--','0'), 
                tds[2].contents[0].replace(',','').replace('--','0'),
                tds[3].contents[0].replace(',','').replace('--','0'),
                tds[4].contents[0].replace(',','').replace('--','0')])
        

    '''
    for idx, tr in enumerate(soup.find_all('tr')):
        if idx != 0:
            tds = tr.find_all('td')
            
            if(len(tds) <= 1): #skip unuseful items
                continue
                
            if debug:
                print(tds)
                print(len(tds))
                print(tds[0].contents[0].text)
                print(tds[1].contents[0])
                print(tds[2].contents[0])
                print(tds[3].contents[0])
                print(tds[4].contents[0])
                print('--------------------------------')
            data.append([tds[0].contents[0].text.replace(',',''), 
                    tds[1].contents[0].replace(',',''), 
                    tds[2].contents[0].replace(',',''), 
                    tds[3].contents[0].replace(',',''), 
                    tds[4].contents[0].replace(',','')])

    '''

    return data

def handle_sina_comm_data(data, stock_code, stock_name, year, target_type, tmp_column, data_column):

    df = pd.DataFrame(data)
    df = df.T

    df['stock_code'] = stock_code
    df['stock_name'] = stock_name

    df.to_csv('./csv_data/'+ year + '_' + target_type + '_' +  stock_code + '_tmp.csv', encoding='utf-8-sig')

    if '银行' in stock_name:
        return df

    try:
        df.columns = tmp_column  #change column name
    except Exception as e:
        print(len(df.columns), len(tmp_column), url_balance)


    #change column order
    df = df.loc[:, data_column] 
    

    if debug:
        print(df)

    '''
    delete column or row: https://blog.csdn.net/songyunli1111/article/details/79306639
    #df.drop(columns=['B', 'C']) #delete column name 'B' and 'C'
    '''
    df = df.drop(index=[0])  #delete row0
    df = df.reset_index(drop=True)

    df.to_csv('./csv_data/'+ year + '_' + target_type + '_' + stock_code + '.csv', encoding='utf-8-sig')

    return df


def get_sina_income_data(stock_code, stock_name, year, browser):

    target_type = 'income'

    #https://money.finance.sina.com.cn/corp/go.php/vFD_ProfitStatement/stockid/600660/ctrl/2024/displaytype/4.phtml
    url_income = 'https://money.finance.sina.com.cn/corp/go.php/vFD_ProfitStatement/stockid/'\
        + stock_code \
        + '/ctrl/'\
        + year\
        + '/displaytype/4.phtml'

    #catch html data
    data = get_sina_comm_data(browser, url_balance)

    tmp_column = ['record_date', 'hbzj', 'jyxjrzc', 'ysjrzc', 'yspjjyszk', 'yspj', 'yszk', \
    'yskxrz', 'yfkx', 'qtyskhj', 'yslx', 'ysgl', 'qtysk', 'mrfsjrzc', \
    'ch', 'hfwcydsdzc', 'ynndqdfldzc', 'dtfy', 'dclldzcsy', 'qtldzc', \
    'ldzchj', 'ffdkjdk', 'kgcsjrzc', 'cyzdqtz', 'cqysk', \
    'cqgqtz', 'tzxfdc', 'zjgchj', 'zjgc', 'gcwz', 'gdzcjqlhj', \
    'gdzcje', 'gdzcql', 'scxswzc', 'gyxswzc', 'yqzc', 'syqzc', \
    'wxzc', 'kfzc', 'sy', 'cqdtfy', 'dysdszc', 'qtfldzc', 'fldzchj', \
    'zczj', 'dqjk', 'jyxjrfz', 'yfpjjyfzk', 'yfpj', 'yfzk', \
    'yskx', 'yfsxfjyj', 'yfzgxc', 'yjsf', 'qtyfkhj', 'yflx', \
    'yfgl', 'qtyfk', 'ytfy', 'ynnddysy', 'yfdqzq', 'ynndqdfldfz', \
    'qtldfz', 'ldfzhj', 'cqjk', 'yfzq', 'zlfz', 'cqyfzgxc', \
    'cqyfkhj', 'cqyfk', 'zxyfk', 'yjfldfz', 'dysdsfz', \
    'cqdysy', 'qtfldfz', 'fldfzhj', 'fzhj', 'sszbhgb', 'zbgj', \
    'jkcg', 'qtzhsy', 'zxcb', 'yygj', 'ybfxzb', 'wfplr', \
    'gsymgsgdqyhj', 'ssgdqy', 'syzqyhgdqyhj', 'fzhsyzqyhgdqyzj', \
    'stock_code', 'stock_name']

    data_column = ['record_date', 'stock_code', 'stock_name', 'hbzj', \
    'jyxjrzc', 'ysjrzc', 'yspjjyszk', 'yspj', 'yszk', \
    'yskxrz', 'yfkx', 'qtyskhj', 'yslx', 'ysgl', 'qtysk', 'mrfsjrzc', \
    'ch', 'hfwcydsdzc', 'ynndqdfldzc', 'dtfy', 'dclldzcsy', 'qtldzc', \
    'ldzchj', 'ffdkjdk', 'kgcsjrzc', 'cyzdqtz', 'cqysk', \
    'cqgqtz', 'tzxfdc', 'zjgchj', 'zjgc', 'gcwz', 'gdzcjqlhj', \
    'gdzcje', 'gdzcql', 'scxswzc', 'gyxswzc', 'yqzc', 'syqzc', \
    'wxzc', 'kfzc', 'sy', 'cqdtfy', 'dysdszc', 'qtfldzc', 'fldzchj', \
    'zczj', 'dqjk', 'jyxjrfz', 'yfpjjyfzk', 'yfpj', 'yfzk', \
    'yskx', 'yfsxfjyj', 'yfzgxc', 'yjsf', 'qtyfkhj', 'yflx', \
    'yfgl', 'qtyfk', 'ytfy', 'ynnddysy', 'yfdqzq', 'ynndqdfldfz', \
    'qtldfz', 'ldfzhj', 'cqjk', 'yfzq', 'zlfz', 'cqyfzgxc', \
    'cqyfkhj', 'cqyfk', 'zxyfk', 'yjfldfz', 'dysdsfz', \
    'cqdysy', 'qtfldfz', 'fldfzhj', 'fzhj', 'sszbhgb', 'zbgj', \
    'jkcg', 'qtzhsy', 'zxcb', 'yygj', 'ybfxzb', 'wfplr', \
    'gsymgsgdqyhj', 'ssgdqy', 'syzqyhgdqyhj', 'fzhsyzqyhgdqyzj']


    df = handle_sina_comm_data(data, stock_code, stock_name, year, target_type, tmp_column, data_column)

    return df




def get_sina_balance_data(stock_code, stock_name, year, browser):

    target_type = 'balance'

    #url_balance='https://money.finance.sina.com.cn/corp/go.php/vFD_BalanceSheet/stockid/600660/ctrl/2023/displaytype/4.phtml'
    url_balance='https://money.finance.sina.com.cn/corp/go.php/vFD_BalanceSheet/stockid/'\
        + stock_code \
        + '/ctrl/'\
        + year\
        + '/displaytype/4.phtml'

    #catch html data
    data = get_sina_comm_data(browser, url_balance)

    tmp_column = ['record_date', 'hbzj', 'jyxjrzc', 'ysjrzc', 'yspjjyszk', 'yspj', 'yszk', \
    'yskxrz', 'yfkx', 'qtyskhj', 'yslx', 'ysgl', 'qtysk', 'mrfsjrzc', \
    'ch', 'hfwcydsdzc', 'ynndqdfldzc', 'dtfy', 'dclldzcsy', 'qtldzc', \
    'ldzchj', 'ffdkjdk', 'kgcsjrzc', 'cyzdqtz', 'cqysk', \
    'cqgqtz', 'tzxfdc', 'zjgchj', 'zjgc', 'gcwz', 'gdzcjqlhj', \
    'gdzcje', 'gdzcql', 'scxswzc', 'gyxswzc', 'yqzc', 'syqzc', \
    'wxzc', 'kfzc', 'sy', 'cqdtfy', 'dysdszc', 'qtfldzc', 'fldzchj', \
    'zczj', 'dqjk', 'jyxjrfz', 'yfpjjyfzk', 'yfpj', 'yfzk', \
    'yskx', 'yfsxfjyj', 'yfzgxc', 'yjsf', 'qtyfkhj', 'yflx', \
    'yfgl', 'qtyfk', 'ytfy', 'ynnddysy', 'yfdqzq', 'ynndqdfldfz', \
    'qtldfz', 'ldfzhj', 'cqjk', 'yfzq', 'zlfz', 'cqyfzgxc', \
    'cqyfkhj', 'cqyfk', 'zxyfk', 'yjfldfz', 'dysdsfz', \
    'cqdysy', 'qtfldfz', 'fldfzhj', 'fzhj', 'sszbhgb', 'zbgj', \
    'jkcg', 'qtzhsy', 'zxcb', 'yygj', 'ybfxzb', 'wfplr', \
    'gsymgsgdqyhj', 'ssgdqy', 'syzqyhgdqyhj', 'fzhsyzqyhgdqyzj', \
    'stock_code', 'stock_name']

    data_column = ['record_date', 'stock_code', 'stock_name', 'hbzj', \
    'jyxjrzc', 'ysjrzc', 'yspjjyszk', 'yspj', 'yszk', \
    'yskxrz', 'yfkx', 'qtyskhj', 'yslx', 'ysgl', 'qtysk', 'mrfsjrzc', \
    'ch', 'hfwcydsdzc', 'ynndqdfldzc', 'dtfy', 'dclldzcsy', 'qtldzc', \
    'ldzchj', 'ffdkjdk', 'kgcsjrzc', 'cyzdqtz', 'cqysk', \
    'cqgqtz', 'tzxfdc', 'zjgchj', 'zjgc', 'gcwz', 'gdzcjqlhj', \
    'gdzcje', 'gdzcql', 'scxswzc', 'gyxswzc', 'yqzc', 'syqzc', \
    'wxzc', 'kfzc', 'sy', 'cqdtfy', 'dysdszc', 'qtfldzc', 'fldzchj', \
    'zczj', 'dqjk', 'jyxjrfz', 'yfpjjyfzk', 'yfpj', 'yfzk', \
    'yskx', 'yfsxfjyj', 'yfzgxc', 'yjsf', 'qtyfkhj', 'yflx', \
    'yfgl', 'qtyfk', 'ytfy', 'ynnddysy', 'yfdqzq', 'ynndqdfldfz', \
    'qtldfz', 'ldfzhj', 'cqjk', 'yfzq', 'zlfz', 'cqyfzgxc', \
    'cqyfkhj', 'cqyfk', 'zxyfk', 'yjfldfz', 'dysdsfz', \
    'cqdysy', 'qtfldfz', 'fldfzhj', 'fzhj', 'sszbhgb', 'zbgj', \
    'jkcg', 'qtzhsy', 'zxcb', 'yygj', 'ybfxzb', 'wfplr', \
    'gsymgsgdqyhj', 'ssgdqy', 'syzqyhgdqyhj', 'fzhsyzqyhgdqyzj']


    df = handle_sina_comm_data(data, stock_code, stock_name, year, target_type, tmp_column, data_column)

    return df


def get_sina_real_data(stock_code, stock_name, year, browser):
    
    df_balance = get_sina_balance_data(stock_code, stock_name, year, browser)
    #df_income  = get_sina_income_data(stock_code, stock_name, year, browser)
    df = df_income 
    return df

def get_sina_fina_by_soup(stock_code, stock_name):
    
    time.sleep(random.randint(1, 2))

    df = pd.DataFrame()
    browser = open_browser()

    this_year = int(time.strftime("%Y", time.localtime()))
    #get continuous 5 years data
    target_years = 5
    target_years = 1
    for yy in range(target_years):
        year = str(this_year - yy)
        df = get_sina_real_data(stock_code, stock_name, year, browser)

    close_broser(browser)

    return df


    
def get_sina_fina_by_selenium():
    browser = open_browser()

    data = []

    url_balance='https://money.finance.sina.com.cn/corp/go.php/vFD_BalanceSheet/stockid/600660/ctrl/2023/displaytype/4.phtml'
    browser.get(url_balance)
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
    stock_df=stock_df.head(4)
    data_list = np.array(stock_df)
    data_list = data_list.tolist()

    processes = 4
    number = len(stock_df)
    with multiprocessing.Pool(int(processes)) as pool:
        pool.map(worker, data_list)




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

url_balance='https://money.finance.sina.com.cn/corp/go.php/vFD_BalanceSheet/stockid/600660/ctrl/2023/displaytype/4.phtml'
browser.get(url_balance)



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
