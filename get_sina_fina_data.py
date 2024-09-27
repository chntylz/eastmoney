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


# how to handle soup tbody, please refer to follow link
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


from HData_sina_balance import *
from HData_sina_income import *
from HData_sina_cashflow import *
from HData_sina_fina import *

hdata_sina_balance = HData_sina_balance("usr","usr")
hdata_sina_income  = HData_sina_income("usr","usr")
hdata_sina_cashflow= HData_sina_cashflow("usr","usr")
hdata_sina_fina    = HData_sina_fina("usr","usr")

debug = 0
debug = 0


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

    if debug:
        print(url)

    data = []
    gen_cols = []
    browser.get(url)
    html_doc=browser.page_source

    soup = BeautifulSoup(html_doc, 'html.parser')


    ####################################################
    tbodys = soup.find_all('tbody')
    
   
    i=0
    tbody_find = False
    for i in range(len(tbodys)):
        if '报表日期' in tbodys[i].text or '报告日期' in tbodys[i].text: #check where valid data is located tbodys 
            tbody_find = True
            if debug:
                print(" right tbody is found, i=%d" % i)
            break
        else:
            if False:
                print(i)
            pass
    

    if tbody_find == False:
        print('### correct tbodys_find is False , return')
        return data, gen_cols
        

    tbody = tbodys[i]

    
    for idx, tr in enumerate(tbody.find_all('tr')):
        
        abbr = ''
        tds = tr.find_all('td')
        length = len(tds)

        if False:
            print('--------------------------------')
            print(tds)
        
        #'/corp/view/vFD_FinanceSummaryHistory.php?stockid=600660&type=TOTLIABSHAREQUI&cate=zcfz0'
        # we assume there must be 2 items exist
        if length >=2:
            try:
                
                href = tds[0].contents[0].attrs['href']
                tmp=href[href.find('type'):]
                start=tmp.find('=')
                end=tmp.find('&')
                if end > 0:
                    abbr = tmp[start+1 : end]
                else:
                    abbr = tmp[start+1 : ]
                abbr = abbr.lower()
                gen_cols.append(abbr)
                if False:
                    print('href:%s'% href)
                    print('tmp:%s' % tmp)
                    print('start:%d'%start)
                    print('end:%d' % end)
                    print('abbr:%s' % abbr)
            except Exception as e:
                if debug:
                    print('error: %s' % tds)
                if '报表日期' in tds[0].contents[0].text or '报告日期' in tds[0].contents[0].text:
                    abbr = 'record_date'
                    if debug:
                        print('abbr:%s'%abbr)
                    gen_cols.append(abbr)
                else:
                    href = tds[0].contents[0].find_all('a')
                    href = href[0].attrs['href']
                    tmp=href[href.find('type'):]
                    start=tmp.find('=')
                    end=tmp.find('&')
                    if end > 0:
                        abbr = tmp[start+1 : end]
                    else:
                        abbr = tmp[start+1 : end]
                    abbr = abbr.lower()
                    if debug:
                        print('abbr:%s'%abbr)
                    gen_cols.append(abbr)

        if length == 2:
            data.append([tds[0].contents[0].text.replace(',',''), 
                tds[1].contents[0].replace(',','').replace('--','0').replace('\'','0')]) 
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

    i = 0
    for i, col in enumerate(gen_cols):
        gen_cols[i]=col.replace('\n','').replace(' ','')

        
    return data, gen_cols

def handle_sina_comm_data(data, stock_code, stock_name, year, target_type, data_column):

    if debug:
        print('len(data)=%d, len(data_column)=%d '%(len(data), len(data_column)))
        print(data_column)
    
    df = pd.DataFrame(data)
    df = df.T

    try:
        #change column name
        df.columns = data_column

        #change column order
        #df = df.loc[:, data_column]
    except Exception as e:
        print(data, stock_code, stock_name, year, target_type, data_column)

    '''
    df['stock_code'] = stock_code
    df['stock_name'] = stock_name
    '''
    
    len_of_df = len(df)

    # 在第 1 列位置插入新列 'stock_name'
    new_column_data = []
    i = 0
    for i in range(len_of_df):
        new_column_data.append(stock_name)
    df.insert(1, 'stock_name', new_column_data)

    # 在第 1 列位置插入新列 'stock_code'
    new_column_data = []
    i = 0
    for i in range(len_of_df):
        new_column_data.append(stock_code)
    df.insert(1, 'stock_code', new_column_data)

    if debug:
        print(np.array(df[0:1])[0])  #print the first row of df
        df.to_csv('./csv_data/'+ year + '_' + target_type + '_' +  stock_code + '_tmp.csv', encoding='utf-8-sig')

    if target_type == 'balance' and  '银行' in stock_name:
        return df

    if debug:
        print(df)

    '''
    delete column or row: https://blog.csdn.net/songyunli1111/article/details/79306639
    #df.drop(columns=['B', 'C']) #delete column name 'B' and 'C'
    '''
    df = df.drop(index=[0])  #delete row0
    df = df.reset_index(drop=True)
    
    #str to date format  for database format
    df['record_date']=df['record_date'].apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d'))


    if debug:
        df.to_csv('./csv_data/'+ year + '_' + target_type + '_' + stock_code + '.csv', encoding='utf-8-sig')

    return df


def sina_check_table(database):
    table_exist = database.table_is_exist() 
    if debug:
        print('table_exist=%d' % table_exist)

    if table_exist:
        #database.db_hdata_sina_create()
        if debug:
            print('table already exist')
    else:
        database.db_hdata_sina_create()
        if debug:
            print('table not exist, create')



def sina_update_database(database, df, data, stock_code, stock_name, year, target_type, data_column):
    
    sina_check_table(database)

    try:
        #database.db_hdata_sina_create()
        database.copy_from_stringio(df)
    except Exception as e:
        print("### insert data into database")
        print(data, stock_code, stock_name, year, target_type, data_column)
        print(df)
        df.to_csv('./csv_data/'+ year + '_' + target_type + '_' +  stock_code + '_error.csv', encoding='utf-8-sig')



def get_sina_fina_data(stock_code, stock_name, year, browser):

    target_type = 'fina'


    #https://money.finance.sina.com.cn/corp/go.php/vFD_FinancialGuideLine/stockid/600660/ctrl/2024/displaytype/4.phtml
    url = 'https://money.finance.sina.com.cn/corp/go.php/vFD_FinancialGuideLine/stockid/'\
        + stock_code \
        + '/ctrl/'\
        + year\
        + '/displaytype/4.phtml'


    df = pd.DataFrame()

    #catch html data
    data, data_column = get_sina_comm_data(browser, url)
    if len(data) == 0 or len(data_column) == 0:
        print('### get html source is NULL, return');
        return df

    data_column = [ 'record_date', 'diluted_eps', 'weighted_eps', 'eps_adjusted', 'eps_after_deducting_non_recurring_gains_and_losses', \
    'net_assets_per_share_before_adjustment', 'net_assets_per_share_adjusted', 'operating_cash_flow_per_share', \
    'capital_reserve_per_share', 'retained_eps', 'adjusted_net_assets_per_share', 'total_asset_profit_rate', \
    'main_business_profit_rate', 'total_asset_net_profit_rate', 'cost_and_expense_profit_rate', \
    'operating_profit_rate', 'main_business_cost_rate', 'net_profit_margin', 'return_on_equity', \
    'return_on_net_assets', 'return_on_assets', 'gross_profit_margin', 'proportion_of_three_expenses', \
    'non_main_business_proportion', 'main_business_profit_proportion', 'dividend_payout_rate', \
    'investment_return_rate', 'main_business_profit', 'net_asset_return_rate', \
    'weighted_net_asset_return_rate', 'net_profit_after_deducting_non_recurring_gains_and_losses', \
    'main_business_income_growth_rate', 'net_profit_growth_rate', 'net_asset_growth_rate', \
    'total_asset_growth_rate', 'accounts_receivable_turnover_rate_times', 'accounts_receivable_turnover_days_days', \
    'inventory_turnover_days_days', 'inventory_turnover_rate_times', 'fixed_asset_turnover_rate_times', \
    'total_asset_turnover_rate_times', 'total_asset_turnover_days_days', 'current_asset_turnover_rate_times', \
    'current_asset_turnover_days_days', 'shareholders_equity_turnover_rate_times', 'current_ratio', \
    'quick_ratio', 'cash_ratio', 'interest_coverage_ratio', 'long_term_debt_to_working_capital_ratio', \
    'shareholders_equity_ratio', 'long_term_debt_ratio', 'shareholders_equity_to_fixed_assets_ratio', \
    'liabilities_to_owners_equity_ratio', 'long_term_assets_to_long_term_funds_ratio', \
    'capitalization_ratio', 'fixed_asset_net_value_ratio', 'capitalization_fix_ratio', 'equity_ratio', \
    'liquidation_value_ratio', 'fixed_assets_ratio', 'asset_liability_ratio', 'total_assets', \
    'net_operating_cash_flow_to_sales_revenue_ratio', 'operating_cash_flow_return_on_assets', \
    'net_operating_cash_flow_to_net_profit_ratio', 'net_operating_cash_flow_to_debt_ratio', \
    'cash_flow_ratio', 'short_term_stock_investment', 'short_term_bond_investment_yuan', \
    'short_term_other_operating_investment_yuan', 'long_term_stock_investment_yuan', \
    'long_term_bond_investment_yuan', 'long_term_other_operating_investment_yuan', \
    'accounts_receivable_within_1_year_yuan', 'accounts_receivable_within_1_2_years_yuan', \
    'accounts_receivable_within_2_3_years_yuan', 'accounts_receivable_within_3_years_yuan', \
    'prepayments_within_1_year_yuan', 'prepayments_within_1_2_years_yuan', \
    'prepayments_within_2_3_years_yuan', 'prepayments_within_3_years_yuan', \
    'other_receivables_within_1_year_yuan', 'other_receivables_within_1_2_years_yuan', \
    'other_receivables_within_2_3_years_yuan', 'other_receivables_within_3_years_yuan' ]

    df = handle_sina_comm_data(data, stock_code, stock_name, year, target_type, data_column)

    sina_update_database(hdata_sina_fina, df, data, stock_code, stock_name, year, target_type, data_column)
       
    return df


def get_sina_cashflow_data(stock_code, stock_name, year, browser):

    target_type = 'cashflow'

    #https://money.finance.sina.com.cn/corp/go.php/vFD_CashFlow/stockid/600660/ctrl/2024/displaytype/4.phtml
    url = 'https://money.finance.sina.com.cn/corp/go.php/vFD_CashFlow/stockid/'\
        + stock_code \
        + '/ctrl/'\
        + year\
        + '/displaytype/4.phtml'

    df = pd.DataFrame()

    #catch html data
    data, data_column = get_sina_comm_data(browser, url)
    if len(data) == 0 or len(data_column) == 0:
        print('### get html source is NULL, return');
        return df

    df = handle_sina_comm_data(data, stock_code, stock_name, year, target_type, data_column)

    sina_update_database(hdata_sina_cashflow, df, data, stock_code, stock_name, year, target_type, data_column)

    return df



def get_sina_income_data(stock_code, stock_name, year, browser):

    target_type = 'income'

    #https://money.finance.sina.com.cn/corp/go.php/vFD_ProfitStatement/stockid/600660/ctrl/2024/displaytype/4.phtml
    url = 'https://money.finance.sina.com.cn/corp/go.php/vFD_ProfitStatement/stockid/'\
        + stock_code \
        + '/ctrl/'\
        + year\
        + '/displaytype/4.phtml'

    df = pd.DataFrame()

    #catch html data
    data, data_column = get_sina_comm_data(browser, url)
    if len(data) == 0 or len(data_column) == 0:
        print('### get html source is NULL, return');
        return df

    df = handle_sina_comm_data(data, stock_code, stock_name, year, target_type, data_column)

    sina_update_database(hdata_sina_income, df, data, stock_code, stock_name, year, target_type, data_column)

    return df


def get_sina_balance_data(stock_code, stock_name, year, browser):

    target_type = 'balance'

    #url_balance='https://money.finance.sina.com.cn/corp/go.php/vFD_BalanceSheet/stockid/600660/ctrl/2023/displaytype/4.phtml'
    url='https://money.finance.sina.com.cn/corp/go.php/vFD_BalanceSheet/stockid/'\
        + stock_code \
        + '/ctrl/'\
        + year\
        + '/displaytype/4.phtml'


    df = pd.DataFrame()

    #catch html data
    data, data_column = get_sina_comm_data(browser, url)
    if len(data) == 0 or len(data_column) == 0:
        print('### get html source is NULL, return');
        return df

    df = handle_sina_comm_data(data, stock_code, stock_name, year, target_type, data_column)

    sina_update_database(hdata_sina_balance, df, data, stock_code, stock_name, year, target_type, data_column)

    return df


def get_sina_real_data(stock_code, stock_name, year, browser):
    
    df = pd.DataFrame()
    df_balance = get_sina_balance_data(stock_code, stock_name, year, browser)
    df_income  = get_sina_income_data(stock_code, stock_name, year, browser)
    df_cashflow  = get_sina_cashflow_data(stock_code, stock_name, year, browser)
    df_fina      = get_sina_fina_data(stock_code, stock_name, year, browser)
    return df

def get_sina_fina_by_soup(stock_code, stock_name):
    
    time.sleep(random.randint(1, 2))

    df = pd.DataFrame()
    browser = open_browser()

    this_year = int(time.strftime("%Y", time.localtime()))
    #get continuous 5 years data
    target_years = 5
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
    print("start_time: %s" % start_time)

    stock_df=get_daily_zlje2()
    stock_df = stock_df.sort_values('f12', ascending=1)
    stock_df = stock_df.reset_index(drop=True)
    print(stock_df.head(5))
    #stock_df=stock_df.head(4)
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

from bs4 import BeautifulSoup
import pandas as pd

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


url = 'https://money.finance.sina.com.cn/corp/go.php/vFD_ProfitStatement/stockid/600660/ctrl/2024/displaytype/4.phtml'
browser.get(url)

url = 'https://money.finance.sina.com.cn/corp/go.php/vFD_CashFlow/stockid/600660/ctrl/2024/displaytype/4.phtml'
browser.get(url)

html_doc=browser.page_source

soup = BeautifulSoup(html_doc, 'html.parser')

data, data_column = get_sina_comm_data(browser, url)

####################################################
tbodys = soup.find_all('tbody')
i=0
for i in range(len(tbodys)):
    if '报表日期' in tbodys[i].text or '报告日期' in tbodys[i].text: #check where valid data is located tbodys
        print(" right tbody is found")
        print(i)
        break
    else:
        if False:
            print(i)
        pass

tbody = tbodys[i]

trs = tbody.find_all('tr')
tr=trs[0]
tds=tr.find_all('td')





df = pd.DataFrame(data)
df = df.T




browser.close()
browser.quit()

'''


'''
import pandas as pd

# 创建一个示例 DataFrame
data = {
    'A': [1, 2, 3],
        'B': [4, 5, 6]
}
df = pd.DataFrame(data)

# 在第 1 列位置插入新列 'C'
new_column_data = [7, 8, 9]
df.insert(1, 'C', new_column_data)
print('插入新列 C 后的 DataFrame:')
print(df)

'''

