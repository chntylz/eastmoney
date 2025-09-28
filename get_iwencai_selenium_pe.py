#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import os
import datetime
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from selenium.webdriver.common.action_chains import ActionChains


from get_daily_zlje import *

import random
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
my_dbg = logging.info


# basic
import numpy as np
import pandas as pd

from HData_iwencai_pe import *
from HData_eastmoney_day import *

hdata_pe=HData_iwencai_pe("usr","usr")
hdata_day=HData_eastmoney_day("usr","usr")



debug = 0
debug = 1

global_first_time = True

#https://medium.com/@dharmendradiwaker12/extracting-data-from-canvas-line-charts-with-selenium-python-and-javascript-2cc931104264
def scrape_canvas_data(driver):
    """
    Scrapes tooltip data from a canvas on a given page URL.

    Parameters:
    url (str): The URL of the webpage containing the canvas element.
    hover_steps (int): The number of steps to move horizontally across the canvas to hover and extract data.
    
    Returns:
    list: A list of dictionaries containing date and transaction data.
    """
    try:
        hover_steps=20
        # Locate the canvas element
        canvas = driver.find_element(By.XPATH, "//canvas[@data-zr-dom-id='zr_0']")

        # Scroll into view of the canvas to ensure it's fully visible
        driver.execute_script("arguments[0].scrollIntoView(true);", canvas)
        #time.sleep(1)  # Wait for the canvas to load

        # Get canvas dimensions using JavaScript
        canvas_width = driver.execute_script("return arguments[0].width;", canvas)
        canvas_height = driver.execute_script("return arguments[0].height;", canvas)

        # Calculate x_offset_step based on the number of hover steps
        x_offset_step = canvas_width // hover_steps
        x_offset_step = 1

        # List to store the extracted data
        extracted_data = {}

        # Hover over the canvas in small steps
        #for x_offset in range( 0, canvas_width, x_offset_step):
        for x_offset in range( canvas_width, canvas_width + 1, x_offset_step):
            try:
                # Set y_offset to somewhere in the middle of the canvas
                y_offset = canvas_height // 2

                # JavaScript to simulate the hover action
                hover_script = f"""
                    var event = new MouseEvent('mousemove', {{
                        clientX: arguments[0].getBoundingClientRect().left + {x_offset}, 
                        clientY: arguments[0].getBoundingClientRect().top + {y_offset}, 
                        bubbles: true
                    }});
                    arguments[0].dispatchEvent(event);
                """

                # Execute the hover action at the current x_offset
                driver.execute_script(hover_script, canvas)
                time.sleep(1)  # Wait briefly to let the tooltip appear

                # Check for tooltip and extract data
                try:
                    date_element = WebDriverWait(driver, 30).until(
                        EC.visibility_of_element_located((By.XPATH, "//div[contains(@style, 'display: inline-block; margin-right: 10px; height:17px; line-height:17px;color: #262626')]"))
                    )
                    transaction_element = driver.find_element(By.XPATH, "//span[contains(@style, 'color: #262626;line-height:17px;height:17px;')]")

                    
                    # Extract data and store it in the list
                    date_value = date_element.text
                    transaction_value = transaction_element.text
                    extracted_data[date_value] = float(transaction_value)
                    if debug:
                        my_dbg('date_value:%s, transaction_value:%s' % (date_value, transaction_value)  )
                    
                except:
                    # No tooltip found at this point, continue to the next
                    pass
            except Exception as outer_exception:
                # Handle any other issues like JavaScript or element issues
                my_dbg(f"Error at x_offset: {x_offset}, {outer_exception}")
    except:
        pass

    record_date = ''
    pe_pct = 0
    try:
        for key, value in extracted_data.items():
            record_date = key
            pe_pct =  value
            if debug:
                my_dbg(f"{key}: {value}")  
                my_dbg(record_date, pe_pct)  
    except Exception as e:
        if debug:
            my_dbg(" pe failed" )
            my_dbg(e)
        pass

    return record_date, pe_pct


def get_browser_real():
    browser = None
    
    # 添加无头headless模式
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument(
            'user-agent=Mozilla/5.0 (X11; Linux x86_64)'
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36')

    chrome_options.add_argument("disable-infobars")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--headless=new")  # 新版无头模式
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--disable-dev-shm-usage')  # 解决内存问题

    # 尝试不同的Chrome驱动路径
    driver_paths = [
        '/usr/bin/chromedriver',
        '/snap/bin/chromium.chromedriver',
        '/usr/local/bin/chromedriver',
        '/usr/bin/google-chrome/chromedriver'
    ]

    max_retries = 3
    retry_count = 0

    while retry_count < max_retries and browser is None:
        for path in driver_paths:
            if os.path.exists(path):
                try:
                    browser = webdriver.Chrome(executable_path=path, options=chrome_options)
                    my_dbg(f"成功使用驱动路径: {path}")
                    break
                except Exception as e:
                    my_dbg(f"尝试路径 {path} 失败: {e}")
                    continue

        # 如果所有路径都失败，尝试使用系统默认
        if browser is None:
            try:
                browser = webdriver.Chrome(options=chrome_options)
                my_dbg("成功使用系统默认驱动")
            except Exception as e:
                my_dbg(f"使用系统默认驱动失败: {e}")
                retry_count += 1
                if retry_count < max_retries:
                    my_dbg(f"等待60秒后重试... (第 {retry_count+1} 次)")
                    time.sleep(60)

    # 加载stealth脚本防止检测
    if browser is not None:
        try:
            with open('./stealth.min.js') as f:
                js = f.read()
            browser.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": js
            })
        except Exception as e:
            my_dbg(f"加载stealth脚本失败: {e}")
        
    return browser
    

def get_iwencai_pe(driver, stock_code):
    url='https://iwencai.com/unifiedwap/result?w='+ stock_code + 'pe'
    if debug:
        my_dbg(url)

    record_date = ''
    pe = 100
    max_retries = 3
    retries = 0

    while retries < max_retries:
        try: 
            driver.get(url)
            time.sleep(random.randint(5,20))  # 减少等待时间，提高效率

            # 检查是否有会话错误
            page_source = driver.page_source
            if 'invalid session id' in page_source.lower():
                my_dbg(f"会话无效，尝试重新连接 (第 {retries+1} 次)")
                retries += 1
                if retries >= max_retries:
                    return stock_code, '', 100
                # 刷新页面
                driver.refresh()
                time.sleep(3)
                continue

            global global_first_time  # 声明要修改全局变量
            if global_first_time: 
                try:
                    # 处理证书警告
                    driver.find_element(By.ID, "details-button").click()
                    time.sleep(1)
                    driver.find_element(By.ID, "proceed-link").click()
                except Exception as e:
                    if debug:
                        my_dbg(f"处理证书警告失败: {e}")
                    pass

                global_first_time = False

            # 尝试获取数据
            record_date, pe = scrape_canvas_data(driver)
            
            # 如果获取到有效数据，跳出循环
            if record_date and pe > 0:
                break
            else:
                my_dbg(f"未获取到有效数据，重试 (第 {retries+1} 次)")
                retries += 1
                time.sleep(3)

        except Exception as e:
            my_dbg(f"访问失败: {e}")
            my_dbg(f'stock_code={stock_code}')
            retries += 1
            if retries >= max_retries:
                return stock_code, '', 100
            time.sleep(3)
            continue

    if debug:
        my_dbg(stock_code, record_date, pe)

    return stock_code, record_date, pe

def check_table():
    table_exist = hdata_pe.table_is_exist() 
    my_dbg('table_exist=%d' % table_exist)
    if table_exist:
        #hdata_pe.db_hdata_iwencai_create()
        my_dbg('table already exist, recreate')
    else:
        hdata_pe.db_hdata_iwencai_create()
        my_dbg('table not exist, create')


def do_scrapy_pe(stock_df, pe_file):

    with open(pe_file,'a') as f:
        f.write( 'timestamp,stock_code,record_date,iwencai_pe\n')

    headless=False
    #driver = get_browser_real()
    driver = get_browser(headless=False) 

    pe_list = []

    stock_df_len = len(stock_df)
    for i in range(stock_df_len):
        stock_code = stock_df.stock_code[i]
        if stock_code[0] == '9' or stock_code[0] == '2':
            continue
        if debug:
            my_dbg(stock_code)

        # 尝试获取PE数据，增加异常处理
        try:
            stock_code, record_date, pe_pct = get_iwencai_pe(driver, stock_code)
        
            # 如果获取失败，尝试重新创建驱动并再次获取
            if pe_pct == 0 or not record_date:
                my_dbg(f'首次获取失败，尝试重新连接 (stock_code:{stock_code})')
                try:
                    # 关闭当前驱动
                    driver.quit()
                    # 创建新驱动
                    driver = get_browser(headless=False) 
                    # 再次尝试获取数据
                    stock_code, record_date, pe_pct = get_iwencai_pe(driver, stock_code)
                except Exception as e:
                    my_dbg(f'重新连接失败: {e}')
        except Exception as e:
            my_dbg(f'获取PE数据异常: {e}')
            pe_pct = 100  # 设置默认值

        cur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        with open(pe_file,'a') as f:
            f.write('%s,%s,%s,%s\n' % (cur_time, stock_code, record_date, pe_pct))

        pe_list.append([cur_time, stock_code, record_date, pe_pct])

    driver.quit()

    data_column = ['run_date', 'stock_code', 'record_date', 'iwencai_pe' ]
    pe_df=pd.DataFrame(pe_list, columns=data_column)
    pe_df['record_date'] = pd.to_datetime(pe_df['record_date'], errors='coerce')
    pe_df = pe_df.dropna(subset=['record_date'])  # 删除无效日期

    return pe_df


if __name__ == '__main__':

    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    #stock_df = get_latest_zlje_from_db()
    stock_df = hdata_day.get_latest_data_from_hdata()
    #stock_df =  stock_df.head(1)

    stock_df_len = len(stock_df)
    my_dbg(stock_df.head(5))
    my_dbg('stock_df.len:%s' % len(stock_df))

    exec_command = "mkdir -p csv" 
    os.system(exec_command)

    pe_file = time.strftime("%Y-%m-%d", time.localtime())
    pe_file = './csv/' + pe_file +'_pe.csv'

    with open(pe_file,'w') as f:
        f.write( 'timestamp,stock_code,record_date,iwencai_pe\n')

    pe_first_df = pd.DataFrame()
    if True:
        pe_first_df = do_scrapy_pe(stock_df, pe_file)
    else: 
        pe_first_df = pd.read_csv('./csv/pe.csv',encoding='gbk', dtype={'stock_code':str})

    pe_zero_df = pe_first_df[(pe_first_df['iwencai_pe'] == 0.0) | (pe_first_df['iwencai_pe'] == 100.0)]
    pe_zero_df = pe_zero_df.reset_index(drop=True)

    pe_non_zero_df = pe_first_df[pe_first_df['iwencai_pe'] != 0.0]
    pe_non_zero_df = pe_non_zero_df.reset_index(drop=True)

    #第二次, pe为0的重抓一次
    pe_second_df = do_scrapy_pe(pe_zero_df, pe_file)

    pe_df = pd.concat([pe_first_df, pe_second_df])
    pe_df = pe_df.reset_index(drop=True)

    if len(pe_df) > 4000:
        #check table exist
        check_table()

        hdata_pe.delete_data_from_hdata(
                start_date=datetime.datetime.now().date().strftime("%Y-%m-%d"),
                end_date=datetime.datetime.now().date().strftime("%Y-%m-%d")
                )

        hdata_pe.copy_from_stringio(pe_df)
 
    exec_command = "cp -f " + pe_file  + "  csv/pe.csv" 
    os.system(exec_command)

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    my_dbg("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    my_dbg("main_company.py t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
    
    
