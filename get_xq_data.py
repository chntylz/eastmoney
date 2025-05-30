#!/#!/usr/bin/env python  
# -*- coding: utf-8 -*-


import pandas as pd
import json
import requests
import re

import time
import datetime

from file_interface import *
from comm_selenium import *

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
import time, datetime
import pandas as pd
import os
import re

import random

from PIL import Image
from selenium.webdriver.common.action_chains import ActionChains

debug = 1

def _init():
    global _global_browser
    _global_browser = {}

def set_browser(key):
    _global_browser[0] = key

def xq_get_browser():
    return  _global_browser[0]

def xq_login(driver):
    my_dbg('xq_login')
    time.sleep(1)
    driver.get('https://xueqiu.com/user/login')
    time.sleep(2)
    driver.find_element_by_name("username").clear()
    driver.find_element_by_name("username").send_keys('chntylz@gmail.com')
    driver.find_element_by_name("password").clear()
    driver.find_element_by_name("password").send_keys('820820')
    driver.find_element_by_class_name("button").click()
    time.sleep(1)



def get_captcha(driver):
    # 隐藏滑块图片
    js_hide_slice = 'document.getElementsByClassName("geetest_canvas_slice")[0].style.display="none"'
    driver.execute_script(js_hide_slice)
    # 截取背景图片
    bg_path = './bg.png'
    driver.find_element_by_class_name("geetest_canvas_bg").screenshot(bg_path)
    # 显示滑块隐藏背景图
    js_display_slice = 'document.getElementsByClassName("geetest_canvas_slice")[0].style.display="block"'
    js_hide_bg = 'document.getElementsByClassName("geetest_canvas_bg")[0].style.display="none"'
    driver.execute_script(js_display_slice + ";"+ js_hide_bg)
    # 截取滑块图片
    slice_path = './slice.png'
    driver.find_element_by_class_name("geetest_canvas_slice").screenshot(slice_path)
    # 显示完整图片
    js_display_full = 'document.getElementsByClassName("geetest_canvas_fullbg")[0].style.display="block"'
    driver.execute_script(js_display_full)
    # 截取完整图片
    full_path = './full.png'
    driver.find_element_by_class_name("geetest_canvas_fullbg").screenshot(full_path)

    # 还原所有的样式
    js_slice = 'document.getElementsByClassName("geetest_canvas_slice")[0].style.display=""'
    js_bg = 'document.getElementsByClassName("geetest_canvas_bg")[0].style.display=""'
    js_full = 'document.getElementsByClassName("geetest_canvas_fullbg")[0].style.display="none"'
    driver.execute_script(js_slice+";"+js_bg+";"+js_full)

    return bg_path, slice_path, full_path

# 滑块x坐标
def get_slice_x(img_slice):
    img = Image.open(img_slice)
    w, h = img.size
    for x in range(w):
        for y in range(h):
            rgb = img.getpixel((x, y))
            if rgb[0] + rgb[1] + rgb[2] < 570:
                my_dbg("滑块坐标", x)
                return x

# 缺口x坐标
def get_bg_x(img_bg, img_full):
    bg = Image.open(img_bg)
    full = Image.open(img_full)
    w,h = bg.size
    for x in range(w):
        for y in range(h):
            bg_point = bg.getpixel((x, y))
            full_point = full.getpixel((x, y))
            r = bg_point[0] -full_point[0]
            g = bg_point[1] - full_point[1]
            b = bg_point[2] - full_point[2]
            abs_value = abs(r)+abs(g)+abs(b)
            if abs_value >180:
                my_dbg("缺口坐标", x)
                return x

# 计算距离
def get_distance(img_slice, img_bg, img_full):
    slice_x = get_slice_x(img_slice)
    bg_x = get_bg_x(img_bg, img_full)
    my_dbg("距离计算", abs(slice_x-bg_x))
    return abs(slice_x-bg_x)


# 模拟人工移动
def get_track(distance):
    v = 0 # 初速度
    tracks = [] # 运动轨迹

    current = 0 # 当前的距离
    mid = distance * 5/8
    distance += 10
    while current < distance:
        t = random.randint(1, 4)/10
        if current < mid:
            a = random.randint(1, 3)
        else:
            a = -random.randint(2,4)
        v0 = v
        # 位移和时间的关系
        s = v0*t + 0.5 * a * t**2
        # 当前的位置
        current += abs(s)
        # 记录位移量
        tracks.append(round(s))
        v= v0 + a* t
    temp=10+(current-distance)
    for i in range(4):
        num = -random.randint(2,3)
        tracks.append(num)
        temp +=num
    tracks.append(abs(temp)) if temp < 0 else tracks.append(-temp)

    return tracks
    
# 移动滑块
def move_slider(driver, tracks):
    element = driver.find_element(by=By.CLASS_NAME, value='geetest_slider_button')
    ActionChains(driver).click_and_hold(element).perform()
    for x in tracks:
        ActionChains(driver).move_by_offset(xoffset=x, yoffset=0).perform()
    ActionChains(driver).release(element).perform()



def start_slider_login(driver):

    ret = 0

    img_bg, img_slice, img_full = get_captcha(driver)
    time.sleep(1)
    distance = get_distance(img_slice, img_bg, img_full)

    count = 6
    while count > 0:
        tracks = get_track(distance)
        move_slider(driver, tracks)

        locator = (By.CLASS_NAME,'geetest_panel_error_title') # 判断是否滑动成功的一个依据
        try:
            WebDriverWait(driver=driver, timeout=2, poll_frequency=0.2).until(EC.element_to_be_clickable(locator), message="") # 判断提示语是否出现,如果出现就点击下一步重试按钮
            driver.find_element_by_class_name('geetest_panel_error_content').click()
            time.sleep(2)
            # 获取缺口图,滑块图,完整图
            img_bg, img_slice, img_full = get_captcha(driver)
            # 获取距离
            distance = get_distance(img_slice, img_bg, img_full)
            my_dbg(distance)
            time.sleep(1)
            count -= 1
            continue
        except Exception as e:
            pass

        locator = (By.XPATH,'//div[@class="geetest_wrap"]')
        try:
            WebDriverWait(driver=driver, timeout=2, poll_frequency=0.2).until_not(EC.visibility_of_element_located(locator),
                                                                              message="")  # 判断  如果这个可见然后取反 就是不可见,验证码不可见,就证明已经成功了
            my_dbg("校验成功了")
            time.sleep(3)
            ret = 1
            driver.save_screenshot('xueqiu.png')
            break
        except TimeoutException as e:
            count -= 1

    return ret


def xq_login2(driver):
    #selenium example
    #https://vimsky.com/examples/detail/python-ex-selenium.webdriver.support.ui-WebDriverWait-send_keys-method.html
    #https://www.cnblogs.com/denise1108/p/10551019.html
    my_dbg('xq_login')
    driver.get('https://xueqiu.com/')
    time.sleep(2)
    driver.find_element('xpath', '//*[@id="modal__login__main"]/div[1]/div[1]/div[1]/a[2]').click()

    elem = WebDriverWait(driver, 30).until(EC.visibility_of_element_located((By.NAME, 'username')))
    time.sleep(2)
    elem.send_keys('chntylz@gmail.com')
    elem = WebDriverWait(driver, 30).until(EC.visibility_of_element_located((By.NAME, 'password')))
    time.sleep(2)
    elem.send_keys('820820')
    driver.find_element('xpath', '//*[@id="modal__login__main"]/div[2]/label/i').click()
    time.sleep(2)

    driver.find_element('xpath', '//*[@id="modal__login__main"]/div[2]/div[2]').click()
    time.sleep(2)



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

    my_dbg(url)
    _global_browser = xq_get_browser()
    try:
        pass
        # xq_login2(_global_browser)
    except Exception as e:
        my_dbg(e)
        my_dbg('alread login in')
    finally:
        pass

    html = ''
    try: 
        _global_browser.get(url)
        _global_browser.implicitly_wait(5)
        html = _global_browser.page_source
    except Exception as e:
        my_dbg(e)
        #_global_browser.close()
        #_global_browser.quit()
    finally:
        #_global_browser.close() 
        ##_global_browser.quit()
        pass
     
    if debug:
        my_dbg(html)
    
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
        my_dbg(e)
        if debug:
            my_dbg(jigou_df.head(5))
            my_dbg(url)
            my_dbg(html)
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
    my_dbg(url)

    _global_browser = xq_get_browser()
    try:
        pass
        # xq_login2(browser)
    except Exception as e:
        my_dbg(e)
        my_dbg('alread login in')
    finally:
        pass


    html = ''
    try: 
        _global_browser.get(url)
        _global_browser.implicitly_wait(5)
        html = _global_browser.page_source
    except Exception as e:
        my_dbg(e)
        # _global_browser.close()
        # _global_browser.quit()
    finally:
        # _global_browser.close()
        # _global_browser.quit()
        pass

    if debug:
        my_dbg(html)
    
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
        xq_login2(_global_browser)
        my_dbg(e)
        my_dbg(url)
        my_dbg(html)
    finally:
        pass
 
    return data_df


def xq_get_fund(stock_code, report_date):

    data_df = pd.DataFrame()

    fund_report_date = str(int(string2timestamp(report_date)*1000))

    url='https://stock.xueqiu.com/v5/stock/f10/cn/org_holding/detail.json?'\
            + 'symbol=' + stock_code + '&timestamp=' + fund_report_date + '&extend=true'

    html = ''
    my_dbg(url)

    _global_browser = xq_get_browser()
    try:
        _global_browser.implicitly_wait(15)
        #time.sleep(5)
        _global_browser.get(url)
        html = _global_browser.page_source
    except Exception as e:
        my_dbg(e)
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
        #xq_login2(_global_browser)
        
        my_dbg('xq_get_fund() %s %s' % (stock_code, report_date))
        my_dbg(e)
        my_dbg(url)
        my_dbg(html)
    finally:
        pass


    data_df = pd.DataFrame(data)

    return data_df



if __name__ == '__main__':

    _init()

    browser = get_browser()
    set_browser(browser)
    xq_login2(browser)
    start_slider_login(browser)


    fina_data = xq_get_raw_data2('SZ000977')
    fina_data = fina_data['data']['list']

    df = pd.DataFrame(fina_data) 

    my_dbg(df)
 
