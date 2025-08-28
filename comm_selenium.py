#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from file_interface import *

import random
proxy_support = 0

def get_browser(headless=None, proxy=None):

    path_chromedriver='/usr/bin/chromedriver'
    path_chromedriver='/snap/bin/chromium.chromedriver'
    #path_chromedriver='/home/aaron/chrome/138.0.7204.168/driver/chromedriver-linux64/chromedriver'
    browser = None
    
    # 添加无头headlesss
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument(
            'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'\
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36')
     
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
     
    
    chrome_options.add_argument("disable-infobars");
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
     
    chrome_options.add_argument("--disable-extensions")
    if headless is None:
        chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--ignore-certificate-errors')
    #chrome_options.add_argument("blink-settings=imagesEnabled=false")  #image disable
     
    #chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disk-cache-dir=/dev/shm  --disk-cache-size=4096000000')

    # 生成0或1
    random_number = 0
    if proxy_support :
       random_number = random.randint(0, 1)
       print(f"selenium proxy: {random_number}")

    if random_number:
        chrome_options.add_argument('--proxy-server=142.171.166.165:3128')
    elif proxy :
        chrome_options.add_argument('--proxy-server=142.171.166.165:3128')
        #chrome_options.add_argument('--proxy-server=http://147.75.34.86:9401')
        #chrome_options.add_argument('--proxy-server=socks5://220.167.89.46:1080')
    else:
        pass

    chrome_options.add_argument("--window-size=1920,1080")  # 最小推荐尺寸

    try:
        browser = webdriver.Chrome(executable_path=path_chromedriver,
            chrome_options=chrome_options)
    except:
        time.sleep(60)
        try:
            browser = webdriver.Chrome(executable_path=path_chromedriver,
                chrome_options=chrome_options)
        except:
            pass
    finally:
        if browser is None:
            try:
                time.sleep(60)
                browser = webdriver.Chrome(executable_path=path_chromedriver,
                    chrome_options=chrome_options)
            except:
                pass

    browser.maximize_window()  # 最大化窗口
    wait = WebDriverWait(browser, 10)
    with open('./stealth.min.js') as f:
        js = f.read()
    browser.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": js
        })
        
    return browser
    
    

'''
options.add_argument(‘headless’) # 无头模式
options.add_argument(‘window-size={}x{}’.format(width, height)) # 直接配置大小和set_window_size一样
options.add_argument(‘disable-gpu’) # 禁用GPU加速
options.add_argument(‘proxy-server={}’.format(self.proxy_server)) # 配置代理
options.add_argument(’–no-sandbox’) # 沙盒模式运行
options.add_argument(’–disable-setuid-sandbox’) # 禁用沙盒
options.add_argument(’–disable-dev-shm-usage’) # 大量渲染时候写入/tmp而非/dev/shm
options.add_argument(’–user-data-dir={profile_path}’.format(profile_path)) # 用户数据存入指定文件
options.add_argument('no-default-browser-check) # 不做浏览器默认检查
options.add_argument("–disable-popup-blocking") # 允许弹窗
options.add_argument("–disable-extensions") # 禁用扩展
options.add_argument("–ignore-certificate-errors") # 忽略不信任证书
options.add_argument("–no-first-run") # 初始化时为空白页面
options.add_argument(’–start-maximized’) # 最大化启动
options.add_argument(’–disable-notifications’) # 禁用通知警告
options.add_argument(’–enable-automation’) # 通知(通知用户其浏览器正由自动化测试控制)
options.add_argument(’–disable-xss-auditor’) # 禁止xss防护
options.add_argument(’–disable-web-security’) # 关闭安全策略
options.add_argument(’–allow-running-insecure-content’) # 允许运行不安全的内容
options.add_argument(’–disable-webgl’) # 禁用webgl
options.add_argument(’–homedir={}’) # 指定主目录存放位置
options.add_argument(’–disk-cache-dir={临时文件目录}’) # 指定临时文件目录
options.add_argument(‘disable-cache’) # 禁用缓存
options.add_argument(‘excludeSwitches’, [‘enable-automation’]) # 开发者模式
————————————————

                            版权声明：本文为博主原创文章，遵循 CC 4.0 BY-SA 版权协议，转载请附上原文出处链接和本声明。
                        
原文链接：https://blog.csdn.net/weixin_44929594/article/details/122513925
'''
