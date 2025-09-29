#!/usr/bin/env python
# -*- coding: utf-8 -*-

from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import random
import time  # 添加time模块导入
import psycopg2  # 添加psycopg2模块导入
import requests
from requests.exceptions import ConnectionError, RequestException, Timeout

proxy_support = 1

def check_proxy(proxy, timeout=5):
    """检查代理IP是否可用
    
    参数:
        proxy (str): 格式为 "ip:port" 的代理地址
        timeout (int): 请求超时时间（秒）
    
    返回:
        bool: 代理可用返回True，否则返回False
    """
    test_url = 'http://www.baidu.com'  # 用百度作为测试网站
    proxies = {
        'http': f'http://{proxy}',
        'https': f'http://{proxy}'
    }
    
    try:
        response = requests.get(test_url, proxies=proxies, timeout=timeout)
        if response.status_code == 200:
            print(f"代理 {proxy} 可用")
            return True
        else:
            print(f"代理 {proxy} 不可用，状态码: {response.status_code}")
            return False
    except (ConnectionError, RequestException, Timeout) as e:
        print(f"代理 {proxy} 不可用，错误: {e.__class__.__name__}")
        return False

def get_browser(headless=False, proxy=None, window_on_top=False):
    # 配置 ChromeDriver 路径和 Chrome 浏览器路径
    chrome_driver_path = "/home/aaron/eastmoney/chrome/chromedriver-linux64/chromedriver"  # 替换为你的驱动路径
    chrome_binary_path = "/home/aaron/eastmoney/chrome/chrome-linux64/chrome"        # 替换为你的浏览器路径
    service = Service(executable_path=chrome_driver_path)  # 指定驱动路径
    
    browser = None
    
    chrome_options = Options()
    chrome_options.binary_location = chrome_binary_path  # 指定 Chrome 二进制路径

    chrome_options.add_argument(
            'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.7339.207 Safari/537.36')
    
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    
    chrome_options.add_argument("disable-infobars");
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    
    chrome_options.add_argument("--disable-extensions")
    
    if headless is True:
        # 添加无头headlesss
        chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--ignore-certificate-errors')
    #chrome_options.add_argument("blink-settings=imagesEnabled=false")  #image disable
    
    #chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disk-cache-dir=/dev/shm  --disk-cache-size=4096000000')

    # 如果需要使用代理
    if proxy:
        # 从数据库读取代理并选择一个可用的
        selected_proxy = get_random_valid_proxy_from_db()
        
        # 如果没有找到可用代理，从预设代理列表中随机选择一个
        if not selected_proxy:
            print("没有找到可用的数据库代理，从预设代理列表中随机选择")
            # 定义预设代理列表
            #proxy_list = ["142.171.166.165:3128", "101.43.29.22:3128", "localhost"]
            proxy_list = ["101.43.29.22:3128", "localhost"]
            # 随机选择一个代理
            selected_proxy = random.choice(proxy_list)
        
        # 设置代理
        print(f"使用代理: {selected_proxy}")
        if selected_proxy != "localhost":
            chrome_options.add_argument(f'--proxy-server={selected_proxy}')
        else:
            print("使用localhost，不设置代理服务器参数")
    else:
        print(f"不使用代理")
        pass

    chrome_options.add_argument("--window-size=1920,1080")  # 最小推荐尺寸

    try:
        browser = webdriver.Chrome(service=service, options=chrome_options)
    except:
        time.sleep(60)
        try:
            browser = webdriver.Chrome(service=service, options=chrome_options)
        except:
            pass
    finally:
        if browser is None:
            try:
                time.sleep(60)
                browser = webdriver.Chrome(service=service, options=chrome_options)
            except:
                pass

    # 只在window_on_top为True时最大化窗口
    if window_on_top:
        browser.maximize_window()  # 最大化窗口
    
    wait = WebDriverWait(browser, 10)
    with open('./stealth.min.js') as f:
        js = f.read()
        browser.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", { "source": js })
        
    return browser

def get_random_valid_proxy_from_db():
    """从数据库读取代理，随机遍历代理，找到第一个可用的代理就返回
    
    返回:
        str: 格式为 "ip:port" 的代理地址，如果没有可用代理则返回None
    """
    try:
        # 连接数据库（使用用户指定的localhost连接信息）
        connection = psycopg2.connect(
            host="localhost",   # 数据库主机
            port="5432",
            database="usr",
            user="usr",
            password="usr"
        )
        
        cursor = connection.cursor()
        
        # 从ip_proxy表中读取所有ip和port
        cursor.execute("SELECT ip, port FROM ip_proxy")
        proxies = cursor.fetchall()
        
        print(f"从数据库读取到 {len(proxies)} 个代理")
        
        # 随机打乱代理列表
        random.shuffle(proxies)
        
        # 随机遍历代理，找到第一个可用的代理就返回
        for ip, port in proxies:
            proxy = f"{ip}:{port}"
            if check_proxy(proxy):
                # 找到可用代理，关闭数据库连接并返回
                cursor.close()
                connection.close()
                return proxy
            else:
                # 如果代理不可用，从数据库中删除
                try:
                    cursor.execute(
                        "DELETE FROM ip_proxy WHERE ip = %s AND port = %s",
                        (ip, port)
                    )
                    connection.commit()
                    print(f"已从数据库删除不可用的代理: {proxy}")
                except (Exception, psycopg2.Error) as error:
                    print(f"从数据库删除代理时出错: {error}")
        
    except (Exception, psycopg2.Error) as error:
        print(f"从数据库读取代理时出错: {error}")
    
    finally:
        # 关闭数据库连接
        if 'connection' in locals() and connection:
            if 'cursor' in locals() and cursor:
                cursor.close()
            connection.close()
    
    # 如果没有找到可用代理，返回None
    return None
