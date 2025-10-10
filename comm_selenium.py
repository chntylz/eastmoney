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


# 定义预设代理列表
proxy_list = ["142.171.166.165:3128", "101.43.29.22:3128", "127.0.0.1"]
#proxy_list = ["101.227.40.36:8888","139.155.243.8:3001" ]
#proxy_list = ["114.80.37.90:3081","39.105.27.30:3128" ]

'''
proxy_list = ["111.3.102.207:30001",
           "47.243.92.199:3128",
           "8.212.165.33:3333",
           "47.96.42.36:80",
           "123.128.12.93:9055",
           "121.43.43.217:10007",
           "114.80.37.90:3081",
           "114.80.37.90:3081",
           "103.118.44.31:8080",
           "113.45.158.25:3128",
           "39.105.27.30:3128",
           "8.212.165.33:3333"]
'''

# 存储上一次选择的代理
last_proxy = None

def get_proxy():
    global last_proxy
    # 过滤掉上一次使用的代理
    available_proxies = [p for p in proxy_list if p != last_proxy]
    
    # 如果所有代理都和上次一样（比如列表只有一个），则允许重复
    if not available_proxies:
        available_proxies = proxy_list[:]
    
    # 随机选择一个
    selected_proxy = random.choice(available_proxies)
    
    # 更新上一次选择
    last_proxy = selected_proxy
    return selected_proxy


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
    #1
    chrome_driver_path = "/home/aaron/eastmoney/chrome/chromedriver-linux64/chromedriver"  # 替换为你的驱动路径
    chrome_binary_path = "/home/aaron/eastmoney/chrome/chrome-linux64/chrome"        # 替换为你的浏览器路径

    #2
    #chrome_driver_path = "/home/aaron/eastmoney/chrome/141.0.7390.54/chromedriver-linux64/chromedriver"  # 替换为你的驱动路径
    #chrome_binary_path = "/usr/bin/google-chrome"        # 替换为你的浏览器路径

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
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--ignore-certificate-errors')
    #chrome_options.add_argument("blink-settings=imagesEnabled=false")  #image disable
    
    #chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disk-cache-dir=/dev/shm  --disk-cache-size=4096000000')

    # 复用你本地已登录的 Chrome 配置（包含真实的 WebGL 设置）
    #chrome_options.add_argument("--user-data-dir=/home/aaron/.config/google-chrome")
    #chrome_options.add_argument("--profile-directory=Default")

    #强制使用 OpenGL（Linux 推荐）
    #chrome_options.add_argument("--use-gl=desktop")  # 使用桌面 OpenGL
    
    '''
    chrome_options.add_argument('--disable-webgl')
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-software-rasterizer")
    '''

    # 如果需要使用代理
    if proxy:
        # 从数据库读取代理并选择一个可用的
        #selected_proxy = get_random_valid_proxy_from_db()
        selected_proxy = None

        # 如果没有找到可用代理，从预设代理列表中随机选择一个, 并且保证连续两次不重复
        if not selected_proxy:
            '''
            print("没有找到可用的数据库代理，从预设代理列表中随机选择")
            # 定义预设代理列表
            proxy_list = ["142.171.166.165:3128", "101.43.29.22:3128", "127.0.0.1"]
            # 随机选择一个代理
            selected_proxy = random.choice(proxy_list)
            '''
            selected_proxy = get_proxy()
        
        # 设置代理
        print(f"使用代理: {selected_proxy}")
        if selected_proxy != "127.0.0.1":
            chrome_options.add_argument(f'--proxy-server={selected_proxy}')
        else:
            print("使用127.0.0.1，不设置代理服务器参数")
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
        

    # 删除 navigator.webdriver
    browser.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => false});")

    # 修复 plugins 和 languages
    browser.execute_script("""
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        Object.defineProperty(navigator, 'languages', {get: () => ['zh-CN', 'zh']});
        Object.defineProperty(navigator, 'chrome', {get: () => 1});
    """)

    # 移除 window.navigator.webdriver
    browser.execute_script("delete navigator.__proto__.webdriver;")

    return browser

def get_random_valid_proxy_from_db():
    """从数据库读取代理，随机遍历代理，找到第一个可用的代理就返回
    
    返回:
        str: 格式为 "ip:port" 的代理地址，如果没有可用代理则返回None
    """
    try:
        # 尝试连接数据库（使用用户指定的localhost连接信息）
        try:
            connection = psycopg2.connect(
                host="127.0.0.1",   # 数据库主机
                port="5432",
                database="usr",
                user="usr",
                password="usr"
            )
        except psycopg2.OperationalError as db_error:
            print(f"数据库连接失败: {db_error}")
            print("注意：数据库似乎不可用，请检查数据库服务是否运行或已创建")
            return None
        
        cursor = connection.cursor()
        
        # 检查ip_proxy表是否存在
        try:
            cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'ip_proxy'")
            table_exists = cursor.fetchone()[0] > 0
            
            if not table_exists:
                print("错误：ip_proxy表不存在")
                cursor.close()
                connection.close()
                return None
            
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
            print(f"查询数据库时出错: {error}")
        
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

def roll_to_bottom_by_step(browser):
    '''
    # 分步滚动到底部
    total_height = browser.execute_script("return document.body.scrollHeight")
    scroll_height = 1000  # 每次滚动 1000 像素
    current_scroll = 0

    while current_scroll < total_height:
        browser.execute_script(f"window.scrollBy(0, {scroll_height});")
        current_scroll += scroll_height
        time.sleep(1)  # 每次滚动后暂停 1 秒，模拟真人
    
    '''
    
    total_height = int(browser.execute_script("return document.body.scrollHeight;"))
    viewport_height = browser.execute_script("return window.innerHeight;")
    current_scroll = 0
    while current_scroll < total_height:
        step = random.randint(300, 800)
        current_scroll += step
        browser.execute_script(f"window.scrollTo(0, {current_scroll});")
        time.sleep(random.uniform(0.5, 1.5))


