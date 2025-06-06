from selenium import webdriver
import json

from comm_selenium import *

### 下面是chromedriver路径，自己填
#browser = webdriver.Chrome("###########")
path_chromedriver='/snap/bin/chromium.chromedriver'
# 添加无头headlesss
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument(
        'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'\
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36')
 
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
 

chrome_options.add_argument("disable-infobars");
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
 
chrome_options.add_argument("--disable-extensions")
#chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--disable-software-rasterizer")
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--ignore-certificate-errors')
#chrome_options.add_argument("blink-settings=imagesEnabled=false")  #image disable
 
#chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disk-cache-dir=/dev/shm  --disk-cache-size=4096000000')
browser = webdriver.Chrome(executable_path=path_chromedriver, chrome_options=chrome_options)


### 打开要自动登录的网站
browser.get("https://xueqiu.com/")
with open('cookies.txt', 'r', encoding='utf8') as f:
    listCookies = json.loads(f.read())

    for cookie in listCookies:
        print(cookie)
        cookie_dict = {
            ### 这个domain看cookies第一个字段就知道了，需要找到并填入
            'domain': '########',
            'name': cookie.get('name'),
            'value': cookie.get('value'),
            "expires": '',
            'path': '/',
            'httpOnly': False,
            'HostOnly': False,
            'Secure': False
            }
        browser.add_cookie(cookie_dict)
        browser.refresh()  # 刷新网页,cookies才成功
browser.get("https://xueqiu.com/")
