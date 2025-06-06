from selenium import webdriver
import json



import get_xq_data

from comm_selenium import *

from comm_selenium import *
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait


get_xq_data._init()

#browser = get_browser(headless=1)
browser = get_browser(1)
get_xq_data.set_browser(browser)
get_xq_data.xq_login2(browser)
get_xq_data.start_slider_login(browser)



dictCookies = browser.get_cookies()
jsonCookies = json.dumps(dictCookies)

with open('cookies.txt', 'w') as f:
    f.write(jsonCookies)

print('cookies保存成功！')
browser.close()
browser.quit()
