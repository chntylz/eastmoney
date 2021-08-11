from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
import time
import pandas as pd
import os

# 先chrome，后phantomjs
# browser = webdriver.Chrome()

# 添加无头headlesss
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
browser = webdriver.Chrome(chrome_options=chrome_options)

# browser = webdriver.PhantomJS() # 会报警高提示不建议使用phantomjs，建议chrome添加无头
browser.maximize_window()  # 最大化窗口
wait = WebDriverWait(browser, 10)


def handle_data():
    '''
    element = browser.find_element_by_css_selector('#app > div.container-lg.clearfix > div.container-sm.float-left.stock__main > div.quote-container > table')

    td_content = element.find_elements_by_tag_name("td")
    lst = []  # 存储为list
    for td in td_content:
        lst.append(td.text)

    df=pd.DataFrame(lst)
    df=df[0].str.split('：', expand=True)
    print(df)
    '''

    info1 = browser.find_element_by_class_name('quote-container')
    s1=info1.text
    print(s1)

    #close=browser.find_element_by_class_name('stock-current').text
    #print(close)




url='https://xueqiu.com/S/SZ000338'
#url='http://data.eastmoney.com/bbsj/201806/lrb.html'
browser.get(url)
try:
    handle_data()
except:
    browser.close()



url='https://xueqiu.com/S/SH603520'
#url='http://data.eastmoney.com/bbsj/201806/lrb.html'
browser.get(url)

try:
    handle_data()
except:
    browser.close()

browser.close()


