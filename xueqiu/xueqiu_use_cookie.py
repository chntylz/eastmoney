from selenium import webdriver
import json

import time
import re

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



browser.get('https://xueqiu.com/')

# 首先清除由于浏览器打开已有的cookies
browser.delete_all_cookies()

with open('cookies.txt','r') as f:
    # 使用json读取cookies 注意读取的是文件 所以用load而不是loads
    cookies_list = json.load(f)

    #将expiry类型变为int
    for cookie in cookies_list:
        # 并不是所有cookie都含有expiry 所以要用dict的get方法来获取
        if isinstance(cookie.get('expiry'), float):
            cookie['expiry'] = int(cookie['expiry'])
        browser.add_cookie(cookie)
print('第二次请求')
#重新发送请求(这步是非常必要的，要不然携带完cookie之后仍然在登录界面)
browser.get('https://xueqiu.com/')



url = 'https://stock.xueqiu.com/v5/stock/realtime/quotec.json?symbol=SH688455,SH603332,SH603365,SZ002574,SZ301055,SZ003010,SZ300975,SH603390,SZ300436,SH605337,SH603577,SH605188,SZ301335,SZ300163,SH600468,SZ300886,SH600573,SZ000565,SZ001277,SH605339,SZ301177,SH600081,SH600006,SZ300791,SZ300997,SZ300905,SZ300741,SZ002900,SH603697,SZ002946,SZ301078,SZ300003,SZ300584,SH603065,SH603607,SZ002956,SZ301108,SH900939,SH605005,SZ300198,SZ002580,SZ301230,SH603221,SZ001212,SH600644,SZ001390,SH600419,SH600073,SH601086,SZ300307,SZ002809,SZ300967,SZ300434,SH603398,SH600793,SZ002650,SH600193,SH603906,SZ002289,SH603983,SH688326,SH601008,SZ000639,SZ002200,SH900957,SZ002898,SH603389,SZ300147,SH603017,SZ002422,SZ002717,SH600249,SH600495,SH605006,SZ002816,SZ301068,SZ300860,SZ000989,SH605016,SZ300963,SZ002743,SZ002693,SZ300888,SZ002644,SZ002286,SZ300771,SH603007,SZ300422,SH600612,SH603931,SH603167,SZ301148,SZ300892,SZ000848,SH600686,SZ301075,SH603608,SH605009,SH603102,SH600189,SZ001230,SZ300132,SH603955,SZ002620,SH600200,SH600439,SZ001328,SH603150,SZ002653,SZ003000,SZ002427,SZ300084,SZ002040,SH601579,SZ301289,SZ301277,SZ003006,SH605599,SZ001300,SZ300783,SZ002278,SZ002165,SZ301371,SZ000609,SH605198,SZ002612,SZ301220,SZ002099,SZ002615,SH601825,SZ300321,SZ002393,SH600993,SH605289,SZ300240,SZ000729,SZ003030,SZ002785,SH605069,SZ301079,SZ301015,SZ301040,SH688011,SZ300138,SZ300716,SH605081,SZ002294,SH603208,SZ002695,SZ300950,SH688331,SZ002821,SZ000888,SH600166,SH603306,SH688621,SH600774,SZ000838,SZ300179,SZ301137,SZ002094,SZ301399,SZ000716,SH600805,SZ300237,SZ003043,SZ002923,SH688070,SH688169,SH603130,SZ001313,SZ002161,SH603566,SZ002630,SH603388,SZ002437'
html = ''
try:
    browser.get(url)
    time.sleep(4)
    browser.implicitly_wait(10)
    html = browser.page_source
except:
    browser.close()
    browser.quit()
finally:
    browser.close()
    browser.quit()


p1 = re.compile(r'[(](.*?)[)]', re.S)
response_array = re.findall(p1, html)
api_param = json.loads(response_array[0])
rawdata = api_param['data']
data_df = pd.DataFrame(rawdata)

print(data_df)

browser.close()
browser.quit()
