#!/#!/usr/bin/env python  
# -*- coding: utf-8 -*-


import pandas as pd
import json
import requests
import re

import time
import datetime


from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
import time, datetime
import pandas as pd
import os
import re



from HData_eastmoney_zlje import *
from HData_eastmoney_zlje_3 import *
from HData_eastmoney_zlje_5 import *
from HData_eastmoney_zlje_10 import *

from file_interface import *

debug = 0
#debug = 1


hdata_zlje = HData_eastmoney_zlje('usr', 'usr')
hdata_zlje_3 = HData_eastmoney_zlje_3('usr', 'usr')
hdata_zlje_5 = HData_eastmoney_zlje_5('usr', 'usr')
hdata_zlje_10 = HData_eastmoney_zlje_10('usr', 'usr')

#https://data.eastmoney.com/zjlx/detail.html

url_1 = 'http://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112308511204703039876_1610688047489&fid=f62 &po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf3%2Cf62%2Cf184%2Cf66%2Cf69%2Cf72%2Cf75%2Cf78%2Cf81%2Cf84%2Cf87%2Cf204%2Cf205%2Cf124'
url_3 = 'http://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112309724568186220448_1610691602607&fid=f267&po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf127%2Cf267%2Cf268%2Cf269%2Cf270%2Cf271%2Cf272%2Cf273%2Cf274%2Cf275%2Cf276%2Cf257%2Cf258%2Cf124'
url_5 = 'http://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112309724568186220448_1610691602607&fid=f164&po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf109%2Cf164%2Cf165%2Cf166%2Cf167%2Cf168%2Cf169%2Cf170%2Cf171%2Cf172%2Cf173%2Cf257%2Cf258%2Cf124'
url_10= 'http://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112309724568186220448_1610691602607&fid=f174&po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf160%2Cf174%2Cf175%2Cf176%2Cf177%2Cf178%2Cf179%2Cf180%2Cf181%2Cf182%2Cf183%2Cf260%2Cf261%2Cf124'

mapping = {
    "f2": "zxj",
    "f3": "zdf",
    "f127": "zdf",
    "f109": "zdf",
    "f160": "zdf",
    "f12": "code",
    "f14": "name",
    "f62": "zlje",
    "f184": "zljzb",
    "f66": "cddje",
    "f69": "cddjzb",
    "f72": "ddje",
    "f75": "ddjzb",
    "f78": "zdje",
    "f81": "zdjzb",
    "f84": "xdje",
    "f87": "xdjzb",
    "f267": "zlje",
    "f268": "zljzb",
    "f269": "cddje",
    "f270": "cddjzb",
    "f271": "ddje",
    "f272": "ddjzb",
    "f273": "zdje",
    "f274": "zdjzb",
    "f275": "xdje",
    "f276": "xdjzb",
    "f164": "zlje",
    "f165": "zljzb",
    "f166": "cddje",
    "f167": "cddjzb",
    "f168": "ddje",
    "f169": "ddjzb",
    "f170": "zdje",
    "f171": "zdjzb",
    "f172": "xdje",
    "f173": "xdjzb",
    "f174": "zlje",
    "f175": "zljzb",
    "f176": "cddje",
    "f177": "cddjzb",
    "f178": "ddje",
    "f179": "ddjzb",
    "f180": "zdje",
    "f181": "zdjzb",
    "f182": "xdje",
    "f183": "xdjzb",
    "f205": "zdcode",
    "f204": "zdname",
    "f258": "zdcode",
    "f257": "zdname",
    "f261": "zdcode",
    "f260": "zdname",
    "f225": "zlpm1",
    "f263": "zlpm5",
    "f264": "zlpm10",
    "f124": "record_date"
};

import random
def get_headers():
    '''
    随机获取一个headers
    '''
    user_agents =  ['Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1',\
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',\
            'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11',\
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",\
	    "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",\
	    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",\
	    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",\
	    "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",\
	    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",\
	    "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",\
	    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",\
	    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",\
	    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",\
	    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",\
	    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",\
	    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3"]
    

    user_agents = [
	"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
	"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
	"Mozilla/5.0 (Windows NT 10.0; WOW64; rv:38.0) Gecko/20100101 Firefox/38.0",
	"Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; .NET4.0C; .NET4.0E; .NET CLR 2.0.50727; .NET CLR 3.0.30729; .NET CLR 3.5.30729; InfoPath.3; rv:11.0) like Gecko",
	"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)",
	"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
	"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)",
	"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
	"Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
	"Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11",
	"Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
	"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Maxthon 2.0)",
	"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; TencentTraveler 4.0)",
	"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
	"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; The World)",
	"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SE 2.X MetaSr 1.0; SE 2.X MetaSr 1.0; .NET CLR 2.0.50727; SE 2.X MetaSr 1.0)",
	"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)",
	"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Avant Browser)",
	"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
	"Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
	"Mozilla/5.0 (iPod; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
	"Mozilla/5.0 (iPad; U; CPU OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
	"Mozilla/5.0 (Linux; U; Android 2.3.7; en-us; Nexus One Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
	"MQQBrowser/26 Mozilla/5.0 (Linux; U; Android 2.3.7; zh-cn; MB200 Build/GRJ22; CyanogenMod-7) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
	"Opera/9.80 (Android 2.3.4; Linux; Opera Mobi/build-1107180945; U; en-GB) Presto/2.8.149 Version/11.10",
	"Mozilla/5.0 (Linux; U; Android 3.0; en-us; Xoom Build/HRI39) AppleWebKit/534.13 (KHTML, like Gecko) Version/4.0 Safari/534.13",
	"Mozilla/5.0 (BlackBerry; U; BlackBerry 9800; en) AppleWebKit/534.1+ (KHTML, like Gecko) Version/6.0.0.337 Mobile Safari/534.1+",
	"Mozilla/5.0 (hp-tablet; Linux; hpwOS/3.0.0; U; en-US) AppleWebKit/534.6 (KHTML, like Gecko) wOSBrowser/233.70 Safari/534.6 TouchPad/1.0",
	"Mozilla/5.0 (SymbianOS/9.4; Series60/5.0 NokiaN97-1/20.0.019; Profile/MIDP-2.1 Configuration/CLDC-1.1) AppleWebKit/525 (KHTML, like Gecko) BrowserNG/7.1.18124",
	"Mozilla/5.0 (compatible; MSIE 9.0; Windows Phone OS 7.5; Trident/5.0; IEMobile/9.0; HTC; Titan)",
	"UCWEB7.0.2.37/28/999",
	"NOKIA5700/ UCWEB7.0.2.37/28/999",
	"Openwave/ UCWEB7.0.2.37/28/999",
	"Mozilla/4.0 (compatible; MSIE 6.0; ) Opera/UCWEB7.0.2.37/28/999",
	# iPhone 6：
	"Mozilla/6.0 (iPhone; CPU iPhone OS 8_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/8.0 Mobile/10A5376e Safari/8536.25",

    ]

    headers = {'User-Agent':random.choice(user_agents)}
    return headers

def get_daily_zlje(url=None):
    timestamp=str(round(time.time() * 1000))
    #url = 'http://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112309724568186220448_1610691602607&fid=f62&po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf3%2Cf62%2Cf184%2Cf66%2Cf69%2Cf72%2Cf75%2Cf78%2Cf81%2Cf84%2Cf87%2Cf204%2Cf205%2Cf124'
    if url == 'url_3':
        url = 'http://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112309724568186220448_'\
                + timestamp + '&fid=f267&po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf127%2Cf267%2Cf268%2Cf269%2Cf270%2Cf271%2Cf272%2Cf273%2Cf274%2Cf275%2Cf276%2Cf257%2Cf258%2Cf124'
    elif url == 'url_5':
        url = 'http://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112309724568186220448_'\
                + timestamp + '&fid=f164&po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf109%2Cf164%2Cf165%2Cf166%2Cf167%2Cf168%2Cf169%2Cf170%2Cf171%2Cf172%2Cf173%2Cf257%2Cf258%2Cf124'
    elif url == 'url_10':
        url = 'http://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112309724568186220448_'\
                + timestamp + '&fid=f174&po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf160%2Cf174%2Cf175%2Cf176%2Cf177%2Cf178%2Cf179%2Cf180%2Cf181%2Cf182%2Cf183%2Cf260%2Cf261%2Cf124'
    else:
        url = 'http://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112309724568186220448_'\
                + timestamp +'&fid=f62&po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf3%2Cf62%2Cf184%2Cf66%2Cf69%2Cf72%2Cf75%2Cf78%2Cf81%2Cf84%2Cf87%2Cf204%2Cf205%2Cf124'

    if debug:
        print(url)

    print(url)
    tmp_header = get_headers()
    print(tmp_header)
    response = requests.get(url, headers=tmp_header)
    print(response)

   
    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, response.text)
    api_param = json.loads(response_array[0])
    rawdata = api_param['data']['diff']
    data_df = pd.DataFrame(rawdata)
    


    return data_df



def get_daily_zlje2(url=None):
    timestamp=str(round(time.time() * 1000))
    #url = 'http://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112309724568186220448_1610691602607&fid=f62&po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf3%2Cf62%2Cf184%2Cf66%2Cf69%2Cf72%2Cf75%2Cf78%2Cf81%2Cf84%2Cf87%2Cf204%2Cf205%2Cf124'
    if url == 'url_3':
        url = 'http://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112309724568186220448_'\
                + timestamp + '&fid=f267&po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf127%2Cf267%2Cf268%2Cf269%2Cf270%2Cf271%2Cf272%2Cf273%2Cf274%2Cf275%2Cf276%2Cf257%2Cf258%2Cf124'
    elif url == 'url_5':
        url = 'http://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112309724568186220448_'\
                + timestamp + '&fid=f164&po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf109%2Cf164%2Cf165%2Cf166%2Cf167%2Cf168%2Cf169%2Cf170%2Cf171%2Cf172%2Cf173%2Cf257%2Cf258%2Cf124'
    elif url == 'url_10':
        url = 'http://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112309724568186220448_'\
                + timestamp + '&fid=f174&po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf160%2Cf174%2Cf175%2Cf176%2Cf177%2Cf178%2Cf179%2Cf180%2Cf181%2Cf182%2Cf183%2Cf260%2Cf261%2Cf124'
    else:
        url = 'http://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112309724568186220448_'\
                + timestamp +'&fid=f62&po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf3%2Cf62%2Cf184%2Cf66%2Cf69%2Cf72%2Cf75%2Cf78%2Cf81%2Cf84%2Cf87%2Cf204%2Cf205%2Cf124'

    if debug:
        print(url)

    print(url)

    # 添加无头headlesss
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    browser = webdriver.Chrome(options=chrome_options)

    # browser = webdriver.PhantomJS() # 会报警高提示不建议使用phantomjs，建议chrome添加无头
    browser.maximize_window()  # 最大化窗口
    wait = WebDriverWait(browser, 10)


    
    html = ''
    try: 
        browser.get(url)
        browser.implicitly_wait(10)
        html = browser.page_source
    except:
        browser.close()
        browser.quit()
    finally:
        browser.close()
        browser.quit()


    #print(html)
   
    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, html)
    api_param = json.loads(response_array[0])
    rawdata = api_param['data']['diff']
    data_df = pd.DataFrame(rawdata)
    


    return data_df

def del_column(df, name=None):
    if name in df.columns:
        del df[name]

def handle_raw_data(df):
    nowdate=datetime.datetime.now().date()

    #nowdate=nowdate-datetime.timedelta(1)
   
    del_column(df,name='f204')
    del_column(df,name='f205')
    del_column(df,name='f206')
    del_column(df,name='f257')
    del_column(df,name='f258')
    del_column(df,name='f259')
    del_column(df,name='f260')
    del_column(df,name='f261')
    del_column(df,name='f262')


    if debug:
        print(df)

    df_list = list(df)
    len_df = len(df_list)
    for i in range(0, len_df):
        if debug:
            print(i, len_df,df_list[i])
        new_col_name = mapping[df_list[i]]
        df = df.rename(columns={df_list[i]:new_col_name})

    
    #timestamp -> date
    df['record_date'] = nowdate.strftime("%Y-%m-%d")
    #df['record_date'] = '2021-01-22'

    #delete suspend code
    df = df[df['zxj'] != '-']
    df = df[df['zlje'] != '-']
    df = df.reset_index(drop=True)


    df['record_date'] = nowdate.strftime("%Y-%m-%d")
    #unit: Y
    df['zlje']  = df['zlje'].apply(lambda x: x/1000/1000/100)
    df['cddje'] = df['cddje'].apply(lambda x: x/1000/1000/100)
    df['ddje']  = df['ddje'].apply(lambda x: x/1000/1000/100)
    df['zdje']  = df['zdje'].apply(lambda x: x/1000/1000/100)
    df['xdje']  = df['xdje'].apply(lambda x: x/1000/1000/100)

    df = round(df, 2)

    new_col = ['zxj', 'zdf', 'code', 'name', 'record_date', 'zlje', 'zljzb', 'cddje', 'cddjzb', 'ddje', 'ddjzb', 'zdje', 'zdjzb', 'xdje', 'xdjzb']
    df = df[new_col]
    return df

def check_table():
    table_exist = hdata_zlje.table_is_exist()
    print('table_exist=%d' % table_exist)
    if table_exist:
        #hdata_zlje.db_hdata_xq_create()
        print('table already exist')
    else:
        hdata_zlje.db_hdata_xq_create()
        print('table not exist, create')

    table_3_exist = hdata_zlje_3.table_is_exist()
    print('table_3_exist=%d' % table_3_exist)
    if table_3_exist:
        #hdata_zlje_3.db_hdata_xq_create()
        print('table_3 already exist')
    else:
        hdata_zlje_3.db_hdata_xq_create()
        print('table_3 not exist, create')

    table_5_exist = hdata_zlje_5.table_is_exist()
    print('table_5_exist=%d' % table_5_exist)
    if table_5_exist:
        #hdata_zlje_5.db_hdata_xq_create()
        print('table_5 already exist')
    else:
        hdata_zlje_5.db_hdata_xq_create()
        print('table_5 not exist, create')

    table_10_exist = hdata_zlje_10.table_is_exist()
    print('table_10_exist=%d' % table_10_exist)
    if table_10_exist:
        #hdata_zlje_10.db_hdata_xq_create()
        print('table_10 already exist')
    else:
        hdata_zlje_10.db_hdata_xq_create()
        print('table_10 not exist, create')

    pass

def get_zlje_data_from_db(url=None, curr_date=None):

    hdata_db = hdata_zlje

    if curr_date is None:
        nowdate=datetime.datetime.now().date()
        curr_date = nowdate.strftime('%Y-%m-%d') 
    
    if url == 'url_3':
        hdata_db = hdata_zlje_3
    elif url == 'url_5':
        hdata_db = hdata_zlje_5
    elif url == 'url_10':
        hdata_db = hdata_zlje_10
    else:
        hdata_db = hdata_zlje

    df = hdata_db.get_data_from_hdata(start_date=curr_date, end_date=curr_date)
    
    return df

def delete_zlje_data_from_db(url=None, curr_date=None):

    hdata_db = hdata_zlje

    if curr_date is None:
        nowdate=datetime.datetime.now().date()
        curr_date = nowdate.strftime('%Y-%m-%d') 
    
    if url == 'url_3':
        hdata_db = hdata_zlje_3
    elif url == 'url_5':
        hdata_db = hdata_zlje_5
    elif url == 'url_10':
        hdata_db = hdata_zlje_10
    else:
        hdata_db = hdata_zlje

    hdata_db.delete_data_from_hdata(start_date=curr_date, end_date=curr_date)
    
    pass



def get_zlje(df, stock_code, url=None, curr_date=None):
    zlje =  0

    #zlje_df = get_zlje_data_from_db(url, curr_date)
    zlje_df = df

    tmp_zlje_df = zlje_df[zlje_df['stock_code'] == stock_code]
    tmp_zlje_df = tmp_zlje_df.reset_index(drop=True)
    if debug:
            print(new_code, len(tmp_zlje_df))

    if len(tmp_zlje_df):
        zlje = tmp_zlje_df['zlje'][0]
        zdf  = tmp_zlje_df['zdf'][0]
        zlje = str(zlje) + '<br>' + str(zdf) + '</br>'

    return zlje



if __name__ == '__main__':
    
    nowdate=datetime.datetime.now().date()
    date_string = nowdate.strftime('%Y-%m-%d')

    check_table()

    df = get_daily_zlje2()
    df = handle_raw_data(df)
    #print(list(df))
    if len(df):
        delete_zlje_data_from_db()
    hdata_zlje.copy_from_stringio(df)


    df_3 = get_daily_zlje2(url='url_3')
    df_3 = handle_raw_data(df_3)
    if len(df_3):
        delete_zlje_data_from_db(url='url_3')
    hdata_zlje_3.copy_from_stringio(df_3)
    #print(list(df_3))


    df_5 = get_daily_zlje2(url='url_5')
    df_5 = handle_raw_data(df_5)
    if len(df_5):
        delete_zlje_data_from_db(url='url_5')
    hdata_zlje_5.copy_from_stringio(df_5)
    #print(list(df_5))


    df_10 = get_daily_zlje2(url='url_10')
    df_10 = handle_raw_data(df_10)
    if len(df_10):
        delete_zlje_data_from_db(url='url_10')
    hdata_zlje_10.copy_from_stringio(df_10)
    #print(list(df_10))

    if debug:
        print(list(df))
        print(list(df_3))
        print(list(df_5))
        print(list(df_10))

    df.to_csv('csv/' + nowdate.strftime('%Y-%m-%d')+ '_zlje_1.csv', encoding='gbk')
    df_3.to_csv('csv/' + nowdate.strftime('%Y-%m-%d') + '_zlje_3.csv', encoding='gbk')
    df_5.to_csv('csv/' + nowdate.strftime('%Y-%m-%d') + '_zlje_5.csv', encoding='gbk')
    df_10.to_csv('csv/' + nowdate.strftime('%Y-%m-%d') + '_zlje_10.csv', encoding='gbk')


