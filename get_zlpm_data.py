#!/#!/usr/bin/env python
# -*- coding: utf-8 -*-


import pandas as pd
import json
import requests
import re

import time
import datetime

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


from file_interface import *

from eastmoney_slide import *

'''
http://data.eastmoney.com/zjlx/list.html
To get industry info
https://push2.eastmoney.com/api/qt/clist/get?cb=jQuery1123022981019595514018_1626678575964&fid=f184&po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&fields=f2%2Cf3%2Cf12%2Cf13%2Cf14%2Cf62%2Cf184%2Cf225%2Cf165%2Cf263%2Cf109%2Cf175%2Cf264%2Cf160%2Cf100%2Cf124%2Cf265%2Cf1&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2
'''


debug=1
debug=0
debug=1

import random
def get_headers():
    '''
    随机获取一个headers
    '''
    user_agents =  ['Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1',\
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',\
            'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11']

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

import requests
import json
import pandas as pd
import time
import datetime

def get_zlpm_data_by_get(pn=None):
    """
    获取东方财富-主力排名数据（优化版 GET 请求）
    :param pn: 页码（从1开始）
    :return: DataFrame, 原始响应数据
    """
    nowdate = datetime.datetime.now().date()
    
    # 默认页码
    if pn is None:
        pn = 1

    # API URL（去掉 cb 参数即可返回纯 JSON）
    url = 'https://push2.eastmoney.com/api/qt/clist/get'

    # 请求参数（清晰分离）
    params = {
        'fid': 'f184',           # 排序字段：主力净流入涨幅
        'po': '1',               # 0:降序, 1:升序
        'pz': '200',             # 每页数量
        'pn': str(pn),           # 当前页码
        'np': '1',               # 新版分页
        'fltt': '2',             # 行情数据延迟
        'invt': '2',             # 数据版本
        'ut': 'b2884a393a59ad64002292a3e90d46a5',  # 固定 token
        'fs': 'm:0+t:6+f:!2,m:0+t:13+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:7+f:!2,m:1+t:3+f:!2',
        'fields': 'f2,f3,f12,f13,f14,f62,f184,f225,f165,f263,f109,f175,f264,f160,f100,f124,f265,f1'
    }

    # 请求头（模拟浏览器）
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://quote.eastmoney.com/',
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'
    }

    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()

        # 直接解析 JSON（因为去掉了 cb，返回的是标准 JSON）
        api_param = response.json()

        if not api_param.get('data') or not api_param['data'].get('diff'):
            print(f"数据为空或接口异常: {api_param}")
            return pd.DataFrame(), api_param

        rawdata = api_param['data']['diff']
        data_df = pd.DataFrame(rawdata)

        # 映射字段名
        tmp_column = [
            'close', 'percent', 'stock_code', 'market', 'stock_name', 'percent_5day', 
            'percent_10day', 'zljzb_5day', 'zljzb_10day', 'zljzb', 'zljzb_pm', 
            'zljzb_pm_5day', 'zljzb_pm_10day', 'f1'
        ]
        data_df.columns = tmp_column

        # 删除不需要的列
        columns_to_drop = ['market', 'f1']  # f13 是 market（0=深市,1=沪市），f1 是未知
        data_df = data_df.drop(columns=[col for col in columns_to_drop if col in data_df.columns])

        # 处理 '-' 数据
        numeric_cols = ['close', 'percent', 'percent_5day', 'percent_10day',
                        'zljzb_5day', 'zljzb_10day', 'zljzb', 'zljzb_pm', 
                        'zljzb_pm_5day', 'zljzb_pm_10day']
        for col in numeric_cols:
            data_df[col] = pd.to_numeric(data_df[col].replace('-', 0), errors='coerce')

        # 添加日期
        data_df.insert(1, 'record_date', nowdate.strftime("%Y-%m-%d"))

        # 调整列顺序
        final_columns = [
            'stock_code', 'record_date', 'stock_name', 'close', 'percent', 
            'percent_5day', 'percent_10day', 'zljzb_5day', 'zljzb_10day', 
            'zljzb', 'zljzb_pm', 'zljzb_pm_5day', 'zljzb_pm_10day'
        ]
        data_df = data_df[final_columns]

        # 保存 CSV（GBK 编码）
        data_df.to_csv(f'./csv/real-{nowdate.strftime("%Y-%m-%d")}.csv', 
                       encoding='gbk', index=False)

        # 排序并重置索引
        data_df = data_df.sort_values('stock_code').reset_index(drop=True)

        return data_df, api_param

    except requests.exceptions.RequestException as e:
        print(f"请求失败: {e}")
        return pd.DataFrame(), {}
    except json.JSONDecodeError as e:
        print(f"JSON 解析失败: {e}")
        print(f"响应内容: {response.text}")
        return pd.DataFrame(), {}



def get_zlpm_data(pn=None):
    
    nowdate=datetime.datetime.now().date()
    if debug:
        my_dbg("nowdate is %s"%(nowdate.strftime("%Y-%m-%d")))
    
    timestamp=str(round(time.time() * 1000))

    url = 'https://push2.eastmoney.com/api/qt/clist/get?cb=jQuery1123022981019595514018_'\
            + timestamp \
            + '&fid=f184&po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&'\
            + 'fields=f2%2Cf3%2Cf12%2Cf13%2Cf14%2Cf62%2Cf184%2Cf225%2Cf165%2Cf263%2Cf109%2Cf175%2Cf264%2Cf160%2Cf100%2Cf124%2Cf265%2Cf1&'\
            + 'ut=b2884a393a59ad64002292a3e90d46a5&'\
            + 'fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2'

    url = 'https://push2.eastmoney.com/api/qt/clist/get?cb=jQuery1123022981019595514018_'\
            + timestamp \
            + '&fid=f184&po=1&pz=200&pn='\
            + str(pn) \
            + '&np=1&fltt=2&invt=2&'\
            + 'fields=f2%2Cf3%2Cf12%2Cf13%2Cf14%2Cf62%2Cf184%2Cf225%2Cf165%2Cf263%2Cf109%2Cf175%2Cf264%2Cf160%2Cf100%2Cf124%2Cf265%2Cf1&'\
            + 'ut=b2884a393a59ad64002292a3e90d46a5&'\
            + 'fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2'


    my_dbg(url)

    tmp_header = get_headers()
    response = requests.get(url, headers=tmp_header)

    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, response.text)
    api_param = json.loads(response_array[0])
    rawdata = api_param['data']['diff']
    data_df = pd.DataFrame(rawdata)
    tmp_column = ['close', 'percent', 'stock_code', 'stock_name', 'industry', 'percent_5day', 'percent_10day', \
            'zljzb_5day', 'zljzb_10day', 'zljzb', 'zljzb_pm', 'zljzb_pm_5day', 'zljzb_pm_10day' ]


    if len(data_df):
        del data_df['f1']
        del data_df['f13']
        del data_df['f62']
        del data_df['f124']
        del data_df['f265']
        data_df = data_df.replace('-',0)
        data_df.columns = tmp_column	
        data_df.insert(1, 'record_date', nowdate.strftime("%Y-%m-%d"), allow_duplicates=False)

        new_column = ['stock_code', 'record_date', 'stock_name', 'close', 'percent',  'industry', 'percent_5day', 'percent_10day', \
            'zljzb_5day', 'zljzb_10day', 'zljzb', 'zljzb_pm', 'zljzb_pm_5day', 'zljzb_pm_10day' ]


        data_df = data_df.loc[:, new_column]

        data_df.to_csv('./csv/real-' + nowdate.strftime("%Y-%m-%d")+ '.csv', encoding='gbk')

        if debug:
            my_dbg(data_df)

        data_df = data_df.sort_values('stock_code', ascending=1)
        data_df = data_df.reset_index(drop=True)

    return data_df, api_param

def get_zlpm_data2_final(pn=None):
    
    nowdate=datetime.datetime.now().date()
    if debug:
        my_dbg("nowdate is %s"%(nowdate.strftime("%Y-%m-%d")))
    
    timestamp=str(round(time.time() * 1000))

    url = 'https://push2.eastmoney.com/api/qt/clist/get?cb=jQuery1123022981019595514018_'\
            + timestamp \
            + '&fid=f184&po=1&pz=200&pn='\
            + str(pn) \
            + '&np=1&fltt=2&invt=2&'\
            + 'fields=f2%2Cf3%2Cf12%2Cf13%2Cf14%2Cf62%2Cf184%2Cf225%2Cf165%2Cf263%2Cf109%2Cf175%2Cf264%2Cf160%2Cf100%2Cf124%2Cf265%2Cf1&'\
            + 'ut=b2884a393a59ad64002292a3e90d46a5&'\
            + 'fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2'

    #url = 'https://bot.sannysoft.com/' 
    #browser = get_browser(headless=False, proxy=False)
    browser = get_browser(headless=False, proxy=True)
    my_dbg(url)
   
    html = ''
    try: 
        browser.get(url)
        time.sleep(random.uniform(3, 5))  # 初始加载等待
        browser.implicitly_wait(10)

        # 分步滚动到底部
        roll_to_bottom_by_step(browser)

        html = browser.page_source
    except:
        my_dbg(f"error: get_zlpm_data2_final pn:{pn}")
        browser.close()
        browser.quit()
    finally:
        browser.close()
        browser.quit()


    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, html)
    api_param = json.loads(response_array[0])
    rawdata = api_param['data']['diff']
    data_df = pd.DataFrame(rawdata)

    tmp_column = ['close', 'percent', 'stock_code', 'stock_name','zljlre', 'industry', 'percent_5day', 'record_date', 'percent_10day', \
            'zljzb_5day', 'zljzb_10day', 'zljzb_1day', 'zljzb_pm_1day', 'zljzb_pm_5day', 'zljzb_pm_10day' ]


    if len(data_df):
        del data_df['f1']
        del data_df['f13']
        del data_df['f265']
        data_df = data_df.replace('-',0)
        data_df.columns = tmp_column	

        new_column = ['stock_code', 'record_date', 'stock_name', 'close', 'percent', 'zljlre', 'industry', 'percent_5day', 'percent_10day', \
            'zljzb_5day', 'zljzb_10day', 'zljzb_1day', 'zljzb_pm_1day', 'zljzb_pm_5day', 'zljzb_pm_10day' ]


        data_df = data_df.loc[:, new_column]
        data_df['record_date'] = data_df['record_date'].apply(lambda x: get_date_from_timestamp(int(x)*1000))


        #data_df.to_csv('./csv/real-' + nowdate.strftime("%Y-%m-%d")+ '.csv', encoding='gbk')

        if debug:
            my_dbg(data_df)

        data_df = data_df.sort_values('stock_code', ascending=1)
        data_df = data_df.reset_index(drop=True)

    return data_df, api_param


def get_zlpm_data2():
    my_dbg(f"get_zlpm_data2()")
    data_df = pd.DataFrame()
    api_param = ''
    pn = 1
    max_retries = 3  # 最大重试次数

    while True:
        retry_count = 0
        data_df_tmp = None
        success = False

        # 对当前 pn 进行最多 max_retries 次重试
        while retry_count < max_retries:
            try:
                data_df_tmp, api_param = get_zlpm_data2_final(pn)
                #data_df_tmp, api_param = get_zlpm_data(pn)
                #data_df_tmp, api_param = get_zlpm_data_by_get(pn)
                
                if len(data_df_tmp) > 0:
                    # 成功获取非空数据
                    success = True
                    my_dbg(f"pn:{pn}, 获取到 {len(data_df_tmp)} 条数据")
                    break  # 跳出重试循环
                else:
                    my_dbg(f"pn:{pn}, 第 {retry_count + 1} 次返回空数据，正在重试...")
                    
            except Exception as e:
                my_dbg(f"pn:{pn}, 第 {retry_count + 1} 次调用异常: {e}")
                if retry_count == 1:
                    do_slide()

            retry_count += 1
            time.sleep(10)  # 可选：避免频繁请求，防止被限流

        # =============================
        # 判断重试结果
        # =============================

        if not success:
            my_dbg(f"❌ pn:{pn} 达到最大重试次数 {max_retries}，仍无有效数据，结束采集。")
            break  # 跳出主循环，结束采集

        # 合并数据
        data_df = pd.concat([data_df, data_df_tmp], ignore_index=True)
        pn += 1  # 进入下一页

    # 去重并重置索引
    if not data_df.empty:
        data_df = data_df.drop_duplicates(subset=['stock_code'], keep='first').reset_index(drop=True)

    return data_df, api_param



def get_zlpm_data3():
    
    nowdate=datetime.datetime.now().date()
    if debug:
        my_dbg("nowdate is %s"%(nowdate.strftime("%Y-%m-%d")))
    
    timestamp=str(round(time.time() * 1000))

    url = 'https://push2.eastmoney.com/api/qt/clist/get?cb=jQuery1123022981019595514018_'\
            + timestamp \
            + '&fid=f184&po=1&pz=10000&pn=1'\
            + '&np=2&fltt=2&invt=2&'\
            + 'fields=f2%2Cf3%2Cf12%2Cf13%2Cf14%2Cf62%2Cf184%2Cf225%2Cf165%2Cf263%2Cf109%2Cf175%2Cf264%2Cf160%2Cf100%2Cf124%2Cf265%2Cf1&'\
            + 'ut=b2884a393a59ad64002292a3e90d46a5&'\
            + 'fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2'

    my_dbg(url)

    browser = get_browser()
   
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


    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, html)
    api_param = json.loads(response_array[0])
    rawdata = api_param['data']['diff']
    data_df = pd.DataFrame(rawdata)
    data_df = data_df.T

    tmp_column = ['close', 'percent', 'stock_code', 'stock_name','zljlre', 'industry', 'percent_5day', 'record_date', 'percent_10day', \
            'zljzb_5day', 'zljzb_10day', 'zljzb_1day', 'zljzb_pm_1day', 'zljzb_pm_5day', 'zljzb_pm_10day' ]


    if len(data_df):
        del data_df['f1']
        del data_df['f13']
        del data_df['f265']
        data_df = data_df.replace('-',0)
        data_df.columns = tmp_column	

        new_column = ['stock_code', 'record_date', 'stock_name', 'close', 'percent', 'zljlre', 'industry', 'percent_5day', 'percent_10day', \
            'zljzb_5day', 'zljzb_10day', 'zljzb_1day', 'zljzb_pm_1day', 'zljzb_pm_5day', 'zljzb_pm_10day' ]


        data_df = data_df.loc[:, new_column]
        data_df['record_date'] = data_df['record_date'].apply(lambda x: get_date_from_timestamp(int(x)*1000))


        #data_df.to_csv('./csv/real-' + nowdate.strftime("%Y-%m-%d")+ '.csv', encoding='gbk')

        if debug:
            my_dbg(data_df)

        data_df = data_df.sort_values('stock_code', ascending=1)
        data_df = data_df.reset_index(drop=True)

    return data_df, api_param






if __name__ == '__main__':

    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    df, api_param = get_zlpm_data2()
    my_dbg(df)

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    my_dbg("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    my_dbg("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))


