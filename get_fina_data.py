#!/#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import json
import requests
import re

import time
import datetime

import random

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

debug=0

'''
业绩报表
http://data.eastmoney.com/bbsj/202106/yjbb.html
http://datacenter-web.eastmoney.com/api/data/get?callback=jQuery112305938889278481287_1626682926066&st=UPDATE_DATE%2CSECURITY_CODE&sr=-1%2C-1&ps=50&p=1&type=RPT_LICO_FN_CPD&sty=ALL&token=894050c76af8597a853f5b408b759f5d&filter=(REPORTDATE%3D%272021-06-30%27)

资产负债表
balance
http://data.eastmoney.com/bbsj/202106/zcfz.html
http://datacenter-web.eastmoney.com/api/data/get?callback=jQuery11230002487053379782944_1626683115977&st=NOTICE_DATE%2CSECURITY_CODE&sr=-1%2C-1&ps=50&p=1&type=RPT_DMSK_FN_BALANCE&sty=ALL&token=894050c76af8597a853f5b408b759f5d&filter=(SECURITY_TYPE_CODE+in+(%22058001001%22%2C%22058001008%22))(REPORT_DATE%3D%272021-06-30%27)

利润表
income
http://data.eastmoney.com/bbsj/202106/lrb.html
http://datacenter-web.eastmoney.com/api/data/get?callback=jQuery1123033303123159764536_1626683309851&st=NOTICE_DATE%2CSECURITY_CODE&sr=-1%2C-1&ps=50&p=1&type=RPT_DMSK_FN_INCOME&sty=ALL&token=894050c76af8597a853f5b408b759f5d&filter=(SECURITY_TYPE_CODE+in+(%22058001001%22%2C%22058001008%22))(REPORT_DATE%3D%272021-06-30%27)



现金流量表
cashflow
http://data.eastmoney.com/bbsj/202106/xjll.html

------------------------------------------------------------------

20240906
业绩报表
cpd
https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery112302295057226083017_1725597423881&sortColumns=UPDATE_DATE%2CSECURITY_CODE&sortTypes=-1%2C-1&pageSize=50&pageNumber=1&reportName=RPT_LICO_FN_CPD&columns=ALL&filter=(REPORTDATE%3D%272024-06-30%27)
change to ->
order by REPORTDATE and CSECURITY_CODE, pagesize=500, pagenumber=150
https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery112302295057226083017_1725597423881&sortColumns=REPORTDATE%2CSECURITY_CODE&sortTypes=-1%2C-1&pageSize=500&pageNumber=150&reportName=RPT_LICO_FN_CPD&columns=ALL


资产负债表
balance
https://data.eastmoney.com/bbsj/202406/zcfz.html
https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery1123062742764734556_1725595827452&sortColumns=NOTICE_DATE%2CSECURITY_CODE&sortTypes=-1%2C-1&pageSize=50&pageNumber=1&reportName=RPT_DMSK_FN_BALANCE&columns=ALL&filter=(SECURITY_TYPE_CODE+in+(%22058001001%22%2C%22058001008%22))(TRADE_MARKET_CODE!%3D%22069001017%22)(REPORT_DATE%3D%272024-06-30%27)
change to ->
order by REPORT_DATE and CSECURITY_CODE, pagesize=500, pagenumber=150
https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery1123062742764734556_1725595827452&sortColumns=REPORT_DATE%2CSECURITY_CODE&sortTypes=-1%2C-1&pageSize=500&pageNumber=150&reportName=RPT_DMSK_FN_BALANCE&columns=ALL

利润表
income
https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery1123042893057157239967_1725596942438&sortColumns=NOTICE_DATE%2CSECURITY_CODE&sortTypes=-1%2C-1&pageSize=50&pageNumber=1&reportName=RPT_DMSK_FN_INCOME&columns=ALL&filter=(SECURITY_TYPE_CODE+in+(%22058001001%22%2C%22058001008%22))(TRADE_MARKET_CODE!%3D%22069001017%22)(REPORT_DATE%3D%272024-06-30%27)
change to -> 
order by REPORT_DATE and CSECURITY_CODE, pagesize=500, pagenumber=150
https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery1123042893057157239967_1725596942438&sortColumns=REPORT_DATE%2CSECURITY_CODE&sortTypes=-1%2C-1&pageSize=500&pageNumber=150&reportName=RPT_DMSK_FN_INCOME&columns=ALL


现金流量表
cashflow
https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery112306324084828446912_1725597270087&sortColumns=NOTICE_DATE%2CSECURITY_CODE&sortTypes=-1%2C-1&pageSize=50&pageNumber=1&reportName=RPT_DMSK_FN_CASHFLOW&columns=ALL&filter=(SECURITY_TYPE_CODE+in+(%22058001001%22%2C%22058001008%22))(TRADE_MARKET_CODE!%3D%22069001017%22)(REPORT_DATE%3D%272024-06-30%27)

change to -> 
order by REPORT_DATE and CSECURITY_CODE, pagesize=500, pagenumber=150
https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery112306324084828446912_1725597270087&sortColumns=REPORT_DATE%2CSECURITY_CODE&sortTypes=-1%2C-1&pageSize=500&pageNumber=150&reportName=RPT_DMSK_FN_CASHFLOW&columns=ALL

'''

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


def get_current_date(date=None):
    fina_date = date
    if fina_date == None:
        nowdate = datetime.datetime.now().date()
        target_date = ['03-31', '06-30', '09-30', '12-31']
        tmp_date = nowdate.strftime('%m-%d')
        if tmp_date < target_date[0]:
            fina_date = (nowdate - datetime.timedelta(365)).strftime('%Y-') + target_date[3]
        elif tmp_date < target_date[1]:
            fina_date = nowdate.strftime('%Y-') + target_date[0]
        elif tmp_date < target_date[2]:
            fina_date = nowdate.strftime('%Y-') + target_date[1]
        elif tmp_date < target_date[3]:
            fina_date = nowdate.strftime('%Y-') + target_date[2]
        else: 
            print('error case')
    print('fina_date is %s' % fina_date)
    return fina_date


def get_date_from_str(x):
    if x != 0:
        x=x[0:10]
    else:
        x='1900-01-01'
    return x


def handle_raw_df(df):
    
    if len(df) == 0:
        return df

    #大写 to 小写
    df.columns = df.columns.map(lambda x:x.lower())

    if 'trade_market_code' in df.columns:
        del df['trade_market_code']


    if 'trade_market' in df.columns:
        del df['trade_market']

    if 'security_type_code' in df.columns:
        del df['security_type_code']

    if 'security_type' in df.columns:
        del df['security_type']

    if 'update_date' in df.columns:
        del df['update_date']

    if 'deduct_basic_eps' in df.columns:
        del df['deduct_basic_eps']

    if 'assigndscrpt' in df.columns:
        del df['assigndscrpt']

    if 'payyear' in df.columns:
        del df['payyear']

    #if 'publishname' in df.columns:
    #   del df['publishname']

    if 'zxgxl' in df.columns:
        del df['zxgxl']

    if 'notice_date' in df.columns:
        del df['notice_date']

    if 'org_code' in df.columns:
        del df['org_code']

    if 'trade_market_zjg' in df.columns:
        del df['trade_market_zjg']

    if 'isnew' in df.columns:
        del df['isnew']

    if 'datatype' in df.columns:
        del df['datatype']

    if 'datayear' in df.columns:
        del df['datayear']


    if 'datemmdd' in df.columns:
        del df['datemmdd']

    if 'eitime' in df.columns:
        del df['eitime']


    if 'secucode' in df.columns:
        del df['secucode']

    #balance del begin

    if 'industry_code'  in df.columns: 
        del df['industry_code']

    if 'market'  in df.columns: 
        del df['market']

    if 'date_type_code'  in df.columns: 
        del df['date_type_code']

    if 'report_type_code'  in df.columns: 
        del df['report_type_code']

    if 'data_state'  in df.columns: 
        del df['data_state']
    #balance del  end

    
    if 'reportdate' in df.columns:
        df['reportdate'] = df['reportdate'].apply(lambda x: get_date_from_str(x))

    if 'report_date' in df.columns:
        df['report_date'] = df['report_date'].apply(lambda x: get_date_from_str(x))

    df=df.fillna(0)

    return df

def get_fina_data(page=1):
    
    timestamp=str(round(time.time() * 1000))
    
    url = 'http://datacenter-web.eastmoney.com/api/data/get?callback='\
            + 'jQuery112305938889278481287_'\
            + timestamp \
            + '&st=UPDATE_DATE%2CSECURITY_CODE&sr=-1%2C-1&ps=500'\
            + '&p='\
            + str(page) \
            + '&type=RPT_LICO_FN_CPD&sty=ALL&'\
            # '&filter=(REPORTDATE%3D%272021-06-30%27)'

    print(url)
    tmp_header = get_headers()
    response = requests.get(url, headers=tmp_header)
    print(response)

    response.encoding = 'utf-8'  
    p1 = re.compile(r'[(](.*?)[)]', re.S)  #'xxx(yyy)zzz', 提取括号的内容yyy

    #把中间的'(' ')' 替换成'-', 才能正确的把json 解析出来
    s=response.text
    f1 = s.find('(')
    s = s[:f1] + '~' + s[f1+1 : ]  #first '(' -> '~'
    f2 = s.rfind(')')              #last  ')' -> '@'
    s = s[:f2] + '@' + s[f2+1 : ]

    s = s.replace('(', '-')
    s = s.replace(')', '-')

    s = s.replace('~', '(' )
    s = s.replace('@', ')' )

    response_array = re.findall(p1, s)
    api_param = json.loads(response_array[0])

    rawdata = api_param['result']['data']
    df = pd.DataFrame(rawdata)
    
    if len(df):
        df.columns = df.columns.map(lambda x:x.lower()) #大写 to 小写
        df = handle_raw_df(df)

    return df, api_param


def get_fina_data2(page=1):
    
    timestamp=str(round(time.time() * 1000))
    
    url = 'http://datacenter-web.eastmoney.com/api/data/get?callback='\
            + 'jQuery112305938889278481287_'\
            + timestamp \
            + '&st=UPDATE_DATE%2CSECURITY_CODE&sr=-1%2C-1&ps=500'\
            + '&p='\
            + str(page) \
            + '&type=RPT_LICO_FN_CPD&sty=ALL&'\
            # '&filter=(REPORTDATE%3D%272021-06-30%27)'

    print(url)

    browser = get_broswer()

    html = ''
    try:
        browser.get(url)
        browser.implicitly_wait(5)
        html = browser.page_source
    except:
        browser.close()
        browser.quit()
    finally:
        browser.close()
        browser.quit()


    if debug:
        print(html)
 
    p1 = re.compile(r'[(](.*?)[)]', re.S)  #'xxx(yyy)zzz', 提取括号的内容yyy

    #把中间的'(' ')' 替换成'-', 才能正确的把json 解析出来
    s=html
    f1 = s.find('(')
    s = s[:f1] + '~' + s[f1+1 : ]  #first '(' -> '~'
    f2 = s.rfind(')')              #last  ')' -> '@'
    s = s[:f2] + '@' + s[f2+1 : ]

    s = s.replace('(', '-')
    s = s.replace(')', '-')

    s = s.replace('~', '(' )
    s = s.replace('@', ')' )

    response_array = re.findall(p1, s)
    api_param = json.loads(response_array[0])

    rawdata = api_param['result']['data']
    df = pd.DataFrame(rawdata)
    
    if len(df):
       df = handle_raw_df(df)

    return df, api_param


#20240906
def get_fina_data3(fina='cpd', page=1):

    time.sleep(random.randint(1, 2))
    
    timestamp=str(round(time.time() * 1000))
    
    cpd_url = 'https://datacenter-web.eastmoney.com/api/data/v1/get?callback='\
            + 'jQuery112302295057226083017_'\
            + timestamp \
            + '&sortColumns=REPORTDATE%2CSECURITY_CODE&sortTypes=-1%2C-1&'\
            + 'pageSize=500'\
            + '&pageNumber='\
            + str(page) \
            + '&reportName=RPT_LICO_FN_CPD&columns=ALL'

    balance_url = 'https://datacenter-web.eastmoney.com/api/data/v1/get?callback='\
            + 'jQuery1123062742764734556_'\
            + timestamp \
            + '&sortColumns=REPORT_DATE%2CSECURITY_CODE&sortTypes=-1%2C-1&'\
            + 'pageSize=500&'\
            + 'pageNumber='\
            + str(page) \
            + '&reportName=RPT_DMSK_FN_BALANCE&columns=ALL'

    income_url = 'https://datacenter-web.eastmoney.com/api/data/v1/get?callback='\
            + 'jQuery1123042893057157239967_'\
            + timestamp \
            + '&sortColumns=REPORT_DATE%2CSECURITY_CODE&sortTypes=-1%2C-1&'\
            + 'pageSize=500&'\
            + 'pageNumber='\
            + str(page) \
            + '&reportName=RPT_DMSK_FN_INCOME&columns=ALL'

    cashflow_url = 'https://datacenter-web.eastmoney.com/api/data/v1/get?callback='\
            + 'jQuery112306324084828446912_'\
            + timestamp \
            + '&sortColumns=REPORT_DATE%2CSECURITY_CODE&sortTypes=-1%2C-1&'\
            + 'pageSize=500&'\
            + 'pageNumber='\
            + str(page) \
            + '&reportName=RPT_DMSK_FN_CASHFLOW&columns=ALL'

    if fina == 'cpd':
        url = cpd_url;
    elif fina == 'balance':
        url = balance_url;
    elif fina == 'income':
        url = income_url;
    elif fina == 'cashflow':
        url = cashflow_url;
    else :
        url = cpd_url;

    print(url)

    browser = get_broswer()

    html = ''
    try:
        browser.get(url)
        browser.implicitly_wait(5)
        html = browser.page_source
    except:
        browser.close()
        browser.quit()
    finally:
        browser.close()
        browser.quit()


    if debug:
        print(html)
 
    p1 = re.compile(r'[(](.*?)[)]', re.S)  #'xxx(yyy)zzz', 提取括号的内容yyy

    #把中间的'(' ')' 替换成'-', 才能正确的把json 解析出来
    s=html
    f1 = s.find('(')
    s = s[:f1] + '~' + s[f1+1 : ]  #first '(' -> '~'
    f2 = s.rfind(')')              #last  ')' -> '@'
    s = s[:f2] + '@' + s[f2+1 : ]

    s = s.replace('(', '-')
    s = s.replace(')', '-')

    s = s.replace('~', '(' )
    s = s.replace('@', ')' )

    response_array = re.findall(p1, s)
    api_param = json.loads(response_array[0])

    rawdata = api_param['result']['data']
    df = pd.DataFrame(rawdata)
    
    if len(df):
       df = handle_raw_df(df)

    return df, api_param



    
    
if __name__ == '__main__':

    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    df, api_param = get_fina_data3('cpd', 1)
    print(df)

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))

