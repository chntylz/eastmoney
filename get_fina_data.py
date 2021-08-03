#!/#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import json
import requests
import re

import time
import datetime

import random


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

def get_fina_data(code):
    timestamp=str(round(time.time() * 1000))


    url='http://datacenter-web.eastmoney.com/api/data/get?callback=jQuery112305938889278481287_1626682926066&st=UPDATE_DATE%2CSECURITY_CODE&sr=-1%2C-1&ps=50&p=1&type=RPT_LICO_FN_CPD&sty=ALL&token=894050c76af8597a853f5b408b759f5d&filter=(REPORTDATE%3D%272021-06-30%27)'

    tmp_header = get_headers()
    response = requests.get(url, headers=tmp_header)

    response.encoding = 'utf-8'  
    p1 = re.compile(r'[(](.*?)[)]', re.S)

    #把中间的'(' ')' 替换成'-', 才能正确的把json 解析出来
    s=response.text
    f1 = s.find('(')
    s = s[:f1] + '~' + s[f1+1 : ]
    f2 = s.rfind(')')
    s = s[:f2] + '@' + s[f2+1 : ]

    s = s.replace('(', '-')
    s = s.replace(')', '-')

    s = s.replace('~', '(' )
    s = s.replace('@', ')' )

    response_array = re.findall(p1, s)
    api_param = json.loads(response_array[0])

    rawdata = api_param['result']['data']
    df = pd.DataFrame(rawdata)
    return df, api_param


    
if __name__ == '__main__':

    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    df, api_param = get_fina_data('000977')
    print(df)

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))

