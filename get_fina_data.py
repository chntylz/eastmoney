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
http://data.eastmoney.com/bbsj/202106/zcfz.html
http://datacenter-web.eastmoney.com/api/data/get?callback=jQuery11230002487053379782944_1626683115977&st=NOTICE_DATE%2CSECURITY_CODE&sr=-1%2C-1&ps=50&p=1&type=RPT_DMSK_FN_BALANCE&sty=ALL&token=894050c76af8597a853f5b408b759f5d&filter=(SECURITY_TYPE_CODE+in+(%22058001001%22%2C%22058001008%22))(REPORT_DATE%3D%272021-06-30%27)


利润表
http://data.eastmoney.com/bbsj/202106/lrb.html
http://datacenter-web.eastmoney.com/api/data/get?callback=jQuery1123033303123159764536_1626683309851&st=NOTICE_DATE%2CSECURITY_CODE&sr=-1%2C-1&ps=50&p=1&type=RPT_DMSK_FN_INCOME&sty=ALL&token=894050c76af8597a853f5b408b759f5d&filter=(SECURITY_TYPE_CODE+in+(%22058001001%22%2C%22058001008%22))(REPORT_DATE%3D%272021-06-30%27)



现金流量表
http://data.eastmoney.com/bbsj/202106/xjll.html
'''

def get_headers():
    '''
    随机获取一个headers
    '''
    user_agents =  ['Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1',\
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',\
            'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11']
    headers = {'User-Agent':random.choice(user_agents)}
    return headers


def get_holder_data(code):
    timestamp=str(round(time.time() * 1000))

    url='https://datacenter-web.eastmoney.com/api/data/get?'\
        + 'callback=jQuery112306002872431271067_'\
        + timestamp\
        + '&st=REPORTDATE&sr=-1&ps=1000&p=1&sty=ALL&filter='\
        + '(SECURITY_CODE%3D%22000153%22)&token=894050c76af8597a853f5b408b759f5d&'\
        + 'type=RPT_LICO_FN_CPD'

    tmp_header = get_headers()
    response = requests.get(url, headers=tmp_header)

    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, response.text)
    api_param = json.loads(response_array[0])

    rawdata = api_param['result']['data']
    df = pd.DataFrame(rawdata)
    return df, api_param


if __name__ == '__main__':

    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    df, api_param = get_holder_data('000977')
    print(df)

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))

