#!/#!/usr/bin/env python  
# -*- coding: utf-8 -*-

#龙虎榜机构统计
#LHBJGTJ
#https://data.eastmoney.com/stock/jgmmtj.html
#https://datainterface3.eastmoney.com/EM_DataCenter_V3/api/LHBJGTJ/GetHBJGTJ?js=jQuery1123042907327005290474_1624455435981&sortfield=PBuy&sortdirec=1&pageSize=50&pageNum=1&tkn=eastmoney&code=&mkt=0&dateNum=&cfg=lhbjgtj&startDateTime=2021-06-23&endDateTime=2021-06-23



import pandas as pd
import json
import requests
import re
import numpy as np

import time
import datetime

import sys
sys.path.append("..")
from file_interface import *

from HData_eastmoney_dragon import *

debug = 0

hdata_dragon = HData_eastmoney_dragon('usr', 'usr')

def check_table():
    table_exist = hdata_dragon.table_is_exist()
    print('table_exist=%d' % table_exist)
    if table_exist:
        #hdata_dragon.db_hdata_xq_create()
        print('table already exist')
    else:
        hdata_dragon.db_hdata_xq_create()
        print('table not exist, create')


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



def get_dragon_tiger(date=None, url_type=None):

    timestamp=str(round(time.time() * 1000))

    if date == None:
        nowdate=datetime.datetime.now().date()
        date = nowdate.strftime("%Y-%m-%d")

    url = url_lhb = 'https://datainterface3.eastmoney.com/EM_DataCenter_V3/api/LHBGGDRTJ/GetLHBGGDRTJ?'\
            + 'js=jQuery112307979656966674489_'\
            + timestamp \
            + '&sortColumn=&sortRule=1&pageSize=5000&pageNum=1&tkn=eastmoney&'\
            + 'dateNum=&cfg=lhbggdrtj&mkt=0&startDateTime='\
            + date \
            + '&endDateTime='\
            + date


    #https://www.zhihu.com/question/31600760
    url_jg='https://datainterface3.eastmoney.com/EM_DataCenter_V3/api/LHBJGTJ/GetHBJGTJ?'\
            + 'js=jQuery1123029660230486644434_'\
            + timestamp \
            + '&sortfield=PBuy&sortdirec=1&pageSize=5000&pageNum=1&tkn=eastmoney&code=&'\
            + 'mkt=0&dateNum=&cfg=lhbjgtj&startDateTime='\
            + date \
            + '&endDateTime='\
            + date

    if url_type == None:
        url = url_lhb
    else:
        url = url_jg
   
    print(url)
    tmp_header = get_headers()
    print(tmp_header)
    response = requests.get(url, headers=tmp_header)
    print(response)

    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, response.text)
    api_param = json.loads(response_array[0])
    tmp_column= api_param['Data'][0]['FieldName']
    tmp_column=tmp_column.split(',')
    rawdata = api_param['Data'][0]['Data']
    data_df = pd.DataFrame(rawdata)
    if len(data_df):
        data_df = data_df[0].str.split('|', expand=True)
        data_df.columns=tmp_column

    return data_df


if __name__ == '__main__':

    cript_name, para1 = check_input_parameter()

    nowdate=datetime.datetime.now().date()
    nowdate=nowdate-datetime.timedelta(int(para1))
    
    check_table()

    jg_df = get_dragon_tiger(nowdate.strftime("%Y-%m-%d"), url_type='all_jg')
    time.sleep(15)
    all_df = get_dragon_tiger(nowdate.strftime("%Y-%m-%d"))
    
    if len(jg_df) or len(all_df):

        jg_df.columns = jg_df.columns.map(lambda x:x.lower())
        all_df.columns = all_df.columns.map(lambda x:x.lower())

        all_df = all_df.drop_duplicates(subset=['scode', 'tdate'], keep='first')
        jg_df  = jg_df.drop_duplicates(subset=['scode', 'tdate'], keep='first')

        conj_df = pd.merge(all_df, jg_df, how='outer', on=['scode', 'tdate'])
        conj_df = conj_df.fillna(0)
        conj_df = conj_df.replace('',0)
        conj_df = conj_df.reset_index(drop=True)
        
        conj_df['dp'] = conj_df['dp'].apply(lambda x: str(x).replace(',', '_') )
        conj_df['ctypedes_x'] = conj_df['ctypedes_x'].apply(lambda x: str(x).replace(',', '_') )
        conj_df['ctypedes_y'] = conj_df['ctypedes_y'].apply(lambda x: str(x).replace(',', '_') )


        cur_time = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())
        conj_df.to_csv('./csv_data/' + 'dragon-' + cur_time + '.csv', encoding='gbk')
        hdata_dragon.delete_data_from_hdata(\
            start_date=nowdate.strftime("%Y-%m-%d"), \
            end_date=nowdate.strftime("%Y-%m-%d"))
        hdata_dragon.copy_from_stringio(conj_df)
    else:
        print('dragon not found!!!')
