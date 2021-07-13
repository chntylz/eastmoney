#!/#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import json
import requests
import re

import time
import datetime

def get_holder_data(code):
    timestamp=str(round(time.time() * 1000))
    url='http://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery112308619214168139351_1625036256522&sortColumns=END_DATE&sortTypes=-1&pageSize=10000&pageNumber=1&reportName=RPT_HOLDERNUM_DET&columns=SECURITY_CODE%2CSECURITY_NAME_ABBR%2CCHANGE_SHARES%2CCHANGE_REASON%2CEND_DATE%2CINTERVAL_CHRATE%2CAVG_MARKET_CAP%2CAVG_HOLD_NUM%2CTOTAL_MARKET_CAP%2CTOTAL_A_SHARES%2CHOLD_NOTICE_DATE%2CHOLDER_NUM%2CPRE_HOLDER_NUM%2CHOLDER_NUM_CHANGE%2CHOLDER_NUM_RATIO%2CEND_DATE%2CPRE_END_DATE&quoteColumns=f2%2Cf3&filter=(SECURITY_CODE%3D%22002064%22)&source=WEB&client=WEB'

    url='http://datacenter-web.eastmoney.com/api/data/v1/get?callback='\
            + 'jQuery112308619214168139351_'\
            + timestamp \
            + '&sortColumns=END_DATE&sortTypes=-1&pageSize=10000&pageNumber=1&'\
            + 'reportName=RPT_HOLDERNUM_DET&'\
            + 'columns=SECURITY_CODE%2CSECURITY_NAME_ABBR'\
            + '%2CCHANGE_SHARES%2CCHANGE_REASON%2CEND_DATE'\
            + '%2CINTERVAL_CHRATE%2CAVG_MARKET_CAP%2CAVG_HOLD_NUM'\
            + '%2CTOTAL_MARKET_CAP%2CTOTAL_A_SHARES%2CHOLD_NOTICE_DATE'\
            + '%2CHOLDER_NUM%2CPRE_HOLDER_NUM%2CHOLDER_NUM_CHANGE'\
            + '%2CHOLDER_NUM_RATIO%2CEND_DATE%2CPRE_END_DATE&'\
            + 'quoteColumns=f2%2Cf3&filter=(SECURITY_CODE%3D%22'\
            + code \
            + '%22)&source=WEB&client=WEB'


    response = requests.get(url)
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

