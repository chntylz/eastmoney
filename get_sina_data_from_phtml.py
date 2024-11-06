#!/#!/usr/bin/env python
# -*- coding: utf-8 -*-


import pandas as pd
import csv
import os
import re
import  datetime
import time

from get_daily_zlje import *

import multiprocessing


import random

debug = 0 
debug = 1 
debug = 0 

def get_sina_data_from_phtml(stock_code, type_table):
    
    time.sleep(random.randint(5,15))

    if type_table == 'balance':
        url = 'https://money.finance.sina.com.cn/corp/go.php/vDOWN_BalanceSheet/displaytype/4/stockid/' + stock_code  + '/ctrl/all.phtml'
    elif type_table == 'income':  
        url = 'https://money.finance.sina.com.cn/corp/go.php/vDOWN_ProfitStatement/displaytype/4/stockid/' + stock_code  + '/ctrl/all.phtml'
    elif type_table == 'cashflow':  
        url = 'https://money.finance.sina.com.cn/corp/go.php/vDOWN_CashFlow/displaytype/4/stockid/' + stock_code  + '/ctrl/all.phtml'
    else:
        print('### type_table is null, return')


    #download file
    csv_file = './sina/' + stock_code + '_' + type_table + '.csv'
    cmd = 'curl -s  ' + url + ' >' + ' ' + csv_file
    if debug:
        print(cmd)
    os.system(cmd)

    df = pd.DataFrame()
    #read data from csv
    try:
        df=pd.read_csv(csv_file , sep='\s', encoding='gbk', engine='python')
        #df=pd.read_csv(csv_file , sep='\s', encoding='utf-8', engine='python')
    except Exception as e:
        print('error %s %s: %s' % (stock_code, type_table, e))
        return

    df=df.T

    #delete file
    cmd = 'rm -f ' + csv_file
    if debug:
        print(cmd)
    os.system(cmd)

    csv_file = './sina/' +  stock_code + '_' + type_table + '_new.csv'
    df.to_csv(csv_file, encoding='utf-8-sig', float_format='%.2f')

def worker(data):
    if debug:
        print(data)
    stock_code = data[2]
    stock_name = data[3]


    get_sina_data_from_phtml(stock_code, 'balance')
    get_sina_data_from_phtml(stock_code, 'income')
    get_sina_data_from_phtml(stock_code, 'cashflow')

    return


if __name__ == '__main__':

    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())



    stock_df=get_daily_zlje2()
    stock_df = stock_df.sort_values('f12', ascending=1)
    stock_df = stock_df.reset_index(drop=True)
    print(stock_df.head(5))
    #stock_df=stock_df.head(4)


    data_list = np.array(stock_df)
    data_list = data_list.tolist()

    processes = 4
    number = len(stock_df)
    with multiprocessing.Pool(int(processes)) as pool:
        pool.map(worker, data_list)

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))


