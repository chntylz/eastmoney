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

from bs4 import BeautifulSoup
import random

from HData_sina_balance import *
from HData_sina_income import *
from HData_sina_cashflow import *
from HData_sina_fina import *

hdata_sina_balance = HData_sina_balance("usr","usr")
hdata_sina_income  = HData_sina_income("usr","usr")
hdata_sina_cashflow= HData_sina_cashflow("usr","usr")
hdata_sina_fina    = HData_sina_fina("usr","usr")



debug = 0 
debug = 1 
debug = 0 


def insert_to_database(df, type_table):

    cols = []
    database = ''

    income_cols = [ 'record_date', 'stock_code', 'stock_name', 'biztotinco', 'bizinco', 'biztotcost', 'bizcost', 'biztax', \
                 'salesexpe', 'manaexpe', 'finexpe', 'deveexpe', 'asseimpaloss', 'valuechgloss', \
                 'inveinco', 'assoinveprof', 'exchggain', 'perprofit', 'nonoreve', 'nonoexpe', \
                 'noncassetsdisl', 'totprofit', 'incotaxexpe', 'netprofit', 'parenetp', 'minysharrigh', \
                 'eps', 'basiceps', 'dilutedeps', 'othercompinco', 'compincoamt', 'parecompincoamt', 'minysharincoamt' ]
    

    balance_cols = [ 'record_date', 'stock_code', 'stock_name', 'current_assets', 'curfds', \
    'tradfinasset', 'derifinaasset', 'notesaccorece', \
	'notesrece', 'accorece', 'recfinanc', 'prep', 'otherrecetot', 'interece', 'dividrece', \
	'otherrece', 'purcresaasset', 'inve', 'accheldfors', 'expinoncurrasset', 'prepexpe', \
	'unseg', 'othercurrasse', 'totcurrasset', 'noncurrent_assets', 'lendandloan', 'avaisellasse', 'holdinvedue', \
	'longrece', 'equiinve', 'inveprop', 'consprogtot', 'consprog', 'engimate', 'fixedassecleatot', \
	'fixedassenet', 'fixedasseclea', 'prodasse', 'comasse', 'hydrasset', 'ruseassets', 'intaasset', \
	'deveexpe', 'goodwill', 'logprepexpe', 'defetaxasset', 'othernoncasse', 'totalnoncassets', \
	'totasset', 'current_debt', 'shorttermborr', 'tradfinliab', 'notesaccopaya', 'notespaya', 'accopaya', \
	'advapaym', 'copepoun', 'copeworkersal', 'taxespaya', 'otherpaytot', 'intepaya', 'divipaya', \
	'otherpay', 'accrexpe', 'defereve', 'shorttermbdspaya', 'duenoncliab', 'othercurreliabi', \
	'totalcurrliab', 'noncurrent_debt', 'longborr', 'bdspaya', 'leaseliab', 'lcopeworkersal', 'longpayatot', \
	'longpaya', 'specpaya', 'expenoncliab', 'defeincotaxliab', 'longdefeinco', 'othernoncliabi', \
	'totalnoncliab', 'totliab', 'equity', 'paidincapi', 'capisurp', 'treastk', 'ocl', 'specrese', 'rese', \
    'generiskrese', 'undiprof', 'paresharrigh', 'minysharrigh', 'righaggr', 'totliabsharequi' ]



    if type_table == 'balance':
        cols = balance_cols
        database = hdata_sina_balance
    elif type_table == 'income':  
        cols = income_cols
        database = hdata_sina_income
    elif type_table == 'cashflow':  
        database = hdata_sina_cashflow
    else:
        print('### type_table is null, return')

   
    df.columns = cols

    #str to date format  for database format
    df['record_date']=df['record_date'].apply(lambda x: datetime.datetime.strptime(x, '%Y%m%d').date().strftime("%Y-%m-%d"))

    database.copy_from_stringio(df)

    return df



def get_sina_data_from_phtml(stock_code, stock_name,  type_table):
    
    time.sleep(random.randint(5,15)) #add time to avoid sina crawl rules

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
        df=pd.read_csv(csv_file , sep='\s', encoding='gbk', engine='python', index_col=0)
        #df=pd.read_csv(csv_file , sep='\s', encoding='utf-8', engine='python')
    except Exception as e:
        print('error %s %s: %s' % (stock_code, type_table, e))
        return

    df=df.T
    if '单位' in df.columns:
        del df['单位']

    df.insert(0, 'stock_name' , stock_name, allow_duplicates=False)
    df.insert(0, 'stock_code' , stock_code, allow_duplicates=False)


    df=df.fillna(0)
    df=df.reset_index()

    #delete file
    cmd = 'rm -f ' + csv_file
    if debug:
        print(cmd)
    os.system(cmd)

    csv_file = './sina/' +  stock_code + '_' + type_table + '_new.csv'
    df.to_csv(csv_file, encoding='utf-8-sig', float_format='%.2f')

    if '银行' in stock_name:
        pass
    else:
        insert_to_database(df, type_table)
        pass

    return df

def worker(data):
    if debug:
        print(data)
    stock_code = data[2]
    stock_name = data[3]


    get_sina_data_from_phtml(stock_code, stock_name, 'balance')
    #get_sina_data_from_phtml(stock_code, stock_name, 'income')
    #get_sina_data_from_phtml(stock_code, stock_name, 'cashflow')

    return


if __name__ == '__main__':

    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


    hdata_sina_income.db_hdata_sina_create()
    hdata_sina_balance.db_hdata_sina_create()
    hdata_sina_cashflow.db_hdata_sina_create()

    stock_df=get_daily_zlje2()
    stock_df = stock_df.sort_values('f12', ascending=1)
    stock_df = stock_df.reset_index(drop=True)
    print(stock_df.head(5))
    stock_df=stock_df.head(4)


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


