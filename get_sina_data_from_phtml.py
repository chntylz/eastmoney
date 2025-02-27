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


update_all = 0    #create new database

csv_exist = 0


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

    cash_cols =  ['record_date', 'stock_code', 'stock_name', 'cashfromop', 'laborgetcash', 'taxrefd', 'receotherbizcash', 'bizcashinfl', 
             'labopayc', 'payworkcash', 'paytax', 'payacticash', 'bizcashoutf', 'mananetr', 
             'cashfrominvent', 'withinvgetcash', 'inveretugetcash', 'fixedassetnetc', 'subsnetc', 'receinvcash', 
             'invcashinfl', 'acquassetcash', 'invpayc', 'subspaynetcash', 'payinvecash', 
             'invcashoutf', 'invnetcashflow', 'cashfromfinancingactivities', 'invrececash', 'subsrececash', 'recefromloan', 
             'issbdrececash', 'recefincash', 'fincashinfl', 'debtpaycash', 'diviprofpaycash', 
             'subspaydivid', 'finrelacash', 'fincashoutf', 'finnetcflow', 'chgexchgchgs', 
             'cashnetr', 'inicashbala', 'finalcashbala', 'cashnote', 'netprofit', 'minysharrigh', 
             'unreinveloss', 'asseimpa', 'assedepr', 'intaasseamor', 'longdefeexpenamor', 
             'prepexpedecr', 'accrexpeincr', 'dispfixedassetloss', 'fixedassescraloss', 
             'valuechgloss', 'defeincoincr', 'estidebts', 'finexpe', 'inveloss', 'defetaxassetdecr', 
             'defetaxliabincr', 'inveredu', 'receredu', 'payaincr', 'unseparachg', 'unfiparachg', 
             'other', 'biznetcflow', 'debtintocapi', 'expiconvbd', 'finfixedasset', 'cashfinalbala', 
             'cashopenbala', 'equfinalbala', 'equopenbala', 'cashneti' ]

    if type_table == 'balance':
        cols = balance_cols
        database = hdata_sina_balance
    elif type_table == 'income':  
        cols = income_cols
        database = hdata_sina_income
    elif type_table == 'cashflow':  
        cols = cash_cols
        database = hdata_sina_cashflow
    else:
        print('### type_table is null, return')

    try:
        df.columns = cols
        #str to date format  for database format
        df['record_date']=df['record_date'].apply(lambda x: datetime.datetime.strptime(x, '%Y%m%d').date().strftime("%Y-%m-%d"))
        if update_all:
            pass
        else:
            df = df.head(1)  #only update the latest item
        database.copy_from_stringio(df)
    except Exception as e:
        print("### error (%s):%s %s" % (e, type_table, df.head(1)))

    return df

def get_sina_data_from_phtml(stock_code, stock_name,  type_table):
    
    if csv_exist:
        pass
    else:
        time.sleep(random.randint(5,10)) #add time to avoid sina crawl rules

    if type_table == 'balance':
        url = 'https://money.finance.sina.com.cn/corp/go.php/vDOWN_BalanceSheet/displaytype/4/stockid/' + stock_code  + '/ctrl/all.phtml'
    elif type_table == 'income':  
        url = 'https://money.finance.sina.com.cn/corp/go.php/vDOWN_ProfitStatement/displaytype/4/stockid/' + stock_code  + '/ctrl/all.phtml'
    elif type_table == 'cashflow':  
        url = 'https://money.finance.sina.com.cn/corp/go.php/vDOWN_CashFlow/displaytype/4/stockid/' + stock_code  + '/ctrl/all.phtml'
    else:
        print('### type_table is null, return')

    csv_file = './sina/' + stock_code + '_' + type_table + '.csv'


    if csv_exist:
        pass
    else:
        #download file
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

    if debug:
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

    csv_balance = './sina/' + stock_code + '_balance.csv'
    csv_income  = './sina/' + stock_code + '_income.csv'
    csv_cashflow = './sina/' + stock_code + '_cashflow.csv'

    if os.path.exists(csv_balance) and \
        os.path.exists(csv_income) and \
        os.path.exists(csv_cashflow):
        print('%s %s already exists' % (stock_code, stock_name))

        if csv_exist:
            get_sina_data_from_phtml(stock_code, stock_name, 'balance')
            get_sina_data_from_phtml(stock_code, stock_name, 'income')
            get_sina_data_from_phtml(stock_code, stock_name, 'cashflow')
        else:
            return

    get_sina_data_from_phtml(stock_code, stock_name, 'balance')
    get_sina_data_from_phtml(stock_code, stock_name, 'income')
    get_sina_data_from_phtml(stock_code, stock_name, 'cashflow')


    return


if __name__ == '__main__':

    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    stock_df=get_daily_zlje3()
    stock_df.to_csv('./sina/zlje.csv', encoding='utf-8-sig', float_format='%.2f')
    stock_df = stock_df.sort_values('f12', ascending=1)
    stock_df = stock_df.reset_index(drop=True)
    print(stock_df.head(5))
    #stock_df=stock_df.head(4)
    #exit()


    data_list = np.array(stock_df)
    data_list = data_list.tolist()

    '''
    #only update the latest item
    if  update_all:
        hdata_sina_income.db_hdata_sina_create()
        hdata_sina_balance.db_hdata_sina_create()
        hdata_sina_cashflow.db_hdata_sina_create()
    '''

    processes = 4
    number = len(stock_df)
    with multiprocessing.Pool(int(processes)) as pool:
        pool.map(worker, data_list)

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))


