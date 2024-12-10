#!/#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from file_interface import *
import pandas as pd
#keep 0.01 accrucy
pd.set_option('display.float_format',lambda x : '%.2f' % x)


from get_daily_zlje import *

import multiprocessing

from HData_sina_fina import *
from HData_sina_income import *
from HData_sina_balance  import *
from HData_sina_cashflow import *

import  datetime
import time 

debug = 0
debug = 1
debug = 0

hdata_fina     = HData_sina_fina("usr","usr")
hdata_income   = HData_sina_income("usr","usr")
hdata_balance  = HData_sina_balance("usr","usr")
hdata_cashflow = HData_sina_cashflow("usr","usr")


def my_print():
    if debug:
        print()

def income_analysis_roe(df):
    df_len=len(df)
    flag = True
    #连续5 年的ROE 大于15%
    i = 0
    for i in range(df_len):
        roe  = df.net_asset_return_rate[i]
        if debug:
            print('roe=%s ' % roe )
        if float(roe) < 15:
            if debug:
                print(df.iloc[i])
            flag = False
            break

    return flag


def income_analysis_liab(df):
    df_len=len(df)
    flag = True
    #zong zi chan fuzhailv  < 60%
    #asset_liability_ratio
    i = 0
    for i in range(df_len):
        asset_liability_ratio = df.asset_liability_ratio[i]
        if debug:
            print('asset_liability_ratio=%s ' % asset_liability_ratio )
        if float(asset_liability_ratio) >= 60:
            if debug:
                print(df.iloc[i])
            flag = False
            break

    return flag

def income_analysis_cash_of_netincome(df):
    df_len=len(df)
    flag = True
    #连续5 年的净利润现金含量大于80%
    i = 0
    for i in range(df_len):
        cash_of_netincome  = df.net_operating_cash_flow_to_net_profit_ratio[i]
        if debug:
            print('cash_of_netincome=%s ' % cash_of_netincome )
        if float(cash_of_netincome) < 0.8:
            if debug:
                print(df.iloc[i])
            flag = False
            break

    return flag


def income_analysis_gross_rate(df):
    df_len=len(df)
    flag = True
    #连续5 年的毛利率大于30%, 用 主营业务利润率 替代
    i = 0
    for i in range(df_len):
        main_profit_rate  = df.main_business_profit_rate[i]
        if debug:
            print('main_profit_rate  =%s ' %  main_profit_rate  )
        if float(main_profit_rate) < 30:
            if debug:
                print(df.iloc[i])
            flag = False
            break

    return flag


def  fina_analysis_by_weimiao(stock_code, stock_name):

    code = stock_code
    #code = '600660'
    flag = False

    df_fina     = hdata_fina.get_data_from_hdata(stock_code=code)

    if len(df_fina) == 0:
        return [code, stock_name, flag]
    
    df_fina = df_fina.sort_values('record_date', ascending=0)
    df_fina = df_fina.reset_index(drop=True)
    df_fina=df_fina.head(3)

    key_day = '12-31'
    key_day = '09-30'
    key_day = df_fina.record_date[0][5:]

    df_y_fina = df_fina[df_fina['record_date'].str.contains(key_day)]
    df_y_fina = df_y_fina.reset_index(drop=True)
    #df.to_csv('./csv_data/sina_fina.csv', encoding='utf-8-sig')
   

    if debug:
        print(df_fina)
        print(df_y_fina)

    if  income_analysis_liab(df_y_fina)  and \
        income_analysis_roe(df_y_fina)  and \
        income_analysis_cash_of_netincome(df_y_fina)  and \
        income_analysis_gross_rate(df_y_fina):
        print(code,  stock_name)
        flag = True

    return [code, stock_name, flag]

def worker(data):
    if debug:
        print(data)
    stock_code = data[2]
    stock_name = data[3]

    ret  = fina_analysis_by_weimiao(stock_code, stock_name)
    return ret



if __name__ == '__main__':

    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    stock_df=get_daily_zlje2()
    stock_df = stock_df.sort_values('f12', ascending=1)
    stock_df = stock_df.reset_index(drop=True)
    print(stock_df.head(5))
    #stock_df=stock_df.head(4)
    #exit()


    data_list = np.array(stock_df)
    data_list = data_list.tolist()

    processes = 4
    number = len(stock_df)
    mplist = []
    with multiprocessing.Pool(int(processes)) as pool:
       mplist.append(pool.map(worker, data_list))


    data_column = ['stock_code', 'stock_name', 'flag']
    update_df=pd.DataFrame(mplist[0], columns=data_column)
    weimiao_df = update_df[update_df['flag'] == True]
    del weimiao_df['flag']
    weimiao_df['stock_name']  = weimiao_df['stock_name'].apply(lambda x: x.replace(' ', ''))
    weimiao_df['stock_name']  = weimiao_df['stock_name'].apply(lambda x: x.replace('\"', ''))
    print(update_df)
    print(weimiao_df)
    weimiao_df.to_csv('./cgi-bin/weimiao.txt', sep=' ',  index=False)
    
 

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))


