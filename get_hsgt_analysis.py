#!/usr/bin/env python
#coding:utf-8
import os,sys,gzip
import json


from file_interface import *


import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
import numpy as np

from HData_hsgt import *

from comm_generate_web_html import *

import  datetime

#keep 0.01 accrucy
pd.set_option('display.float_format',lambda x : '%.2f' % x)

###################################################################################


debug=0
#debug=1


hdata_hsgt=HData_hsgt("usr","usr")

###################################################################################

def hsgt_get_stock_list():
    df=hdata_hsgt.get_all_list_of_stock()
    if debug:
        print("df size is %d"% (len(df)))
    
    return df

#get all hsgt data from hdata_hsgt_table order by record_date desc
def hsgt_get_all_data():
    nowdate=datetime.datetime.now().date()
    from_date=nowdate-datetime.timedelta(150)
    print("from_date is %s"%(from_date.strftime("%Y-%m-%d")))


    #df=hdata_hsgt.get_all_hdata_of_stock(from_date.strftime("%Y-%m-%d"))
    df=hdata_hsgt.get_data_from_hdata(start_date=from_date.strftime("%Y-%m-%d"))
    if debug:
        print("df size is %d"% (len(df)))
    
    return df


#get hsgt sum of days
def hsgt_get_delta_m_of_day(df, days):
    delta_dict={2:'delta2_m',  3:'delta3_m', 4:'delta4_m', 5:'delta5_m', 10:'delta10_m', 21:'delta21_m', 120:'delta120_m'}
    target_column=delta_dict[days]
    df[target_column] = df['delta1_m']
    for i in range(1, days):
        if debug:
            print('i=%d, days=%d'%(i, days))
        src_column='money_sft_'+ str(i)
        df[target_column] = df[target_column] + df[src_column]

    return df


def hsgt_handle_all_data(df):
    all_df=df

    latest_date=all_df.loc[0,'record_date']
    print('lastet_date:%s' % (latest_date))

    del all_df['open']
    del all_df['high']
    del all_df['low']
    del all_df['volume']
    
    #the_first_line - the_second_line
    all_df['delta_close']  = all_df.groupby('stock_code')['close'].apply(lambda i:i.diff(-1))    
    all_df['a_pct'] = all_df['delta_close'] * 100 / (all_df['close'] - all_df['delta_close']) 
    del all_df['delta_close'] 
    
    all_df['percent_tmp'] = all_df['percent']
    del all_df['percent']
    all_df['hk_pct'] = all_df['percent_tmp']
    del all_df['percent_tmp']

    all_df['delta1']  = all_df.groupby('stock_code')['hk_pct'].apply(lambda i:i.diff(-1))    
    all_df['delta1_share'] = all_df.groupby('stock_code')['share_holding'].apply(lambda i:i.diff(-1))
    all_df['delta1_m'] = all_df['close'] * all_df['delta1_share'] / 10000;
    del all_df['delta1_share']

    all_df['delta2']  =all_df.groupby('stock_code')['hk_pct'].apply(lambda i:i.diff(-2))                                                                                                                  
    all_df['delta3']  =all_df.groupby('stock_code')['hk_pct'].apply(lambda i:i.diff(-3))
    all_df['delta4']  =all_df.groupby('stock_code')['hk_pct'].apply(lambda i:i.diff(-4))
    all_df['delta5']  =all_df.groupby('stock_code')['hk_pct'].apply(lambda i:i.diff(-5))
    all_df['delta10'] =all_df.groupby('stock_code')['hk_pct'].apply(lambda i:i.diff(-10))
    all_df['delta21'] =all_df.groupby('stock_code')['hk_pct'].apply(lambda i:i.diff(-21))
    all_df['delta120']=all_df.groupby('stock_code')['hk_pct'].apply(lambda i:i.diff(-120))
    
    
    all_df['share_holding'] = all_df['share_holding'].apply(lambda i: i/10000/10000)
    all_df['total_mv'] = all_df['total_mv'].apply(lambda i: i/10000/10000)

    max_number=21
    #temp column added
    for index in range(1, max_number):
        column='money_sft_'+ str(index)
        all_df[column] = all_df.groupby('stock_code')['delta1_m'].shift(index*(-1))

    all_df=all_df.fillna(0)

    all_df=hsgt_get_delta_m_of_day(all_df, 2)
    all_df=hsgt_get_delta_m_of_day(all_df, 3)
    all_df=hsgt_get_delta_m_of_day(all_df, 4)
    all_df=hsgt_get_delta_m_of_day(all_df, 5)
    all_df=hsgt_get_delta_m_of_day(all_df, 10)
    all_df=hsgt_get_delta_m_of_day(all_df, 21)
    #all_df=hsgt_get_delta_m_of_day(all_df, 120)

    all_df=all_df.round(2)

    #temp column delete
    for index in range(1, max_number):
        column='money_sft_'+ str(index)
        del all_df[column]


    if debug:
        print(all_df.head(10))    

    #all_df=all_df[all_df['delta1_m'] != 0]
    #all_df=all_df.reset_index(drop=True)

    return all_df, latest_date


    pass
            


def hsgt_handle_html_special(filename):
    with open(filename,'a') as f:
        f.write('<p style="color: #FF0000"> 《内地与香港股票市场交易互联互通机制若干规定》规定： </p>\n')
        f.write('<p style="color: #FF0000">     单个境外投资者对单个上市公司的持股比例，不得超过该上市公司股份总数的10% </p>\n')
        f.write('<p style="color: #FF0000">     所有境外投资者对单个上市公司A股的持股比例总和，不得超过该上市公司股份总数的30% </p>\n')
        f.write('<p style="color: #FF0000"> ----------------------------------------------------------------------------------------</p>\n')

        f.write('<p style="color: #FF0000"> delta1: delta hk_pct of 1 day </p>\n')
        f.write('<p style="color: #FF0000"> delta1_m: delta money of 1 day, delta share_holding * close </p>\n')
        

    pass

###################################################################################

if __name__ == '__main__':
    df=hsgt_get_all_data()
    all_df, latest_date = hsgt_handle_all_data(df)

    #zig_p_df = all_df[all_df.is_zig > 0]
    zig_p_df = all_df

    save_dir = "hsgt"
    exec_command = "mkdir -p " + (save_dir)
    print(exec_command)
    os.system(exec_command)

    file_name=save_dir + '-' + datetime.datetime.strptime(latest_date,'%Y-%m-%d').strftime("%Y-%m-%d-%w") + '-r0'
    newfile=save_dir + '/' + file_name + '.html'
    comm_handle_html_head(newfile, 'hsgt', latest_date)
    hsgt_handle_html_special(newfile)
    comm_handle_html_body(newfile, all_df, 'top10')
    comm_handle_html_end(newfile)

    file_name=save_dir + '-' + datetime.datetime.strptime(latest_date,'%Y-%m-%d').strftime("%Y-%m-%d-%w") + '-r1'
    newfile=save_dir + '/' + file_name + '.html'
    comm_handle_html_head(newfile, 'hsgt', latest_date)
    hsgt_handle_html_special(newfile)
    comm_handle_html_body(newfile, zig_p_df, 'p_money')
    comm_handle_html_end(newfile)

    file_name=save_dir + '-' + datetime.datetime.strptime(latest_date,'%Y-%m-%d').strftime("%Y-%m-%d-%w") + '-r2'
    newfile=save_dir + '/' + file_name + '.html'
    comm_handle_html_head(newfile, 'hsgt', latest_date)
    hsgt_handle_html_special(newfile)
    comm_handle_html_body(newfile, zig_p_df, 'p_continous_day')
    comm_handle_html_end(newfile)

    file_name=save_dir + '-' + datetime.datetime.strptime(latest_date,'%Y-%m-%d').strftime("%Y-%m-%d-%w") + '-r3'
    newfile=save_dir + '/' + file_name + '.html'
    comm_handle_html_head(newfile, 'hsgt', latest_date)
    hsgt_handle_html_special(newfile)
    comm_handle_html_body(newfile, zig_p_df, 'p_minus_money')
    comm_handle_html_end(newfile)

    file_name=save_dir + '-' + datetime.datetime.strptime(latest_date,'%Y-%m-%d').strftime("%Y-%m-%d-%w") + '-r4'
    newfile=save_dir + '/' + file_name + '.html'
    comm_handle_html_head(newfile, 'hsgt', latest_date)
    hsgt_handle_html_special(newfile)
    comm_handle_html_body(newfile, zig_p_df, 'p_minus_continous_day')
    comm_handle_html_end(newfile)

