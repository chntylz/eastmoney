#!/usr/bin/env python
#coding:utf-8
import os,sys
import psycopg2 #使用的是PostgreSQL数据库
#from HData_xq_day import *
from HData_eastmoney_day import *
from HData_hsgt import *
import  datetime
import time

from file_interface import *
from comm_generate_web_html import *

import numpy as np
import pandas as pd
#keep 0.01 accrucy
pd.set_option('display.float_format',lambda x : '%.2f' % x)


#funcat
from funcat import *
from funcat.data.aaron_backend import AaronDataBackend
set_data_backend(AaronDataBackend())



from HData_eastmoney_zlje import *
from HData_eastmoney_zlje_3 import *
from HData_eastmoney_zlje_5 import *
from HData_eastmoney_zlje_10 import *

#hdata=HData_xq_day("usr","usr")
hdata_day=HData_eastmoney_day("usr","usr")
hsgtdata=HData_hsgt("usr","usr")

zlje_table=HData_eastmoney_zlje("usr","usr")

from HData_eastmoney_dragon import *
dragon_table=HData_eastmoney_dragon("usr","usr")

dict_industry={}
df=None


debug=0
debug=1

today_date=datetime.datetime.now().date()
#test
nowdate=today_date
lastdate=nowdate-datetime.timedelta(1)

curr_day=nowdate.strftime("%Y-%m-%d")
curr_day_w=nowdate.strftime("%Y-%m-%d-%w")
last_day=lastdate.strftime("%Y-%m-%d")
print("curr_day:%s, last_day:%s"%(curr_day, last_day))

stock_data_dir="stock_data"
curr_dir=curr_day_w+'-zig'


def continue_handle_html_body_special(newfile, date):
    f = newfile
    curr_day = date
    with open(newfile,'a') as f:
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('<p> 日期 %s </p>\n' %(curr_day))

        t1 = time.time()
        #df = get_today_item(curr_day)
        print('delta time= %s ' % (time.time() - t1))

        # 找出上涨的股票
        df_up = df[df['percent'] > 0.00]
        # 走平股数
        df_even = df[df['percent'] == 0.00]
        # 找出下跌的股票
        df_down = df[df['percent'] < 0.00]

        # 找出涨停的股票
        limit_up = df[df['percent'] >= 9.70]
        limit_down = df[df['percent'] <= -9.70]

        s_debug= ('<p> A股上涨个数： %d,  A股下跌个数： %d,  A股走平个数:  %d</p>' % \
                (df_up.shape[0], df_down.shape[0], df_even.shape[0]))
        print(s_debug)
        f.write('%s\n'%(s_debug))

        s_debug=('<p> 涨停数量：%d 个</p>' % (limit_up.shape[0]))
        print(s_debug)
        f.write('%s\n'%(s_debug))

        s_debug=('<p> 跌停数量：%d 个</p>' % (limit_down.shape[0]))
        print(s_debug)
        f.write('%s\n'%(s_debug))

        f.write('<p>-----------------------------------我是分割线-----------------------------------</p>\n')
    
        f.write('<p  style="color:green;">绿色: 当天跳空高开2个点以上 </p>')
        f.write('<p>-----------------------------------我是分割线-----------------------------------</p>\n')
        f.write('\n')

    pass


def continue_handle_html_end_special(newfile, dict_industry):
    with open(newfile,'a') as f:
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('<p>-----------------------------------我是分割线-----------------------------------</p>\n')
        f.write('\n')
        f.write('<p>industry %s</p>\n' % \
                (sorted(dict_industry.items(),key=lambda x:x[1],reverse=True)))
        f.write('\n')
        f.write('<p>-----------------------------------我是分割线-----------------------------------</p>\n')

    pass
    
   
def generate_html(df):
    os.system('mkdir -p ' + stock_data_dir)
    os.system('mkdir -p ' + stock_data_dir +'/' + curr_dir)
    newfile='%s/%s'%(stock_data_dir, curr_dir + '/' + curr_dir + '.html')
    comm_handle_html_head(newfile, stock_data_dir, curr_day )
    continue_handle_html_body_special(newfile, curr_day)
    comm_handle_html_body(newfile, df)
    continue_handle_html_end_special(newfile, dict_industry)
    comm_handle_html_end(newfile, curr_dir)

   

def get_current_k_data(date):
    print(date)
    df = hdata.get_data_from_hdata(start_date=date, \
            end_date=date)
    if len(df):
        print(df.head(2))
    return df

def convert_to_html_df(df):
    dict_industry.clear()
    if len(df) < 1:
        print('#error, df data len < 1, return')
        return df
    df = df.reset_index(drop=True)
    if len(df):
        html_df = comm_generate_web_dataframe_new(df, curr_dir, curr_day, dict_industry )
        if debug:
            print('dict_industry:%s' % dict_industry)
    else:
        html_df = df
        print('#error, html_df data len < 1, return None')
    return html_df

def combine_zlje_data(db_table, df):

    k_df = df.copy(deep=True)

    zlje_df = db_table.get_data_from_hdata(start_date=curr_day, end_date=curr_day)
    zlje_df = zlje_df.sort_values('stock_code')
    zlje_df = zlje_df.reset_index(drop=True)

    '''
    k_df['stock_code_new'] = k_df['stock_code']
    k_df['stock_code'] = k_df['stock_code'].apply(lambda x: x[2:])
    k_df = k_df.sort_values('stock_code')
    k_df = k_df.reset_index(drop=True)
    '''

    ret_df = pd.merge(k_df, zlje_df, how='inner', on=['stock_code', 'record_date'])
    #ret_df['stock_code'] = ret_df['stock_code_new']

    if 'zlje' in ret_df.columns:
        ret_df = ret_df.sort_values('zlje', ascending=0)
    if 'jmmoney' in ret_df.columns:
        ret_df = ret_df.sort_values('jmmoney', ascending=0)

    ret_df = ret_df.reset_index(drop=True)

    return ret_df

 
if __name__ == '__main__':

    script_name, para1 = check_input_parameter()
    print("%s, %d"%(script_name, int(para1)))
    today_date=datetime.datetime.now().date()
    nowdate=today_date-datetime.timedelta(int(para1))

    print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d"))) 
    #test
    lastdate=nowdate-datetime.timedelta(1)

    curr_day=nowdate.strftime("%Y-%m-%d")
    curr_day_w=nowdate.strftime("%Y-%m-%d-%w")
    last_day=lastdate.strftime("%Y-%m-%d")
    print("curr_day:%s, last_day:%s"%(curr_day, last_day))

    df = kline_data(stock_code=None, start_date=curr_day, end_date=curr_day, limit=0)

    #zig
    print('start zig')
    curr_dir=curr_day_w+'-zig'
    zig_df = df[(df.is_zig == 1) | (df.is_zig == 2) ]
    html_zig_df = convert_to_html_df(zig_df)
    if len(html_zig_df):
        html_zig_df = html_zig_df.sort_values('zig', ascending=1)
        generate_html(html_zig_df)
    else:
        print('#error, html_zig_df len < 1')

    #quad
    print('start quad')
    curr_dir=curr_day_w+'-quad'
    quad_df = df[(df.is_quad == 1) & (df.is_zig > 0)]
    html_quad_df =  convert_to_html_df(quad_df)
    if len(html_quad_df):
        html_quad_df = html_quad_df.sort_values('zig', ascending=1)
        generate_html(html_quad_df)
    else:
        print('#error, html_quad_df len < 1')


    #peach
    print('start peach')
    curr_dir=curr_day_w+'-peach'
    peach_df = df[(df.is_peach == 1) & (df.is_zig > 0)]
    html_peach_df = convert_to_html_df(peach_df)
    if len(html_peach_df):
        html_peach_df = html_peach_df.sort_values('zig', ascending=1)
        generate_html(html_peach_df)
    else:
        print('#error, html_peach_df len < 1')

    #5days
    print('start 5days')
    curr_dir=curr_day_w+'-5days'
    up_days_df = df[(df.is_up_days == 1) & (df.is_zig > 0)]
    html_up_days_df = convert_to_html_df(up_days_df)
    if len(html_up_days_df):
        html_up_days_df = html_up_days_df.sort_values('zig', ascending=1)
        generate_html(html_up_days_df)
    else:
        print('#error, html_5days_df len < 1')

    #macd
    print('start macd')
    curr_dir=curr_day_w+'-macd'
    macd_df = df[(df.is_macd == 1) & (df.is_zig > 0)]
    html_macd_df = convert_to_html_df(macd_df)
    if len(html_macd_df):
        html_macd_df = html_macd_df.sort_values('zig', ascending=1)
        generate_html(html_macd_df)
    else:
        print('#error, html_macd_df len < 1')


    #cup_tea
    print('start cup_tea')
    curr_dir=curr_day_w+'-cuptea'
    cuptea_df = df[(df.is_cup_tea == 1) & (df.is_zig > 0)]
    html_cuptea_df = convert_to_html_df(cuptea_df)
    if len(html_cuptea_df):
        html_cuptea_df = html_cuptea_df.sort_values('zig', ascending=1)
        generate_html(html_cuptea_df)
    else:
        print('#error, html_cuptea_df len < 1')

    #cup_tea
    print('start duck_head')
    curr_dir=curr_day_w+'-duckhead'
    duckhead_df = df[(df.is_duck_head == 1) & (df.is_zig > 0)]
    html_duckhead_df = convert_to_html_df(duckhead_df)
    if len(html_duckhead_df):
        html_duckhead_df = html_duckhead_df.sort_values('zig', ascending=1)
        generate_html(html_duckhead_df)
    else:
        print('#error, html_duckhead_df len < 1')

    #cross3line
    print('start cross3line')
    curr_dir=curr_day_w+'-cross3line'
    #cross3line_df = df[(df.is_cross3line == 1) & (df.is_zig > 0)]
    cross3line_df = df[(df.is_cross3line == 1)]
    html_cross3line_df = convert_to_html_df(cross3line_df)
    if len(html_cross3line_df):
        html_cross3line_df = html_cross3line_df.sort_values('zig', ascending=1)
        generate_html(html_cross3line_df)
    else:
        print('#error, html_cross3line_df len < 1')



    #basic
    print('start basic')
    curr_dir=curr_day_w
    basic_df = df[(df.is_2d3pct > 1) & (df.is_zig > 0)]
    html_basic_df = convert_to_html_df(basic_df)
    html_basic_df = html_basic_df.sort_values('zig', ascending=1)
    if len(html_basic_df):
        generate_html(html_basic_df)
    else:
        print('#error, html_basic_df len < 1')

    '''
    #zlje
    print('start zlje')
    curr_dir=curr_day_w+'-zlje'
    zlje_df = combine_zlje_data(zlje_table, df)
    if debug:
        print(zlje_df)
    html_zlje_df = convert_to_html_df(zlje_df)
    if len(html_zlje_df):
        generate_html(html_zlje_df)
    else:
        print('#error, html_zlje_df len < 1')
    '''
    
    #dragon
    print('start dragon')
    curr_dir=curr_day_w+'-dragon'
    dragon_df = combine_zlje_data(dragon_table, df)
    if debug:
        print(dragon_df)
    html_dragon_df = convert_to_html_df(dragon_df)
    if len(html_dragon_df):
        generate_html(html_dragon_df)
    else:
        print('#error, html_dragon_df len < 1')

 
    curr_dir=curr_day_w
    os.system('cp -rf ' + stock_data_dir +'/' + curr_dir + '*  /var/www/html/stock_data/' )

