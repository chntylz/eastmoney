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

from HData_eastmoney_jigou import *
jigou_table=HData_eastmoney_jigou("usr","usr")

from HData_eastmoney_dragon import *
dragon_table=HData_eastmoney_dragon("usr","usr")

from HData_eastmoney_holder import *
holder_table=HData_eastmoney_holder("usr","usr")

dict_industry={}


debug=0
debug=0

'''
today_date=datetime.datetime.now().date()
#test
nowdate=today_date
lastdate=nowdate-datetime.timedelta(1)

curr_day=nowdate.strftime("%Y-%m-%d")
curr_day_w=nowdate.strftime("%Y-%m-%d-%w")
last_day=lastdate.strftime("%Y-%m-%d")
print("curr_day:%s, last_day:%s"%(curr_day, last_day))
'''

def continue_handle_html_body_special(df_global, newfile, date):
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
        df_up = df_global[df_global['percent'] > 0.00]
        # 走平股数
        df_even = df_global[df_global['percent'] == 0.00]
        # 找出下跌的股票
        df_down = df_global[df_global['percent'] < 0.00]

        # 找出涨停的股票
        limit_up = df_global[df_global['percent'] >= 9.70]
        limit_down = df_global[df_global['percent'] <= -9.70]

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
    
   
def generate_html(df_global, df, stock_data_dir, curr_dir, curr_day):
    os.system('mkdir -p ' + stock_data_dir)
    os.system('mkdir -p ' + stock_data_dir +'/' + curr_dir)
    newfile='%s/%s'%(stock_data_dir, curr_dir + '/' + curr_dir + '.html')
    comm_handle_html_head(newfile, stock_data_dir, curr_day )
    continue_handle_html_body_special(df_global, newfile, curr_day)
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

def convert_to_html_df(df, curr_dir, curr_day):
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

def combine_zlje_data(db_table=None, first_df=None, second_df=None, curr_day=None):

    if first_df is None:
        return None


    df1 = first_df.copy(deep=True)
    ret_df = pd.DataFrame()

    if db_table is not None:
        print(curr_day)
        second_df = db_table.get_data_from_hdata(start_date=curr_day, end_date=curr_day)
        print(second_df)
        second_df = second_df.sort_values('stock_code')
        second_df = second_df.reset_index(drop=True)
        df2 = second_df
        ret_df = pd.merge(df1, df2, how='inner', on=['stock_code', 'record_date'])
    else:
        df2 = second_df
        ret_df = pd.merge(df1, df2, how='inner', on=['stock_code'])

    if 'zlje_x' in ret_df.columns:
        ret_df = ret_df.sort_values('zlje_x', ascending=False)

    if 'jmmoney' in ret_df.columns:
        ret_df = ret_df.sort_values('jmmoney', ascending=False)

    if 'holder_num_ratio' in ret_df.columns:
        ret_df = ret_df.sort_values('holder_num_ratio', ascending=True)

    #for jigou
    if 'delta_ratio' in ret_df.columns:
        ret_df = ret_df.sort_values('delta_ratio', ascending=False)


    ret_df = ret_df.reset_index(drop=True)

    return ret_df


def get_latest_jigou_data():
    jigou_df=  jigou_table.get_data_from_hdata()
    jigou_df = jigou_df.sort_values('record_date', ascending=False)
    jigou_df = jigou_df.reset_index(drop=True)
    jigou_df = jigou_df[jigou_df['record_date'] == jigou_df['record_date'][0]]
    jigou_df = jigou_df.sort_values('delta_ratio', ascending=False)
    jigou_df = jigou_df.reset_index(drop=True)
    return jigou_df


def get_holder_data(current_date):
    #nowdate = datetime.datetime.now().date()
    nowdate = current_date
    lastdate = nowdate - datetime.timedelta(365 * 3) #3 years ago
    print('nowdate:%s, lastdate:%s' % (nowdate, lastdate))
    holder_data  =  holder_table.get_data_from_hdata( start_date=lastdate.strftime("%Y%m%d"), \
            end_date=nowdate.strftime("%Y%m%d"))
    return holder_data

def handle_holder_data_continuous(holder_raw_df):
    
    df_tmp = holder_raw_df
    df_tmp=df_tmp[~df_tmp['holder_num'].isin([0])]  #delete the line which holder_num value is 0
    df_tmp = df_tmp.fillna(0)

    data_list = []
    group_by_stock_code_df=df_tmp.groupby('stock_code')

    for stock_code, group_df in group_by_stock_code_df:
        if debug:
            print(stock_code)
            print(group_df.head(1))

        i = holder_pct_i = 0
        group_df = group_df.sort_values('record_date', ascending=0)

        group_df=group_df.reset_index(drop=True) #reset index
        max_date=group_df.loc[0, 'record_date']
        holder_num = group_df.loc[0, 'holder_num']
        holder_num_ratio = group_df.loc[0, 'holder_num_ratio']

        length=len(group_df)
        for i in range(length-1):
            if group_df.loc[i]['holder_num_ratio'] <= 0:
                pass
            else:
                break

        #algorithm
        if(i > 1):
            i_holder_num = group_df.loc[i, 'holder_num']
            holder_pct_i =  (holder_num - i_holder_num ) * 100 / i_holder_num
            pass
            #if group_df.loc[0]['holder_num'] < group_df.loc[1]['holder_num']:  #decline, skip
            #   continue
        else:
            continue



        if debug:
            print(max_date, stock_code, holder_num, i, holder_num_ratio, holder_pct_i ) 
        
        data_list.append([max_date, stock_code, holder_num, i, holder_num_ratio, holder_pct_i ]) 

    data_column=['max_date', 'stock_code', 'holder_num', \
            'holer_cont_d', 'holder_num_ratio', 'holder_pct_i' ]

    ret_df = pd.DataFrame(data_list, columns=data_column)
    ret_df = ret_df.fillna(0)
    ret_df=ret_df.round(2)

    ret_df = ret_df.sort_values('holder_num_ratio', ascending=1)

    if debug:
        print(ret_df)

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
    
    stock_data_dir="stock_data"
    curr_dir=curr_day_w+'-zig'


    df_global  = kline_data(stock_code=None, start_date=curr_day, end_date=curr_day, limit=0)

    #delete 68???? kechuangban
    df = df_global[~(df_global.stock_code.str[:2] == '68')]

    '''
    #zlje
    print('#############################################################')
    print('start zlje')
    curr_dir=curr_day_w+'-zlje'
    zlje_df = combine_zlje_data(db_table=zlje_table, first_df=df, second_df=None)
    if debug:
        print(zlje_df)
    html_zlje_df = convert_to_html_df(zlje_df, curr_dir, curr_day)
    #html_zlje_df = html_zlje_df.sort_values('zig', ascending=1)
    if len(html_zlje_df):
        generate_html(df_global, html_zlje_df, stock_data_dir, curr_dir, curr_day)
    else:
        print('#error, html_zlje_df len < 1')
    '''
 


    df = k_df = df[df.is_zig > 0]

    #zig
    print('#############################################################')
    print('start zig')
    curr_dir=curr_day_w+'-zig'
    zig_df = df[(df.is_zig == 1) | (df.is_zig == 2) ]
    html_zig_df = convert_to_html_df(zig_df, curr_dir, curr_day)
    if len(html_zig_df):
        html_zig_df = html_zig_df.sort_values('zig', ascending=1)
        generate_html(df_global, html_zig_df, stock_data_dir, curr_dir, curr_day)
    else:
        print('#error, html_zig_df len < 1')

    #double_volume  and turnoverrate > 4
    print('#############################################################')
    print('start double volume')
    curr_dir=curr_day_w+'-volume'
    volume_df = df[(df.is_d_volume == 1) & (df.is_zig > 0) & (df.percent > 3.0) & (df.turnoverrate > 4.0)]
    html_volume_df =  convert_to_html_df(volume_df, curr_dir, curr_day)
    if len(html_volume_df):
        html_volume_df = html_volume_df.sort_values('zig', ascending=1)
        generate_html(df_global, html_volume_df, stock_data_dir, curr_dir, curr_day)
    else:
        print('#error, html_volume_df len < 1')



    #quad
    print('#############################################################')
    print('start quad')
    curr_dir=curr_day_w+'-quad'
    quad_df = df[(df.is_quad == 1) & (df.is_zig > 0)]
    html_quad_df =  convert_to_html_df(quad_df, curr_dir, curr_day)
    if len(html_quad_df):
        html_quad_df = html_quad_df.sort_values('zig', ascending=1)
        generate_html(df_global, html_quad_df, stock_data_dir, curr_dir, curr_day)
    else:
        print('#error, html_quad_df len < 1')


    #peach
    print('#############################################################')
    print('start peach')
    curr_dir=curr_day_w+'-peach'
    peach_df = df[(df.is_peach == 1) & (df.is_zig > 0)]
    html_peach_df = convert_to_html_df(peach_df, curr_dir, curr_day)
    if len(html_peach_df):
        html_peach_df = html_peach_df.sort_values('zig', ascending=1)
        generate_html(df_global, html_peach_df, stock_data_dir, curr_dir, curr_day)
    else:
        print('#error, html_peach_df len < 1')

    #5days
    print('#############################################################')
    print('start 5days')
    curr_dir=curr_day_w+'-5days'
    up_days_df = df[(df.is_up_days == 1) & (df.is_zig > 0)]
    html_up_days_df = convert_to_html_df(up_days_df, curr_dir, curr_day)
    if len(html_up_days_df):
        html_up_days_df = html_up_days_df.sort_values('zig', ascending=1)
        generate_html(df_global, html_up_days_df, stock_data_dir, curr_dir, curr_day)
    else:
        print('#error, html_5days_df len < 1')

    #macd
    print('#############################################################')
    print('start macd')
    curr_dir=curr_day_w+'-macd'
    macd_df = df[(df.is_macd == 1) & (df.is_zig > 0)]
    html_macd_df = convert_to_html_df(macd_df, curr_dir, curr_day)
    if len(html_macd_df):
        html_macd_df = html_macd_df.sort_values('zig', ascending=1)
        generate_html(df_global, html_macd_df, stock_data_dir, curr_dir, curr_day)
    else:
        print('#error, html_macd_df len < 1')


    #cup_tea
    print('#############################################################')
    print('start cup_tea')
    curr_dir=curr_day_w+'-cuptea'
    cuptea_df = df[(df.is_cup_tea == 1) & (df.is_zig > 0)]
    html_cuptea_df = convert_to_html_df(cuptea_df, curr_dir, curr_day)
    if len(html_cuptea_df):
        html_cuptea_df = html_cuptea_df.sort_values('zig', ascending=1)
        generate_html(df_global, html_cuptea_df, stock_data_dir, curr_dir, curr_day)
    else:
        print('#error, html_cuptea_df len < 1')

    #cup_tea
    print('#############################################################')
    print('start duck_head')
    curr_dir=curr_day_w+'-duckhead'
    duckhead_df = df[(df.is_duck_head == 1) & (df.is_zig > 0)]
    html_duckhead_df = convert_to_html_df(duckhead_df, curr_dir, curr_day)
    if len(html_duckhead_df):
        html_duckhead_df = html_duckhead_df.sort_values('zig', ascending=1)
        generate_html(df_global, html_duckhead_df, stock_data_dir, curr_dir, curr_day)
    else:
        print('#error, html_duckhead_df len < 1')

    #cross3line
    print('#############################################################')
    print('start cross3line')
    curr_dir=curr_day_w+'-cross3line'
    #cross3line_df = df[(df.is_cross3line == 1) & (df.is_zig >= 0)]
    cross3line_df = df[(df.is_cross3line == 1)]
    html_cross3line_df = convert_to_html_df(cross3line_df, curr_dir, curr_day)
    if len(html_cross3line_df):
        #html_cross3line_df = html_cross3line_df.sort_values('a_pct', ascending=0)
        html_cross3line_df = html_cross3line_df.sort_values('zig', ascending=1)
        generate_html(df_global, html_cross3line_df, stock_data_dir, curr_dir, curr_day)
    else:
        print('#error, html_cross3line_df len < 1')



    #basic
    print('#############################################################')
    print('start basic')
    curr_dir=curr_day_w
    basic_df = df[(df.is_2d3pct > 1) & (df.is_zig > 0)]
    html_basic_df = convert_to_html_df(basic_df, curr_dir, curr_day)
    html_basic_df = html_basic_df.sort_values('zig', ascending=1)
    if len(html_basic_df):
        generate_html(df_global, html_basic_df, stock_data_dir, curr_dir, curr_day)
    else:
        print('#error, html_basic_df len < 1')

    #zlje
    print('#############################################################')
    print('start zlje')
    curr_dir=curr_day_w+'-zlje'
    zlje_df = combine_zlje_data(db_table=zlje_table, first_df=k_df, second_df=None)
    if debug:
        print(zlje_df)
    html_zlje_df = convert_to_html_df(zlje_df, curr_dir, curr_day)
    #html_zlje_df = html_zlje_df.sort_values('zig', ascending=1)
    if len(html_zlje_df):
        generate_html(df_global, html_zlje_df, stock_data_dir, curr_dir, curr_day)
    else:
        print('#error, html_zlje_df len < 1')
     
    #jigou
    print('#############################################################')
    print('start jigou')
    curr_dir=curr_day_w+'-jigou'
    jigou_raw_df =  get_latest_jigou_data()

    jigou_df = combine_zlje_data(db_table=None, first_df=k_df, second_df=jigou_raw_df)
    if debug:
        print(jigou_df)
    html_jigou_df = convert_to_html_df(jigou_df, curr_dir, curr_day)
    if len(html_jigou_df):
        generate_html(df_global, html_jigou_df, stock_data_dir, curr_dir, curr_day)
    else:
        print('#error, html_jigou_df len < 1')

   
    
    #dragon
    print('#############################################################')
    print('start dragon')
    curr_dir=curr_day_w+'-dragon'
    dragon_df = combine_zlje_data(db_table=dragon_table, first_df=k_df, second_df=None)
    if debug:
        print(dragon_df)
    if len(dragon_df):
        dragon_df = dragon_df[(dragon_df.percent > 0.0)]
        html_dragon_df = convert_to_html_df(dragon_df, curr_dir, curr_day)
        html_dragon_df = html_dragon_df.sort_values('zig', ascending=1)
        if len(html_dragon_df):
            generate_html(df_global, html_dragon_df, stock_data_dir, curr_dir, curr_day)
        else:
            print('#error, html_dragon_df len < 1')

    #holder
    print('#############################################################')
    print('start holder')
    curr_dir=curr_day_w+'-holder'

    holder_raw_df = get_holder_data(nowdate)
    if debug:
        print('holder_raw_df', holder_raw_df)

    holder_df = handle_holder_data_continuous(holder_raw_df)
    if debug:
        print('holder_df', holder_df)
    
    holder_df = combine_zlje_data(db_table=None, first_df=k_df, second_df=holder_df)

    if debug:
        print('holder_df', holder_df)
    html_holder_df = convert_to_html_df(holder_df, curr_dir, curr_day)
    if len(html_holder_df):
        generate_html(df_global, html_holder_df, stock_data_dir, curr_dir, curr_day)
    else:
        print('#error, html_holder_df len < 1')

    print('#############################################################')

 
    curr_dir=curr_day_w
    os.system('cp -rf ' + stock_data_dir +'/' + curr_dir + '*  /var/www/html/stock_data/' )

