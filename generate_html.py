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

from HData_eastmoney_fina import *
fina_table=HData_eastmoney_fina("usr","usr")

from HData_eastmoney_dragon import *
dragon_table=HData_eastmoney_dragon("usr","usr")

from HData_eastmoney_holder import *
holder_table=HData_eastmoney_holder("usr","usr")

dict_industry={}


debug=0
debug=1
debug=0

'''
today_date=datetime.datetime.now().date()
#test
nowdate=today_date
lastdate=nowdate-datetime.timedelta(1)

curr_day=nowdate.strftime("%Y-%m-%d")
curr_day_w=nowdate.strftime("%Y-%m-%d-%w")
last_day=lastdate.strftime("%Y-%m-%d")
my_dbg("curr_day:%s, last_day:%s"%(curr_day, last_day))
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
        my_dbg('delta time= %s ' % (time.time() - t1))

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
        my_dbg(s_debug)
        f.write('%s\n'%(s_debug))

        s_debug=('<p> 涨停数量：%d 个</p>' % (limit_up.shape[0]))
        my_dbg(s_debug)
        f.write('%s\n'%(s_debug))

        s_debug=('<p> 跌停数量：%d 个</p>' % (limit_down.shape[0]))
        my_dbg(s_debug)
        f.write('%s\n'%(s_debug))

        f.write('<p>-----------------------------------我是分割线-----------------------------------</p>\n')
    
        f.write('<p  style="color:green;">绿色: 当天跳空高开2个点以上 </p>\n')
        cailian_focus_url='https://www.cls.cn/subject/1135'
        f.write('<a href="%s" target="_blank" style="color:red;">  焦点复盘  </a>\n'%  (cailian_focus_url))
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
    my_dbg(date)
    df = hdata.get_data_from_hdata(start_date=date, \
            end_date=date)
    if len(df):
        my_dbg(df.head(2))
    return df

def convert_to_html_df_multi(df, curr_dir, curr_day):
    dict_industry.clear()
    if len(df) < 1:
        my_dbg('#error, df data len < 1, return')
        return df
    df = df.reset_index(drop=True)
    if len(df):
        html_df = comm_generate_web_dataframe_multi(df, curr_dir, curr_day, dict_industry )
        if debug:
            my_dbg('dict_industry:%s' % dict_industry)
    else:
        html_df = df
        my_dbg('#error, html_df data len < 1, return None')
    return html_df

def convert_to_html_df(df, curr_dir, curr_day):
    dict_industry.clear()
    if len(df) < 1:
        my_dbg('#error, df data len < 1, return')
        return df
    df = df.reset_index(drop=True)
    if len(df):
        html_df = comm_generate_web_dataframe_new(df, curr_dir, curr_day, dict_industry )
        if debug:
            my_dbg('dict_industry:%s' % dict_industry)
    else:
        html_df = df
        my_dbg('#error, html_df data len < 1, return None')
    return html_df

def combine_zlje_data(db_table=None, first_df=None, second_df=None, curr_day=None):

    if first_df is None:
        return None


    df1 = first_df.copy(deep=True)
    ret_df = pd.DataFrame()

    if db_table is not None:
        my_dbg(curr_day)
        second_df = db_table.get_data_from_hdata(start_date=curr_day, end_date=curr_day)
        my_dbg(second_df)
        second_df = second_df.sort_values('stock_code')
        second_df = second_df.reset_index(drop=True)
        df2 = second_df
        ret_df = pd.merge(df1, df2, how='inner', on=['stock_code', 'record_date'])
    else:
        df2 = second_df
        ret_df = pd.merge(df1, df2, how='inner', on=['stock_code'])

    my_dbg(ret_df.columns)

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

    if debug:
        my_dbg(" get_latest_jigou_data ")
        my_dbg(jigou_df)

    group_by_stock_code_df=jigou_df.groupby('stock_code')
    jigou_df = jigou_df.sort_values('record_date', ascending=False)
    jigou_df = jigou_df.reset_index(drop=True)
    jigou_df = jigou_df[jigou_df.record_date == jigou_df.record_date[0]]
    #jigou_df = jigou_df[jigou_df['record_date'] == jigou_df['record_date'][0]]  #bug: some updated, some not
    jigou_df = jigou_df.sort_values('record_date', ascending=False)
    jigou_df = jigou_df.reset_index(drop=True)
    return jigou_df


def get_latest_fina_data():
    fina_df=  fina_table.get_data_from_hdata()

    if debug:
        my_dbg(" get_latest_fina_data ")
        my_dbg(fina_df)

    group_by_stock_code_df=fina_df.groupby('stock_code')
    fina_df = fina_df.sort_values('record_date', ascending=False)
    fina_df = fina_df.reset_index(drop=True)
    fina_df = fina_df[fina_df.record_date == fina_df.record_date[0]]
    #fina_df = fina_df[fina_df['record_date'] == fina_df['record_date'][0]]  #bug: some updated, some not
    fina_df = fina_df.sort_values('ystz', ascending=False)
    fina_df = fina_df.reset_index(drop=True)
    return fina_df


def get_holder_data(current_date):
    #nowdate = datetime.datetime.now().date()
    nowdate = current_date
    lastdate = nowdate - datetime.timedelta(365 * 3) #3 years ago
    my_dbg('nowdate:%s, lastdate:%s' % (nowdate, lastdate))
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
            my_dbg(stock_code)
            my_dbg(group_df.head(1))

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
            my_dbg(max_date, stock_code, holder_num, i, holder_num_ratio, holder_pct_i ) 
        
        data_list.append([max_date, stock_code, holder_num, i, holder_num_ratio, holder_pct_i ]) 

    data_column=['max_date', 'stock_code', 'holder_num', \
            'holer_cont_d', 'holder_num_ratio', 'holder_pct_i' ]

    ret_df = pd.DataFrame(data_list, columns=data_column)
    ret_df = ret_df.fillna(0)
    ret_df=ret_df.round(2)

    ret_df = ret_df.sort_values('holder_num_ratio', ascending=1)

    if debug:
        my_dbg(ret_df)

    return ret_df


 
if __name__ == '__main__':

    script_name, para1 = check_input_parameter()
    my_dbg("%s, %d"%(script_name, int(para1)))

    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


    today_date=datetime.datetime.now().date()
    nowdate=today_date-datetime.timedelta(int(para1))

    my_dbg("nowdate is %s"%(nowdate.strftime("%Y-%m-%d"))) 
    #test
    lastdate=nowdate-datetime.timedelta(1)


    curr_day=nowdate.strftime("%Y-%m-%d")
    curr_day_w=nowdate.strftime("%Y-%m-%d-%w")
    last_day=lastdate.strftime("%Y-%m-%d")
    my_dbg("curr_day:%s, last_day:%s"%(curr_day, last_day))
    
    stock_data_dir="stock_data"
    curr_dir=curr_day_w+'-zig'


    df_global  = kline_data(stock_code=None, start_date=curr_day, end_date=curr_day, limit=0)

    #delete 68???? kechuangban
    df = df_global[~(df_global.stock_code.str[:2] == '68')]

    '''
    #zlje
    my_dbg('#############################################################')
    my_dbg('start zlje')
    curr_dir=curr_day_w+'-zlje'
    zlje_df = combine_zlje_data(db_table=zlje_table, first_df=df, second_df=None)
    if debug:
        my_dbg(zlje_df)
    html_zlje_df = convert_to_html_df(zlje_df, curr_dir, curr_day)
    #html_zlje_df = html_zlje_df.sort_values('zig', ascending=1)
    if len(html_zlje_df):
        generate_html(df_global, html_zlje_df, stock_data_dir, curr_dir, curr_day)
    else:
        my_dbg('#error, html_zlje_df len < 1')
    '''
 

    top_size = 500

    #df = k_df = df[df.is_zig > 0]
    k_df = df

    #zig
    my_dbg('#############################################################')
    my_dbg('start zig')
    curr_dir=curr_day_w+'-zig'
    zig_df = df[(df.is_zig > 0) & (df.is_zig <= 10)]
    html_zig_df = convert_to_html_df(zig_df, curr_dir, curr_day)
    if len(html_zig_df):
        html_zig_df = html_zig_df.sort_values('zig', ascending=1)
        generate_html(df_global, html_zig_df, stock_data_dir, curr_dir, curr_day)
    else:
        my_dbg('#error, html_zig_df len < 1')

    #exit()

    #double_volume  and turnoverrate > 4
    my_dbg('#############################################################')
    my_dbg('start double volume')
    curr_dir=curr_day_w+'-volume'
    volume_df = df[(df.is_d_volume == 1) & (df.is_zig > 0) & (df.percent > 3.0) & (df.turnoverrate > 4.0)]
    html_volume_df =  convert_to_html_df(volume_df, curr_dir, curr_day)
    if len(html_volume_df):
        html_volume_df = html_volume_df.sort_values('zig', ascending=1)
        generate_html(df_global, html_volume_df, stock_data_dir, curr_dir, curr_day)
    else:
        my_dbg('#error, html_volume_df len < 1')



    #quad
    my_dbg('#############################################################')
    my_dbg('start quad')
    curr_dir=curr_day_w+'-quad'
    quad_df = df[(df.is_quad == 1) & (df.is_zig > 0)]
    html_quad_df =  convert_to_html_df(quad_df, curr_dir, curr_day)
    if len(html_quad_df):
        html_quad_df = html_quad_df.sort_values('zig', ascending=1)
        generate_html(df_global, html_quad_df, stock_data_dir, curr_dir, curr_day)
    else:
        my_dbg('#error, html_quad_df len < 1')


    #peach
    my_dbg('#############################################################')
    my_dbg('start peach')
    curr_dir=curr_day_w+'-peach'
    peach_df = df[(df.is_peach == 1) & (df.is_zig > 0)]
    html_peach_df = convert_to_html_df(peach_df, curr_dir, curr_day)
    if len(html_peach_df):
        html_peach_df = html_peach_df.sort_values('zig', ascending=1)
        generate_html(df_global, html_peach_df, stock_data_dir, curr_dir, curr_day)
    else:
        my_dbg('#error, html_peach_df len < 1')

    #5days
    my_dbg('#############################################################')
    my_dbg('start 5days')
    curr_dir=curr_day_w+'-5days'
    up_days_df = df[(df.is_up_days == 1) & (df.is_zig > 0)]
    html_up_days_df = convert_to_html_df(up_days_df, curr_dir, curr_day)
    if len(html_up_days_df):
        html_up_days_df = html_up_days_df.sort_values('zig', ascending=1)
        generate_html(df_global, html_up_days_df, stock_data_dir, curr_dir, curr_day)
    else:
        my_dbg('#error, html_5days_df len < 1')

    #macd
    my_dbg('#############################################################')
    my_dbg('start macd')
    curr_dir=curr_day_w+'-macd'
    macd_df = df[(df.is_macd == 1) & (df.is_zig > 0)]
    html_macd_df = convert_to_html_df(macd_df, curr_dir, curr_day)
    if len(html_macd_df):
        html_macd_df = html_macd_df.sort_values('zig', ascending=1)
        generate_html(df_global, html_macd_df, stock_data_dir, curr_dir, curr_day)
    else:
        my_dbg('#error, html_macd_df len < 1')


    #cup_tea
    my_dbg('#############################################################')
    my_dbg('start cup_tea')
    curr_dir=curr_day_w+'-cuptea'
    cuptea_df = df[(df.is_cup_tea == 1) & (df.is_zig > 0)]
    html_cuptea_df = convert_to_html_df(cuptea_df, curr_dir, curr_day)
    if len(html_cuptea_df):
        html_cuptea_df = html_cuptea_df.sort_values('zig', ascending=1)
        generate_html(df_global, html_cuptea_df, stock_data_dir, curr_dir, curr_day)
    else:
        my_dbg('#error, html_cuptea_df len < 1')

    #duck_head
    my_dbg('#############################################################')
    my_dbg('start duck_head')
    curr_dir=curr_day_w+'-duckhead'
    duckhead_df = df[(df.is_duck_head == 1) & (df.is_zig > 0)]
    html_duckhead_df = convert_to_html_df(duckhead_df, curr_dir, curr_day)
    if len(html_duckhead_df):
        html_duckhead_df = html_duckhead_df.sort_values('zig', ascending=1)
        generate_html(df_global, html_duckhead_df, stock_data_dir, curr_dir, curr_day)
    else:
        my_dbg('#error, html_duckhead_df len < 1')

    #cross3line
    my_dbg('#############################################################')
    my_dbg('start cross3line')
    curr_dir=curr_day_w+'-cross3line'
    cross3line_df = df[(df.is_cross3line == 1) & (df.is_zig >= 0)]
    #cross3line_df = df[(df.is_cross3line == 1)]
    html_cross3line_df = convert_to_html_df(cross3line_df, curr_dir, curr_day)
    if len(html_cross3line_df):
        #html_cross3line_df = html_cross3line_df.sort_values('a_pct', ascending=0)
        html_cross3line_df = html_cross3line_df.sort_values('zig', ascending=1)
        generate_html(df_global, html_cross3line_df, stock_data_dir, curr_dir, curr_day)
    else:
        my_dbg('#error, html_cross3line_df len < 1')



    #basic
    my_dbg('#############################################################')
    my_dbg('start basic')
    curr_dir=curr_day_w
    #basic_df = df[(df.is_2d3pct > 1) & (df.is_zig > 0)]
    basic_df = df[(df.percent > 5)]
    html_basic_df = convert_to_html_df(basic_df, curr_dir, curr_day)
    html_basic_df = html_basic_df.sort_values('a_pct', ascending=0)
    if len(html_basic_df):
        generate_html(df_global, html_basic_df, stock_data_dir, curr_dir, curr_day)
    else:
        my_dbg('#error, html_basic_df len < 1')


    #pe and pe_pct iwencai_pe
    my_dbg('#############################################################')
    my_dbg('start pe and pe_pct')
    curr_dir=curr_day_w + '-pepct'
    pe_df = df[(df.pe > 0 ) & (df.iwencai_pe < 30) & (df.is_zig > 0)]
    html_pe_df = convert_to_html_df(pe_df, curr_dir, curr_day)
    html_pe_df = html_pe_df.sort_values('pe', ascending=1)
    html_pe_df = html_pe_df.head(top_size)
    if len(html_pe_df):
        generate_html(df_global, html_pe_df, stock_data_dir, curr_dir, curr_day)
    else:
        my_dbg('#error, html_pe_df len < 1')

      
    #fina
    my_dbg('#############################################################')
    my_dbg('start fina')
    curr_dir=curr_day_w+'-fina'
    fina_raw_df =  get_latest_fina_data()

    fina_raw_df = fina_raw_df[(fina_raw_df.ystz > 50) & (fina_raw_df.sjltz > 50)]

    '''
    'basic_eps',  每股收益
    'basic_eps',  每股收益
    'total_operate_income', 营业总收入
    'parent_netprofit',     净利润
    'weightavg_roe',   净资产收益率
    'ystz',  营收入同比增长
    'sjltz', 净利润同比增长
    'bps',   每股净资产
    'mgjyxjje',每股经营现金流量
    'xsmll',    销售毛利率
    'yshz',     营收季度环比增长率
    'sjlhz',    净利润环比增长率
    '''

    fina_df = combine_zlje_data(db_table=None, first_df=k_df, second_df=fina_raw_df)
    if debug:
        my_dbg(fina_df.head(5))
    html_fina_df = convert_to_html_df(fina_df, curr_dir, curr_day)
    html_fina_df = html_fina_df.head(top_size)
    if len(html_fina_df):
        generate_html(df_global, html_fina_df, stock_data_dir, curr_dir, curr_day)
    else:
        my_dbg('#error, html_fina_df len < 1')


    #exit()

    #zlje
    my_dbg('#############################################################')
    my_dbg('start zlje')
    curr_dir=curr_day_w+'-zlje'
    zlje_df = combine_zlje_data(db_table=zlje_table, first_df=k_df, second_df=None)
    if debug:
        my_dbg(zlje_df.head(5))
    html_zlje_df = convert_to_html_df(zlje_df, curr_dir, curr_day)
    my_dbg(html_zlje_df.columns)
    html_zlje_df = html_zlje_df.sort_values('zlje', ascending=False)
    html_zlje_df = html_zlje_df.reset_index(drop=True)
    #html_zlje_df = html_zlje_df.sort_values('zig', ascending=1)
    html_zlje_df = html_zlje_df.head(top_size)
    if len(html_zlje_df):
        generate_html(df_global, html_zlje_df, stock_data_dir, curr_dir, curr_day)
    else:
        my_dbg('#error, html_zlje_df len < 1')
     
    #jigou
    my_dbg('#############################################################')
    my_dbg('start jigou')
    curr_dir=curr_day_w+'-jigou'
    jigou_raw_df =  get_latest_jigou_data()

    jigou_df = combine_zlje_data(db_table=None, first_df=k_df, second_df=jigou_raw_df)
    if debug:
        my_dbg(jigou_df.head(5))
    html_jigou_df = convert_to_html_df(jigou_df, curr_dir, curr_day)
    html_jigou_df = html_jigou_df.head(top_size)
    if len(html_jigou_df):
        generate_html(df_global, html_jigou_df, stock_data_dir, curr_dir, curr_day)
    else:
        my_dbg('#error, html_jigou_df len < 1')

   
    
    #dragon
    my_dbg('#############################################################')
    my_dbg('start dragon')
    curr_dir=curr_day_w+'-dragon'
    dragon_df = combine_zlje_data(db_table=dragon_table, first_df=k_df, second_df=None)
    if debug:
        my_dbg(dragon_df.head(5))
    if len(dragon_df):
        dragon_df = dragon_df[(dragon_df.percent > 0.0)]
        html_dragon_df = convert_to_html_df(dragon_df, curr_dir, curr_day)
        html_dragon_df = html_dragon_df.sort_values('zig', ascending=1)
        if len(html_dragon_df):
            generate_html(df_global, html_dragon_df, stock_data_dir, curr_dir, curr_day)
        else:
            my_dbg('#error, html_dragon_df len < 1')

    #holder
    my_dbg('#############################################################')
    my_dbg('start holder')
    curr_dir=curr_day_w+'-holder'

    holder_raw_df = get_holder_data(nowdate)
    if debug:
        my_dbg('holder_raw_df', holder_raw_df.head(5))

    holder_df = handle_holder_data_continuous(holder_raw_df)
    if debug:
        my_dbg('holder_df', holder_df.head(5))
    
    holder_df = combine_zlje_data(db_table=None, first_df=k_df, second_df=holder_df)

    if debug:
        my_dbg('holder_df', holder_df.head(5))
    html_holder_df = convert_to_html_df(holder_df, curr_dir, curr_day)
    html_holder_df = html_holder_df.head(top_size)
    if len(html_holder_df):
        generate_html(df_global, html_holder_df, stock_data_dir, curr_dir, curr_day)
    else:
        my_dbg('#error, html_holder_df len < 1')

    my_dbg('#############################################################')

 
    curr_dir=curr_day_w
    os.system('cp -rf ' + stock_data_dir +'/' + curr_dir + '*  /var/www/html/stock_data/' )

    t2 = time.time()
    my_dbg("%s: t1:%s, t2:%s, delta=%s"%(script_name, t1, t2, t2-t1))

