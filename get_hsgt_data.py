#!/usr/bin/env python
#coding:utf-8
import os,sys,gzip
import json

import sys

from file_interface import *

import pandas as pd
import json
import requests
import re
from bs4 import BeautifulSoup

import time, datetime
import os
import random

from comm_selenium import *


import psycopg2 #使用的是PostgreSQL数据库

from HData_hsgt import *
from HData_eastmoney_day import *

from HData_xq_fina import *
from HData_xq_holder import *
from HData_eastmoney_fina import *
from HData_eastmoney_holder import *
from HData_eastmoney_jigou import *

from HData_eastmoney_zlje import *
from HData_eastmoney_zlje_3 import *
from HData_eastmoney_zlje_5 import *
from HData_eastmoney_zlje_10 import *

from get_data_from_db import *

import  datetime


#funcat
from funcat import *
from funcat.data.aaron_backend import AaronDataBackend
set_data_backend(AaronDataBackend())


debug=0
debug=1
debug=0

#file_path='/home/ubuntu/tmp/a_stock/hkexnews_scrapy/hkexnews_scrapy/json/20190823.json.gz'
hdata_day=HData_eastmoney_day("usr","usr")

hdata_hsgt=HData_hsgt("usr","usr")
#hdata_fina=HData_xq_fina("usr","usr")
hdata_fina=HData_eastmoney_fina("usr","usr")
hdata_holder=HData_eastmoney_holder("usr","usr")
#hdata_holder=HData_xq_holder("usr","usr")
hdata_jigou=HData_eastmoney_jigou("usr","usr")


def hsgt_get_hk(url):
    browser = get_broswer()
    browser.get(url)
    html_doc=browser.page_source
    soup = BeautifulSoup(html_doc, 'html.parser')

    #get hsgt date
    h2s=soup.find_all('h2')  
    cur_date=h2s[1].text.replace('/', '-')  #the second h2
    year_pos=cur_date.rfind('20')
    cur_date=cur_date[year_pos:year_pos+10]


    #get hsgt data
    tbodys = soup.find_all('tbody')
    tbody = tbodys[1]  #the second tbody

    code = ''
    stock_cname=''
    stock_code=''
    share_holding=''
    percent=''
    date=''
    data_list = []
    for tr_idx, tr in enumerate(tbody.find_all('tr')):
        tds = tr.find_all('td')
        length = len(tds)
        if debug:
            print('-------------------------------- tr_idx=%d' % tr_idx)
        for td_idx, td in enumerate(tds):
            if debug:
                #print('td_idx %d' % td_idx)
                pass
            final_td=td
            for div_idx, div in enumerate(td.find_all('div')):
                final_div=div
                #print('div_idx %d' % div_idx)
                if td_idx == 0:
                    if div_idx == 0:
                        if debug:
                            print(div.contents[0].text)
                    if div_idx == 1:
                        if debug:
                            print(div.contents[0].text)
                        code = div.contents[0].text
                if td_idx == 1:
                    if div_idx == 0:
                        if debug:
                            print(div.contents[0].text)
                    if div_idx == 1:
                        if debug:
                            print(div.contents[0].text)
                        stock_cname=div.contents[0].text
                        sharp_pos=stock_cname.rfind('#')
                        if stock_cname[sharp_pos+1] == ' ':
                            stock_code=str(stock_cname[sharp_pos+2:sharp_pos+8])
                        else:
                            stock_code=str(stock_cname[sharp_pos+1:sharp_pos+7])
                if td_idx == 2:
                    if div_idx == 0:
                        if debug:
                            print(div.contents[0].text)
                    if div_idx == 1:
                        if debug:
                            print(div.contents[0].text)
                        share_holding=div.contents[0].text.replace(',','')
                if td_idx == 3:
                    if div_idx == 0:
                        if debug:
                            print(div.contents[0].text)
                    if div_idx == 1:
                        if debug:
                            print(div.contents[0].text)
                        percent=div.contents[0].text.replace('%','')
        data_list.append([cur_date, stock_code, share_holding, percent])
        print(code, stock_cname, cur_date, stock_code, share_holding, percent)


    data_column=['record_date', 'stock_code', 'share_holding', 'percent']
    df = pd.DataFrame(data_list, columns=data_column)

    return df



def hsgt_get_hk_sz():
    url = 'https://www3.hkexnews.hk/sdw/search/mutualmarket.aspx?t=sz'
    df =  hsgt_get_hk(url)
    return df


def hsgt_get_hk_sh():
    url = 'https://www3.hkexnews.hk/sdw/search/mutualmarket.aspx?t=sh'
    df =  hsgt_get_hk(url)
    return df


def hsgt_get_sh_sz():
    list_tmp=[]

    df = pd.DataFrame()
    sz_df = hsgt_get_hk_sz()    
    sh_df = hsgt_get_hk_sh()
    df = pd.concat([sh_df, sz_df])
    print('len(df)=%d '% len(df) )


    ####get zlje start####
    zlje_df   = get_zlje_data_from_db(url='url'     )
    zlje_3_df = get_zlje_data_from_db(url='url_3'   )
    zlje_5_df = get_zlje_data_from_db(url='url_5'   )
    zlje_10_df = get_zlje_data_from_db(url='url_10' )
    ####get zlje end####


    #['20240930', '301611', '436017', '0.10']
    for idx, d in df.iterrows():
        #get date
        hsgt_date=d[0]
        hsgt_code=str(d[1])
        
        #ETF, skip
        if hsgt_code[0] == '5' or hsgt_code[0] == '1':
            print('ETF %s skip' % hsgt_code)
            continue
        
        if debug:
            print(idx) # 输出每行的索引值
            print(d[0], d[1],d[2],d[3])

        #### zlje start ####
        zlje = get_zlje(zlje_df,     hsgt_code, curr_date=hsgt_date)
        if zlje:
            zlje = float(zlje[: zlje.find('<br>')])

        zlje_3 = get_zlje(zlje_3_df,   hsgt_code, curr_date=hsgt_date)
        if zlje_3:
            zlje_3 = float(zlje_3[: zlje_3.find('<br>')])

        zlje_5 = get_zlje(zlje_5_df,   hsgt_code, curr_date=hsgt_date)
        if zlje_5:
            zlje_5 = float(zlje_5[: zlje_5.find('<br>')])

        zlje_10 = get_zlje(zlje_10_df,  hsgt_code, curr_date=hsgt_date)
        if zlje_10:
            zlje_10 = float(zlje_10[: zlje_10.find('<br>')])
        #### zlje end ####

        #### fina start ####
        #eastmoney fina
        fina_df = hdata_fina.get_data_from_hdata(stock_code = hsgt_code)  # eastmoney
        fina_df = fina_df.sort_values('record_date', ascending=0)
        fina_df = fina_df.reset_index(drop=True)
        
        fina_date = hsgt_date
        op_yoy = net_yoy = 0
        if len(fina_df):
            fina_date = fina_df['record_date'][0]
            op_yoy = fina_df['ystz'][0]
            net_yoy = fina_df['sjltz'][0]

            if debug:
                print(fina_df.head(1))
 
        #### fina end ####
 
        #### holder start ####
        # eastmoney holder
        holder_df = hdata_holder.get_data_from_hdata(stock_code = hsgt_code)
        holder_df = holder_df .sort_values('record_date', ascending=0)
        holder_df = holder_df .reset_index(drop=True)
        h0 = h1 = h2 = h_num = h_avg = delta_price  = 0
        if len(holder_df) > 0:
            h0 = round(holder_df['holder_num_ratio'][0], 2)
            h_num = round(holder_df['holder_num'][0]/10000, 2)
            h_avg = round(holder_df['avg_hold_num'][0]/10000,2)
            delta_price  = round(holder_df['interval_chrate'][0],2)
        if len(holder_df) > 1:
            h1 = round(holder_df['holder_num_ratio'][1], 2)
        if len(holder_df) > 2:
            h2 = round(holder_df['holder_num_ratio'][2], 2)

        '''
        h_chg = str(h0) + ' ' + str(h1) + ' ' + str(h2) +' '\
                + str(h_avg) + ' '+ str(delta_price )
        '''
        h_chg = '<br>'+ str(h0) + '%' +' ' + str(h1) + '%' + ' ' + str(h2) + '%' + ' </br>'\
                + 't' + str(h_num) + ' ' + 'a' + str(h_avg) + ' '+ 'd' + str(delta_price)


        #### eastmoney  jigou data ####
        jigou_df = hdata_jigou.get_data_from_hdata(stock_code = hsgt_code)
        jigou_df = jigou_df.sort_values('record_date', ascending=False)
        jigou_df = jigou_df.reset_index(drop=True)
        float_ratio = delta_ratio = 0
        if len(jigou_df) > 0:
            float_ratio = jigou_df['freeshares_ratio'][0]
            delta_ratio = jigou_df['delta_ratio'][0]
        jigou = ''
        if delta_ratio < 0:
            jigou = str(float_ratio) + str(delta_ratio)
        else:
            jigou = str(float_ratio) + '+' + str(delta_ratio)

        #get share_holding
        hsgt_holding=float(d[2])
        
        #get percent
        hsgt_percent=float(d[3])

        #get open, close, high, low, and volume
        day_df=hdata_day.get_data_from_hdata(stock_code=hsgt_code, 
                start_date=hsgt_date, end_date=hsgt_date)
        if debug:
            print("hsgt_date:%s, hsgt_code:%s, hsgt_holding:%s, hsgt_percent:%s "% \
                 (hsgt_date, hsgt_code, hsgt_holding, hsgt_percent))
            print('print day_df: %s'% day_df.head(1))

        #for date format transfer
        nowdate = datetime.datetime.strptime(hsgt_date, '%Y-%m-%d').date()
        if debug:
            print("%s %s" % (nowdate, type(nowdate)))

        retry = 0
        while True:
            if debug:
                print('retry=%d' % retry)

            if len(day_df) > 0:
                break;
        
            if retry > 10:
                break

            retry = retry + 1
            nowdate=nowdate-datetime.timedelta(retry)
            tmp_date=nowdate.strftime("%Y-%m-%d")  #don't use hsgt_date when this stock was suppended
            day_df=hdata_day.get_data_from_hdata(stock_code=hsgt_code, 
                start_date=tmp_date, end_date=tmp_date)

        if len(day_df) > 0:
            day_dict=day_df.to_dict()

            hsgt_cname=day_dict['stock_name'][0]

            hsgt_open=day_dict['open'][0]
            hsgt_close=day_dict['close'][0]
            hsgt_high=day_dict['high'][0]
            hsgt_low=day_dict['low'][0]
            hsgt_volume=day_dict['volume'][0]
            hsgt_mkt_cap=day_dict['mkt_cap'][0]
            hsgt_is_zig=int(day_dict['is_zig'][0])
            hsgt_is_quad=int(day_dict['is_quad'][0])
            hsgt_is_peach=int(day_dict['is_peach'][0])
            hsgt_is_cross3line = int(day_dict['is_cross3line'][0])
            
            list_tmp.append([hsgt_date, hsgt_code, hsgt_cname, hsgt_holding, hsgt_percent, \
                    hsgt_open, hsgt_close, hsgt_high, hsgt_low, hsgt_volume, hsgt_mkt_cap, \
                    hsgt_is_zig, hsgt_is_quad, hsgt_is_peach, hsgt_is_cross3line, \
                    op_yoy, net_yoy,\
                    zlje, zlje_3, zlje_5, zlje_10,\
                    h0, h1, h2, jigou ])
        else:
            print('############## code:%s, date=%s, daily data is null!!! ##############' % (hsgt_code, hsgt_date))

    if debug:
        print(list_tmp)

    dataframe_cols = ['record_date', 'stock_code','hsgt_cname', 'share_holding', 'hk_pct', \
            'open', 'close', 'high', 'low', 'volume', 'total_mv', \
            'is_zig', 'is_quad', 'is_peach', 'is_cross3line', \
            'op_yoy', 'net_yoy',\
            'zlje', 'zlje_3', 'zlje_5', 'zlje_10', \
            'holder_0', 'holder_1', 'holder_2', 'jigou' ]

    df = pd.DataFrame(list_tmp, columns=dataframe_cols)
   
    '''
    index =  df["record_date"]

    df = pd.DataFrame(list_tmp, index=index, columns=dataframe_cols)
    del df["record_date"]
    '''
    
    if debug:
        print(df.head(10))

    df.to_csv('./csv/' + datetime.datetime.now().date().strftime('%Y-%m-%d') + '_hsgt.csv', encoding='utf-8')

    #hdata_hsgt.insert_optimize_stock_hdatadate(df)
    hdata_hsgt.copy_from_stringio(df)
    return

def hsgt_get_day_item_from_json(file_path):
    line_num=0
    list_tmp=[]
    line_count=len(gzip.open(file_path).readlines())

   
    ####get zlje start####
    zlje_df   = get_zlje_data_from_db(url='url'     )
    zlje_3_df = get_zlje_data_from_db(url='url_3'   )
    zlje_5_df = get_zlje_data_from_db(url='url_5'   )
    zlje_10_df = get_zlje_data_from_db(url='url_10' )
    ####get zlje end####

    for line in gzip.open(file_path):
        line_num = line_num + 1   
        if line_num == 1 or line_num == line_count:
            continue
        if debug:
            print("line_num:%d, %s"%(line_num, line))
        
        #{"date": "2019-08-23", "stock_ename": "SHENZHEN MINDRAY BIO-MEDICAL ELECTRONICS CO., LTD. (A #300760)", "code": "77760", "share_holding": "19601172", "percent": "1.61%"}
        line=str(line)
        begin=line.rfind('{')
        end=line.rfind('}')
        s=line[begin:end+1]
        s=s.replace('\\"', '_')
        s=s.replace('\'', '-')
        s=s.replace('\\', '')

        if debug:
            print('s---->%s'%(s))
        d=json.loads(s, strict=False)
        line=d

        #get date
        hsgt_date=line['date']

        #get stock_ename
        hsgt_ename=line['stock_ename']
        position=hsgt_ename.rfind('#')
        hsgt_code=hsgt_ename[position+1: -1]
        if len(hsgt_code) < 6:
            print('error data! hsgt_code:%s'%(hsgt_code))
            hsgt_code='0'.join(hsgt_code)

        #### zlje start ####
        zlje = get_zlje(zlje_df,     hsgt_code, curr_date=hsgt_date)
        if zlje:
            zlje = float(zlje[: zlje.find('<br>')])

        zlje_3 = get_zlje(zlje_3_df,   hsgt_code, curr_date=hsgt_date)
        if zlje_3:
            zlje_3 = float(zlje_3[: zlje_3.find('<br>')])

        zlje_5 = get_zlje(zlje_5_df,   hsgt_code, curr_date=hsgt_date)
        if zlje_5:
            zlje_5 = float(zlje_5[: zlje_5.find('<br>')])

        zlje_10 = get_zlje(zlje_10_df,  hsgt_code, curr_date=hsgt_date)
        if zlje_10:
            zlje_10 = float(zlje_10[: zlje_10.find('<br>')])
        #### zlje end ####


        #### fina start ####
        if hsgt_code[0:1] == '6':
            hsgt_code_new= 'SH' + hsgt_code 
        else:
            hsgt_code_new= 'SZ' + hsgt_code 

        #### fina start ####
        fina_df = hdata_fina.get_data_from_hdata(stock_code = hsgt_code_new)
        
        fina_df = fina_df.sort_values('record_date', ascending=0)
        fina_df = fina_df.reset_index(drop=True)

        op_yoy = net_yoy = 0
        if len(fina_df):
            op_yoy = fina_df['operating_income_yoy'][0]
            net_yoy = fina_df['net_profit_atsopc_yoy'][0]
            
            if debug:
                print(fina_df)
        #### fina end ####
 
 

        '''
        #eastmoney holder data
        holder_df = hdata_holder.get_data_from_hdata(stock_code = hsgt_code)
        holder_df = holder_df.sort_values('record_date', ascending=0)
        holder_df = holder_df.reset_index(drop=True)
        h0 = h1 = h2 = 0
        if len(holder_df) > 0:
            h0 = holder_df['holder_num_ratio'][0]
        if len(holder_df) > 1:
            h1 = holder_df['holder_num_ratio'][1]
        if len(holder_df) > 2:
            h2 = holder_df['holder_num_ratio'][2]
        '''

        #### holder start ####
        #xueqiu holder data
        holder_df = hdata_holder.get_data_from_hdata(stock_code = hsgt_code)
        holder_df = holder_df.sort_values('record_date', ascending=0)
        holder_df = holder_df.reset_index(drop=True)
        h0 = h1 = h2 = 0
        if len(holder_df) > 0:
            h0 = holder_df['chg'][0]
        if len(holder_df) > 1:
            h1 = holder_df['chg'][1]
        if len(holder_df) > 2:
            h2 = holder_df['chg'][2]
        #### holder start ####

        #### holder jigou ####
        jigou_df = hdata_jigou.get_data_from_hdata(stock_code = hsgt_code)
        jigou_df = jigou_df.sort_values('record_date', ascending=False)
        jigou_df = jigou_df.reset_index(drop=True)
        float_ratio = delta_ratio = 0
        if len(jigou_df) > 0:
            float_ratio = jigou_df['freeshares_ratio'][0]
            delta_ratio = jigou_df['delta_ratio'][0]
        
        jigou = ''
        if delta_ratio < 0:
            jigou = str(float_ratio) + str(delta_ratio)
        else:
            jigou = str(float_ratio) + '+' + str(delta_ratio)



        
        #get stock_cname
        hsgt_cname=symbol(hsgt_code)
        pos_s=hsgt_cname.rfind('[')
        pos_e=hsgt_cname.rfind(']')
        hsgt_cname=hsgt_cname[pos_s+1: pos_e]
        #print(hsgt_cname)

        #get share_holding
        hsgt_holding=float(line['share_holding'])
        
        #get percent
        hsgt_percent=line['percent']
        position=hsgt_ename.rfind('%')
        hsgt_percent=float(hsgt_percent[:position])

        if hsgt_code[0:1] == '6':
            stock_code_new= 'SH' + hsgt_code
        else:
            stock_code_new= 'SZ' + hsgt_code
        stock_code_new= hsgt_code

        #get open, close, high, low, and volume
        day_df=hdata_day.get_data_from_hdata(stock_code=stock_code_new, 
                start_date=hsgt_date, end_date=hsgt_date)
        if debug:
            print("line_num:%d, hsgt_date:%s, hsgt_code:%s, hsgt_holding:%s, hsgt_percent:%s,\
                    hsgt_ename:%s, hsgt_cname:%s"% \
                 (line_num, hsgt_date, hsgt_code, hsgt_holding, hsgt_percent,\
                     hsgt_ename, hsgt_cname))
            print('print day_df:')
            print(day_df)
        if len(day_df) > 0:
            day_dict=day_df.to_dict()

            hsgt_open=day_dict['open'][0]
            hsgt_close=day_dict['close'][0]
            hsgt_high=day_dict['high'][0]
            hsgt_low=day_dict['low'][0]
            hsgt_volume=day_dict['volume'][0]
            hsgt_mkt_cap=day_dict['mkt_cap'][0]
            hsgt_is_zig=int(day_dict['is_zig'][0])
            hsgt_is_quad=int(day_dict['is_quad'][0])
            hsgt_is_peach=int(day_dict['is_peach'][0])
            hsgt_is_cross3line = int(day_dict['is_cross3line'][0])
            



            list_tmp.append([hsgt_date, hsgt_code, hsgt_cname, hsgt_holding, hsgt_percent, \
                    hsgt_open, hsgt_close, hsgt_high, hsgt_low, hsgt_volume, hsgt_mkt_cap, \
                    hsgt_is_zig, hsgt_is_quad, hsgt_is_peach, hsgt_is_cross3line, \
                    op_yoy, net_yoy,\
                    zlje, zlje_3, zlje_5, zlje_10,\
                    h0, h1, h2, jigou ])
        else:
            print('############## code:%s, name=%s, daily data is null!!! ##############' % (hsgt_code, hsgt_cname))

    if debug:
        print(list_tmp)

    dataframe_cols = ['record_date', 'stock_code','hsgt_cname', 'share_holding', 'hk_pct', \
            'open', 'close', 'high', 'low', 'volume', 'total_mv', \
            'is_zig', 'is_quad', 'is_peach', 'is_cross3line', \
            'op_yoy', 'net_yoy',\
            'zlje', 'zlje_3', 'zlje_5', 'zlje_10', \
            'holder_0', 'holder_1', 'holder_2', 'jigou' ]

    df = pd.DataFrame(list_tmp, columns=dataframe_cols)
    
    index =  df["record_date"]

    df = pd.DataFrame(list_tmp, index=index, columns=dataframe_cols)
    del df["record_date"]
    
    if debug:
        print(df)

    df.to_csv('./csv/' + datetime.datetime.now().date().strftime('%Y-%m-%d') + '_hsgt_debug.csv', encoding='utf-8')

    hdata_hsgt.insert_optimize_stock_hdatadate(df)


    return


def hsgt_get_all_data():

    latest_date = hdata_hsgt.db_get_latest_date_of_stock()
    if debug:
        print(type(latest_date))


    if latest_date:
        latest_date = str(latest_date)
        latest_date = latest_date.replace('-', '')
    else:
        latest_date='20180101'

    print('latest_date:%s'%(latest_date))

    curr_dir=cur_file_dir()#获取当前.py脚本文件的文件路径
    json_dir=curr_dir+'/hkexnews_scrapy/hkexnews_scrapy/json'
    #json_dir='/home/aaron/eastmoney/hkexnews_scrapy/hkexnews_scrapy/json'
    all_files=getAllFiles(json_dir)
    #if debug:
    if 0:
        print(all_files)

    for tmp_file in all_files:
        file_size=get_FileSize(tmp_file)
        if debug:
            print("%s size is:%f"%(tmp_file, file_size))
       
        if file_size < 1 :
            if debug:
                print(tmp_file)
            continue

        position=tmp_file.rfind('.json.gz')
        curr_date=tmp_file[position-8:position]
        result=compare_time(curr_date, latest_date)
        if debug:
            print("result=%s, curr_date %s < latest_date:%s"%(result, curr_date, latest_date))

        if result is False: 
            #insert data into hdata_hsgt_table
            hsgt_get_day_item_from_json(tmp_file)

    return



def check_table():
    table_exist = hdata_hsgt.table_is_exist() 
    print('table_exist=%d' % table_exist)
    if table_exist:
        #hdata_hsgt.db_hdata_date_create()
        print(' hdata_hsgt table already exist, recreate')
    else:
        hdata_hsgt.db_hdata_date_create()
        print(' hdata_hsgt table not exist, create')



if __name__ == '__main__':

    check_table()
    hsgt_get_sh_sz()
    
    #old method
    #hsgt_get_all_data()

