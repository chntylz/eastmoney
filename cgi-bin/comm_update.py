#!/usr/bin/env python3
# -*- coding: utf-8 -*- 
import os,sys,time, datetime
import cgi

import psycopg2 
import numpy as np
import pandas as pd

from HData_hsgt import *
from HData_eastmoney_fina import *
from HData_eastmoney_holder import *



from HData_xq_fina import *
from HData_xq_holder import *
from HData_xq_simple_day import * 




hdata_hsgt=HData_hsgt("usr","usr")

#eastmoney
hdata_fina=HData_eastmoney_fina("usr","usr")
hdata_holder=HData_eastmoney_holder("usr","usr")

#xueqiu
'''
hdata_fina=HData_xq_fina("usr","usr")
hdata_holder=HData_xq_holder("usr","usr")
'''


hdata_xq_simple = HData_xq_simple_day('usr', 'usr')

from comm_generate_web_html import *

from HData_eastmoney_zlje import *
from HData_eastmoney_zlje_3 import *
from HData_eastmoney_zlje_5 import *
from HData_eastmoney_zlje_10 import *

from get_daily_zlje import *


debug=0


nowdate=datetime.datetime.now().date()
str_date= nowdate.strftime("%Y-%m-%d")

def is_work_time():
    ret = False
    s_time = datetime.datetime.strptime(str(datetime.datetime.now().date())+'9:25', '%Y-%m-%d%H:%M')
    e_time = datetime.datetime.strptime(str(datetime.datetime.now().date())+'15:10', '%Y-%m-%d%H:%M')

    n_time = datetime.datetime.now()

    if n_time > s_time and n_time < e_time:
        ret = True
    else:
        ret = False

    return ret

 
from HData_eastmoney_fund import *
hdata_fund=HData_eastmoney_fund("usr","usr")


def get_fund_info(stock_code, fund_df):

    fu_num=fu_delta=fu_value=fu_ratio=fu_chg_share=fu_chg_ratio=0
    ret_fund_info=''
    df = fund_df
    tmp_df = df[df['stock_code'] == stock_code]
    if len(tmp_df):
        tmp_df = tmp_df.reset_index(drop=True)
        fu_num = tmp_df.loc[0]['hold_num'] 
        fu_delta = tmp_df.loc[0]['delta_hold'] 
        fu_value = round(tmp_df.loc[0]['hold_value']/10000/10000,2 )
        fu_ratio =  round(tmp_df.loc[0]['freeshares_ratio'], 2)
        fu_chg_share =  round(tmp_df.loc[0]['holdcha_num']/10000/10000, 2)
        fu_chg_ratio =  round(tmp_df.loc[0]['holdcha_ratio'] , 2)
        
        ret_fund_info = str(int(fu_num))
        if fu_delta > 0:
            ret_fund_info += '+' 
            ret_fund_info += str(int(fu_delta))
        else:
            ret_fund_info += str(int(fu_delta))

        
        ret_fund_info += ':' 
        ret_fund_info += str(fu_value)

        ret_fund_info += '_' 
        ret_fund_info += str(fu_ratio)

        ret_fund_info += '<br>' 

        if fu_chg_share > 0:
            ret_fund_info += str(fu_chg_share)
        else:
            ret_fund_info += str(fu_chg_share)


        if fu_chg_ratio > 0:
            ret_fund_info += '+' 
            ret_fund_info += str(fu_chg_ratio)
        else:
            ret_fund_info += str(fu_chg_ratio)

        ret_fund_info += '</br>' 
    return ret_fund_info
        


   



def get_fund_data(date=None):
    nowdate = date
    if date is None: 
        nowdate = datetime.datetime.now().date()    
    lastdate = nowdate - datetime.timedelta(365 * 1) #1 years ago

    #get the latest date from guizhoumaotai
    maxdate = hdata_fund.db_get_maxdate_of_stock('600519')

    #print('nowdate:%s, lastdate:%s' % (nowdate, lastdate))
    df = hdata_fund.get_data_from_hdata( start_date=lastdate.strftime("%Y-%m-%d"), \
            end_date=nowdate.strftime("%Y-%m-%d"))
    
    df = df.sort_values('record_date', ascending=0)
    df = df.reset_index(drop=True)

    df['hold_last']=df.groupby('stock_code')['hold_num'].shift((-1))
    #insert the fourth column
    df.insert(6, 'delta_hold', df['hold_num'] - df['hold_last'])
    df = df.fillna(0)

    del df['hold_last']

    #df = df.sort_values('delta_quantity', ascending=0)
    df = df.sort_values('hold_num', ascending=0)
    df = df.reset_index(drop=True)
    df = df[df['record_date'] == maxdate.strftime("%Y-%m-%d")]
    df = df.reset_index(drop=True)
    return df



def get_stock_info(file_name):
    stock_list = []
    with open(file_name) as f:
        for line in f:
            if debug:
                print (line, len(line))
            if len(line) < 6 or '#' in line:
                if debug:
                    print('unvalid line data, skip!')
                continue
            space_pos = line.rfind(' ')
            stock_list.append([line[0:space_pos], line[space_pos+1: -1]])

    return stock_list


def show_realdata(file_name):
    #my_list=['300750','300552', '000401', '300458','300014', '601958', '601117', '600588', '002230']
    #my_list_cn=['ningdeshidai','wanjikeji', 'jidongshuini', 'quanzhikeji', 'yiweilineng', 'jinmugufen', 'zhongguohuaxue', 'yongyouwangluo', 'kedaxunfei']

    data_list = []

    fund_df = get_fund_data()

    #file_name = 'my_optional.txt'
    my_list = get_stock_info(file_name)
    if debug:
        print(my_list)
    length=len(my_list)
   
    
    ####get zlje start####
    zlje_df   = get_zlje_data_from_db(url='url',     curr_date=str_date)
    zlje_3_df = get_zlje_data_from_db(url='url_3',   curr_date=str_date)
    zlje_5_df = get_zlje_data_from_db(url='url_5',   curr_date=str_date)
    zlje_10_df = get_zlje_data_from_db(url='url_10', curr_date=str_date)
    ####get zlje end####

    xq_simple_df = hdata_xq_simple.get_data_from_hdata( start_date=nowdate.strftime("%Y-%m-%d"), \
            end_date=nowdate.strftime("%Y-%m-%d"))
    i = 0
    eastmoney_begin = 0
    for i in range(length):
        new_date        = str_date
        new_date        = str_date[2:]
        stock_code_new  = my_list[i][0]
        new_code        = stock_code_new[2:]
        new_name        = my_list[i][1]
        if debug:
            print("i=%d,  new_code:%s" %(i, new_code))

        new_pre_price = new_price = new_percent = total_mv = 0
        
        if new_name == '5GETF':
            eastmoney_begin = i

        if i >= eastmoney_begin:
            if debug: 
                print('use xq realtime data')

            real_df = xq_simple_df[xq_simple_df['stock_code'] == stock_code_new]
            real_df = real_df.reset_index(drop=True)
            if len(real_df):
                new_pre_price   = real_df['current'][0] - real_df['chg'][0]
                new_price       = real_df['current'][0]
                new_percent     = real_df['percent'][0]
                total_mv        = round(real_df['market_capital'][0]/10000/10000, 2)

        else:
            if debug: 
                print('use eastmoney data')

        '''
        real_df  =  get_his_data(stock_code_new, def_cnt=2)
        if len(real_df) > 1:
            new_pre_price   = real_df['close'][0]
            new_price       = real_df['close'][1]
            new_percent     = real_df['percent'][1]
        elif len(real_df) > 0:
            new_pre_price   = 0
            new_price       = real_df['close'][0]
            new_percent     = real_df['percent'][0]
        '''
      
        hsgt_df = hdata_hsgt.get_data_from_hdata(stock_code=new_code, limit=60)
        

        new_hsgt_date, new_hsgt_share_holding, new_hsgt_percent, \
                new_hsgt_delta1, new_hsgt_deltam,\
                conti_day, money_total, \
                is_zig, is_quad, is_peach = comm_handle_hsgt_data(hsgt_df)
        
        #### zlje start ####
        zlje    = get_zlje(zlje_df,     new_code, curr_date=str_date)
        zlje_3  = get_zlje(zlje_3_df,   new_code, curr_date=str_date)
        zlje_5  = get_zlje(zlje_5_df,   new_code, curr_date=str_date)
        zlje_10 = get_zlje(zlje_10_df,  new_code, curr_date=str_date)
        #### zlje end ####


        
        #### fina start ####
        if new_code[0:1] == '6':
            stock_code_new= 'SH' + new_code 
        else:
            stock_code_new= 'SZ' + new_code 

        #eastmoney fina
        fina_df = hdata_fina.get_data_from_hdata(stock_code = new_code)  # eastmoney
        #fina_df = hdata_fina.get_data_from_hdata(stock_code = stock_code_new)  #xueqiu
        fina_df = fina_df.sort_values('record_date', ascending=0)
        fina_df = fina_df.reset_index(drop=True)
        
        op_yoy = net_yoy = 0
        fina_date = new_date
        if len(fina_df):
            fina_date = fina_df['record_date'][0]
            op_yoy = fina_df['ystz'][0]
            net_yoy = fina_df['sjltz'][0]

            if debug:
                print(stock_code_new)
                print(fina_df)
        fina=str(round(op_yoy,2)) +' ' + str(round(net_yoy,2))
        new_date = fina_date + '<br>'+ fina + '</br>'
        
		#xueqiu
        '''
        fina_df = hdata_fina.get_data_from_hdata(stock_code = stock_code_new)
        
        fina_df = fina_df.sort_values('record_date', ascending=0)
        fina_df = fina_df.reset_index(drop=True)
        #print(fina_df)
        
        op_yoy = net_yoy = 0
        fina_date = new_date
        if len(fina_df):
            fina_date = fina_df['record_date'][0]
            op_yoy = fina_df['operating_income_yoy'][0]
            net_yoy = fina_df['net_profit_atsopc_yoy'][0]
            
            if debug:
                print(stock_code_new)
                print(fina_df)
        
        fina=str(round(op_yoy,2)) +' ' + str(round(net_yoy,2))
        new_date = fina_date + '<br>'+ fina + '</br>'
        '''
        #### fina end ####


        
        #### holder start ####
		# eastmoney holder
        holder_df = hdata_holder.get_data_from_hdata(stock_code = new_code)
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


        #xueqiu holder
        '''
        holder_df = hdata_holder.get_data_from_hdata(stock_code = new_code)
        holder_df = holder_df .sort_values('record_date', ascending=0)
        holder_df = holder_df .reset_index(drop=True)
        h0 = h1 = h2 = h_num = h_avg = delta_price = 0
        if len(holder_df) > 0:
            h0 = round(holder_df['chg'][0], 2)
            h_num = round(holder_df['holder_num'][0]/10000, 2)
            h_avg = round(holder_df['per_float'][0]/10000,2)
            delta_price = round(holder_df['price_ratio'][0],2)
        if len(holder_df) > 1:
            h1 = round(holder_df['chg'][1], 2)
        if len(holder_df) > 2:
            h2 = round(holder_df['chg'][2], 2)
        h_chg = '<br>'+ str(h0) + '%' +' ' + str(h1) + '%' + ' ' + str(h2) + '%' + ' </br>'\
                + 't' + str(h_num) + ' ' + 'a' + str(h_avg) + ' '+ 'd' + str(delta_price) 
        '''
        #### holder end ####


        #### fund start ####
        #fund_info = get_fund_info(new_code, fund_df)
        #print('stock_code =%s, fund_info：%s' % (new_code, fund_info))
        #### fund end ####

        #### jigou ####
        jigou_df = hdata_jigou.get_data_from_hdata(stock_code = new_code)
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

        fund_info = jigou

        


        data_list.append([new_date, new_code, new_name, total_mv, new_pre_price, new_price, new_percent, \
                is_peach, is_zig, is_quad, zlje, zlje_3, zlje_5, zlje_10, \
                h_chg, fund_info, \
                new_hsgt_date, new_hsgt_share_holding, new_hsgt_percent, \
                new_hsgt_delta1, new_hsgt_deltam, conti_day, money_total])


        #data_list.append([str_date, my_list[i], my_list_cn[i], df['pre_close'][0], df['price'][0] ])

    data_column = ['curr_date', 'code', 'name', 'total_mv', 'pre_price', 'price', 'a_pct', \
            'peach', 'zig', 'quad', 'zlje', 'zlje_3', 'zlje_5', 'zlje_10', \
            'holder_change', 'jigou', \
            'hk_date', 'hk_share', 'hk_pct', 'hk_delta1', 'hk_deltam', 'days', 'hk_m_total']

    ret_df=pd.DataFrame(data_list, columns=data_column)
    ret_df['m_per_day'] = ret_df.hk_m_total / ret_df.days
    ret_df = ret_df.fillna(0)
    ret_df=ret_df.round(2)

 
    return ret_df

   
def cgi_generate_html(df):
    cgi_handle_html_head('comm_update', refresh=1)
    cgi_handle_html_body(df)
    cgi_handle_html_end()
    
    
    

if __name__ == '__main__':

    file_name = 'my_optional.txt'
    df=show_realdata(file_name)
    # df = df.sort_values('hk_m_total', ascending=0)
    df = df.sort_values('a_pct', ascending=0)
    if debug:
        print(df)

    cgi_generate_html(df)
