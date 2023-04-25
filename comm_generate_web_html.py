#!/usr/bin/env python
# -*- coding: utf-8 -*- 
import os,sys,time, datetime
import numpy as np
import pandas as pd

import sys


#keep 0.01 accrucy
pd.set_option('display.float_format',lambda x : '%.2f' % x)



#funcat
from funcat import *
from funcat.data.aaron_backend import AaronDataBackend
set_data_backend(AaronDataBackend())


import psycopg2

from HData_hsgt import *
from HData_eastmoney_day import *
from HData_eastmoney_holder import *
from HData_eastmoney_jigou import *
from HData_xq_fina import *
from HData_xq_holder import *

from get_data_from_db import *

from file_interface import *

from HData_eastmoney_zlje import *
from HData_eastmoney_zlje_3 import *
from HData_eastmoney_zlje_5 import *
from HData_eastmoney_zlje_10 import *


hsgtdata=HData_hsgt("usr","usr")
hdata_day=HData_eastmoney_day("usr","usr")
#hdata_holder=HData_eastmoney_holder("usr","usr")
hdata_jigou=HData_eastmoney_jigou("usr","usr")
hdata_fina=HData_xq_fina("usr","usr")
hdata_holder=HData_xq_holder("usr","usr")


#get basic stock info
basic_df = zlpm_data(stock_code=None, start_date=None, end_date=None, limit=0)
basic_df = basic_df.set_index('stock_code')


debug=0
#debug=1



  
############################################################################################################
 
def cgi_write_headline_column(df):

    print('    <tr>\n')
    #headline
    col_len=len(list(df))
    for j in range(0, col_len): 
        print('        <th>\n')
        print('        <a> %s</a>\n'%(list(df)[j]))
        print('        </th>\n')

    '''
    #add total_mv
    print('        <td>\n')
    print('           <a> total_mv </a>\n')
    print('        </td>\n')
    '''

    print('    </tr>\n')

  
def cgi_handle_html_head(title_name, refresh=0):
    print("Content-type: text/html")
    print("")

    print('<!DOCTYPE html>\n')
    print('<html>\n')
    print('<head>\n')
    print('<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />\n')
    if refresh:
        print('<meta http-equiv="refresh" content="30">\n')
    print('<title> %s-%s </title>\n' % (title_name, datetime.datetime.now().date()))
    print('\n')
    print('\n')
    print('<style type="text/css">a {text-decoration: none}\n')
    print('\n')
    print('\n')

    print('/* gridtable */\n')
    print('table {\n')
    print('    font-size:18px;\n')
    print('    color:#000;\n')
    print('    border-width: 1px;\n')
    print('    border-color: #333333;\n')
    print('    border-collapse: collapse;\n')
    print('}\n')

    print('table tr {\n')
    print('    border-width: 1px;\n')
    print('    padding: 8px;\n')
    print('    border-style: solid;\n')
    print('    border-color: #333333;\n')
    print('}\n')


    print('table th {\n')
    print('    border-width: 1px;\n')
    print('    padding: 8px;\n')
    print('    border-style: solid;\n')
    print('    border-color: #333333;\n')
    print('}\n')

    print('table td {\n')
    print('    border-width: 1px;\n')
    print('    padding: 8px;\n')
    print('    border-style: solid;\n')
    print('    border-color: #333333;\n')
    print('}\n')

    '''
    print('    table tr:nth-child(odd){\n')
    print('    background-color: #eeeeee;\n')
    print('    }\n')
    '''

    print('/* /gridtable */\n')

    print('\n')
    print('\n')
    print('</style>\n')

    print('</head>\n')
    print('\n')
    print('\n')

def cgi_handle_link(stock_code):

    tmp_stock_code=stock_code
    if tmp_stock_code[0:1] == '6':
        stock_code_new='SH'+tmp_stock_code
    else:
        stock_code_new='SZ'+tmp_stock_code
        
    xueqiu_url='https://xueqiu.com/S/' + stock_code_new
    hsgt_url='../../cgi-bin/hsgt-search.cgi?name=' + tmp_stock_code
    cgi_url = xueqiu_url + '/detail#/ZYCWZB'    
    #holder_url = 'https://xueqiu.com/snowman/S/' + stock_code_new + '/detail#/GDRS'
    holder_url = xueqiu_url + '/detail#/GDRS'

    season_pos, date_list = get_curr_season()
    jigou_url  = 'https://data.eastmoney.com/zlsj/detail/' + date_list[season_pos] +'-0-' + stock_code + '.html'

    return xueqiu_url, hsgt_url, cgi_url, holder_url, jigou_url
 
    
def cgi_write_to_file( df):
    print('<table >\n')

    #headline
    cgi_write_headline_column(df)

    #dataline
    #print('%s\n'%(list(df)))
    df_len=len(df)
    for i in range(0, df_len): #loop line

        print('    <tr>\n')
        a_array=df[i:i+1].values  #get line of df
        tmp_stock_code=a_array[0][1] 
        tmp_stock_code=tmp_stock_code[:6]
        xueqiu_url, hsgt_url, cgi_url, holder_url, jigou_url = cgi_handle_link(tmp_stock_code)

        col_name = list(df)
        col_len=len(col_name)
        for j in range(0, col_len): #loop column
            #set align right begin from the third column
            if j>2:
                print('        <td align="right">\n')
            else:
                print('        <td>\n')

            element_value = a_array[0][j] #get a[i][j] element
            #df_cgi_column=['record_date', 'stock_code', 'stock_name', 'or_yoy', 'netprofit_yoy', 'conti_day']
            if(j == 0): 
                print('           <a href="%s" target="_blank"> %s</a>\n'%(cgi_url, element_value))
            elif(j == 1): 
                print('           <a href="%s" target="_blank"> %s</a>\n'%(hsgt_url, element_value))
            elif(j == 2):
                print('           <a href="%s" target="_blank"> %s</a>\n'%(xueqiu_url, element_value))
            elif(j == col_len - 1):
                print('           <a> %.2f</a>\n'%(element_value))
            elif 'holder_change' in col_name[j]:
                print('           <a href="%s" target="_blank"> %s</a>\n'%(holder_url, element_value))
            elif 'jigou' in col_name[j]:
                print('           <a href="%s" target="_blank"> %s</a>\n'%(jigou_url, element_value))
            elif ('a_pct' in col_name[j]) or ('hk_deltam' in col_name[j]):
                if float(element_value) > 0:
                    print('           <a> <font color="red"> %s </font></a>\n'%(element_value))
                else:
                    print('           <a>  <font color="green"> %s </font></a>\n'%(element_value))

            else:
                print('           <a> %s</a>\n'%(element_value))
                                
            print('        </td>\n')

        '''
        #add total_mv
        print('        <td align="right">\n')
        print('           <a> %s </a>\n' %  (comm_get_total_mv(tmp_stock_code)))
        print('        </td>\n')
        '''


        print('    </tr>\n')

    print('</table>\n')

    pass


def cgi_hsgt_part_body():
    print ('   <form action="hsgt-search.cgi">')
    print ('   code or name <input type="text" name="name" />')
    print ('   <input type="submit" />')
    print ('   </form>')
    print ('   <a href="%s" target="_blank"> [picture]</a>' % ('../html/test.png'))
    print ('   <p></p>')
    pass
    
def cgi_handle_html_body(df, form=0):
    print('<body>\n')
    print('\n')
    print('\n')
    print('\n')
    #print('<p>----------------------------------------------------------------------</p>\n')
    #print('<p>----------------------------------------------------------------------</p>\n')
    print('\n')
    print('\n')

    if form:
        cgi_hsgt_part_body()

    cgi_write_to_file(df)

    print('        <td>\n')
    print('        </td>\n')
    print('</body>\n')
    pass

def cgi_handle_html_end():
    print('\n')
    print('\n')
    print('</html>\n')
    print('\n')

    pass
    
 

############################################################################################################

def comm_get_total_mv(stock_code):
    total_mv_df = db_daily.get_data_from_hdata(stock_code=stock_code, limit=1)
    if (len(total_mv_df)) > 0:
        total_mv = total_mv_df['total_mv'][0]/10000 
    else:
        total_mv = 0

    return round(total_mv,2)



def hsgt_get_daily_data(all_df):
    latest_date=all_df.loc[0,'record_date']
    daily_df=all_df[all_df['record_date'] == latest_date]
    return daily_df


def hsgt_daily_sort(daily_df, orderby='delta1'):
    sort_df=daily_df.sort_values(orderby, ascending=0)
    return sort_df;


def hsgt_get_continuous_info(df, select):
    all_df = df
    data_list = []
    group_by_stock_code_df=all_df.groupby('stock_code')
    for stock_code, group_df in group_by_stock_code_df:
        if debug:
            print(stock_code)
            print(group_df.head(1))
        
        group_df    = group_df.reset_index(drop=True) #reset index
        max_date    = group_df.loc[0, 'record_date']
        stock_cname = group_df.loc[0, 'stock_cname']
        total_mv    = group_df.loc[0, 'total_mv']
        hk_pct      = group_df.loc[0, 'hk_pct']
        delta1      = group_df.loc[0, 'delta1']
        delta1_m    = group_df.loc[0, 'delta1_m']
        close       = group_df.loc[0, 'close']
        a_pct       = group_df.loc[0, 'a_pct']
        is_zig      = group_df.loc[0, 'is_zig']
        is_quad     = group_df.loc[0, 'is_quad']
        is_peach    = group_df.loc[0, 'is_peach']
        is_cross3line    = group_df.loc[0, 'is_cross3line']
        op_yoy      = group_df.loc[0, 'op_yoy']      
        net_yoy     = group_df.loc[0, 'net_yoy']  
        #op_net_yop  = str(op_yoy) + ' ' + str(net_yoy)
        zlje        = group_df.loc[0, 'zlje']     
        zlje_3      = group_df.loc[0, 'zlje_3']   
        zlje_5      = group_df.loc[0, 'zlje_5']   
        zlje_10     = group_df.loc[0, 'zlje_10']  
        #zhulijine   = str(zlje) + ' ' + str(zlje_3) + ' ' + str(zlje_5) + ' ' + str(zlje_10)
        holder_0    = group_df.loc[0, 'holder_0'] 
        holder_1    = group_df.loc[0, 'holder_1'] 
        holder_2    = group_df.loc[0, 'holder_2'] 
        #holder_change = str(holder_0) + ' ' +  str(holder_1) + ' ' +  str(holder_2) 

        jigou = group_df.loc[0, 'jigou'] 

        length=len(group_df)
        money_total = 0
        flag_m = group_df.loc[0]['delta1_m']

        if 'minus' in select:
            #money < 0
            if flag_m < 0:
                conti_flag = 1
            else:
                conti_flag = 0
            
            for i in range(length):
                delta_m = group_df.loc[i]['delta1_m']
                if debug:
                    print('delta_m=%f'%(delta_m))

                if delta_m < 0:
                    tmp_flag = 1
                else:
                    tmp_flag = 0

                if conti_flag == tmp_flag:
                    money_total = money_total + delta_m
                else:
                    break
 
        else: 
            # money > 0
            if flag_m > 0:
                conti_flag = 1
            else:
                conti_flag = 0
            
            for i in range(length):
                delta_m = group_df.loc[i]['delta1_m']
                if debug:
                    print('delta_m=%f'%(delta_m))

                if delta_m >= 0:
                    tmp_flag = 1
                else:
                    tmp_flag = 0

                if conti_flag == tmp_flag:
                    money_total = money_total + delta_m
                else:
                    break
                
        money_total = round(money_total,2)
        if debug:
            print(max_date, stock_code, stock_cname, total_mv, hk_pct, close, a_pct, \
                    is_peach, is_zig, is_quad, is_cross3line, \
                    op_yoy, net_yoy, \
                    zlje, zlje_3, zlje_5, zlje_10, \
                    holder_0, holder_1, holder_2, jigou, \
                    delta1, i, money_total)

        data_list.append([max_date, stock_code, stock_cname, total_mv, hk_pct, close, a_pct, \
                is_peach, is_zig, is_quad, is_cross3line,\
                op_yoy, net_yoy, \
                zlje, zlje_3, zlje_5, zlje_10, \
                holder_0, holder_1, holder_2, jigou, \
                delta1, delta1_m, i, money_total])  #i  == conti_day

    data_column=['record_date', 'stock_code', 'stock_cname', 'total_mv', 'hk_pct', 'close', 'a_pct', \
            'peach', 'zig', 'quad', 'c3line', \
            'op_yoy', 'net_yoy', \
            'zlje', 'zl3', 'zl5', 'zl10', \
            'h0', 'h1', 'h2', 'jigou', \
            'delta1', 'delta1_m', 'conti_day', 'money_total']

    ret_df = pd.DataFrame(data_list, columns=data_column)
    ret_df['m_per_day'] = ret_df.money_total / ret_df.conti_day
    ret_df=ret_df.round(2)
    #ret_df = ret_df.sort_values('money_total', ascending=0)
    #ret_df = ret_df.sort_values('conti_day', ascending=0)
    if select == 'p_money':
        ret_df = ret_df.sort_values('money_total', ascending=0)
    elif select == 'p_minus_money':
        ret_df = ret_df.sort_values('money_total', ascending=1)
    elif select == 'p_continous_day':
        ret_df = ret_df.sort_values('conti_day', ascending=0)
    elif select == 'p_minus_continous_day':
        ret_df = ret_df.sort_values('conti_day', ascending=0)

    return ret_df
############################################################################################################



def comm_write_headline_column(f, df):

    f.write('    <tr>\n')
    #headline
    col_len=len(list(df))
    for j in range(0, col_len): 
        if debug:
            print(('list(df)[%d]=%s') % (j, list(df)[j]))

        f.write('        <td>\n')
        if (j == 0):
            f.write('           <a> record__date</a>\n') #align
        else:
            f.write('           <a> %s</a>\n'%(list(df)[j]))
        f.write('        </td>\n')

    f.write('    </tr>\n')


def comm_handle_link(stock_code):

    tmp_stock_code=stock_code
    if tmp_stock_code[0:1] == '6':
        stock_code_new='SH'+tmp_stock_code
    else:
        stock_code_new='SZ'+tmp_stock_code
        
    xueqiu_url='https://xueqiu.com/S/' + stock_code_new
    hsgt_url='../../cgi-bin/hsgt-search.cgi?name=' + tmp_stock_code
    fina_url   = xueqiu_url + '/detail#/ZYCWZB'    

    holder_url = xueqiu_url + '/detail#/GDRS'    
    add_url    = xueqiu_url + '/detail#/GGZJC'    
    season_pos, date_list = get_curr_season()
    jigou_url  = 'https://data.eastmoney.com/zlsj/detail/' + date_list[season_pos] +'-0-' + stock_code + '.html'
    return xueqiu_url, hsgt_url, fina_url, holder_url, jigou_url

   
def comm_write_to_file(f, k, df, filename):

    jigou_date = ['03-31', '06-30', '09-30', '12-31']

    f.write('<table class="gridtable">\n')

    #headline
    comm_write_headline_column(f, df)

    #dataline
    #f.write('%s\n'%(list(df)))
    df_len=len(df)
    for i in range(0, df_len): #loop line

        f.write('    <tr>\n')
        a_array=df[i:i+1].values  #get line of df
        tmp_stock_code=a_array[0][1] 
        xueqiu_url, hsgt_url, fina_url, holder_url, jigou_url = comm_handle_link(tmp_stock_code)
        #print(filename[11:21])
        dragon_url = 'https://data.eastmoney.com/stock/lhb,' + filename[11:21] + ',' + tmp_stock_code + '.html'

        col_name = list(df)
        col_len=len(col_name)
        for j in range(0, col_len): #loop column
            f.write('        <td>\n')
            element_value = a_array[0][j] #get a[i][j] element
            if debug:
                print('element_value: %s' % element_value)
            '''
            list(df)[0]=record_date
            list(df)[1]=stock_code
            list(df)[2]=stock_cname
            list(df)[3]=hk_pct
            list(df)[4]=close
            list(df)[5]=a_pct
            list(df)[6]=is_zig
            list(df)[7]=is_quad
            list(df)[8]=is_peach
            list(df)[9]=delta1
            list(df)[10]=delta1_m
            list(df)[11]=conti_day
            list(df)[12]=money_total
            list(df)[13]=m_per_day
            '''
            if k == -1: # normal case
                #data_column=['record_date', 'stock_code', 'stock_cname', 'hk_pct', 'close', 'delta1', 'delta1_m', 'conti_day', 'money_total']
                if(j == 0): 
                    f.write('           <a href="%s" target="_blank"> %s</a>\n'%\
                            (fina_url, element_value))
                elif (list(df)[j] == 'op_yoy'):
                    f.write('           <a href="%s" target="_blank"> %s</a>\n'%\
                            (fina_url, element_value))
                elif(j == 1): 
                    f.write('           <a href="%s" target="_blank"> %s[hsgt]</a>\n'%\
                            (hsgt_url, element_value))
                elif(j == 2):
                    f.write('           <a href="%s" target="_blank"> %s</a>\n'%\
                            (xueqiu_url, element_value))
                elif(j == 3):
                    f.write('           <a> %.2f</a>\n'%(element_value))
                elif (list(df)[j] == 'holder_change') \
                        or (list(df)[j] == 'holder_pct') \
                        or (list(df)[j] == 'h0'):
                    f.write('           <a href="%s" target="_blank"> %s</a>\n'%\
                            (holder_url, element_value))
                elif list(df)[j] == 'jigou':
                    f.write('           <a href="%s" target="_blank"> %s</a>\n'%\
                            (jigou_url, element_value))
                #fix bug:  must be real number, not datetime.date for holder function
                elif list(df)[j] == 'hk_date':
                    f.write('           <a> %s</a>\n'%(element_value))
                elif (list(df)[j] == 'dragon') or (list(df)[j] == 'lhb') :
                    f.write('           <a href="%s" target="_blank"> %s</a>\n'%\
                            (dragon_url, element_value))
                else:
                    f.write('           <a> %s</a>\n'%(element_value))
            
            else: #special case for get red color column
                #set color to delta column, 6 == the position of hk_pct
                '''
                list(df)[0]=record_date
                list(df)[1]=stock_code
                list(df)[2]=stock_cname
                list(df)[3]=share_holding
                list(df)[4]=close
                list(df)[5]=is_zig
                list(df)[6]=is_quad
                list(df)[7]=is_peach
                list(df)[8]=a_pct
                list(df)[9]=hk_pct
                list(df)[10]=delta1
                list(df)[11]=delta1_m
                list(df)[12]=delta2
                list(df)[13]=delta3
                list(df)[14]=delta4
                list(df)[15]=delta5
                list(df)[16]=delta10
                list(df)[17]=delta21
                list(df)[18]=delta120
                list(df)[19]=delta2_m
                list(df)[20]=delta3_m
                list(df)[21]=delta4_m
                list(df)[22]=delta5_m
                list(df)[23]=delta10_m
                list(df)[24]=delta21_m
                '''

               #record_date,  stock_code,  stock_cname, share_holding,   close, a_pct,  hk_pct,  \
               #        delta1,  delta2,  delta3,  delta4,  delta5,  delta10, delta21, delta120, \
               #        delta1_m,    delta2_m,  delta3_m, delta4_m, delta5_m, delta10_m, delta21_m
                if (j == k + 21):
                    f.write('           <a style="color: #FF0000"> %s</a>\n'%(element_value))
                else:
                    if(j == 0): 
                        f.write('           <a href="%s" target="_blank"> %s </a>\n'%(fina_url, element_value))
                    elif(j == 1): 
                        f.write('           <a href="%s" target="_blank"> %s[hsgt]</a>\n'%(hsgt_url, element_value))
                    elif(j == 2):
                        f.write('           <a href="%s" target="_blank"> %s</a>\n'%(xueqiu_url, element_value))
                    else:
                        f.write('           <a> %s</a>\n'%(element_value))
            
                                
            f.write('        </td>\n')

        f.write('    </tr>\n')

    f.write('</table>\n')

    pass
    
         
def comm_handle_html_head(filename, title, latest_date):
    with open(filename,'w') as f:
        f.write('<!DOCTYPE html>\n')
        f.write('<html>\n')
        f.write('<head>\n')
        f.write('<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />\n')
        f.write('<title> %s-%s </title>\n'%(title, latest_date))
        f.write('\n')
        f.write('\n')
        f.write('<style type="text/css">a {text-decoration: none}\n')
        f.write('\n')
        f.write('\n')

        f.write('/* gridtable */\n')
        f.write('table {\n')
        f.write('    font-size:18px;\n')
        f.write('    color:#000;\n')
        f.write('    border-width: 1px;\n')
        f.write('    border-color: #333333;\n')
        f.write('    border-collapse: collapse;\n')
        f.write('}\n')
        f.write('table tr {\n')
        f.write('    border-width: 1px;\n')
        f.write('    padding: 8px;\n')
        f.write('    border-style: solid;\n')
        f.write('    border-color: #333333;\n')
        f.write('}\n')
        f.write('table th {\n')
        f.write('    border-width: 1px;\n')
        f.write('    padding: 8px;\n')
        f.write('    border-style: solid;\n')
        f.write('    border-color: #333333;\n')
        f.write('}\n')
        f.write('table td {\n')
        f.write('    border-width: 1px;\n')
        f.write('    padding: 8px;\n')
        f.write('    border-style: solid;\n')
        f.write('    border-color: #333333;\n')
        f.write('}\n')
        
        '''
        f.write('    table tr:nth-child(odd){\n')
        f.write('    background-color: #eeeeee;\n')
        f.write('    }\n')

        f.write('    table tr:nth-child(even){\n')
        f.write('    background-color: #cccccc;\n')
        f.write('    }\n')
        '''

        f.write('/* /gridtable */\n')

        f.write('\n')
        f.write('\n')
        f.write('</style>\n')

        f.write('</head>\n')
        f.write('\n')
        f.write('\n')
 
        f.write('<body>\n')
       

    pass

#filename includes hsgt or fina
def comm_handle_html_body(filename, all_df, select='topy10'):
    if debug:
        print('filename: %s' % filename)
    with open(filename,'a') as f:
        if 'hsgt' in filename:
            daily_df  = hsgt_get_daily_data(all_df)
            daily_net = daily_df['delta1_m'].sum()
            f.write('<p style="color: #FF0000"> delta1_m sum is: %.2fw rmb </p>\n'%(daily_net))
            if select == 'top10':

                delta_list = ['hk_pct', 'delta1', 'delta1_m', 'delta2', 'delta3', 'delta4',  \
                        'delta5', 'delta10', 'delta21', 'delta120', \
                        'delta2_m', 'delta3_m', 'delta4_m', 'delta5_m', \
                        'delta10_m', 'delta21_m']
                lst_len = len(delta_list)
                for k in range(0, lst_len):
                    f.write('           <p style="color: #FF0000">------------------------------------top10 order by %s desc---------------------------------------------- </p>\n'%(delta_list[k]))
                    delta_tmp = hsgt_daily_sort(daily_df, delta_list[k])
                    #top20
                    delta_tmp = delta_tmp.head(20)
                    comm_write_to_file(f, k, delta_tmp, filename)

            elif select == 'p_money':
                conti_df = hsgt_get_continuous_info(all_df, 'p_money')
                #select condition
                conti_df = conti_df[ (conti_df.money_total / conti_df.conti_day > 1000) & (conti_df.money_total > 2000) &(conti_df.delta1_m > 1000)] 
                comm_write_to_file(f, -1, conti_df, filename)

            elif select == 'p_minus_money':
                conti_df = hsgt_get_continuous_info(all_df, 'p_minus_money')
                #select condition
                conti_df = conti_df[ (conti_df.money_total / conti_df.conti_day < -1000) & (conti_df.money_total < -2000) &(conti_df.delta1_m < -1000)] 
                comm_write_to_file(f, -1, conti_df, filename)

            elif select == 'p_continous_day':
                conti_df = hsgt_get_continuous_info(all_df, 'p_continous_day')
                #select condition
                conti_df = conti_df[conti_df.money_total > 2000] 
                comm_write_to_file(f, -1, conti_df, filename)
  
            elif select == 'p_minus_continous_day':
                conti_df = hsgt_get_continuous_info(all_df, 'p_minus_continous_day')
                #select condition
                conti_df = conti_df[conti_df.money_total < -2000] 
                comm_write_to_file(f, -1, conti_df, filename)
            


        else:
            comm_write_to_file(f, -1, all_df, filename)
    pass

def comm_handle_html_end(filename, target_dir=''):
    with open(filename,'a') as f:
        f.write('        <td>\n')
        f.write('        </td>\n')
        f.write('</body>\n')
        f.write('\n')
        f.write('\n')
        f.write('</html>\n')
        f.write('\n')

    if 'hsgt' in filename:
        #copy to /var/www/html/hsgt
        os.system('mkdir -p /var/www/html/hsgt')
        exec_command = 'cp -rf ' + filename + ' /var/www/html/hsgt/'
        os.system(exec_command)
    elif 'fina' in filename:
        #copy to /var/www/html/fina
        os.system('mkdir -p /var/www/html/stock_data/finance')
        exec_command = 'cp -rf ' + filename + ' /var/www/html/stock_data/finance/'
        os.system(exec_command)
    else:
        exec_command = 'cp -rf ' + filename + ' /var/www/html/stock_data/' + target_dir + '/'
        os.system(exec_command)

    print(exec_command)
    if debug:
        print(exec_command)
    pass




def comm_get_hsgt_continous_info(df):
    hsgt_df = df
    hsgt_df_len = len(hsgt_df)
    money_total = 0
    flag_m = hsgt_df.loc[0]['delta1_m']
    if flag_m > 0:
        conti_flag = 1
    else:
        conti_flag = 0

    for i in range(hsgt_df_len):
        delta_m = hsgt_df.loc[i]['delta1_m']
        if debug:
            print('delta_m=%f'%(delta_m))

        if delta_m >= 0:
            tmp_flag = 1
        else:
            tmp_flag = 0

        if conti_flag == tmp_flag:
            money_total = money_total + delta_m
        else:
            break
    
    money_total = round(money_total, 2)
    
    return i, money_total

 

def comm_handle_hsgt_data(df):

    all_df =df 

    unit_yi = 10000 * 10000

    del all_df['open']
    del all_df['high']
    del all_df['low']
    del all_df['volume']

    if len(all_df) > 0:
        #the_first_line - the_second_line
        all_df['delta1']  = all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-1))
        all_df['delta1_share'] = all_df.groupby('stock_code')['share_holding']\
                .apply(lambda i:i.diff(-1))
        all_df['delta1_m'] = all_df['close'] * all_df['delta1_share'] / unit_yi;
        del all_df['delta1_share']

        all_df=all_df[all_df['delta1_m'] != 0]
        all_df=all_df.reset_index(drop=True)

        if debug:
            print(type(all_df))
            print(all_df.head(2))

    hsgt_df = all_df

    hsgt_df_len = len(hsgt_df)
    if hsgt_df_len > 1:
        hsgt_date           = hsgt_df['record_date'][0]
        hsgt_date           = hsgt_date[5:] 
        hsgt_share          = hsgt_df['share_holding'][0] / unit_yi
        hsgt_percent        = hsgt_df['percent'][0]
        hsgt_delta1         = hsgt_df['percent'][0] - hsgt_df['percent'][1]
        hsgt_delta1         = round(hsgt_delta1, 2)
        hsgt_deltam         = (hsgt_df['share_holding'][0] - hsgt_df['share_holding'][1])\
                * hsgt_df['close'][0] / 10000
        hsgt_deltam         = round(hsgt_deltam, 2)
        conti_day, money_total= comm_get_hsgt_continous_info(hsgt_df)
        
        is_zig              = hsgt_df['is_zig'][0]
        is_quad             = hsgt_df['is_quad'][0]
        is_peach            = hsgt_df['is_peach'][0]

    elif hsgt_df_len > 0:
        hsgt_date           = hsgt_df['record_date'][0]
        hsgt_date           = hsgt_date[5:] 
        hsgt_share          = hsgt_df['share_holding'][0] / unit_yi
        hsgt_percent        = hsgt_df['percent'][0]
        hsgt_delta1         = hsgt_df['percent'][0]
        hsgt_deltam         = hsgt_df['share_holding'][0] * hsgt_df['close'][0]/10000
        hsgt_deltam         = round(hsgt_deltam, 2)
        conti_day           = 1
        money_total         = hsgt_deltam
        
        is_zig              = hsgt_df['is_zig'][0]
        is_quad             = hsgt_df['is_quad'][0]
        is_peach            = hsgt_df['is_peach'][0]
    else:
        hsgt_date           = ''
        hsgt_share          = 0
        hsgt_percent        = 0
        hsgt_delta1         = 0
        hsgt_deltam         = 0
        conti_day           = 0
        money_total         = 0
        is_zig              = 0
        is_quad             = 0
        is_peach            = 0

    return hsgt_date, hsgt_share, hsgt_percent, hsgt_delta1, hsgt_deltam, \
            conti_day, money_total, is_zig, is_quad, is_peach   



def insert_industry(dict_name, key):
    if dict_name.get(key) is None :
        dict_name.setdefault(key, 1)
    else:
        dict_name[key]=dict_name[key] + 1
    pass

def comm_generate_web_dataframe_new(input_df, curr_dir, curr_day, dict_industry):
    
    unit_yi = 10000 * 10000
    shell_cmd = 'mkdir -p stock_data/' + curr_dir
    os.system(shell_cmd)

    txt_file = 'stock_data/' + curr_dir + '/' + curr_day +'.txt'
    
    with open(txt_file,'w') as f:
        f.write('\n')

    zlje_df   = get_zlje_data_from_db(url='url',     curr_date=curr_day)
    zlje_3_df = get_zlje_data_from_db(url='url_3',   curr_date=curr_day)
    zlje_5_df = get_zlje_data_from_db(url='url_5',   curr_date=curr_day)
    zlje_10_df = get_zlje_data_from_db(url='url_10', curr_date=curr_day)

    daily_df = input_df

    data_list = []
    len_df = len(daily_df)
    i = 0
    for i in range(len_df):
        stock_code=daily_df.stock_code[i]
        stock_name = symbol(stock_code)
        pos_s=stock_name.rfind('[')
        pos_e=stock_name.rfind(']')
        stock_name=stock_name[pos_s+1: pos_e]
        
        #save stock_code to txt_file
        with open(txt_file,'a') as f:
            f.write('%s \n' % stock_code)

        hsgt_df = hsgtdata.get_data_from_hdata(stock_code=stock_code, end_date=curr_day, limit=60)
        hsgt_date, hsgt_share, hsgt_percent, hsgt_delta1, hsgt_deltam, conti_day, money_total, \
            is_zig, is_quad, is_peach = comm_handle_hsgt_data(hsgt_df)
        
        close = daily_df.close[i]
        close_p = daily_df.percent[i]
        is_zig  =daily_df.is_zig[i]
        is_quad =daily_df.is_quad[i]
        is_peach=daily_df.is_peach[i]

        is_2d3pct=daily_df.is_2d3pct[i]
        is_cup_tea=daily_df.is_cup_tea[i]
        is_cross3line=daily_df.is_cross3line[i]
        total_mv=round(daily_df.mkt_cap[i] / unit_yi, 2)

        industry_name = ''
        try:
            industry_name = basic_df.loc[stock_code]['industry']
        except:
            industry_name = 'Null'
            print('except industry_name %s %s' % (stock_code, stock_name))
        insert_industry(dict_industry, industry_name)

        zlje = get_zlje(zlje_df, stock_code, curr_date=curr_day)
        zlje_3 = get_zlje(zlje_3_df, stock_code, curr_date=curr_day)
        zlje_5 = get_zlje(zlje_5_df, stock_code, curr_date=curr_day)
        zlje_10 = get_zlje(zlje_10_df, stock_code, curr_date=curr_day)

        #### fina start ####
        if stock_code[0:1] == '6':
            stock_code_new= 'SH' + stock_code 
        else:
            stock_code_new= 'SZ' + stock_code 
        fina_df = hdata_fina.get_data_from_hdata(stock_code = stock_code_new)
        fina_df = fina_df.sort_values('record_date', ascending=0)
        fina_df = fina_df.reset_index(drop=True)
        
        fina_date = curr_day
        op_yoy = net_yoy = 0
        if len(fina_df):
            fina_date = fina_df['record_date'][0]
            op_yoy = fina_df['operating_income_yoy'][0]
            net_yoy = fina_df['net_profit_atsopc_yoy'][0]
            
            if debug:
                print(stock_code_new)
                print(fina_df)

        fina=str(round(op_yoy,2)) +' ' + str(round(net_yoy,2))
        new_date = fina_date + '<br>'+ fina + '</br>'
        #### fina end ####
 
        #### holder start ####
        '''
        # eastmoney holder
        holder_df = hdata_holder.get_data_from_hdata(stock_code = stock_code)
        holder_df = holder_df .sort_values('record_date', ascending=0)
        holder_df = holder_df .reset_index(drop=True)
        h0 = h1 = h2 = avg_hold_num = interval_chrate = 0
        if len(holder_df) > 0:
            h0 = round(holder_df['holder_num_ratio'][0], 2)
            avg_hold_num = round(holder_df['avg_hold_num'][0]/10000,2)
            interval_chrate = round(holder_df['interval_chrate'][0],2)
        if len(holder_df) > 1:
            h1 = round(holder_df['holder_num_ratio'][1], 2)
        if len(holder_df) > 2:
            h2 = round(holder_df['holder_num_ratio'][2], 2)
        h_chg = str(h0) + ' ' + str(h1) + ' ' + str(h2) +' '\
                + str(avg_hold_num) + ' '+ str(interval_chrate)
        '''

        #xueqiu holder
        holder_df = hdata_holder.get_data_from_hdata(stock_code = stock_code)
        holder_df = holder_df.sort_values('record_date', ascending=0)
        holder_df = holder_df.reset_index(drop=True)
        h0 = h1 = h2 = h_num = h_avg = delta_price = 0
        if len(holder_df) > 0:
            h0 = holder_df['chg'][0]
            h_num = round(holder_df['holder_num'][0]/10000, 2)
            h_avg = round(holder_df['per_float'][0]/10000, 2)
            delta_price  = holder_df['price_ratio'][0]
        if len(holder_df) > 1:
            h1 = holder_df['chg'][1]
        if len(holder_df) > 2:
            h2 = holder_df['chg'][2]

        h_chg = '<br>'+ str(h0) + '%' +' ' + str(h1) + '%' + ' ' + str(h2) + '%' + ' </br>'\
                + 't' + str(h_num) + ' ' + 'a' + str(h_avg) + ' '+ 'd' + str(delta_price)

        #stock_code = stock_code + '<br>'+ h_chg + '</br>'

#        #### holder jigou ####
        jigou_df = hdata_jigou.get_data_from_hdata(stock_code = stock_code)
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

        data_list.append([new_date, stock_code, stock_name, close_p, close, \
                hsgt_date, hsgt_share, hsgt_percent, hsgt_delta1, hsgt_deltam, conti_day, \
                money_total, total_mv, industry_name, \
                is_peach, is_zig, is_quad, is_2d3pct, is_cup_tea, is_cross3line,\
                zlje, zlje_3, zlje_5, zlje_10,h_chg, \
                jigou])

    data_column = ['cur_date', 'code', 'name', 'a_pct', 'close', \
            'hk_date', 'hk_share', 'hk_pct', 'hk_delta1', 'hk_deltam', 'conti_day', \
            'hk_m_total', 'total_mv', 'industry', \
            'peach', 'zig', 'quad', '2d3pct', 'cup_tea', 'cross3', \
            'zlje', 'zlje_3', 'zlje_5', 'zlje_10', 'holder_change' ,\
            'jigou']

    ret_df=pd.DataFrame(data_list, columns=data_column)
    ret_df['m_per_day'] = ret_df.hk_m_total / ret_df.conti_day
    ret_df = ret_df.fillna(0)
    ret_df=ret_df.round(2)
    if debug:
        print(ret_df)

    data_column = ['cur_date', 'code', 'name', 'total_mv', 'industry',  'a_pct', 'close', \
            'peach', 'zig', 'quad', '2d3pct', 'cup_tea', 'cross3',\
            'zlje', 'zlje_3', 'zlje_5', 'zlje_10', 'holder_change',\
            'jigou', \
            'hk_date', 'hk_share', 'hk_pct', \
            'hk_delta1', 'hk_deltam', 'conti_day', \
            'hk_m_total', 'm_per_day']

    ret_df=ret_df.loc[:,data_column]

    if 'pbuy' in input_df.columns:
        ret_df.insert(12, 'dragon', round(input_df['pbuy'] / 10000/10000, 2))
        ret_df.insert(12, 'lhb', round(input_df['jmmoney'] / 10000/10000, 2))
 
    return ret_df



