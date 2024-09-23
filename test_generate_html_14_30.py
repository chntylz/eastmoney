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


from test_generate_html import *

dict_industry={}


debug=0
debug=0
 
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

    df_global  = kline_data(stock_code=None, start_date=curr_day, end_date=curr_day, limit=0)
    print(df_global.head(5))

    #delete 68???? kechuangban
    df = df_global[~(df_global.stock_code.str[:2] == '68')]

    df = k_df = df[df.is_zig > 0]

    #volume  and turnoverrate > 5
    print('#############################################################')
    print('start double volume')
    curr_dir=curr_day_w+'-volume'
    volume_df = df[(df.is_d_volume == 1)] 
    html_volume_df =  convert_to_html_df(volume_df, curr_dir, curr_day)
    if len(html_volume_df):
        html_volume_df = html_volume_df.sort_values('zig', ascending=1)
        generate_html(df_global,  html_volume_df, stock_data_dir, curr_dir, curr_day)
    else:
        print('#error, html_volume_df len < 1')


    print('#############################################################')

    curr_dir=curr_day_w
    os.system('cp -rf ' + stock_data_dir +'/' + curr_dir + '*  /var/www/html/stock_data/' )

