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

from HData_eastmoney_holder import *
holder_table=HData_eastmoney_holder("usr","usr")

from test_generate_html import *

dict_industry={}
df_global=None


debug=0
debug=0


stock_data_dir="stock_data"

 
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

    df_global = df = k_df = kline_data(stock_code=None, start_date=curr_day, end_date=curr_day, limit=0)

   
    #dragon
    print('#############################################################')
    print('start dragon')
    curr_dir=curr_day_w+'-dragon'
    dragon_df = combine_zlje_data(db_table=dragon_table, first_df=k_df, second_df=None)
    if debug:
        print(dragon_df)
    html_dragon_df = convert_to_html_df(dragon_df, curr_dir, curr_day)
    if len(html_dragon_df):
        generate_html(df_global, html_dragon_df, stock_data_dir, curr_dir, curr_day)
    else:
        print('#error, html_dragon_df len < 1')


 
    curr_dir=curr_day_w
    os.system('cp -rf ' + stock_data_dir +'/' + curr_dir + '*  /var/www/html/stock_data/' )

