#!/usr/bin/python3
# -*- coding: utf-8 -*- 
import os,sys

import datetime
import psycopg2 
import numpy as np
import matplotlib.pyplot as plt

from zig import *
from plot import *
from HData_eastmoney_day import *
from HData_eastmoney_holder import *
from HData_sina_fina import *

import multiprocessing

debug=0
#debug=1


hdata_day=HData_eastmoney_day("usr","usr")
hdata_holder=HData_eastmoney_holder("usr","usr")
hdata_fina=HData_sina_fina("usr","usr")

def plot_stock_picture(nowdate, nowcode, nowname):

    plt.style.use('bmh')
    fig = plt.figure(figsize=(24, 30),dpi=120)
    new_nowcode = nowcode
    day_df = hdata_day.get_data_from_hdata(stock_code=new_nowcode, \
            end_date=nowdate.strftime("%Y-%m-%d"), \
            limit=300)
    
    holder_df = hdata_holder.get_data_from_hdata(stock_code=new_nowcode, \
            end_date=nowdate.strftime("%Y-%m-%d"), \
            limit=300)

    fina_df = hdata_holder.get_data_from_hdata(stock_code=new_nowcode, \
            end_date=nowdate.strftime("%Y-%m-%d"), \
            limit=300)

   

    save_dir = 'picture'
    sub_name = ''
    plot_picture(nowdate, nowcode, nowname, day_df, holder_df, fina_df, save_dir, fig, sub_name)
    plt.close('all')

def worker(name):
    if debug:
        print("Worker %s %s started" % (name[0], name[1]))
        print(name)
    nowdate = name[0]
    if debug:
        print("%s %s" % (nowdate, type(nowdate)))

    nowdate = datetime.datetime.strptime(nowdate, '%Y-%m-%d').date()
    if debug:
        print("%s %s" % (nowdate, type(nowdate)))
    stock_code = name[1]
    stock_name = name[2]
    stock_name = name[1]

    plot_stock_picture(nowdate, stock_code, stock_name)
    return


if __name__ == '__main__':
    
    t1 = time.time()

    retry = 0
    nowdate=datetime.datetime.now().date()
    nowdate=nowdate-datetime.timedelta(retry)
    print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d"))) 

    df = hdata_day.get_data_from_hdata(\
            start_date=nowdate.strftime("%Y-%m-%d"), \
            end_date=nowdate.strftime("%Y-%m-%d")\
            )

    while True:
        if debug:
            print('retry=%d' % retry)

        if len(df) > 0:
            break;
        
        retry = retry + 1

        nowdate=datetime.datetime.now().date()
        nowdate=nowdate-datetime.timedelta(retry)

        df = hdata_day.get_data_from_hdata(\
            start_date=nowdate.strftime("%Y-%m-%d"), \
            end_date=nowdate.strftime("%Y-%m-%d")\
            )

    
    #df = df.head(4)  # small size for test
    data_list = np.array(df)
    data_list = data_list.tolist()

    processes = 4
    with multiprocessing.Pool(int(processes)) as pool:
        pool.map(worker, data_list)


    
    t2 = time.time()
    print("t2-t1=%s"%(t2-t1)) 
