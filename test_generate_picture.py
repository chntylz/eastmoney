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
from comm_generate_web_html import *


debug=0
#debug=1


hdata_day=HData_eastmoney_day("usr","usr")

def plot_stock_picture(nowcode, nowname):

    plt.style.use('bmh')
    fig = plt.figure(figsize=(24, 30),dpi=120)
    new_nowcode = nowcode
    detail_info = hdata_day.get_data_from_hdata(stock_code=new_nowcode, \
            end_date=nowdate.strftime("%Y-%m-%d"), \
            limit=300)

    if debug:
        print(detail_info)

    save_dir = 'picture'
    sub_name = ''
    plot_picture(nowdate, nowcode, nowname, detail_info, save_dir, fig, sub_name)
    plt.close('all')

def worker(name):
    if debug:
        print("Worker %s %s started" % (name[0], name[1]))
        print(name)
    stock_code = name[1]
    stock_name = name[2]
    stock_name = name[1]

    plot_stock_picture(stock_code, stock_name)
    return


if __name__ == '__main__':
    
    t1 = time.time()

    nowdate=datetime.datetime.now().date()
    #nowdate=nowdate-datetime.timedelta(1)
    print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d"))) 

    nowdate_df = hdata_day.get_data_from_hdata(\
            start_date=nowdate.strftime("%Y-%m-%d"), \
            end_date=nowdate.strftime("%Y-%m-%d")\
            )


    #nowdate_df = nowdate_df.head(10)  # small size for test
    data_list = np.array(nowdate_df)
    data_list = data_list.tolist()


    processes = 4
    with multiprocessing.Pool(int(processes)) as pool:
        pool.map(worker, data_list)


    
    t2 = time.time()
    print("t2-t1=%s"%(t2-t1)) 
