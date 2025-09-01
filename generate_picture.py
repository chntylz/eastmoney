#!/usr/bin/python3
# -*- coding: utf-8 -*- 
import os,sys
import gc
import datetime
import psycopg2 
import numpy as np
import matplotlib.pyplot as plt

from zig import *
from plot import *
from HData_eastmoney_day import *
from HData_eastmoney_holder import *
from HData_eastmoney_jigou import *
from HData_sina_fina import *

import multiprocessing

debug=0
debug=1
debug=0


hdata_day=HData_eastmoney_day("usr","usr")
hdata_holder=HData_eastmoney_holder("usr","usr")
hdata_jigou=HData_eastmoney_jigou("usr","usr")
hdata_fina=HData_sina_fina("usr","usr")

def plot_stock_picture(nowdate, nowcode, nowname):
    
    my_dbg('%s: %s, %s, %s' % ( plot_stock_picture, nowdate, nowcode, nowname))

    plt.style.use('bmh')
    fig = plt.figure(figsize=(24, 30),dpi=120)
    new_nowcode = nowcode
    day_df = hdata_day.get_data_from_hdata(stock_code=new_nowcode, \
            end_date=nowdate.strftime("%Y-%m-%d"), \
            limit=300)
    
    holder_df = hdata_holder.get_data_from_hdata(stock_code=new_nowcode, \
            end_date=nowdate.strftime("%Y-%m-%d"), \
            limit=300)

    fina_df = hdata_fina.get_data_from_hdata(stock_code=new_nowcode, \
            end_date=nowdate.strftime("%Y-%m-%d"), \
            limit=300)
    
    jigou_df = hdata_jigou.get_data_from_hdata(stock_code=new_nowcode, \
            end_date=nowdate.strftime("%Y-%m-%d"), \
            limit=300)

    save_dir = 'picture'
    sub_name = ''
    plot_picture(nowdate, nowcode, nowname, day_df, holder_df, fina_df, jigou_df, save_dir, fig, sub_name)
    plt.clf()
    plt.cla()
    plt.close(fig)  # # 显式关闭单个图形:ml-citation{ref="1,3" data="citationList"}
    plt.close('all')  # 关闭所有已打开的图形窗口:ml-citation{ref="1,3" data="citationList"}
    # 显式删除大型对象以释放内存
    del day_df, holder_df, fina_df, jigou_df
    gc.collect()

def worker(name):
    nowdate    = name[0]
    stock_code = name[1]
    stock_name = name[2]
    stock_name = name[1]

    #for special stock test
    #if True:
    if False:
        if stock_code != '600660' and stock_code != '603991':
            return
    else:
        pass

    if debug:
        my_dbg("Worker %s %s started" % (name[0], name[1]))
        my_dbg(name)
        my_dbg("%s %s" % (nowdate, type(nowdate)))

    #for date format test
    nowdate = datetime.datetime.strptime(nowdate, '%Y-%m-%d').date()
    if debug:
        my_dbg("%s %s" % (nowdate, type(nowdate)))

    plot_stock_picture(nowdate, stock_code, stock_name)
    return


if __name__ == '__main__':
    
    t1 = time.time()

    retry = 0
    nowdate=datetime.datetime.now().date()
    nowdate=nowdate-datetime.timedelta(retry)
    my_dbg("nowdate is %s"%(nowdate.strftime("%Y-%m-%d"))) 
    
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    df = hdata_day.get_data_from_hdata(\
            start_date=nowdate.strftime("%Y-%m-%d"), \
            end_date=nowdate.strftime("%Y-%m-%d")\
            )

    while True:
        if debug:
            my_dbg('retry=%d' % retry)

        if len(df) > 0:
            break;

        if retry > 10:
            break

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

    processes = multiprocessing.cpu_count()
    processes = 8
    with multiprocessing.Pool(processes) as pool:
        pool.map(worker, data_list)
    # 清理进程池资源
    pool.close()
    pool.join()

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    my_dbg("start_time: %s, last_time: %s" % (start_time, last_time))

    
    t2 = time.time()
    my_dbg("t2-t1=%s"%(t2-t1))
