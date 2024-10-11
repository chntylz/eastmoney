#!/usr/bin/env python  
# -*- coding: utf-8 -*-
# 2019-05-24, aaron


import  datetime

from zig import *


# basic
import numpy as np
import pandas as pd

# get data
#import pandas_datareader as pdr


# visual
import matplotlib.pyplot as plt
#import mpl_finance as mpf
import mplfinance as mpf
#%matplotlib inline

from mplfinance.original_flavor import candlestick_ohlc, candlestick2_ochl, volume_overlay

#time
import datetime as datetime
import time
import os
import sys

#talib
import talib

#from Algorithm import *

#delete runtimer warning
import warnings
warnings.simplefilter(action = "ignore", category = RuntimeWarning)


#funcat
from funcat import *
from funcat.data.aaron_backend import AaronDataBackend
set_data_backend(AaronDataBackend())



#debug switch
debug = 0
#debug = 1


def check_is_bottom(nowdate, nowcode, nowname, within):
    is_bottom = False
    
    loop = 0
    while loop < within:
        loop = loop + 1
    
        nowdate=nowdate-datetime.timedelta(int(loop))
        #funcat call
        T(str(nowdate))
        S(nowcode)
        if debug:
            print(' check_is_bottom ')
            print(str(nowdate), nowcode, nowname, O, H, L, C)


        ##############################################################################

        X_11=CLOSE/MA(CLOSE,40)*100<78;
        X_12=CLOSE/MA(CLOSE,60)*100<74;
        X_13=HIGH>LOW*1.051;
        X_14=X_13 and COUNT(X_13,5)>1;
        X_15=IF(X_14 and (X_11 or X_12),2,0);
        X_16=CLOSE/REF(CLOSE,25)<=1.1;
        X_17=SMA(MAX(CLOSE-REF(CLOSE,2),0),7,1)/SMA(ABS(CLOSE-REF(CLOSE,2)),7,1)*100<15;
        X_18=(CLOSE-LLV(LOW,8))/(HHV(HIGH,8)-LLV(LOW,8))*100;
        X_19=SMA(X_18,2,1);
        X_20=SMA(X_19,2,1);
        X_21=IF(X_19>REF(X_19,1) and REF(X_19,1)<REF(X_19,2) and X_19<23,1.5,0);
        if X_16 and  X_15 and X_17 and X_21:
            if debug:
                print('check_is_bottom ### %s, %s, %s' %(str(nowdate), nowcode, nowname))
            is_bottom = True
            break

    return is_bottom
            


def plot_picture(nowdate, nowcode, nowname, detail_info, save_dir, fig, sub_name):
    step = 20
    degree = 30
    if debug:
        print("code:%s, name:%s" % (nowcode, nowname ))


    #skip ST
    if ('ST' in nowname):
        if debug:
            print("skip code: code:%s, name:%s" % (nowcode, nowname ))
        
        return

    if False:
        print(detail_info)
    
    #fix NaN bug
    if len(detail_info) < 3  or (detail_info is None):
        # print('NaN: code:%s, name:%s' % (nowcode, nowname ))
        return
    
    #funcat call
    T(str(nowdate))
    S(nowcode)
    #print(str(nowdate), nowcode, nowname, O, H, L, C)
    today_p = ((C - REF(C, 1))/REF(C, 1))
    today_p = round (today_p.value, 4)


    #detail_info.index = detail_info.index.format(formatter=lambda x: x.strftime('%Y-%m-%d'))
    #print(detail_info.index[2])
    
    detail_info['close'].fillna(value=0, inplace=True)   
    
    ma_5  = talib.MA(np.array(detail_info['close'], dtype=float), 5)
    ma_13 = talib.MA(np.array(detail_info['close'], dtype=float), 13)
    ma_21 = talib.MA(np.array(detail_info['close'], dtype=float), 21)
    if debug:
        print("ma_5.size:%d, ma_13.size:%d, ma_21.size:%d" % (ma_5.size, ma_13.size, ma_21.size))
    
    detail_info['k'], detail_info['d'] = talib.STOCH(detail_info['high'], detail_info['low'], detail_info['close'])
    detail_info['k'].fillna(value=0, inplace=True)
    detail_info['d'].fillna(value=0, inplace=True)

    #ma_vol
    ma_vol_50 = talib.MA(np.array(detail_info['volume'], dtype=float), 50)

    # 调用talib计算MACD指标
    # detail_info['MACD'],detail_info['MACDsignal'],detail_info['MACDhist'] = talib.MACD(np.array(detail_info['close']),
    #                                    fastperiod=6, slowperiod=12, signalperiod=9)   

    # dif: 12， 与26日的差别
    # dea:dif的9日以移动平均线
    # 计算MACD指标
    dif, dea, macd_hist = talib.MACD(np.array(detail_info['close'], dtype=float), fastperiod=12, slowperiod=26, signalperiod=9)

    '''
    #创建数据
    x = np.linspace(-5, 5, 100)
    y1 = np.sin(x)
    y2 = np.cos(x)

    #创建figure窗口，figsize设置窗口的大小
    plt.figure(num=3, figsize=(8, 5))
    #画曲线1
    plt.plot(x, y1)
    #画曲线2
    plt.plot(x, y2, color='blue', linewidth=5.0, linestyle='--')    
    #设置坐标轴范围
    plt.xlim((-5, 5))
    plt.ylim((-2, 2))
    #设置坐标轴名称
    plt.xlabel('xxxxxxxxxxx')
    plt.ylabel('yyyyyyyyyyy')
    #设置坐标轴刻度
    my_x_ticks = np.arange(-5, 5, 0.5)
    #对比范围和名称的区别
    #my_x_ticks = np.arange(-5, 2, 0.5)
    my_y_ticks = np.arange(-2, 2, 0.3)
    plt.xticks(my_x_ticks)
    plt.yticks(my_y_ticks)

    #显示出所有设置
    plt.show()

    '''
  
    '''

    add_axes(rect,projection,polar,frame_on,)
    rect元组： (left, bottom, width, height), 
            所有值均为画布figure的宽度和高度的比例值，
            参数left与bottom为矩形绘图区域axes左下角的点所在位置占画布长度与宽度的比例；
            参数width与height为绘图区域axes的长与宽占画布长度与宽度的比例
    projection  可选参数，坐标系的投影类型，默认为矩形
    polar   可选参数，当取值为True，相当于projection=‘polar’
    '''
    date_series=detail_info['record_date']
    dates = date_series.tolist()

    h_delta = 0.02

    #plt.title(nowcode + ': ' + nowname)  #this will add xy axis system [0.0-1.0]
    
    ax05 = fig.add_axes([0, 0.72, 1, 0.1])
    ax05.grid()    
    ax04 = fig.add_axes([0, 0.55, 1, 0.15])
    ax03 = fig.add_axes([0, 0.38, 1, 0.15])
    ax02 = fig.add_axes([0, 0.26, 1, 0.1])
    ax01 = fig.add_axes([0, 0.14, 1, 0.1])
    ax00 = fig.add_axes([0, 0.02, 1, 0.1])

    #zig
    ax05.set_title(nowcode + '-zig-' + nowname)
    ax05.set_xticks(range(0, len(detail_info.index), step))
    #ax05.set_xticklabels(detail_info.index[::step])
    ax05.set_xticklabels(date_series[::step],  rotation=degree)

    #add label and vlines for zig
    z_df, z_peers, z_d, z_k, z_buy_state=zig(detail_info)
    ax05.plot(z_df)
    z_len = len(z_peers)
    for i in range(z_len): 
        #print("i%d"%i)
        x1 = z_peers[i]
        y1 = z_df[z_peers[i]]

        text1=str(z_d[x1]) + '-' + str(z_k[x1]) + '-' + str(dates[x1])
        ax05.annotate(text1, xy=(x1, y1 ), xytext=(x1-10 , y1-2), color="b",arrowprops=dict(facecolor='red', shrink=0.05))

        if i == 0 or i == (z_len - 1):
            #skip plot.vlines for first and last
            continue

        if debug:
            print("y1:%s" % y1)
        if z_buy_state[i] == 1:
            ax05.vlines(x1, 0, y1, colors='red')
        else:
            ax05.vlines(x1, 0, y1, colors='green')

    #calculate buy or sell
    buy_flag = ''
    if z_len >= 3:  # it should have one valid zig data at least
        if z_peers[-1] - z_peers[-2] < 10: #delta days  < 10 from today
            if z_buy_state[-2] == 1:  #valid zig must 1, that means valley
                if debug:
                    print('%s gold node, buy it!!' % nowcode )
                buy_flag = '-buy'

    #check the last zig status
    z_status = ''
    if z_len >=2:
        z_status = '-z' + str(z_buy_state[-2])+ '_' + str(z_peers[-1] - z_peers[-2])
    else:
        z_status = '-z0'

    
    #check whether it is bottom or not, 2020-05-01
    if check_is_bottom(nowdate, nowcode, nowname, 3):
        buy_flag = buy_flag + '-bottom'


    #boll, candles
    ax04.set_xticks(range(0, len(detail_info.index), step))
    ax04.set_xticklabels(date_series[::step],  rotation=degree)  #index transfer to date
    candlestick2_ochl(ax04, detail_info['open'], detail_info['close'], detail_info['high'],
                                  detail_info['low'], width=0.6, colorup='r', colordown='g', alpha=0.75)
    #boll
    upperband, middleband, lowerband = talib.BBANDS(np.array(detail_info['close']),timeperiod=20, nbdevdn=2, nbdevup=2)
    ax04.plot(upperband, label="upper")
    ax04.plot(middleband, label="middle")
    ax04.plot(lowerband, label="bottom")

    #candles
    ax03.set_xticks(range(0, len(detail_info.index), step))
    ax03.set_xticklabels(date_series[::step],  rotation=degree)  #index transfer to date
    candlestick2_ochl(ax03, detail_info['open'], detail_info['close'], detail_info['high'],
        detail_info['low'], width=0.6, colorup='r', colordown='g', alpha=0.75)
    #plt.rcParams['font.sans-serif']=['Microsoft JhengHei'] 

    #k-line
    #print("ma_5:->")
    #print(ma_5)
    #print("ma_5:<-")
    ax03.plot(ma_5, label='MA5')
    ax03.plot(ma_13, label='MA13')
    ax03.plot(ma_21, label='MA21')

    #kd
    ax02.plot(detail_info['k'], label='K-Value')
    ax02.plot(detail_info['d'], label='D-Value')
    ax02.set_xticks(range(0, len(detail_info.index), step))
    ax02.set_xticklabels(date_series[::step],  rotation=degree)  #index transfer to date

    #macd
    ax01.plot(dif, color="y", label="dif")
    ax01.plot(dea, color="b", label="dea")
    red_hist = np.where(macd_hist > 0 , macd_hist, 0)
    green_hist = np.where(macd_hist < 0 , macd_hist, 0)

    ax01.bar(detail_info.index, red_hist, label="Red-MACD", color='r')
    ax01.bar(detail_info.index, green_hist, label="Green-MACD", color='g')

    ax01.set_xticks(range(0, len(detail_info.index), step))
    ax01.set_xticklabels(date_series[::step],  rotation=degree)  #index transfer to date


    #volume
    volume_overlay(ax00, detail_info['open'], detail_info['close'], detail_info['volume'], \
        colorup='r', colordown='g', width=0.5, alpha=0.8)
    ax00.set_xticks(range(0, len(detail_info.index), step))
    ax00.set_xticklabels(detail_info['record_date'][::step], rotation=degree)
    #ax00.set_xticklabels(date_series[::step],  rotation=degree)  #index transfer to date
    ax00.plot(ma_vol_50, label='MA50')

    ax03.legend();
    ax02.legend();
    ax01.legend();

    figure_name = nowcode + '.png'

    if debug:
        print('figure_name:%s' % figure_name)
    
    #if False:
    if True:
        fig.savefig(figure_name)
    else:
        os.system("echo " + figure_name + "> ./" + figure_name)

    exec_command = "mkdir -p " + save_dir
    os.system(exec_command)

    exec_command_1 = "cp -f " + figure_name + "  /var/www/html/picture/"
    if debug:
        print("%s"%(exec_command_1))
    os.system(exec_command_1)

    exec_command_2 = "rm -f " + figure_name
    if debug:
        print("%s"%(exec_command_2))
    os.system(exec_command_2)


    plt.clf()
    plt.cla()


