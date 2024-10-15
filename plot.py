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
debug = 1
debug = 0


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
            

def combine_fina(a_df, b_df):
    aa=a_df
    bb=b_df

    df = aa.append(bb, ignore_index=True, sort=False)
    df = df.sort_values('record_date')
    df = df.reset_index(drop=True)
    df = df.fillna(0)
    df_len = len(df)


    i = 0
    tmp_c = 0
    tmp_profit_rate = 0
    tmp_netprofit_rate = 0

    #find the first valid data
    while ( i < df_len):
        if(df.close[i]):  
            tmp_c = df.close[i]
            break
        i = i + 1

    i = 0
    while ( i < df_len):
        if(df.main_business_income_growth_rate[i]):
            tmp_profit_rate = df.main_business_income_growth_rate[i]
            break
        i = i + 1


    i = 0
    while ( i < df_len):
        if(df.net_profit_growth_rate[i]):
            tmp_netprofit_rate = df.net_profit_growth_rate[i]
            break
        i = i + 1

    #set 0 with valid data
    i = 0
    while ( i < df_len):
        if(df.close[i] == 0):  
            #df.close[i] = tmp_c       
            df.loc[i,'close'] = tmp_c

        if(df.main_business_income_growth_rate[i] == 0):  
            #df.main_business_income_growth_rate[i] = tmp_profit_rate
            df.loc[i,'main_business_income_growth_rate'] = tmp_profit_rate

        if(df.net_profit_growth_rate[i] == 0):  
            #df.net_profit_growth_rate[i] = tmp_netprofit_rate
            df.loc[i,'net_profit_growth_rate'] = tmp_netprofit_rate

        if(df.close[i]):    
            tmp_c = df.close[i]        

        if(df.main_business_income_growth_rate[i]):
            tmp_profit_rate = df.main_business_income_growth_rate[i]

        if(df.net_profit_growth_rate[i]):
            tmp_netprofit_rate = df.net_profit_growth_rate[i]

        i = i + 1

    return df


def combine_jigou(a_df, b_df):
    aa=a_df
    bb=b_df

    df = aa.append(bb, ignore_index=True, sort=False)
    df = df.sort_values('record_date')
    df = df.reset_index(drop=True)
    df = df.fillna(0)
    df_len = len(df)


    i = 0
    tmp_c = 0
    tmp_h = 0

    #find the first valid data
    while ( i < df_len):
        if(df.close[i]):  
            tmp_c = df.close[i]
            break
        i = i + 1

    i = 0
    while ( i < df_len):
        if(df.freeshares_ratio[i]):
            tmp_h = df.freeshares_ratio[i]
            break
        i = i + 1

    #set 0 with valid data
    i = 0
    while ( i < df_len):
        if(df.close[i] == 0):  
            #df.close[i] = tmp_c       
            df.loc[i,'close'] = tmp_c

        if(df.freeshares_ratio[i] == 0):  
            #df.freeshares_ratio[i] = tmp_h
            df.loc[i,'freeshares_ratio'] = tmp_h

        if(df.close[i]):    
            tmp_c = df.close[i]        

        if(df.freeshares_ratio[i]):
            tmp_h = df.freeshares_ratio[i]

        i = i + 1

    return df



def combine_holder(a_df, b_df):
    aa=a_df
    bb=b_df

    df = aa.append(bb, ignore_index=True, sort=False)
    df = df.sort_values('record_date')
    df = df.reset_index(drop=True)
    df = df.fillna(0)
    df_len = len(df)


    i = 0
    tmp_c = 0
    tmp_h = 0

    #find the first valid data
    while ( i < df_len):
        if(df.close[i]):  
            tmp_c = df.close[i]
            break
        i = i + 1

    i = 0
    while ( i < df_len):
        if(df.holder_num[i]):
            tmp_h = df.holder_num[i]
            break
        i = i + 1

    #set 0 with valid data
    i = 0
    while ( i < df_len):
        if(df.close[i] == 0):  
            #df.close[i] = tmp_c       
            df.loc[i,'close'] = tmp_c

        if(df.holder_num[i] == 0):  
            #df.holder_num[i] = tmp_h
            df.loc[i,'holder_num'] = tmp_h

        if(df.close[i]):    
            tmp_c = df.close[i]        

        if(df.holder_num[i]):
            tmp_h = df.holder_num[i]

        i = i + 1

    return df


def close_plot(axes,c_holder_df, step, degree):
    df = c_holder_df
    axes.set_title(df.stock_code[0]) 
    c_close = df['close']
    if debug:
        print('record_date: %s' % df['record_date'])
        print('c_close: %s' % c_close)
    axes.plot(c_close, label = 'close')
    axes.set_xticks(range(0, len(df.index), step))
    axes.set_xticklabels(df['record_date'][::step],  rotation=degree)
    axes.legend();




def holder_plot(axes, holder_df, c_holder_df, step, degree):
    df = c_holder_df
    #holder
    holder = c_holder_df['holder_num']
    if debug:
        print('holder_num: %s' % holder)
    axes.plot(holder,  '-r', label = 'holder')
    axes.set_xticks(range(0, len(df.index), step))
    axes.set_xticklabels(df['record_date'][::step],  rotation=degree)
    axes.legend();
    #mark holder num
    h_len = len(c_holder_df)
    i = 0
    j = 0
    for i in range(h_len):
        #print('i:%d j:%d' % (i, j))
        if j >= len(holder_df):
            break
        #compare original holder data with new combined data
        if c_holder_df.record_date[i] == holder_df.record_date[j]:  
            j = j+1
            x1 = i
            y1 = c_holder_df.holder_num[i]
            #text1 = str(c_holder_df.record_date[i]) + '-' + str(c_holder_df.holder_num[i])
            text1 = str(c_holder_df.holder_num[i])
            axes.annotate(text1, xy=(x1, y1), xytext=(x1, y1),fontsize = 16, color="b")
        i = i + 1


def fina_yy_plot(axes, fina_df, c_fina_df, step, degree):
    df = c_fina_df
    #yy_rate
    yy_rate = c_fina_df['main_business_income_growth_rate']
    if debug:
        print('yy_rate : %s' % yy_rate  )
    axes.plot(yy_rate ,  '-r', label = 'yy_rate ')
    axes.set_xticks(range(0, len(df.index), step))
    axes.set_xticklabels(df['record_date'][::step],  rotation=degree)
    axes.legend();
    #mark add
    df_len = len(c_fina_df)
    i = 0
    j = 0
    for i in range(df_len):
        #print('i:%d j:%d' % (i, j))
        if j >= len(fina_df):
            break
        #compare original data with new combined data
        if c_fina_df.record_date[i] == fina_df.record_date[j]:  
            j = j+1
            x1 = i
            y1 = c_fina_df.main_business_income_growth_rate[i]
            text1 = str(c_fina_df.main_business_income_growth_rate[i])
            axes.annotate(text1, xy=(x1, y1), xytext=(x1, y1),fontsize = 16, color="b")
        i = i + 1


def fina_net_plot(axes, fina_df, c_fina_df, step, degree):
    df = c_fina_df
    #net_rate
    net_rate = c_fina_df['net_profit_growth_rate']
    if debug:
        print('net_rate : %s' % net_rate  )
    axes.plot(net_rate ,  '-b', label = 'net_rate ')
    axes.set_xticks(range(0, len(df.index), step))
    axes.set_xticklabels(df['record_date'][::step],  rotation=degree)
    axes.legend();
    #mark add
    df_len = len(c_fina_df)
    i = 0
    j = 0
    for i in range(df_len):
        #print('i:%d j:%d' % (i, j))
        if j >= len(fina_df):
            break
        #compare original data with new combined data
        if c_fina_df.record_date[i] == fina_df.record_date[j]:  
            j = j+1
            x1 = i
            y1 = c_fina_df.net_profit_growth_rate[i]
            text1 = str(c_fina_df.net_profit_growth_rate[i])
            axes.annotate(text1, xy=(x1, y1), xytext=(x1, y1),fontsize = 16, color="b")
        i = i + 1

def jigou_plot(axes, jigou_df, c_jigou_df, step, degree):
    df = c_jigou_df
    #jigou
    jigou = c_jigou_df['freeshares_ratio']
    if debug:
        print('jigou : %s' % jigou  )
    axes.plot(jigou ,  '-r', label = 'jigou ')
    axes.set_xticks(range(0, len(df.index), step))
    axes.set_xticklabels(df['record_date'][::step],  rotation=degree)
    axes.legend();
    #mark add
    df_len = len(c_jigou_df)
    i = 0
    j = 0
    for i in range(df_len):
        #print('i:%d j:%d' % (i, j))
        if j >= len(jigou_df):
            break
        #compare original data with new combined data
        if c_jigou_df.record_date[i] == jigou_df.record_date[j]:  
            j = j+1
            x1 = i
            y1 = c_jigou_df.freeshares_ratio[i]
            text1 = str(c_jigou_df.freeshares_ratio[i])
            axes.annotate(text1, xy=(x1, y1), xytext=(x1, y1),fontsize = 16, color="b")
        i = i + 1



def zig_plot(axes, day_df):

    buy_flag = ''
    z_status = ''

    date_series=day_df['record_date']
    dates = date_series.tolist()

    #add label and vlines for zig
    z_df, z_peers, z_d, z_k, z_buy_state=zig(day_df)
    if debug:
        print('z_df: %s'    % z_df)
        print('z_peers: %s' % z_peers)
        print('z_d: %s'     % z_d)
        print('z_k: %s'     % z_k)
        print('z_buy_state: %s' % z_buy_state)

    axes.plot(z_df, label = 'candles-zig')
    z_len = len(z_peers)
    for i in range(z_len): 
        #print("i%d"%i)
        x1 = z_peers[i]
        y1 = z_df[z_peers[i]]

        text1=str(z_d[x1]) + '-' + str(z_k[x1]) + '-' + str(dates[x1])

        #https://matplotlib.net/stable/api/_as_gen/matplotlib.axes.Axes.annotate.html#matplotlib.axes.Axes.annotate
        #https://matplotlib.net/stable/gallery/text_labels_and_annotations/annotation_demo.html#sphx-glr-gallery-text-labels-and-annotations-annotation-demo-py
        #axes.annotate(text1, xy=(x1, y1 ), xytext=(x1-10 , y1-2), color="b",\
        #        arrowprops=dict(facecolor='red', shrink=0.05))
        axes.annotate(text1, xy=(x1, y1 ), xytext=(x1-10 , y1-2),fontsize = 16, color="b")

        if i == 0 or i == (z_len - 1):
            #skip plot.vlines for first and last
            continue

        if debug:
            print("y1:%s" % y1)
        if z_buy_state[i] == 1:
            axes.vlines(x1, 0, y1, colors='red', linewidth=1)
        else:
            axes.vlines(x1, 0, y1, colors='green', linewidth=1)

    #calculate buy or sell
    if z_len >= 3:  # it should have one valid zig data at least
        if z_peers[-1] - z_peers[-2] < 10: #delta days  < 10 from today
            if z_buy_state[-2] == 1:  #valid zig must 1, that means valley
                if debug:
                    print('%s gold node, buy it!!' % nowcode )
                buy_flag = '-buy'

    #check the last zig status
    if z_len >=2:
        z_status = '-z' + str(z_buy_state[-2])+ '_' + str(z_peers[-1] - z_peers[-2])
    else:
        z_status = '-z0'

    return buy_flag, z_status


def plot_picture(nowdate, nowcode, nowname, day_df, holder_df, fina_df, jigou_df, save_dir, fig, sub_name):

    step = 20
    degree = 5

    buy_flag = ''
    z_status = ''

    if debug:
        print("code:%s, name:%s" % (nowcode, nowname ))

    c_holder_df = combine_holder(day_df, holder_df)
    c_fina_df = combine_fina(day_df, fina_df)
    c_jigou_df = combine_jigou(day_df, jigou_df)

    if debug:
        print('day_df: %s' % day_df.head(2))
        print('holder_df: %s' % holder_df.head(2))
        print('fina_df: %s' % fina_df.head(2))
        print('jigou_df: %s' % jigou_df.head(2))

        print('c_holder_df: %s' % c_holder_df.head(2))
        print('c_fina_df: %s' % c_fina_df.head(2))
        print('c_jigou_df: %s' % c_jigou_df.head(2))
 

    #skip ST
    if ('ST' in nowname):
        if debug:
            print("skip code: code:%s, name:%s" % (nowcode, nowname ))
        
        return

    if False:
        print(day_df)
    
    #fix NaN bug
    if len(day_df) < 3  or (day_df is None):
        # print('NaN: code:%s, name:%s' % (nowcode, nowname ))
        return
    
    #funcat call
    T(str(nowdate))
    S(nowcode)
    #print(str(nowdate), nowcode, nowname, O, H, L, C)
    today_p = ((C - REF(C, 1))/REF(C, 1))
    today_p = round (today_p.value, 4)


    #day_df.index = day_df.index.format(formatter=lambda x: x.strftime('%Y-%m-%d'))
    #print(day_df.index[2])
    
    day_df['close'].fillna(value=0, inplace=True)   
    
    ma_5  = talib.MA(np.array(day_df['close'], dtype=float), 5)
    ma_13 = talib.MA(np.array(day_df['close'], dtype=float), 13)
    ma_21 = talib.MA(np.array(day_df['close'], dtype=float), 21)
    if debug:
        print("ma_5.size:%d, ma_13.size:%d, ma_21.size:%d" % (ma_5.size, ma_13.size, ma_21.size))
    
    day_df['k'], day_df['d'] = talib.STOCH(day_df['high'], day_df['low'], day_df['close'])
    day_df['k'].fillna(value=0, inplace=True)
    day_df['d'].fillna(value=0, inplace=True)

    #ma_vol
    ma_vol_50 = talib.MA(np.array(day_df['volume'], dtype=float), 50)
    ma_vol_100 = talib.MA(np.array(day_df['volume'], dtype=float), 100)

    # 调用talib计算MACD指标
    # day_df['MACD'],day_df['MACDsignal'],day_df['MACDhist'] = talib.MACD(np.array(day_df['close']),
    #                                    fastperiod=6, slowperiod=12, signalperiod=9)   

    # dif: 12， 与26日的差别
    # dea:dif的9日以移动平均线
    # 计算MACD指标
    dif, dea, macd_hist = talib.MACD(np.array(day_df['close'], dtype=float), fastperiod=12, slowperiod=26, signalperiod=9)

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
    date_series=day_df['record_date']
    dates = date_series.tolist()

    h_delta = 0.02

    #plt.title(nowcode + ': ' + nowname)  #this will add xy axis system [0.0-1.0]
    #                    left  bottom right height
    ax07 = fig.add_axes([0.05, 0.95, 0.95, 0.04])
    ax06 = fig.add_axes([0.05, 0.84, 0.95, 0.1])
    ax05 = fig.add_axes([0.05, 0.72, 0.95, 0.1])
    ax05.grid()    
    ax04 = fig.add_axes([0.05, 0.55, 0.95, 0.15])
    ax03 = fig.add_axes([0.05, 0.38, 0.95, 0.15])
    ax02 = fig.add_axes([0.05, 0.26, 0.95, 0.1])
    ax01 = fig.add_axes([0.05, 0.14, 0.95, 0.1])
    ax00 = fig.add_axes([0.05, 0.02, 0.95, 0.1])
    
    #                    left  bottom right height
    ax07 = fig.add_axes([0.05, 0.88, 0.95, 0.1])
    ax06 = fig.add_axes([0.05, 0.77, 0.95, 0.1])
    ax05 = fig.add_axes([0.05, 0.66, 0.95, 0.1])
    ax05.grid()    
    ax04 = fig.add_axes([0.05, 0.50, 0.95, 0.15])
    ax03 = fig.add_axes([0.05, 0.34, 0.95, 0.15])
    ax02 = fig.add_axes([0.05, 0.23, 0.95, 0.1])
    ax01 = fig.add_axes([0.05, 0.12, 0.95, 0.1])
    ax00 = fig.add_axes([0.05, 0.01, 0.95, 0.1])
    ####################################################################################################################
    #jigou
    axes = ax07
    close_plot(axes, c_jigou_df, step, degree)

    axes_sub0 = axes.twinx()
    jigou_plot(axes_sub0, jigou_df, c_jigou_df, step, degree)
 
    ####################################################################################################################
    #holder
    axes = ax06
    close_plot(axes, c_holder_df, step, degree)

    axes_sub0 = axes.twinx()
    holder_plot(axes_sub0, holder_df, c_holder_df, step, degree)

    #add zig together with candles
    axes_sub1 = axes.twinx()
    buy_flag, z_status = zig_plot(axes_sub1, c_holder_df)

    ####################################################################################################################
    #fina
    axes = ax05
    close_plot(axes, c_fina_df, step, degree)

    axes_sub0 = axes.twinx()
    fina_yy_plot(axes_sub0, fina_df, c_fina_df, step, degree)

    axes_sub1 = axes.twinx()
    fina_net_plot(axes_sub1, fina_df, c_fina_df, step, degree)
    
    #add zig together with candles
    axes_sub2 = axes.twinx()
    buy_flag, z_status = zig_plot(axes_sub2, c_fina_df)
    ####################################################################################################################

    #debug
    #if False:
    if True:
        ####################################################################################################################
        #boll, candles
        ax04.set_xticks(range(0, len(day_df.index), step))
        ax04.set_xticklabels(date_series[::step],  rotation=degree)  #index transfer to date
        candlestick2_ochl(ax04, day_df['open'], day_df['close'], day_df['high'],
                                      day_df['low'], width=0.6, colorup='r', colordown='g', alpha=0.75)
        #boll
        upperband, middleband, lowerband = talib.BBANDS(np.array(day_df['close']),timeperiod=20, nbdevdn=2, nbdevup=2)
        ax04.plot(upperband, label="upper")
        ax04.plot(middleband, label="middle")
        ax04.plot(lowerband, label="bottom")
        ax04.legend();
        ####################################################################################################################

        #candles
        ax03.set_xticks(range(0, len(day_df.index), step))
        ax03.set_xticklabels(date_series[::step],  rotation=degree)  #index transfer to date
        candlestick2_ochl(ax03, day_df['open'], day_df['close'], day_df['high'],
            day_df['low'], width=0.6, colorup='r', colordown='g', alpha=0.75)
        #plt.rcParams['font.sans-serif']=['Microsoft JhengHei'] 

        #k-line
        #print("ma_5:->")
        #print(ma_5)
        #print("ma_5:<-")
        ax03.plot(ma_5, label='MA5')
        ax03.plot(ma_13, label='MA13')
        ax03.plot(ma_21, label='MA21')
        ax03.legend();

        #add zig together with candles
        axes_sub = ax03.twinx()
        buy_flag, z_status = zig_plot(axes_sub, day_df)

        #check whether it is bottom or not, 2020-05-01
        bottom_flag = False
        try:
            bottom_flag = check_is_bottom(nowdate, nowcode, nowname, 3)
        except Exception as e:
            print(e)
        
        if bottom_flag:
            buy_flag = buy_flag + '-bottom'

        ####################################################################################################################
        #kd
        ax02.plot(day_df['k'], label='K-Value')
        ax02.plot(day_df['d'], label='D-Value')
        ax02.set_xticks(range(0, len(day_df.index), step))
        ax02.set_xticklabels(date_series[::step],  rotation=degree)  #index transfer to date
        ax02.legend();
        ####################################################################################################################

        #macd
        ax01.plot(dif, color="y", label="dif")
        ax01.plot(dea, color="b", label="dea")
        red_hist = np.where(macd_hist > 0 , macd_hist, 0)
        green_hist = np.where(macd_hist < 0 , macd_hist, 0)

        ax01.bar(day_df.index, red_hist, label="Red-MACD", color='r')
        ax01.bar(day_df.index, green_hist, label="Green-MACD", color='g')

        ax01.set_xticks(range(0, len(day_df.index), step))
        ax01.set_xticklabels(date_series[::step],  rotation=degree)  #index transfer to date
        ax01.legend();

       ####################################################################################################################


        #volume
        volume_overlay(ax00, day_df['open'], day_df['close'], day_df['volume'], \
            colorup='r', colordown='g', width=0.5, alpha=0.8)
        ax00.set_xticks(range(0, len(day_df.index), step))
        ax00.set_xticklabels(day_df['record_date'][::step], rotation=degree)
        #ax00.set_xticklabels(date_series[::step],  rotation=degree)  #index transfer to date
        #ax00.plot(ma_vol_50, label='MA50')
        ax00.plot(ma_vol_100, label='MA100')
        ax00.legend();
        ####################################################################################################################

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


