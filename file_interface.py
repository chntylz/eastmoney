#!/usr/bin/env python
#coding:utf-8
#https://www.cnblogs.com/shaosks/p/5614630.html

import time
import datetime

import os,sys,os.path
from sys import argv
import shutil

# basic
import numpy as np
import pandas as pd

import talib
#funcat
from funcat import *
from funcat.data.aaron_backend import AaronDataBackend
set_data_backend(AaronDataBackend())

#把时间戳转化为时间: 1479264792 to 2016-11-16 10:53:12
def TimeStampToTime(timestamp):
    timeStruct = time.localtime(timestamp)
    return time.strftime('%Y-%m-%d %H:%M:%S',timeStruct)



#获取文件的大小,结果保留两位小数，单位为Byte
def get_FileSize(filePath):
    fsize = os.path.getsize(filePath)
    fsize = fsize/float(1024)
    return round(fsize,2)


#获取文件的访问时间
def get_FileAccessTime(filePath):
    t = os.path.getatime(filePath)
    return TimeStampToTime(t)


#获取文件的创建时间
def get_FileCreateTime(filePath):
    t = os.path.getctime(filePath)
    return TimeStampToTime(t)



#获取文件的修改时间
def get_FileModifyTime(filePath):
    t = os.path.getmtime(filePath)
    return TimeStampToTime(t)



def getAllFiles(directory):
    files=[]
    for dirpath, dirnames,filenames in os.walk(directory):
        if filenames!=[]:
            for file in filenames:
                files.append(dirpath+'/'+file)
    #files.sort(key=len)
    files.sort()
    return files


#获取脚本文件的当前路径
def cur_file_dir():
    #获取脚本路径
    path = sys.path[0]
    #判断为脚本文件还是py2exe编译后的文件，如果是脚本文件，则返回的是脚本的目录，如果是py2exe编译后的文件，则返回的是编译后的文件路径
    if os.path.isdir(path):
        return path
    elif os.path.isfile(path):
        return os.path.dirname(path)



#compare 2 string date
def compare_time(time1,time2):
    t1 = time.strptime(time1, "%Y%m%d")
    t2 = time.strptime(time2, "%Y%m%d")
    return (t1 <= t2)

#compare 2 string date
def time_is_equal(time1,time2):
    t1 = time.strptime(time1, "%Y%m%d")
    t2 = time.strptime(time2, "%Y%m%d")
    return (t1 == t2)


def is_national_holiday(date='20200707'):
    national_holiday = ['20190913','20190914', '20191002','20191003','20191004', '20191005','20191007', '20200101', '20200124', '20200127', '20200128', '20200129', '20200130', '20200131', '20200406', '20200501', '20200504', '20200505', '20200625', '20200626', '20201001', '20201002','20201001','20201005','20201006','20201007','20201008']
    if date in national_holiday:
        return True
    else:
        return False

#curr_date=datetime.datetime.now().date()
def is_work_day(curr_date):
    weekday=curr_date.strftime("%w")
    if weekday in ['6', '0'] or is_national_holiday(curr_date.strftime("%Y%m%d")):
        return False
    else: 
        return True


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



def check_input_parameter():
# 如果执行的方式错误输出使用方法
    USAGE = '''
        用法错误，正确方式如下：
        python demo.py 1
        '''
    if len(argv) > 2:
        print(USAGE)  # 如果传入的参数不足，输出正确用法
        exit(1) # 异常退出(下面的代码将不会被执行)

    script_name, para1 = argv  # 将传入的参数赋值进行使用
    print("%s, %d"%(script_name, int(para1)))

    return script_name, para1



    #gold cross, return 1
    #dead cross, return -1
    #others, return 0    
def macd_cross(dif, dea):
    ret = 0 
    if len(dif) < 2 or len(dea) < 2:
        print('###error, dif len < 2')
        return ret
    if dif[-1] > dea[-1] and dif[-2] <= dea[-2]:
        ret = 1
    if dif[-1] < dea[-1] and dif[-2] >= dea[-2]:
        ret = -1
    #print('macd_cross ret=%d'% ret)
    return ret




def remove_dir(nowdate, save_dir, sub_name):

    cmd = save_dir + '/' + nowdate.strftime("%Y-%m-%d-%w") + sub_name
    print('%s'%cmd)

    if os.path.exists(cmd):
        shutil.rmtree(cmd)

    #/var/www/html/stock_data/
    cmd = '/var/www/html/' + save_dir + '/' + nowdate.strftime("%Y-%m-%d-%w") + sub_name
    print('%s'%cmd)

    if os.path.exists(cmd):
        shutil.rmtree(cmd)

    pass



# 
def peach_exist(now_date, stock_code, days, data_df):
    ema_cross_flag  = False
    boll_cross_flag = False
    macd_cross_flag = False

    df_len = len(data_df)

    for loop in range(0, days):
        cur_date=data_df['record_date'][df_len- loop - 1]
        data_df = data_df.head(len(data_df)-loop)    
    
        dif, dea, macd_hist = talib.MACD(np.array(data_df['close'], dtype=float), fastperiod=12, slowperiod=26, signalperiod=9)
        upperband, middleband, lowerband = talib.BBANDS(np.array(data_df['close']),timeperiod=20, nbdevdn=2, nbdevup=2)
        
        T(str(cur_date))
        S(stock_code)

        today_p = ((C - REF(C, 1))/REF(C, 1))
        today_p = round (today_p.value, 4)

        yes_p = ((REF(C, 1) - REF(C, 2))/REF(C, 2))
        yes_p = round (yes_p.value, 4)

        cond_0 = C > O and today_p > 0.01 and ( REF(C, 1) <  REF(EMA(C,12), 1) and C > EMA(C,12)) # C cross EMA12
        cond_1 = C > O and today_p > 0.01 and ( REF(C, 1) <  REF(EMA(C,50), 1) and C > EMA(C,50)) # C cross EMA50
        cond_2 = (O < middleband[-1] and C > middleband[-1] ) or (O < C and O > middleband[-1] )
        cond_3 = macd_cross(dif, dea) # macd gold cross

        ema_cross_flag = ema_cross_flag or (cond_0 and cond_1)
        boll_cross_flag = boll_cross_flag or cond_2
        macd_cross_flag = macd_cross_flag or (cond_3  == 1)

    
    return ema_cross_flag and boll_cross_flag and macd_cross_flag 

