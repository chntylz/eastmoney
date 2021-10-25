#!/usr/bin/env python  
# -*- coding: utf-8 -*-
# 2019-05-24, aaron
#time
import datetime as datetime
import time
import sys
import os

import psycopg2 #使用的是PostgreSQL数据库
from HData_eastmoney_day import *
import  datetime

from zig import *
from file_interface import *

# basic
import numpy as np
import pandas as pd

#talib
import talib

from funcat import *

#delete runtimer warning
import warnings
warnings.simplefilter(action = "ignore", category = RuntimeWarning)

#funcat
from funcat import *
from funcat.data.aaron_backend import AaronDataBackend
set_data_backend(AaronDataBackend())

from file_interface import *

hdata=HData_eastmoney_day("usr","usr")

#debug switch
debug = 0
debug = 0
within_days = 8

#return the day(j) and cross_flag(true or false) if P is true during with_days, P is cross(5, 30), etc
def get_cross_info(P):
    
    within_days = 8
    for j in range(0, within_days):
        cross = REF(P, j)
        if debug:
            print('P%d=%s type(cross)=%s' % (j, cross, type(cross)))
        if cross:
            if debug:
                print('j=%d: condition is OK'% j)
            return j, cross
        else:
            if debug:
                print('j=%d: condition is NG'% j)

    return j, cross



def yitoujing(df, k):
    cond_1 = cond_2 = cond_3 = cond_4 = True
    df_len=len(df)
    today_p = df.percent[df_len-k-1]
    yes_p   = df.percent[df_len-k-1-1]
    cond_1 = today_p > 3 and yes_p > 3
    #cond_2 = REF(C,k) > REF(MA(C, 21), k)
    #cond_3 = REF(MA(C, 21),k) > REF(MA(C, 21),k+1)
    #cond_4 = today_p > 9.5 and yes_p > 9.5
    if cond_1 and cond_2 and cond_3 :
        return True
    else:
        return False

def calculate_peach_zig_quad(nowdate, nowdata_df):

    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    #codestock_local=get_stock_list()
    codestock_local=nowdata_df
    stock_len=len(codestock_local)
    update_list=[]  #for update is_peach, is_zig, is_quad in database table

    for i in range(0,stock_len):
        #for i in range(0,5):
        #if (True):
        #i = 0
        #if i > 2:
        #    continue

        draw_flag = False
        is_cross3line = 0
        is_peach = 0
        is_zig = 0
        is_quad = 0
        is_boll = 0
        is_macd = 0
        is_ema = 0
        is_2d3pct  = 0
        is_up_days = 0
        is_cup_tea = 0 
        is_duck_head = 0
        
        


        '''
        nowcode=codestock_local[i][0]
        nowname=codestock_local[i][1]
        if nowcode[0:1] == '6':
            nowcode_new= 'SH' + nowcode
        else:
            nowcode_new= 'SZ' + nowcode
        '''
        nowcode_new=codestock_local.stock_code[i]
        nowcode = nowcode_new[2:]
        nowcode = nowcode_new[:]

        #funcat call
        T(str(nowdate))
        S(nowcode)

        nowname = symbol(nowcode)
        nowname = nowname[nowname.rfind('[') + 1:]
        nowname = nowname[:nowname.rfind(']')]
        # print(str(nowdate), nowcode, nowname, O, H, L, C)

        if debug:
            print("code:%s, name:%s" % (nowcode, nowname ))


        if 0:
            #skip ST
            #if ('ST' in nowname or '300' in nowcode):
            if ('ST' in nowname or '68' in nowcode):
                if debug:
                    print("skip code: code:%s, name:%s" % (nowcode, nowname ))
                continue
        else:
            pass


        if 0:
            if '0126' not in  nowcode:
                continue
            print("code:%s, name:%s" % (nowcode, nowname ))

        '''
        #database table item list
        record_date | stock_code |  open   |  close  |  high   |   low   | volume |  amount  | p_change 
        | is_zig | is_quad | is_macd | is_ema | is_boll_cross | up_days
        '''
        detail_info = hdata.get_data_from_hdata(stock_code=nowcode_new,
                end_date=nowdate.strftime("%Y-%m-%d"),
                limit=700)
        if debug:
            print(detail_info)
           
        df_len = len(detail_info)

        #fix NaN bug
        # if len(detail_info) == 0 or (detail_info is None):
        if len(detail_info) <= within_days  or (detail_info is None):
            # print('NaN: code:%s, name:%s' % (nowcode, nowname ))
            update_list.append([nowdate.strftime("%Y-%m-%d"), nowcode_new, is_peach, is_zig, is_quad, \
                is_macd, is_2d3pct, is_up_days, is_cup_tea, is_duck_head, is_cross3line])
            continue
         
        db_max_date = detail_info['record_date'][len(detail_info)-1]
        if debug:
            print('type(db_max_date)=%s' % type(db_max_date))

        #format transfer '2021-01-01' -> '20210101' 
        if time_is_equal(db_max_date.replace('-',''), nowdate.strftime("%Y%m%d")):
            if debug:
                print('date is ok')
            else:
                pass
        else:
            #invalid data, skip this
            print('###error###: nowcode:%s, database max date:%s, nowdate:%s' % \
                    (nowcode, db_max_date, nowdate.strftime("%Y%m%d")))
            
            continue

        ##############################################################################
        # dif: 12， 与26日的差别
        # dea:dif的9日以移动平均线
        # 计算MACD指标
        dif, dea, macd_hist = talib.MACD(np.array(detail_info['close'], dtype=float),\
                fastperiod=12, slowperiod=26, signalperiod=9)
           
        upperband, middleband, lowerband = talib.BBANDS(np.array(detail_info['close']),\
                timeperiod=20, nbdevdn=2, nbdevup=2)

 
        ##############################################################################
        #cross 3 line   
        '''
	    一阳穿三线形态特征：

	    1、一阳穿多线是由一根阳K线和多条均线组成。该阳线可以是大阳线，也可以为中阳线或者小阳线。

	    2、在该形态出现过程中，是阳线上穿均线，且均线呈上涨走势，投资者可以依据均线的周期来判断股价上涨的周期。

	    3、该形态经常出现在上涨行情的初期或者震荡行情中。

	    4、该形态的形成表示多方力量强势，股价涨势强劲，预示着股价整理结束，即将开始新的行情。

	    一阳穿三线形态操作要点

	    1、三条均线的间距越小，“一阳穿三线”的成功率越高

	    2、成交量必须放大，最好是近期的两倍量以上

	    3、出现位置要越低越好

	    4、出现“一阳穿三线”后的回调，空间越小越好
        '''

        MA5=MA(C,5)
        MA10=MA(C,10)
        MA20=MA(C,20)
        MA30=MA(C,30)
        MA60=MA(C,60)
        cond_1 = CROSS(C,MA5)
        cond_2 = CROSS(C,MA10)
        cond_3 = CROSS(C,MA20)
        cond_4 = CROSS(C,MA30)
        cond_5 = CROSS(C,MA60)
        cond_6 = C > O 


        if debug:
            print(cond_1, cond_2, cond_3, cond_4, cond_5 , cond_6)

        line3_cnt = 0
        if cond_1:
            line3_cnt += 1
        
        if cond_2:
            line3_cnt += 1
        
        if cond_3:
            line3_cnt += 1
        
        if cond_4:
            line3_cnt += 1
        
        if cond_5:
            line3_cnt += 1
       
        if (line3_cnt >= 3 ) and cond_6:
            is_cross3line = 1 

            print("[yi yang chuan san xian] cross: code:%s, name:%s" % \
                (nowcode, nowname ))
            if debug:
                print('is_cross3line %s' % is_cross3line)

          
        ##############################################################################
        '''
        桃园三结义的技术要点：

        1 股价要以阳线的形式上冲EXPMA（线 股价一定要站上这条线 光头阳线比带着影线的阳线强 涨停的光头阳线是最好的阳线

        2. MACD出现金叉 MACD一般有4种金叉，0轴之下的金叉 0轴之下的双次金叉 0轴之上的金叉 0轴之上的2次金叉。 一般是来说二次金叉>0轴上的金叉>0轴下的双次金叉>0轴之下的金叉

        3 BOLL突破中轨 一定是在中轨上方 有2中情况 第一种 从下面穿上来 第二种站在上方

        满足这3个条件 我们就叫桃园三结义。 。买入这种形态之后 就一路上涨。

        尤其是通过长期横盘之后的出现桃园三结义。这样横有多长，竖有多高。很容易出现翻翻行情。

        桃园三结义的买点：

        第一介入点条件达成当天尾盘

        第二介入点第二天开盘价如果开盘价过高。盘中低点买。

        第三介入点出现上面的2个买点后，如果错过，等回踩EXPMA）再买，或者加仓。
        '''

         



        today_p = ((C - REF(C, 1))/REF(C, 1)) 
        today_p = round (today_p.value, 4)

        yes_p = ((REF(C, 1) - REF(C, 2))/REF(C, 2)) 
        yes_p = round (yes_p.value, 4)

        if ( False):
            #is_peach
            cond_5 = peach_exist(nowdate, nowcode, 2, detail_info)
            if cond_5 and today_p > 0.01:
                is_peach = 1 
                print("[tao_yuan_san_jie_yi] peach and macd golden cross: code:%s, name:%s" % \
                    (nowcode, nowname ))
        else:

            # C cross EMA12
            cond_1 = C > O and today_p > 0.01 and \
                    ( REF(C, 1) <  REF(EMA(C,12), 1) and C > EMA(C,12))
            if debug:
                print( REF(C, 1) ,  REF(EMA(C,12), 1) ,  C , EMA(C,12))

            #C cross boll-mid
            cond_2 = (O < middleband[-1] and C > middleband[-1])

            #dif > dea
            #cond_3 = dif[-1] >  dea[-1] # macd gold cross

            #dif dea become big
            cond_3 = dif[-1] > dif[-2] or dea[-1] > dea[-2]
            if debug:
                print(dif[-1] , dif[-2] , dea[-1] , dea[-2])

            #C cross ma5 and ma10
            low=min(REF(C,1), O)
            cond_4 = low < MA(C, 5)  and C > MA(C, 5)

            cond_5 = low < MA(C, 10) and C > MA(C, 10)

            #volume not big
            cond_6 = V < (1.2 * REF(V, 1)) and V > (0.8 * REF(V, 1))
            cond_6 = True


            if debug:
                print(cond_1, cond_2, cond_3, cond_4, cond_5 , cond_6)

            if cond_1 and cond_2 and cond_3 and cond_4 and cond_5 and cond_6:
                is_peach = 1 

                print("[tao_yuan_san_jie_yi_adv] peach and macd golden cross: code:%s, name:%s" % \
                    (nowcode, nowname ))
                if debug:
                    print('is_peach %s' % is_peach)

        ################################################################################################

        #is_zig
        #zig condition
        z_df, z_peers, z_d, z_k, z_buy_state=zig(detail_info)

        if debug:
            print('zig info: z_peers=%s' %(z_peers))
            for k in range(0,len(z_peers)):
                print('zig info: z_peers_date=%s' %(z_d[z_peers[k]]))

            print('zig info: z_d=%s' %(z_d))
            print('zig info: z_k=%s' %(z_k))
            print('zig info: z_buy_state=%s' %(z_buy_state))

        z_len = len(z_peers)
        #calculate buy or sell
        if z_len >= 3:  # it should have one valid zig data at least
            delta_day =  z_peers[-1] - z_peers[-2]
            if z_buy_state[-2] == 1:  #valid zig must 1, that means valley
                is_zig = delta_day
            else:
                is_zig = delta_day * (-1)

        if debug:
            print('is_zig=%s' % is_zig)
            

        ###############################################################################################
        #is_quad
        #cross
        MA5=MA(C,5)
        MA10=MA(C,10)
        MA30=MA(C,30)
        MA60=MA(C,60)
        P1=CROSS(MA5,MA30)
        P2=CROSS(MA5,MA60)
        P3=CROSS(MA10,MA30)
        P4=CROSS(MA10,MA60)
        if debug:
            print('P1=%s, P2=%s, P3=%s, P4=%s'% (P1, P2, P3, P4))

        p1_pos, p1_cross =  get_cross_info(P1)
        p2_pos, p2_cross =  get_cross_info(P2)
        p3_pos, p3_cross =  get_cross_info(P3)
        p4_pos, p4_cross =  get_cross_info(P4)


        if debug:
            print('p1_pos:%s, p1_cross:%s' %(p1_pos, p1_cross))
            print('p2_pos:%s, p2_cross:%s' %(p2_pos, p2_cross))
            print('p3_pos:%s, p3_cross:%s' %(p3_pos, p3_cross))
            print('p4_pos:%s, p4_cross:%s' %(p4_pos, p4_cross))

            print(detail_info.record_date[df_len-1-p1_pos-1])
            print(detail_info.record_date[df_len-1-p2_pos-1])
            print(detail_info.record_date[df_len-1-p3_pos-1])
            print(detail_info.record_date[df_len-1-p4_pos-1])


        # P1 P2 P3 P4 all are true during withdays
        if p1_cross and  p2_cross and  p3_cross and  p4_cross :
            if debug:
                print('!!! %s, %s, %s' %(str(nowdate), nowcode, nowname))

            the_min = min(O, C)

            #cond-1
            c_less_ma5 = False
            s_day = min(p1_pos, p2_pos)
            e_day = max(p1_pos, p2_pos)
            if s_day == e_day:
                if REF(the_min, s_day) >= REF(MA5, s_day):
                    c_less_ma5 = True
                    if debug:
                        print("ma5: s_day(%d) is equal e_day(%d)" %( s_day, e_day))
                    else:
                        pass
                else:
                    pass
            else:
                for ps in range(s_day, e_day + 1):
                    if REF(the_min, ps) >= REF(MA5, ps):
                    #if REF(C, ps) >= REF(MA5, ps):
                        c_less_ma5 = True
                        if debug:
                            print('MA5 condition ok')
                        else:
                            pass
                    else:
                        c_less_ma5 = False
                        if debug:
                            print('MA5 condition not ok')
                        else:
                            pass
                        break


            #cond-2
            c_less_ma60 = False
            s_day = min(p3_pos, p4_pos)
            e_day = min(p1_pos, p2_pos)
            if debug:
                print("s_day(%d)  e_day(%d)" %( s_day, e_day))
            if s_day == e_day:
                if REF(the_min, s_day) >= REF(MA60, s_day):
                    c_less_ma60 = True
                    if debug:
                        print("ma60: s_day(%d) is equal e_day(%d)" %( s_day, e_day))
                    else:
                        pass
            else:
                for ps in range(s_day, e_day + 1):
                    if REF(the_min, ps) >= max(REF(MA60, ps), REF(MA30, ps)):  #the min can not be allowed to enter the quadrilateral
                    #if REF(C, ps) >= REF(MA60, ps):
                        c_less_ma60 = True
                        if debug:
                            print('MA60 condition ok')
                        else:
                            pass
                    else:
                        c_less_ma60 = False
                        if debug:
                            print('MA60 condition not ok, ps=%d' % ps)
                        else:
                            pass
                        break

            if c_less_ma5 and c_less_ma60:
               is_quad = 1
               print('### %s, %s, %s, is_quad=%d' %(str(nowdate), nowcode, nowname, is_quad))

        if debug:
            print('is_quad=%s' % is_quad)


        ###############################################################################################
        #is_macd
        cond_1 = C > O and today_p > 0.01 and yes_p > 0
        cond_2 = MA(C, 5) < MA(C, 21) and MA(C, 13) < MA(C, 21)  # ma5<ma13, ma13<ma21
        cond_3 = dif[-2] - dea[-2] < 0 and dif[-1] - dea[-1] > 0 and dif[-1] < 0 # macd gold cross, and dif < 0
        cond_4 = CROSS( REF(C, 1) , REF(MA(C, 5), 1))  or  CROSS( REF(C, 2) , REF(MA(C, 5), 2)) or  CROSS( REF(C, 3) , REF(MA(C, 5), 3)) # C cross ma5 exist in past 3 days
        if cond_1 and cond_2 and cond_3 and cond_4:
            is_macd = 1
            print('### %s, %s, %s, is_macd=%d' %(str(nowdate), nowcode, nowname, is_macd))
     
        ###############################################################################################
        #is_2d3pct

        df_len = len(detail_info)

        i = 0
        while df_len > i + 1:
            if yitoujing(detail_info, i):
                pass
            else:
                break;
            i = i +1
            
        is_2d3pct = i       
        if i > 1:
            print('### %s, %s, %s, is_2d3pct=%d' %(str(nowdate), nowcode, nowname, is_2d3pct))
            

        ###############################################################################################
        #is_up_days
        for k in range(0, 6):
            yes_c = REF(C, k+1)
            cur_c = REF(C, k)
            cur_o = REF(O, k)
            if debug:
                print(str(nowdate), nowcode, nowname, k, yes_c, cur_c, cur_o)

            if (cur_c  > cur_o) and ( cur_c < yes_c * 1.09):
                continue;
            else:
                break;
        #5day up, up range is lower than 10%
        if 5 == k and (C < REF(C, 5) * 1.1 ): 
            is_up_days = 1
            print('### %s, %s, %s, is_up_days=%d' %(str(nowdate), nowcode, nowname, is_up_days))
     
        ##############################################################################
        # old duck 

        '''
        def moving_average(a, n=3):
            out = talib.MA(a, n)
            return out    
            
        def duck_head(df, min_period=8, mid_period=18, max_period=55):
            """
            上涨中继， 老鸭头
            :return:
            """
            E1 = moving_average(df.close, n=mid_period)
            E2 = moving_average(df.close, n=max_period)
            # 1、前8日中满足“E1<1日前的E1”的天数>=6
            is_min_5_growth = np.sum(np.diff(E1[-min_period-1:-1], 1) > 0) >= 6
            # 2、 今天的E1 > 昨天的E1
            is_mid_today_growth = list(E1)[-1] > list(E1)[-2]
            # 3、最近18天中满足“E2>1日前的E2”的天数>=13
            is_mid_13_growth = np.sum(np.diff(E2[-mid_period:], 1) > 0) >= 13
            # 4、 今天的E2>昨天的E2
            condition_4 = list(E2)[-1] > list(E2)[-2]
            f13 = (df.low[-mid_period:] / E2[-mid_period:] - 1) < 0.1
            # 6、最近18日都满足“E1>E2”
            condition_6 = E1[-mid_period:] > E2[-mid_period:]
            condition_7 = df.close[-min_period:] > E2[-min_period:]
            condition_8 = list(df.close)[-1] > list(E1)[-1]
            condition_9 = list(E1)[-1] / list(E2)[-1] < 1.10
            return is_min_5_growth & is_mid_today_growth & \
            is_mid_13_growth & condition_4 & f13.all(axis=0) & condition_6.all(axis=0) & \
            condition_7.all(axis=0) & condition_8 & condition_9
        '''

        #is_duck_head = duck_head(detail_info)
        #print('### %s, %s, %s, is_duck_head=%d' %(str(nowdate), nowcode, nowname, is_duck_head))
        A1=A2=PDAY1=PDAY2=PDAY3=PDAY4=PDAY5=0
        MA5 = MA(CLOSE,5);
        MA10 = MA(CLOSE,10);
        MA60 = MA(CLOSE,60);
        PDAY1 = BARSLAST(CROSS(MA5,MA60));#{5日均线上穿60日均线}
        PDAY2 = BARSLAST(CROSS(MA10,MA60));#{10日均线上穿60日均线，至此形成鸭颈部}
        if PDAY1 > 100000 or PDAY2 > 100000:
            pass
        else:
            if PDAY2.value:
                PDAY3 = BARSLAST(HIGH==HHV(HIGH,PDAY2.value));#{形成头部，要下跌}
            PDAY4 = BARSLAST(CROSS(MA10,MA5));#{下跌后，5日均线和10日均线死叉}
            PDAY5 = BARSLAST(CROSS(MA5,MA10));#{回落不久，5日均线和10日均线形成金叉，形成嘴部}
            A1= PDAY1>PDAY2 and PDAY2>PDAY3 and PDAY3>PDAY4 and PDAY4>PDAY5 and PDAY5<5;
            if PDAY2.value:
                A2= COUNT(CROSS(MA10,MA5),PDAY2.value)==1;

        if debug:
            print('PDAY1:%s' % PDAY1) 
            print('PDAY2:%s' % PDAY2) 
            print('PDAY3:%s' % PDAY3) 
            print('PDAY4:%s' % PDAY4) 
            print('PDAY5:%s' % PDAY5) 

        if A1 and A2:
            is_duck_head = 1
            print('### %s, %s, %s, is_duck_head=%d' %(str(nowdate), nowcode, nowname, is_duck_head))

        ##############################################################################
        #is_cup_tea 
        # *H1               * 
        #   *        H2    C
        #    *      *  *L2* 
        #     **L1** 

        #BOLL UP 变大

        cond_1 = cond_2 = cond_3 = cond_4 =cond_5 = False
        H2 = L2 = H2_days= H2_date= L2_days= L2_date = 0
        max_botton_days = 0
        
        in_day = 30
        #第一次最高价： 30个交易日的最高价
        try :
            H1 = HHV(REF(H, 1), in_day)    
        except:
            print('### error %s, %s, %s' %(str(nowdate), nowcode, nowname))
            update_list.append([nowdate.strftime("%Y-%m-%d"), nowcode_new, is_peach, is_zig, is_quad, \
                    is_macd, is_2d3pct, is_up_days, is_cup_tea, is_duck_head, is_cross3line])
            continue
        else:
            pass

        #第一次最低收盘价： 30个交易日的最低价
        L1 = LLV(REF(C, 1), in_day)    

        #跌幅不能大于40%
        cond_1 =  L1 >= 0.4 * H1  

        #出现第一次最高价 距离当前的天数
        H1_days = BARSLAST(REF(H, 1) == H1.value )        
        H1_date = detail_info.record_date[df_len-1-H1_days.value -1 ]

        #出现第一次最低收盘价 距离当前的天数
        L1_days = BARSLAST(REF(C, 1) == L1.value)          
        L1_date = detail_info.record_date[df_len-1-L1_days.value-1]

        #print(H1, L1 , H1_days, L1_days)
        if L1_days.value > 1 and H1_days.value > L1_days.value:
            #第二次最高价：第一次最低价以来的最高价(当天除外)
            #H2 = HHV(REF(H, 1), L1_days.value -1)     
            H2 = HHV(REF(C, 1), L1_days.value -1)     
            cond_2 = H2 > 0.8 * H1  
           
            #第二次最高价距离昨天的天数
            #H2_days = BARSLAST(REF(H, 1)== H2.value )      
            H2_days = BARSLAST(REF(C, 1)== H2 )      
            print(nowcode,H2_days)
            if H2_days.value > 200:
                print('### error %s, %s, %s' %(str(nowdate), nowcode, nowname))
                update_list.append([nowdate.strftime("%Y-%m-%d"), nowcode_new, is_peach, is_zig, is_quad, \
                        is_macd, is_2d3pct, is_up_days, is_cup_tea, is_duck_head, is_cross3line])
                continue

            if debug:
                print(H2_days)
                print(df_len-1-H2_days.value-1)
            H2_date = detail_info.record_date[df_len-1-H2_days.value-1]

            #计算底部横盘天数，涨跌幅不能超过2%
            def get_max_days_by_pct():
                N = 1
                target = 0
                while 1:
                    cond_11 = cond_22 = False
                    #N天内，涨跌幅不能超过2%
                    if debug:
                        print('### N=%d %s, %s, %s' %(N, str(nowdate), nowcode, nowname))
                        print('%s: delta_p=%s in %s' % (DATETIME, (HHV(C,N)-LLV(C,N))/LLV(C,N), N))
                        print(HHV(C,N))
                        print(LLV(C,N))
                        print(REF(C,1))

                    try :
                        cond_11 = EXIST(((C-REF(C,1))/C) < -0.02 , N) 
                        cond_22 = EXIST(((C-REF(C,1))/C) > 0.02 , N) 
                    except:
                        return N-1
                    else:
                        pass
                    
                    #if (HHV(C,N)-LLV(C,N))/LLV(C,N) < 0.2 and N < 30:
                    if cond_11 or cond_22 or N > 3:
                        break
                    else:
                        target = max(target, N)
                    N += 1
                if debug:
                    print('target=%s, N=%s' % (target, N))
                return target

            K = 0
            max_botton_days = 1
            while (cond_1 and cond_2):
                #locate to L1_date
                L1_cur_date=datetime.datetime.strptime(L1_date,'%Y-%m-%d')
                H2_cur_date=datetime.datetime.strptime(H2_date,'%Y-%m-%d')
                cur_date_new=L1_cur_date+datetime.timedelta(int(K))
                if debug:
                    print('L1_cur_date=%s, H2_cur_date=%s, cur_date_new=%s, K=%s' %\
                            (L1_cur_date.strftime('%Y%m%d'), \
                            H2_cur_date.strftime('%Y%m%d'), \
                            cur_date_new.strftime('%Y%m%d'), \
                            K))
                if H2_cur_date <= cur_date_new or K > 3:
                    #funcat call, reset to original date
                    T(str(nowdate))
                    break

                T((cur_date_new.strftime('%Y%m%d')))

                max_botton_days = max(max_botton_days , get_max_days_by_pct())
                if debug:
                    print(max_botton_days, K) 
                K +=1

            #底部至少2天
            cond_3 = max_botton_days >= 2 

            if debug:
                print('->1')
            if H2_days.value > 1 and L1_days.value > H2_days.value:
                if debug:
                    print('->2')
                loop = 1
                cond_4 = True
                #H2 -> C 之间， C > MA(C, 10)
                while loop < H2_days.value and cond_3:
                    if REF(C, loop) > REF(MA(C, 10), loop):
                        pass
                    else:
                        cond_4 = False
                        break
                    loop += 1
                
                #第二次最低价：第二次最高价以来的最低价
                L2 = LLV(REF(L, 1), H2_days.value-1)   
                L2_days = BARSLAST(REF(L, 1) == L2.value)          
                L2_date = detail_info.record_date[df_len-1-L2_days.value-1]
                
                if debug:
                    print(L2, L2_days, L2_date)
                
        #突破: 收盘价 > 杯柄的最高价
        cond_5 = C > H2
        if debug:
            print('C=', C,',', 'H2=', H2)

        if debug:
            print('is_cup_tea condition:', cond_1, cond_2, cond_3, cond_4, cond_5)
        if (cond_1 and cond_2 and cond_3 and cond_4 and cond_5):
        #if 1:
            is_cup_tea = 1
            print('H1=%s, H1_days=%s, H1_date=%s, L1=%s , L1_days=%s, L1_date=%s, delta_H1_L1=[%s,%s]' % \
                    (H1, H1_days, H1_date, L1, L1_days, L1_date, H1_days - L1_days, (L1 - H1)/H))
            print('H2=%s, H2_days=%s, H2_date=%s, L2=%s , L2_days=%s, L2_date=%s, delta_L1_H2=[%s,%s]' % \
                    (H2, H2_days, H2_date, L2, L2_days, L2_date, L1_days - H2_days, (H2 - L1)/ L1))
            print('max_botton_days=%s' % max_botton_days) 
            print(cond_1, cond_2, cond_3, cond_4, cond_5)
            print('### %s, %s, %s, is_cup_tea=%d' %(str(nowdate), nowcode, nowname, is_cup_tea))
        if debug:
            print(cond_1, cond_2, cond_3, cond_4, cond_5)
            print('### %s, %s, %s, is_cup_tea=%d' %(str(nowdate), nowcode, nowname, is_cup_tea))

        ###############################################################################################
        
        update_list.append([nowdate.strftime("%Y-%m-%d"), nowcode_new, is_peach, is_zig, is_quad, \
                is_macd, is_2d3pct, is_up_days, is_cup_tea, is_duck_head, is_cross3line])

        if debug:
            print('#############################################################################')
            print([nowdate.strftime("%Y-%m-%d"), nowcode_new, is_peach, is_zig, is_quad, \
                is_macd, is_2d3pct, is_up_days, is_cup_tea, is_duck_head, is_cross3line])

    if debug:
        print('update_list:%s'% update_list)

    data_column=['record_date', 'stock_code', 'is_peach', 'is_zig', 'is_quad', \
            'is_macd', 'is_2d3pct' ,'is_up_days', 'is_cup_tea', 'is_duck_head', 'is_cross3line' ]
    update_df=pd.DataFrame(update_list, columns=data_column)
    if debug:
        print(update_df)
    #hdata.update_allstock_hdatadate(update_df)

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    return update_df

def update_peach_zig_quad(nowdate, df, df1):

    tmp_df = df.sort_values('stock_code', ascending=0)
    tmp_df = tmp_df.reset_index(drop=True)
    tmp_df.to_csv('./csv/cross_condition_1.csv', encoding='gbk')

    tmp_df1 = df1.sort_values('stock_code', ascending=0)
    tmp_df1 = tmp_df1.reset_index(drop=True)
    tmp_df1.to_csv('./csv/cross_condition_2.csv', encoding='gbk')

    tmp_df['is_peach'] = tmp_df1['is_peach']
    tmp_df['is_zig']   = tmp_df1['is_zig']
    tmp_df['is_quad']  = tmp_df1['is_quad']
    tmp_df['is_macd']  = tmp_df1['is_macd']
    tmp_df['is_2d3pct']  = tmp_df1['is_2d3pct']
    tmp_df['is_up_days']  = tmp_df1['is_up_days']
    tmp_df['is_cup_tea']  = tmp_df1['is_cup_tea']
    tmp_df['is_duck_head']  = tmp_df1['is_duck_head']
    tmp_df['is_cross3line']  = tmp_df1['is_cross3line']

    tmp_df.to_csv('./csv/cross_condition.csv', encoding='gbk')

    if debug:
        print(tmp_df)

    #delete first, then insert
    if len(tmp_df) > 3000:
        hdata.delete_data_from_hdata(\
            start_date=nowdate.strftime("%Y-%m-%d"), \
            end_date=nowdate.strftime("%Y-%m-%d")\
            )
        hdata.copy_from_stringio(tmp_df)

if __name__ == '__main__':

    cript_name, para1 = check_input_parameter()

    t1 = time.time()

    nowdate=datetime.datetime.now().date()
    nowdate=nowdate-datetime.timedelta(int(para1))
    print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d"))) 

    nowdate_df = hdata.get_data_from_hdata(\
            start_date=nowdate.strftime("%Y-%m-%d"), \
            end_date=nowdate.strftime("%Y-%m-%d")\
            )

    handle_df = calculate_peach_zig_quad(nowdate, nowdate_df)

    if len(handle_df) > 3000:
        update_peach_zig_quad(nowdate, nowdate_df, handle_df) 

    t2 = time.time()

    
    print("t2-t1=%s"%(t2-t1)) 
