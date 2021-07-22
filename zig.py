# coding: utf-8 
"""
Created on Sat Jan 05 18:53:39 2019
http://www.pianshen.com/article/363258879/
@author: duanqs
"""
import numpy as np
import tushare as ts


debug = 0
 
ZIG_STATE_START = 0
ZIG_STATE_RISE = 1
ZIG_STATE_FALL = 2

def recurse_set_state(buy_state, pos, state):
    if debug:
        print("recurse_set_state")
    if pos is 0:
        pass
    else:
        buy_state[pos] = state;
        if state is 1:
            state = 0
        else:
            state = 1
        recurse_set_state(buy_state, pos - 1, state)
    pass

def get_buy_state(peers, dates, prices):
    length = len(peers)
    if debug:
        print('length is %s' % length)
    buy_state = [];
    for i in range(0, length):
        buy_state.append(0)

    if length < 3:
        return buy_state

    if prices[length - 2] < prices[length - 1]:
        recurse_set_state(buy_state, length - 2, 1)
    else:
        recurse_set_state(buy_state, length - 2, 0)

    if debug:
        print("buy_state:%s" % buy_state)
    return buy_state

def zig(df):
    x = 0.35
    k = df["close"]
    d = df.index
    #print(k)
    #print(d)
    # 循环前的变量初始化
    # 端点 候选点 扫描点 端点列表 拐点线列表 趋势状态
    peer_i = 0
    candidate_i = None
    scan_i = 0
    peers = [0]
    z = np.zeros(len(k))
    state = ZIG_STATE_START
    while True:
        #print(peers)
        scan_i += 1
        if scan_i == len(k) - 1:
            # 扫描到尾部
            if candidate_i is None:
                peer_i = scan_i
                peers.append(peer_i)
            else:
                if state == ZIG_STATE_RISE:
                    if k[scan_i] >= k[candidate_i]:
                        peer_i = scan_i
                        peers.append(peer_i)
                    else:
                        peer_i = candidate_i
                        peers.append(peer_i)
                        peer_i = scan_i
                        peers.append(peer_i)
                elif state == ZIG_STATE_FALL:
                    if k[scan_i] <= k[candidate_i]:
                        peer_i = scan_i
                        peers.append(peer_i)
                    else:
                        peer_i = candidate_i
                        peers.append(peer_i)
                        peer_i = scan_i
                        peers.append(peer_i)
            break
 
        if state == ZIG_STATE_START:
            if k[scan_i] >= k[peer_i] * (1 + x):
                candidate_i = scan_i
                state = ZIG_STATE_RISE
            elif k[scan_i] <= k[peer_i] * (1 - x):
                candidate_i = scan_i
                state = ZIG_STATE_FALL
        elif state == ZIG_STATE_RISE:
            if k[scan_i] >= k[candidate_i]:
                candidate_i = scan_i
            elif k[scan_i] <= k[candidate_i]*(1-x):
                peer_i = candidate_i
                peers.append(peer_i)
                state = ZIG_STATE_FALL
                candidate_i = scan_i
        elif state == ZIG_STATE_FALL:
            if k[scan_i] <= k[candidate_i]:
                candidate_i = scan_i
            elif k[scan_i] >= k[candidate_i]*(1+x):
                peer_i = candidate_i
                peers.append(peer_i)
                state = ZIG_STATE_RISE
                candidate_i = scan_i
    
    #线性插值， 计算出zig的值            
    for i in range(len(peers) - 1):
        peer_start_i = peers[i]
        peer_end_i = peers[i+1]
        start_value = k[peer_start_i]
        end_value = k[peer_end_i]
        a = (end_value - start_value)/(peer_end_i - peer_start_i)# 斜率
        for j in range(peer_end_i - peer_start_i +1):
            z[j + peer_start_i] = start_value + a*j
    
    # print(u'...转向点的阀值、个数、位置和日期...')        
    if debug:
        print('zig: %d, %d' % (x, len(peers)))
        print("zig peers:%s" % peers)
    dates = [d[i] for i in peers]  #时间
    if debug:
        print("zig: dates:%s" % dates)
    prices=[k[i] for i in peers]
    if debug:
        print("zig prices:%s" % prices)
        print(list(k))
        print(list(z))

    buy_state = get_buy_state(peers, dates, prices)
    if debug:
        print("zig buy_state:%s" % buy_state)

    
    return z, peers, d, k, buy_state


    '''
    import tushare as ts
    import  datetime
    import pandas as pd
    import sys
    import os

    token='21dddafc47513ea46b89057b2c4edf7b44882b3e92274b431f199552'
    pro = ts.pro_api(token)


    hist_data = ts.pro_bar(ts_code='000413.SZ', start_date='20180101', end_date='20200205', adj='qfq', freq='D')
    new_data =  pd.DataFrame()
    new_data['datetime'] = hist_data['trade_date']
    new_data['code']     = '000413'
    new_data['open']     = hist_data['open']
    new_data['close']    = hist_data['close']
    new_data['high']     = hist_data['high']
    new_data['low']      = hist_data['low']
    new_data['vol']      = hist_data['vol']
    new_data['amount']   = hist_data['amount']
    new_data['p_change'] = hist_data['pct_chg']

    new_data['datetime']=new_data['datetime'].apply(lambda x: datetime.datetime.strptime(x,'%Y%m%d'))

    hist_data = new_data.set_index('datetime')




    #>>> z, peers, d, k=zig(hist_data)
    0.35 5
    peers:[0, 175, 226, 488, 490]
    dates:[Timestamp('2020-02-05 00:00:00'), Timestamp('2019-04-22 00:00:00'), Timestamp('2019-01-31 00:00:00'), Timestamp('2018-01-04 00:00:00'), Timestamp('2018-01-02 00:00:00')]
    [2.75, 7.14, 4.04, 9.45, 9.28]


    '''
