#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- coding: gbk -*-
# -*- coding: gb2312 -*-
# -*- coding: cp936 -*-  # 或使用文件实际编码

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['Noto Sans CJK JP', 'WenQuanYi Micro Hei', 'sans-serif']
plt.rcParams['axes.unicode_minus'] = False

from datetime import datetime, timedelta

import psycopg2 #使用的是PostgreSQL数据库
from HData_eastmoney_day import *

import multiprocessing

hdata=HData_eastmoney_day("usr","usr")

debug = 0

class MACDStrategy:
    """MACD量化交易策略类 - 基于移动平均线收敛发散指标"""
    def __init__(self, stock_code, start_date, end_date, fast_period=12, slow_period=26, signal_period=9):
        self.stock_code = stock_code
        self.start_date = start_date
        self.end_date = end_date
        self.fast_period = fast_period  # 快线周期(通常为12)
        self.slow_period = slow_period  # 慢线周期(通常为26)
        self.signal_period = signal_period  # 信号线周期(通常为9)
        self.data = None  # 存储股票数据
        self.signals = None  # 存储交易信号
        self.portfolio = None  # 存储投资组合信息

    def get_data(self):
        """获取股票数据"""
        if debug:
            print(f"正在获取 {self.stock_code} 的数据...")
        self.data = hdata.get_data_from_hdata(stock_code=self.stock_code, 
                                            start_date=self.start_date, 
                                            end_date=self.end_date)
                    
        # 将日期列设置为索引
        self.data['date'] = pd.to_datetime(self.data['record_date'])
        self.data.set_index('date', inplace=True)
        if debug:
            print("数据获取完成。")
        return self.data

    def generate_signals(self):
        """生成交易信号 - 基于MACD指标
        只使用DIF线和DEA线的交叉来生成信号：
        DIF线上穿DEA线买入，DIF线下穿DEA线卖出
        """
        if self.data is None:
            self.get_data()

        if debug:
            print("正在生成交易信号...")
        # 创建信号DataFrame
        self.signals = pd.DataFrame(index=self.data.index)
        self.signals['price'] = self.data['close']

        # 计算MACD指标
        # 计算快速移动平均线(EMA)
        self.signals['ema_fast'] = self.data['close'].ewm(span=self.fast_period, adjust=False).mean()
        # 计算慢速移动平均线(EMA)
        self.signals['ema_slow'] = self.data['close'].ewm(span=self.slow_period, adjust=False).mean()
        # 计算DIF线 = 快线 - 慢线
        self.signals['dif'] = self.signals['ema_fast'] - self.signals['ema_slow']
        # 计算DEA线(DIF的移动平均线)
        self.signals['dea'] = self.signals['dif'].ewm(span=self.signal_period, adjust=False).mean()
        # 计算MACD柱 = (DIF - DEA) * 2
        self.signals['macd'] = (self.signals['dif'] - self.signals['dea']) * 2

        # 初始化信号列，全部设为0（表示"无持仓"或"空仓"状态）
        self.signals['signal'] = 0
        self.signals['position'] = 0
        
        # 从slow_period个数据点开始赋值（前面是NaN或无效数据）
        start_idx = max(self.slow_period, self.signal_period) + 10
        
        # 向量化判断金叉和死叉
        dif = self.signals['dif']
        dea = self.signals['dea']

        # 金叉：当前 DIF > DEA，且前一期 DIF <= DEA
        cross_up = (dif > dea) & (dif.shift(1) < dea.shift(1))

        # 死叉：当前 DIF < DEA，且前一期 DIF >= DEA
        cross_down = (dif < dea) & (dif.shift(1) > dea.shift(1))

        # 处理同时满足买入和卖出条件的情况
        # 首先设置所有卖出信号
        self.signals.loc[cross_down, 'signal'] = -1
        # 然后设置买入信号（优先级高于卖出信号）
        self.signals.loc[cross_up, 'signal'] = 1

        
        self.signals['position'] = self.signals['signal']


        '''       
        # 生成交易信号（1表示买入，-1表示卖出）
        self.signals['position'] = self.signals['signal'].diff()
		

        # 对于死叉点，确保position为-1
        # 直接在死叉点设置position为-1，确保卖出信号明确
        self.signals.loc[cross_down, 'position'] = -1
        
        # 对于金叉点，确保position为1
        # 直接在金叉点设置position为1，确保买入信号明确
        self.signals.loc[cross_up, 'position'] = 1
        '''
		
        # 修复position中可能出现的NaN值

        
        # 替换为
        self.signals.fillna({'position': 0}, inplace=True)
        
        if debug:
            print("交易信号生成完成。")
        self.signals.to_csv('csv/macd.csv')
        return self.signals

    def backtest(self, initial_capital=100000):
        """回测策略"""
        if self.signals is None:
            self.generate_signals()

        if debug:
            print("正在回测策略...")
        # 创建投资组合DataFrame
        self.portfolio = pd.DataFrame(index=self.signals.index)
        self.portfolio['price'] = self.signals['price']
        # 明确将shares列初始化为整数类型
        self.portfolio['shares'] = 0
        # 将cash和total列初始化为浮点数类型以避免类型不匹配警告
        self.portfolio['cash'] = float(initial_capital)
        self.portfolio['total'] = float(initial_capital)

        # 执行交易
        for i in range(1, len(self.portfolio)):
            # 前一天有买入信号
            if self.signals['position'].iloc[i] == 1:
                # 用所有现金买入股票
                shares_to_buy = int(self.portfolio['cash'].iloc[i-1] // self.portfolio['price'].iloc[i])
                self.portfolio.loc[self.portfolio.index[i], 'shares'] = shares_to_buy
                # 确保赋值为浮点数以保持类型一致性
                self.portfolio.loc[self.portfolio.index[i], 'cash'] = float(
                    self.portfolio['cash'].iloc[i-1] - shares_to_buy * self.portfolio['price'].iloc[i])
            # 前一天有卖出信号
            #elif self.signals['position'].iloc[i] == -1 or (self.signals['signal'].iloc[i] == 0 and self.signals['signal'].iloc[i-1] == 1):
            elif self.signals['position'].iloc[i] == -1 :
                # 卖出所有股票：使用position为-1或者signal从1变为0时都执行卖出
                # 确保赋值为浮点数以保持类型一致性
                self.portfolio.loc[self.portfolio.index[i], 'cash'] = float(
                    self.portfolio['cash'].iloc[i-1] + self.portfolio['shares'].iloc[i-1] * self.portfolio['price'].iloc[i])
                self.portfolio.loc[self.portfolio.index[i], 'shares'] = 0
            # 无交易信号
            else:
                self.portfolio.loc[self.portfolio.index[i], 'shares'] = self.portfolio['shares'].iloc[i-1]
                # 确保赋值为浮点数以保持类型一致性
                self.portfolio.loc[self.portfolio.index[i], 'cash'] = float(self.portfolio['cash'].iloc[i-1])

            # 计算总资产
            # 确保赋值为浮点数以保持类型一致性
            self.portfolio.loc[self.portfolio.index[i], 'total'] = float(
                self.portfolio['cash'].iloc[i] + self.portfolio['shares'].iloc[i] * self.portfolio['price'].iloc[i])

        # 确保在策略结束时卖出所有持仓（强制平仓）
        if len(self.portfolio) > 0:
            last_day = self.portfolio.index[-1]
            if self.portfolio['shares'].iloc[-1] > 0:
                # 最后一天强制卖出所有股票
                self.portfolio.loc[last_day, 'cash'] = float(
                    self.portfolio['cash'].iloc[-1] + self.portfolio['shares'].iloc[-1] * self.portfolio['price'].iloc[-1])
                self.portfolio.loc[last_day, 'shares'] = 0
                self.portfolio.loc[last_day, 'total'] = float(self.portfolio['cash'].iloc[-1])

        # 计算收益率
        self.portfolio['return'] = self.portfolio['total'].pct_change()
        self.portfolio['cum_return'] = (1 + self.portfolio['return']).cumprod() - 1

        print(f"回测完成, stock_code: {self.stock_code}, 初始资金: {initial_capital} 元, 最终资金: {self.portfolio['total'].iloc[-1]:.2f} 元,  总收益率: {self.portfolio['cum_return'].iloc[-1] * 100:.2f}%")
        return self.portfolio

    def plot_results(self):
        """绘制回测结果"""
        if self.portfolio is None:
            self.backtest()

        # 创建图形和子图
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 15))

        # 绘制价格和移动平均线
        ax1.plot(self.data['close'], label='收盘价')
        
        # 标记买入和卖出信号
        buy_signals = self.signals[self.signals['position'] == 1]
        # 直接使用cross_down条件来标记卖出信号，确保与策略逻辑完全一致
        sell_signals = self.signals[self.signals['position'] == -1]
        ax1.scatter(buy_signals.index, buy_signals['price'], marker='^', color='g', label='买入信号')
        ax1.scatter(sell_signals.index, sell_signals['price'], marker='v', color='r', label='卖出信号')

        ax1.set_title(f'{self.stock_code} 价格与交易信号')
        ax1.legend()

        # 绘制MACD指标
        ax2.plot(self.signals['dif'], label='DIF线')
        ax2.plot(self.signals['dea'], label='DEA线')
        ax2.bar(self.signals.index, self.signals['macd'], label='MACD柱', alpha=0.3)
        ax2.axhline(y=0, color='k', linestyle='--', alpha=0.3)
        ax2.scatter(buy_signals.index, [0]*len(buy_signals), marker='^', color='g')
        ax2.scatter(sell_signals.index, [0]*len(sell_signals), marker='v', color='r')
        ax2.set_title(f'{self.stock_code} MACD指标')
        ax2.legend()

        # 绘制总资产和累计收益率
        ax3.plot(self.portfolio['total'], label='总资产')
        ax3_twin = ax3.twinx()
        ax3_twin.plot(self.portfolio['cum_return'] * 100, color='r', label='累计收益率(%)')

        ax3.set_title('策略表现')
        ax3.set_ylabel('总资产(元)')
        ax3_twin.set_ylabel('累计收益率(%)')

        # 合并图例
        lines1, labels1 = ax3.get_legend_handles_labels()
        lines2, labels2 = ax3_twin.get_legend_handles_labels()
        ax3.legend(lines1 + lines2, labels1 + labels2, loc='upper left')

        plt.tight_layout()
        plt.savefig(f'./picture/{self.stock_code}_macd_strategy_results.png')
        if debug:
            print(f"回测结果图已保存为 {self.stock_code}_macd_strategy_results.png")
        #plt.show()

    def run(self, initial_capital=100000):
        """运行完整策略"""
        self.get_data()
        self.generate_signals()
        self.backtest(initial_capital)
        self.plot_results()

def do_strategy(stock_date, stock_code):
    # 示例用法
    #stock_code = '300502'
    stock_code = stock_code
    
    # 计算start_date为stock_date的30天前
    from datetime import datetime as dt, timedelta
    end_date = stock_date
    date_format = '%Y-%m-%d'
    end_date_obj = dt.strptime(end_date, date_format)
    start_date_obj = end_date_obj - timedelta(days=30)
    start_date = start_date_obj.strftime(date_format)

    fast_period = 12  # MACD快线参数
    slow_period = 26  # MACD慢线参数
    signal_period = 9  # MACD信号线参数
    initial_capital = 100000

    if debug:
            print(f"开始运行MACD量化交易策略 - 股票代码: {stock_code}")
    strategy = MACDStrategy(stock_code, start_date, end_date, fast_period, slow_period, signal_period)
    strategy.run(initial_capital)
    if debug:
            print("策略运行完成。")

def worker(name):
    if debug:
        my_dbg("Worker %s %s started" % (name[0], name[1]))
        my_dbg(name)
    
    stock_date = name[0]
    stock_code = name[1]
    do_strategy(stock_date, stock_code)


if __name__ == '__main__':


    latest_df = hdata.get_latest_data_from_hdata()
    #latest_df = latest_df.head(5)  #debug
    data_list = np.array(latest_df)
    data_list = data_list.tolist()

    processes = multiprocessing.cpu_count()
    number = len(latest_df)
    mplist = []
    with multiprocessing.Pool(processes) as pool:
       mplist.append(
           pool.map(worker, data_list))
 


