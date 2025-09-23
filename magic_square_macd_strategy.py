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

from datetime import datetime
import psycopg2 #使用的是PostgreSQL数据库
from HData_eastmoney_day import *

hdata=HData_eastmoney_day("usr","usr")

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
        print(f"正在获取 {self.stock_code} 的数据...")
        self.data = hdata.get_data_from_hdata(stock_code=self.stock_code, 
                                            start_date=self.start_date, 
                                            end_date=self.end_date)
                    
        # 将日期列设置为索引
        self.data['date'] = pd.to_datetime(self.data['record_date'])
        self.data.set_index('date', inplace=True)
        print("数据获取完成。")
        return self.data

    def generate_signals(self):
        """生成交易信号 - 基于MACD指标"""
        if self.data is None:
            self.get_data()

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

        # 生成买入信号（DIF线上穿DEA线，且MACD从负变正）
        self.signals['signal'] = 0  # 初始化信号列，全部设为0（表示"无持仓"或"空仓"状态）
        
        # 从slow_period个数据点开始赋值（前面是NaN或无效数据）
        start_idx = self.slow_period
        
        # 调整条件长度以匹配索引切片
        # 创建完整的布尔数组，初始值为False
        cross_up = np.zeros(len(self.signals), dtype=bool)
        macd_turn_positive = np.zeros(len(self.signals), dtype=bool)
        cross_down = np.zeros(len(self.signals), dtype=bool)
        macd_turn_negative = np.zeros(len(self.signals), dtype=bool)
        
        # 计算实际需要比较的范围
        compare_range = range(start_idx, len(self.signals))
        
        # 填充条件数组
        for i in compare_range:
            # 条件1: DIF线上穿DEA线
            if (self.signals['dif'].iloc[i] > self.signals['dea'].iloc[i]) and \
               (self.signals['dif'].iloc[i-1] <= self.signals['dea'].iloc[i-1]):
                cross_up[i] = True
            
            # 条件2: MACD从负变正
            if (self.signals['macd'].iloc[i] >= 0) and \
               (self.signals['macd'].iloc[i-1] < 0):
                macd_turn_positive[i] = True
            
            # 条件3: DIF线下穿DEA线
            if (self.signals['dif'].iloc[i] < self.signals['dea'].iloc[i]) and \
               (self.signals['dif'].iloc[i-1] >= self.signals['dea'].iloc[i-1]):
                cross_down[i] = True
            
            # 条件4: MACD从正变负
            if (self.signals['macd'].iloc[i] < 0) and \
               (self.signals['macd'].iloc[i-1] >= 0):
                macd_turn_negative[i] = True
        
        # 买入信号：满足任一条件
        buy_conditions = cross_up | macd_turn_positive
        
        # 设置买入信号
        self.signals.loc[buy_conditions, 'signal'] = 1
        
        # 卖出信号：满足任一条件
        sell_conditions = cross_down | macd_turn_negative
        
        # 设置卖出信号
        self.signals.loc[sell_conditions, 'signal'] = 0

        # 生成交易信号（1表示买入，-1表示卖出）
        self.signals['position'] = self.signals['signal'].diff()
        print("交易信号生成完成。")
        return self.signals

    def backtest(self, initial_capital=100000):
        """回测策略"""
        if self.signals is None:
            self.generate_signals()

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
            elif self.signals['position'].iloc[i] == -1:
                # 卖出所有股票
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

        # 计算收益率
        self.portfolio['return'] = self.portfolio['total'].pct_change()
        self.portfolio['cum_return'] = (1 + self.portfolio['return']).cumprod() - 1

        print("回测完成。")
        print(f"初始资金: {initial_capital} 元")
        print(f"最终资金: {self.portfolio['total'].iloc[-1]:.2f} 元")
        print(f"总收益率: {self.portfolio['cum_return'].iloc[-1] * 100:.2f}%")
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
        plt.savefig(f'{self.stock_code}_macd_strategy_results.png')
        print(f"回测结果图已保存为 {self.stock_code}_macd_strategy_results.png")
        #plt.show()

    def run(self, initial_capital=100000):
        """运行完整策略"""
        self.get_data()
        self.generate_signals()
        self.backtest(initial_capital)
        self.plot_results()


if __name__ == '__main__':
    # 示例用法
    stock_code = '000001'
    start_date = '2024-08-21'
    end_date = '2025-08-20'
    fast_period = 12  # MACD快线参数
    slow_period = 26  # MACD慢线参数
    signal_period = 9  # MACD信号线参数
    initial_capital = 100000

    print(f"开始运行MACD量化交易策略 - 股票代码: {stock_code}")
    strategy = MACDStrategy(stock_code, start_date, end_date, fast_period, slow_period, signal_period)
    strategy.run(initial_capital)
    print("策略运行完成。")