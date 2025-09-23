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
#import akshare as ak  # 用于获取股票数据

import psycopg2 #使用的是PostgreSQL数据库
from HData_eastmoney_day import *


hdata=HData_eastmoney_day("usr","usr")

class MagicSquareStrategy:
    """幻方量化交易策略类 - 基于移动平均线交叉"""
    def __init__(self, stock_code, start_date, end_date, short_window=5, long_window=20):
        self.stock_code = stock_code
        self.start_date = start_date
        self.end_date = end_date
        self.short_window = short_window  # 短期移动平均线窗口
        self.long_window = long_window    # 长期移动平均线窗口
        self.data = None                  # 存储股票数据
        self.signals = None               # 存储交易信号
        self.portfolio = None             # 存储投资组合信息

    def get_data(self):
        """获取股票数据"""
        print(f"正在获取 {self.stock_code} 的数据...")
        # 使用akshare获取股票数据
        #self.data = ak.stock_zh_a_daily(symbol=self.stock_code, start_date=self.start_date, end_date=self.end_date)
        self.data = hdata.get_data_from_hdata(stock_code=self.stock_code,start_date=self.start_date, end_date=self.end_date)
                
        # 将日期列设置为索引
        #self.data['date'] = pd.to_datetime(self.data['date'])
        self.data['date'] = pd.to_datetime(self.data['record_date'])
        self.data.set_index('date', inplace=True)
        print("数据获取完成。")
        return self.data

    def generate_signals(self):
        """生成交易信号"""
        if self.data is None:
            self.get_data()

        print("正在生成交易信号...")
        # 创建信号DataFrame
        self.signals = pd.DataFrame(index=self.data.index)
        self.signals['price'] = self.data['close']

        # 计算移动平均线
        # 使用 rolling(window=N).mean() 计算过去 self.short_window 天的收盘价均值。
        self.signals['short_mavg'] = self.data['close'].rolling(window=self.short_window).mean()  
        self.signals['long_mavg'] = self.data['close'].rolling(window=self.long_window).mean()

        # 生成买入信号（短期均线上穿长期均线）
        self.signals['signal'] = 0                                  #初始化信号列，全部设为 0（表示"无持仓"或"空仓"状态）
        # 修改前: self.signals['signal'][self.short_window:] = np.where(
        self.signals.loc[self.signals.index[self.short_window:], 'signal'] = np.where(      #从第 short_window 个数据点开始赋值（前面是 NaN 或无效数据）
            self.signals['short_mavg'].iloc[self.short_window:] > self.signals['long_mavg'].iloc[self.short_window:], 
            1,  #条件成立（短期均线上穿长期均线）→ 设为 1（买入信号）
            0)  #否则 → 设为 0（保持空仓）

        # 生成交易信号（1表示买入，-1表示卖出）
            # 计算 signal 列的差分（diff()）：
            # 如果信号从 0 → 1：差分为 +1 → 买入信号
            # 如果信号从 1 → 0：差分为 -1 → 卖出信号
            # 其他情况：差分为 0 → 无操作
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
        # 修改: 明确将shares列初始化为整数类型
        self.portfolio['shares'] = 0
        # 修改: 将cash和total列初始化为浮点数类型以避免类型不匹配警告
        self.portfolio['cash'] = float(initial_capital)
        self.portfolio['total'] = float(initial_capital)

        # 执行交易
        for i in range(1, len(self.portfolio)):
            # 前一天有买入信号
            if self.signals['position'].iloc[i] == 1:
                # 用所有现金买入股票
                shares_to_buy = int(self.portfolio['cash'].iloc[i-1] // self.portfolio['price'].iloc[i])
                self.portfolio.loc[self.portfolio.index[i], 'shares'] = shares_to_buy
                # 修改: 确保赋值为浮点数以保持类型一致性
                self.portfolio.loc[self.portfolio.index[i], 'cash'] = float(self.portfolio['cash'].iloc[i-1] - shares_to_buy * self.portfolio['price'].iloc[i])
            # 前一天有卖出信号
            elif self.signals['position'].iloc[i] == -1:
                # 卖出所有股票
                # 修改: 确保赋值为浮点数以保持类型一致性
                self.portfolio.loc[self.portfolio.index[i], 'cash'] = float(self.portfolio['cash'].iloc[i-1] + self.portfolio['shares'].iloc[i-1] * self.portfolio['price'].iloc[i])
                self.portfolio.loc[self.portfolio.index[i], 'shares'] = 0
            # 无交易信号
            else:
                self.portfolio.loc[self.portfolio.index[i], 'shares'] = self.portfolio['shares'].iloc[i-1]
                # 修改: 确保赋值为浮点数以保持类型一致性
                self.portfolio.loc[self.portfolio.index[i], 'cash'] = float(self.portfolio['cash'].iloc[i-1])

            # 计算总资产
            # 修改: 确保赋值为浮点数以保持类型一致性
            self.portfolio.loc[self.portfolio.index[i], 'total'] = float(self.portfolio['cash'].iloc[i] + self.portfolio['shares'].iloc[i] * self.portfolio['price'].iloc[i])

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

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

        # 绘制价格和移动平均线
        ax1.plot(self.data['close'], label='收盘价')
        ax1.plot(self.signals['short_mavg'], label=f'{self.short_window}日均线')
        ax1.plot(self.signals['long_mavg'], label=f'{self.long_window}日均线')

        # 标记买入和卖出信号
        buy_signals = self.signals[self.signals['position'] == 1]
        sell_signals = self.signals[self.signals['position'] == -1]
        ax1.scatter(buy_signals.index, buy_signals['price'], marker='^', color='g', label='买入信号')
        ax1.scatter(sell_signals.index, sell_signals['price'], marker='v', color='r', label='卖出信号')

        ax1.set_title(f'{self.stock_code} 价格与交易信号')
        ax1.legend()

        # 绘制总资产和累计收益率
        ax2.plot(self.portfolio['total'], label='总资产')
        ax2_twin = ax2.twinx()
        ax2_twin.plot(self.portfolio['cum_return'] * 100, color='r', label='累计收益率(%)')

        ax2.set_title('策略表现')
        ax2.set_ylabel('总资产(元)')
        ax2_twin.set_ylabel('累计收益率(%)')

        # 合并图例
        lines1, labels1 = ax2.get_legend_handles_labels()
        lines2, labels2 = ax2_twin.get_legend_handles_labels()
        ax2.legend(lines1 + lines2, labels1 + labels2, loc='upper left')

        plt.tight_layout()
        plt.savefig(f'{self.stock_code}_strategy_results.png')
        print(f"回测结果图已保存为 {self.stock_code}_strategy_results.png")
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
    short_window = 5
    long_window = 13
    initial_capital = 100000

    print(f"开始运行幻方量化交易策略 - 股票代码: {stock_code}")
    strategy = MagicSquareStrategy(stock_code, start_date, end_date, short_window, long_window)
    strategy.run(initial_capital)
    print("策略运行完成。")
