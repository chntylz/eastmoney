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

class DMAStrategy:
    """DMA量化交易策略类 - 基于差异移动平均线"""
    def __init__(self, stock_code, start_date, end_date, short_window=10, long_window=50, ama_window=10):
        self.stock_code = stock_code
        self.start_date = start_date
        self.end_date = end_date
        self.short_window = short_window  # 短期移动平均线窗口
        self.long_window = long_window    # 长期移动平均线窗口
        self.ama_window = ama_window      # DMA的移动平均线窗口
        self.data = None                  # 存储股票数据
        self.signals = None               # 存储交易信号
        self.portfolio = None             # 存储投资组合信息

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
        """生成交易信号 - 基于DMA指标"""
        if self.data is None:
            self.get_data()

        print("正在生成交易信号...")
        # 创建信号DataFrame
        self.signals = pd.DataFrame(index=self.data.index)
        self.signals['price'] = self.data['close']

        # 计算移动平均线
        self.signals['short_mavg'] = self.data['close'].rolling(window=self.short_window).mean()
        self.signals['long_mavg'] = self.data['close'].rolling(window=self.long_window).mean()
        
        # 计算DMA线 (短期均线减去长期均线)
        self.signals['dma'] = self.signals['short_mavg'] - self.signals['long_mavg']
        
        # 计算AMA线 (DMA的移动平均线)
        self.signals['ama'] = self.signals['dma'].rolling(window=self.ama_window).mean()

        # 生成买入信号（DMA线上穿AMA线）
        self.signals['signal'] = 0  # 初始化信号列，全部设为0（表示"无持仓"或"空仓"状态）
        
        # 从long_window个数据点开始赋值（前面是NaN或无效数据）
        start_idx = self.long_window
        self.signals.loc[self.signals.index[start_idx:], 'signal'] = np.where(
            self.signals['dma'].iloc[start_idx:] > self.signals['ama'].iloc[start_idx:],
            1,  # 条件成立（DMA线上穿AMA线）→ 设为1（买入信号）
            0)  # 否则 → 设为0（保持空仓）

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
        ax1.plot(self.signals['short_mavg'], label=f'{self.short_window}日均线')
        ax1.plot(self.signals['long_mavg'], label=f'{self.long_window}日均线')

        # 标记买入和卖出信号
        buy_signals = self.signals[self.signals['position'] == 1]
        sell_signals = self.signals[self.signals['position'] == -1]
        ax1.scatter(buy_signals.index, buy_signals['price'], marker='^', color='g', label='买入信号')
        ax1.scatter(sell_signals.index, sell_signals['price'], marker='v', color='r', label='卖出信号')

        ax1.set_title(f'{self.stock_code} 价格与移动平均线')
        ax1.legend()

        # 绘制DMA和AMA线
        ax2.plot(self.signals['dma'], label='DMA线')
        ax2.plot(self.signals['ama'], label=f'AMA线 ({self.ama_window}日均线)')
        ax2.axhline(y=0, color='k', linestyle='--', alpha=0.3)
        ax2.scatter(buy_signals.index, [0]*len(buy_signals), marker='^', color='g')
        ax2.scatter(sell_signals.index, [0]*len(sell_signals), marker='v', color='r')
        ax2.set_title(f'{self.stock_code} DMA指标')
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
        plt.savefig(f'{self.stock_code}_dma_strategy_results.png')
        print(f"回测结果图已保存为 {self.stock_code}_dma_strategy_results.png")
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
    short_window = 10  # DMA短期均线参数
    long_window = 50   # DMA长期均线参数
    ama_window = 10    # DMA的移动平均线参数
    initial_capital = 100000

    print(f"开始运行DMA量化交易策略 - 股票代码: {stock_code}")
    strategy = DMAStrategy(stock_code, start_date, end_date, short_window, long_window, ama_window)
    strategy.run(initial_capital)
    print("策略运行完成。")