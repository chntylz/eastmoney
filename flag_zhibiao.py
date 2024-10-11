import numpy as np
import pandas as pd
import talib
import matplotlib.pyplot as plt

# 假设我们有一个包含历史价格数据的DataFrame，其中包含'open', 'high', 'low', 'close'列
# data = pd.read_csv('your_data.csv')  # 你可以从CSV文件中加载数据
# data = data[['open', 'high', 'low', 'close']]

# 为了示例，我们创建一些随机的价格数据
np.random.seed(42)
data = pd.DataFrame({
    'open': np.random.randn(100).cumsum(),
    'high': np.random.randn(100).cumsum() + 10,
    'low': np.random.randn(100).cumsum() - 10,
    'close': np.random.randn(100).cumsum()
})

# 计算趋势线斜率
def calculate_trendline(data, period):
    x = np.linspace(0, len(data) - 1, period)
    y_high = data['high'].rolling(window=period).max()
    y_low = data['low'].rolling(window=period).min()
    slope_high, _ = np.polyfit(x, y_high, 1)
    slope_low, _ = np.polyfit(x, y_low, 1)
    return slope_high, slope_low

# 识别旗形形态
def identify_flags(data, trend_threshold=0.8, price_threshold=0.05):
    flags = []
    for i in range(10, len(data) - 10):
        high_points = data['high'][i-10:i+1].idxmax()
        low_points = data['low'][i-10:i+1].idxmin()
        
        # 计算高点和低点的趋势线
        slope_high = calculate_trendline(data['high'].iloc[high_points], 10)[0]
        slope_low = calculate_trendline(data['low'].iloc[low_points], 10)[1]
        
        # 检查趋势线是否平行
        if abs(slope_high - slope_low) < trend_threshold:
            # 检查价格是否在趋势线之间
            if data['high'].iloc[high_points] < data['high'].iloc[low_points] * (1 + price_threshold):
                if data['low'].iloc[low_points] > data['low'].iloc[high_points] * (1 - price_threshold):
                    flags.append((i, high_points, low_points))
    return flags

# 绘制价格图表和识别的旗形形态
def plot_flags(data, flags):
    plt.figure(figsize=(10, 5))
    plt.plot(data['close'])
    for start, high, low in flags:
        plt.plot(data.index[start:start+10], data['high'].iloc[start:start+10], 'r-', label='Flag')
    plt.legend()
    plt.show()

# 识别并绘制旗形形态
flags = identify_flags(data)
plot_flags(data, flags)
