# -*- coding: utf-8 -*-

from HData_eastmoney_day import *
import talib
from talib import abstract
import pandas as pd
import numpy as np
from datetime import datetime
import time
import multiprocessing

from HData_pattern import *


# TA-Lib模式说明字典
pattern_descriptions = {
    'CDL2CROWS': '两只乌鸦\t看跌信号：一根阳线后接两根小阴线，第二根阴线开盘价低于第一根，显示上涨乏力，空头反扑。',
    'CDL3BLACKCROWS': '三只乌鸦\t强烈看跌：连续三根大阴线，每天低开低走，常出现在顶部，预示趋势反转下跌。',
    'CDL3INSIDE': '三内部上涨/下跌\t包含关系的三根K线，第三根突破前两根范围，表示趋势延续或反转（方向由整体位置决定）。',
    'CDL3LINESTRIKE': '三线打击\t三根同向K线（如三阳），第四根跳空低开并收大阴，完全吞噬前三根，强烈反转信号（看跌）。',
    'CDL3OUTSIDE': '三外部上涨/下跌\t第三根K线完全包裹前一根（类似吞没），且三根构成突破结构，趋势延续或反转信号。',
    'CDL3STARSINSOUTH': '南方三星\t看涨信号：出现在下跌末期，三根小阴线逐步下移但下影线变长，显示卖压减弱。',
    'CDL3WHITESOLDIERS': '三个白兵（三只红乌鸦）\t强烈看涨：连续三根大阳线，每天高开高走，出现在底部，预示上涨趋势开始。',
    'CDLABANDONEDBABY': '弃婴形态\t反转信号：十字星与前后K线有跳空缺口，孤立存在，如顶部弃婴（看跌）、底部弃婴（看涨）。',
    'CDLADVANCEBLOCK': '上升三部曲（进三退一失败）\t看跌预警：三根阳线后出现上影线变长、实体变小，显示买方力量衰竭。',
    'CDLBELTHOLD': '执带线\t开盘即最高/最低价，收盘远离开盘，形成大实体。下影执带为看涨，上影执带为看跌。',
    'CDLBREAKAWAY': '跳空脱离\t一种跳空形态，常出现在盘整后突破，趋势启动信号（向上跳空看涨，向下看跌）。',
    'CDLCLOSINGMARUBOZU': '收盘光头光脚（收盘秃头线）\t实体无上下影，收盘价即最高或最低价，显示单边力量强。',
    'CDLCONCEALBABYSWALL': '隐藏的婴儿吞没\t四根K线组合，前三根为下跌中的小实体带下影，第四根大阴吞没，看跌延续。',
    'CDLCOUNTERATTACK': '反击线\t两根相反颜色K线，收盘价相近，显示多空平衡，可能预示趋势停滞或反转。',
    'CDLDARKCLOUDCOVER': '乌云盖顶\t顶部看跌：阳线后接阴线，且阴线深入阳线实体一半以上，显示空头反攻。',
    'CDLDOJI': '十字星\t开盘=收盘，上下影可长可短，代表多空僵持，常为变盘信号。',
    'CDLDOJISTAR': '十字星信号\t单根十字星出现在趋势末端，配合跳空，增强反转意义。',
    'CDLDRAGONFLYDOJI': '蜻蜓十字\t十字星，下影极长，上影无，底部看涨，显示下探后买盘强劲。',
    'CDLENGULFING': '吞没形态\t分多头吞没（阳包阴）和空头吞没（阴包阳），强烈反转信号。',
    'CDLEVENINGDOJISTAR': '暮星十字\t顶部三根K线：阳线 → 十字星（跳空）→ 阴线，强烈看跌反转。',
    'CDLEVENINGSTAR': '暮星\t类似暮星十字，但第二根是小实体而非十字，顶部反转信号。',
    'CDLGAPSIDESIDEWHITE': '并列白色跳空线\t两根跳空并列阳线，若被跌破，可能看跌反转。',
    'CDLGRAVESTONEDOJI': '墓碑十字\t十字星，上影极长，下影无，顶部看跌，显示冲高失败。',
    'CDLHAMMER': '锤头线\t出现在下跌末期，小实体在顶部，长下影，强烈看涨反转信号。',
    'CDLHANGINGMAN': '上吊线\t形状同锤头，但出现在上涨末期，看跌反转信号。',
    'CDLHARAMI': '孕线形态\t大K线后接小K线，小K线实体被包含，显示趋势放缓，可能反转。',
    'CDLHARAMICROSS': '十字孕线\t孕线中第二根为十字星，反转信号更强。',
    'CDLHIGHWAVE': '高浪线\t实体小，上下影都很长，显示市场犹豫，变盘前兆。',
    'CDLHIKKAKE': '刺透形态（改进型）\t一种突破形态，原趋势中短暂反向后继续原方向，趋势延续信号。',
    'CDLHIKKAKEMOD': '改良刺透形态\t对刺透形态的优化版本，过滤假信号。',
    'CDLHOMINGPIGEON': '归巢鸽\t两根阴线，第二根完全在第一根内，显示下跌减速，看跌减弱信号。',
    'CDLIDENTICAL3CROWS': '相同三只乌鸦\t三根几乎一样的阴线，每天低开低收，加速下跌信号。',
    'CDLINNECK': '颈线位\t下跌中两根K线：大阴 → 小阳，小阳收盘接近前阴最低价，看跌延续。',
    'CDLINVERTEDHAMMER': '倒锤头\t小实体在下，长上影，出现在下跌末期，潜在看涨反转。',
    'CDLKICKING': '踢出形态\t两根跳空的光头光脚K线，方向相反，强烈反转信号。',
    'CDLKICKINGBYLENGTH': '按长度踢出\t根据K线长度判断踢出方向，长阳踢出看涨，长阴踢出看跌。',
    'CDLLADDERBOTTOM': '梯底形态\t下跌中连续三根阴线，后两根低点略低但实体缩短，第四根跳空阳线，看涨反转。',
    'CDLLONGLEGGEDDOJI': '长腿十字\t十字星，上下影均很长，显示剧烈震荡，变盘信号。',
    'CDLLONGLINE': '长线蜡烛\t实体很长，显示单边力量极强，趋势延续信号。',
    'CDLMARUBOZU': '光头光脚线\t无上下影的大实体K线，阳线看涨，阴线看跌。',
    'CDLMATCHINGLOW': '相同低价\t两根大阴线，第二根跳空再收回至第一根收盘价附近，底部支撑信号。',
    'CDLMATHOLD': '持续形态（Mat Hold）\t上涨中回调三日，不破前低，第四日回升，上升中继，看涨延续。',
    'CDLMORNINGDOJISTAR': '晨星十字\t底部三根K线：大阴 → 十字星（跳空）→ 大阳，强烈看涨反转。',
    'CDLMORNINGSTAR': '晨星\t类似晨星十字，第二根为小实体，底部反转信号。',
    'CDLONNECK': '一颈线\t类似In-Neck，第二根阳线收盘紧贴前阴最低价，看跌延续。',
    'CDLPIERCING': '刺透形态\t下跌末期，大阴后接大阳，阳线收盘深入阴线实体50%以上，看涨反转。',
    'CDLRICKSHAWMAN': '三轮车夫（Rickshaw Man）\t十字星，上下影极长且对称，市场极度犹豫，变盘前兆。',
    'CDLRISEFALL3METHODS': '上升/下降三法\t上涨中三根小阴回调，不破前低，然后大阳突破，看涨延续；反之亦然。',
    'CDLSEPARATINGLINES': '分道扬镳线\t两根同向K线，中间有跳空，但实体方向一致，趋势延续信号。',
    'CDLSHOOTINGSTAR': '射击之星\t小实体在下，长上影，出现在上涨末期，看跌反转信号。',
    'CDLSHORTLINE': '短线蜡烛\t实体很短，显示市场波动小，趋势可能暂停。',
    'CDLSPINNINGTOP': '纺锤线\t小实体，上下影大致相等，多空平衡，趋势可能反转。',
    'CDLSTALLEDPATTERN': '停滞形态\t上涨中三根阳线，后两根实体变小且带长上影，买力不足，可能反转。',
    'CDLSTICKSANDWICH': '棒形三明治\t三根K线，首尾同色，中间异色且短，反转信号（如两阳夹一阴为看涨）。',
    'CDLTAKURI': '探试线\t即蜻蜓十字 + 极长下影，出现在下跌后，强烈看涨信号。',
    'CDLTASUKIGAP': '跳空并列线\t两根同向跳空K线，第三根反向但未回补缺口，若回补则反转信号。',
    'CDLTHRUSTING': '推入形态\t下跌中，大阴后接阳线，阳线收盘在阴线中部以下，看跌延续。',
    'CDLTRISTAR': '三星形态\t三根十字星连续出现，尤其在顶部或底部，强烈反转信号。',
    'CDLUNIQUE3RIVER': '独特三河底\t下跌末期，第三根为长下影小阳，低点略低但收盘回升，看涨反转。',
    'CDLUPSIDEGAP2CROWS': '跳空双乌鸦\t上涨中跳空阳线后接两根阴线，第二根跳空低开，看跌反转。',
    'CDLXSIDEGAP3METHODS': '跳空三法\t上涨中跳空后三根小阴，不回补缺口，然后大阳创新高，看涨延续。'
}

# 修复后的HData_eastmoney_day类，主要修复get_latest_data_from_hdata方法中的bug
class HData_eastmoney_day_fixed(HData_eastmoney_day):
    def get_latest_data_from_hdata(self):
        """获取所有股票的最新数据并返回pandas DataFrame格式"""
        self.db_connect()
        sql_temp = ' select * from eastmoney_d_table where record_date = '\
                + '(select max(record_date) from eastmoney_d_table as tmp_date); '

        self.cur.execute(sql_temp)
        rows = self.cur.fetchall()
        
        dataframe_cols = [tuple[0] for tuple in self.cur.description]  # 列名和数据库列一致
        df = pd.DataFrame(rows, columns=dataframe_cols)
        df['record_date'] = df['record_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        
        self.conn.commit()
        self.db_disconnect()
        
        return df

# 获取所有股票代码的函数
def get_all_stock_codes():
    """从数据库获取所有股票代码"""
    hdata = HData_eastmoney_day("usr", "usr")
    hdata.db_connect()
    
    # 查询所有不重复的股票代码
    sql_temp = "SELECT DISTINCT stock_code FROM eastmoney_d_table;"
    hdata.cur.execute(sql_temp)
    rows = hdata.cur.fetchall()
    
    # 提取股票代码到列表
    stock_codes = [row[0] for row in rows]
    
    hdata.conn.commit()
    hdata.db_disconnect()
    
    return stock_codes

# 计算单个股票的模式
def calculate_stock_patterns(stock_code, hdata, end_date=None, limit=700):
    """计算单个股票的TA-Lib模式"""
    try:
        # 获取股票的历史数据
        detail_info = hdata.get_data_from_hdata(stock_code=stock_code, 
                                                end_date=end_date, 
                                                limit=limit)
        
        if len(detail_info) == 0:
            return None
        
        # 准备用于TA-Lib计算的数据
        my_open = np.array(detail_info['open'])
        my_high = np.array(detail_info['high'])
        my_low = np.array(detail_info['low'])
        my_close = np.array(detail_info['close'])
        
        # 获取所有模式识别函数
        pattern_functions = talib.get_function_groups()['Pattern Recognition']
        
        # 存储该股票的模式
        patterns = []
        
        # 遍历所有模式识别函数并计算
        for name in pattern_functions:
            try:
                func = abstract.Function(name)
                signal_series = func(my_open, my_high, my_low, my_close)
                
                # 只记录最新非零的信号
                if signal_series[-1] != 0:
                    patterns.append({
                        'function_name': name,
                        'display_name': func.info['display_name'],
                        'value': signal_series[-1]
                    })
            except Exception as e:
                print(f"计算模式 {name} 时出错: {str(e)}")
        
        # 获取股票名称（如果有）
        stock_name = detail_info['stock_name'].iloc[0] if 'stock_name' in detail_info.columns and len(detail_info) > 0 else '未知'
        
        return {
            'stock_code': stock_code,
            'stock_name': stock_name,
            'patterns': patterns,
            'record_date': detail_info['record_date'].iloc[-1] if 'record_date' in detail_info.columns else '未知'
        }
        
    except Exception as e:
        print(f"处理股票 {stock_code} 时出错: {str(e)}")
        return None

# Worker函数用于多进程处理
def worker(params):
    """多进程Worker函数，处理单个股票"""
    stock_code, latest_date, limit = params
    # 每个进程创建自己的数据库连接
    try:
        hdata = HData_eastmoney_day("usr", "usr")
        result = calculate_stock_patterns(stock_code, hdata, end_date=latest_date, limit=limit)
        return result
    except Exception as e:
        print(f"Worker处理股票 {stock_code} 时出错: {str(e)}")
        return None

# 主函数
def main():
    start_time = time.time()
    
    # 获取所有股票代码
    print("正在获取所有股票代码...")
    stock_codes = get_all_stock_codes()
    print(f"共获取到 {len(stock_codes)} 支股票")
    
    # 获取最新日期
    latest_data = HData_eastmoney_day_fixed("usr", "usr").get_latest_data_from_hdata()
    latest_date = latest_data['record_date'].iloc[0] if not latest_data.empty else datetime.now().strftime('%Y-%m-%d')
    print(f"使用最新日期: {latest_date}")
    
    # 准备多进程参数
    limit = 700
    params_list = [(stock_code, latest_date, limit) for stock_code in stock_codes]
    
    # 使用多进程处理
    processes = multiprocessing.cpu_count()
    print(f"使用 {processes} 个进程进行并行处理")
    
    all_results = []
    total = len(stock_codes)
    
    with multiprocessing.Pool(processes=processes) as pool:
        # 使用imap来获取进度
        for i, result in enumerate(pool.imap(worker, params_list)):
            # 显示进度
            if (i + 1) % 10 == 0 or i + 1 == total:
                print(f"已处理 {i + 1}/{total} 支股票 ({(i + 1) / total * 100:.2f}%)")
            
            if result:
                all_results.append(result)
    
    # 输出结果
    df = output_results(all_results)

    
    end_time = time.time()
    print(f"处理完成，耗时 {end_time - start_time:.2f} 秒")

    return df

# 输出结果到CSV文件和控制台
def output_results(results):
    df = pd.DataFrame()
    
    pattern_cols = ['stock_code', 'stock_name', 'record_date', 'pat_func', \
                   'pat_name', 'sig_value', 'pat_des'] 

    """输出结果到CSV文件和控制台"""
    # 准备输出数据
    output_data = []
    
    for result in results:
        stock_code = result['stock_code']
        stock_name = result['stock_name']
        record_date = result['record_date']
        
        # 如果有模式，输出每一个模式
        if result['patterns']:
            for pattern in result['patterns']:
                # 获取模式说明，如果不存在则设为"未知模式"
                pattern_desc = pattern_descriptions.get(pattern['function_name'], '未知模式')
                
                output_data.append({
                    '股票代码': stock_code,
                    '股票名称': stock_name,
                    '记录日期': record_date,
                    '模式函数名': pattern['function_name'],
                    '模式显示名': pattern['display_name'],
                    '信号值': pattern['value'],
                    '模式说明': pattern_desc
                })
                
            # 在控制台显示有模式的股票
            pattern_names = ', '.join([p['display_name'] for p in result['patterns']])
            print(f"股票: {stock_code}({stock_name}) - 模式: {pattern_names}")
        
    # 将结果保存到CSV文件
    if output_data:
        df = pd.DataFrame(output_data)
        df.columns = pattern_cols
        tmp_date = datetime.datetime.now().date().strftime('%Y%m%d_%H%M%S')
        output_file = f"./csv/stock_patterns_{tmp_date}.csv"
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"所有模式结果已保存到 {output_file}")
    
    # 统计信息
    total_stocks = len(results)
    pattern_stocks = sum(1 for r in results if r['patterns'])
    print(f"总股票数: {total_stocks}")
    print(f"有模式信号的股票数: {pattern_stocks} ({pattern_stocks / total_stocks * 100:.2f}%)")
    return df

def check_table():
    table_exist = hdata_pat.table_is_exist() 
    my_dbg('table_exist=%d' % table_exist)
    if table_exist:
        #hdata_pat.db_hdata_pattern_create()
        my_dbg('table already exist, recreate')
    else:
        hdata_pat.db_hdata_pattern_create()
        my_dbg('table not exist, create')


if __name__ == "__main__":

    hdata_pat = HData_pattern("usr","usr")

    #check table exist
    check_table()

    pat_df = main()
    #delete suspend item
    pat_df = pat_df[pat_df['record_date'] == datetime.datetime.now().date().strftime("%Y-%m-%d")] 
    hdata_pat.delete_data_from_hdata(
            start_date=datetime.datetime.now().date().strftime("%Y-%m-%d"),  # 保持不变
            end_date=datetime.datetime.now().date().strftime("%Y-%m-%d")    # 保持不变
            )
    hdata_pat.copy_from_stringio(pat_df)

