#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import cgi
import cgitb
import psycopg2
from datetime import datetime
import pandas as pd
import os
import platform

# 启用错误显示
cgitb.enable()

# 数据库配置
DB_CONFIG = {
    "host": "localhost",
    "database": "usr",
    "user": "usr",
    "password": "usr",
    "port": "5432"
}

# 全局数据库连接
DB_CONN = None

def get_db_connection():
    """获取数据库连接"""
    global DB_CONN
    if DB_CONN is None or DB_CONN.closed:
        DB_CONN = psycopg2.connect(**DB_CONFIG)
    return DB_CONN


def generate_html_header():
    """生成HTML头部"""
    print("Content-Type: text/html; charset=utf-8\n\n")
    print("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>数据与行业查询</title>
        <meta charset="UTF-8">
        <style>
            body { font-family: Arial; margin: 20px; }
            .table-container { max-height: 600px; overflow: auto; margin-top: 20px; }
            table { border-collapse: collapse; width: 100%; }
            th, td { padding: 8px 12px; border: 1px solid #ddd; text-align: left; }
            th { background-color: #4CAF50; color: white; position: sticky; top: 0; }
            tr:nth-child(even) { background-color: #f2f2f2; }
            tr:hover { background-color: #ddd; }
            .stock-link { text-decoration: none; color: inherit; }
            .date-link { text-decoration: none; color: inherit; }
            .sort-link { text-decoration: none; color: inherit; }
            .industry-link { text-decoration: none; color: #0066cc; }
            .industry-link:hover { text-decoration: underline; }
            .header { margin-bottom: 20px; }
        </style>
        <script>
            document.addEventListener('DOMContentLoaded', function() {
                // 为所有日期链接添加点击事件
                document.querySelectorAll('.date-link').forEach(link => {
                    link.addEventListener('click', function() {
                        const stockCode = this.getAttribute('data-code');
                        const stockName = this.getAttribute('data-name');

                        // 发送请求到add_to_optional.cgi
                        fetch('add_to_optional.cgi?stock_code=' + encodeURIComponent(stockCode) + '&stock_name=' + encodeURIComponent(stockName))
                            .then(response => response.text())
                            .then(data => {
                                alert('已添加 ' + stockName + '(' + stockCode + ') 到自选股');
                            })
                            .catch(error => {
                                console.error('添加自选股失败:', error);
                                alert('添加自选股失败，请重试');
                            });
                    });
                });
            });
        </script>
    </head>
    <body>
    """)


def generate_html_footer():
    """生成HTML底部"""
    print("""
    </body>
    </html>
    """)


# 缓存最近日期
LATEST_DATE_CACHE = None

def get_latest_date():
    """获取iwencai_dde_table表中最近有数据的日期"""
    global LATEST_DATE_CACHE
    if LATEST_DATE_CACHE is not None:
        return LATEST_DATE_CACHE

    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # 获取最大日期
            cursor.execute("SELECT MAX(record_date) FROM iwencai_dde_table")
            latest_date = cursor.fetchone()[0]
            
            if not latest_date:
                return None
            
            # 检查该日期是否有数据
            max_retries = 30  # 最多检查30天前的数据
            current_date = latest_date
            for _ in range(max_retries):
                cursor.execute("SELECT COUNT(*) FROM iwencai_dde_table WHERE record_date = %s::date", [current_date])
                count = cursor.fetchone()[0]
                
                if count > 0:
                    # 找到有数据的日期
                    LATEST_DATE_CACHE = current_date
                    return current_date
                
                # 没有数据，检查前一天
                current_date = current_date - pd.Timedelta(days=1)
            
            # 超过重试次数仍未找到数据
            print(f"<p style='color:red'>未找到最近 {max_retries} 天内的有效数据</p>")
            return None
            
    except Exception as e:
        print(f"<p style='color:red'>获取最近日期错误: {str(e)}</p>")
        return None
    finally:
        if 'conn' in locals():
            conn.close()


def display_industry_overview(sort_column='stock_count', sort_order='DESC'):
    """显示行业概览统计"""
    try:
        # 获取最近日期
        latest_date = get_latest_date()
        if not latest_date:
            print("<p>无法获取最近日期</p>")
            return

        conn = get_db_connection()
        with conn.cursor() as cursor:
            # 构建行业统计查询
            query = ""
            query += "SELECT z.industry, COUNT(DISTINCT d.stock_code) as stock_count, "
            query += "       AVG(d.pct) as avg_pct, AVG(d.dde_net) as avg_dde_net, AVG(d.dde_net_all) as avg_dde_all, "
            query += "       AVG(p.pe_pct) as avg_pe_pct, AVG(f.ystz) as avg_ystz, AVG(f.sjltz) as avg_sjltz "
            query += "FROM iwencai_dde_table d "
            query += "LEFT JOIN ( "
            query += "    SELECT stock_code, industry, record_date, "
            query += "           ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY record_date DESC) as rn "
            query += "    FROM eastmoney_zlpm_table "
            query += "    WHERE record_date <= %s::date "
            query += " ) z ON d.stock_code = z.stock_code AND z.rn = 1 "
            query += "LEFT JOIN ( "
            query += "    SELECT stock_code, pe_pct, record_date, "
            query += "           ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY record_date DESC) as rn "
            query += "    FROM iwencai_pe_table "
            query += "    WHERE record_date <= %s::date "
            query += " ) p ON d.stock_code = p.stock_code AND p.rn = 1 "
            query += "LEFT JOIN ( "
            query += "    SELECT stock_code, ystz, sjltz, record_date, "
            query += "           ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY record_date DESC) as rn "
            query += "    FROM eastmoney_fina_table "
            query += "    WHERE record_date <= %s::date "
            query += " ) f ON d.stock_code = f.stock_code AND f.rn = 1 "
            query += "WHERE d.record_date = %s::date AND z.industry IS NOT NULL "
            query += "GROUP BY z.industry "

            # 处理排序逻辑
            valid_columns = ['stock_count', 'avg_pct', 'avg_dde_net', 'avg_dde_all', 'avg_pe_pct', 'avg_ystz', 'avg_sjltz']
            if sort_column in valid_columns:
                query += f"ORDER BY {sort_column} {sort_order} "
            else:
                query += "ORDER BY stock_count DESC "

            params = [latest_date, latest_date, latest_date, latest_date]
            cursor.execute(query, params)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            if not results:
                print(f"<p>未找到 {latest_date} 的行业数据</p>")
                return

            print(f"<div class='header'><h2>行业概览统计 - {latest_date}</h2></div>")

            # 生成表头
            print("<div class='table-container'><table><tr>")
            for col in columns:
                # 为可排序字段添加排序链接
                if col in valid_columns:
                    new_order = 'DESC' if (sort_column == col and sort_order == 'ASC') else 'ASC'
                    print(f"<th><a class='sort-link' href='?overview=1&sort_column={col}&sort_order={new_order}'>{col} {'↑' if new_order == 'DESC' else '↓'}</a></th>")
                else:
                    print(f"<th>{col}</th>")
            print("</tr>")

            # 输出数据
            for row in results:
                print("<tr>")
                for i, value in enumerate(row):
                    tmp_column = columns[i]
                    if tmp_column == 'industry':
                        print(f"<td><a class='industry-link' href='?industry={value}'>{value}</a></td>")
                    elif tmp_column == 'avg_pct':
                        if value is not None:
                            color = 'red' if value > 0 else 'green'
                            print(f"<td style='color:{color}'>{value:.2f}</td>")
                        else:
                            print(f"<td></td>")
                    elif tmp_column == 'avg_dde_net':
                        if value is not None:
                            if value > 100*1000*1000 or value < (-1) * 100*1000*1000:
                                value = value / (100*1000*1000)
                                print(f"<td>{value:.2f}亿</td>")
                            elif value > 10*1000 or value < (-1) * 10*1000:
                                value = value / (10*1000)
                                print(f"<td>{value:.2f}万</td>")
                            else:
                                print(f"<td>{value:.2f}</td>")
                        else:
                            print(f"<td></td>")
                    elif tmp_column in ['avg_pe_pct', 'avg_ystz', 'avg_sjltz']:
                        if value is not None:
                            print(f"<td>{value:.2f}</td>")
                        else:
                            print(f"<td></td>")
                    else:
                        print(f"<td>{value if value is not None else ''}</td>")
                print("</tr>")

            print("</table></div>")
            print(f"<p>共找到 {len(results)} 个行业数据</p>")

    except Exception as e:
        print(f"<p style='color:red'>数据库错误: {str(e)}</p>")
    finally:
        if 'conn' in locals():
            conn.close()

# 添加获取自选股列表的函数
def get_my_optional_stocks():
    """读取自选股列表"""
    # 检测操作系统设置文件路径
    current_os = platform.system()
    if current_os == 'Windows':
        optional_file = r'C:\Users\aaron\Documents\eastmoney\cgi-bin\my_optional.txt'
    else:
        optional_file = '/var/www/cgi-bin/my_optional.txt'
        
    stocks = []
    try:
        if os.path.exists(optional_file):
            with open(optional_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        parts = line.split(' ', 1)
                        if len(parts) >= 2:
                            stock_code = parts[0]
                            stock_name = parts[1]
                            # 去除可能的前缀（SH/SZ），因为数据库中可能只存储数字代码
                            if stock_code.startswith('SH'):
                                stock_code = stock_code[2:]
                            elif stock_code.startswith('SZ'):
                                stock_code = stock_code[2:]
                            stocks.append((stock_code, stock_name))
    except Exception as e:
        print(f"<p style='color:red'>读取自选股文件错误: {str(e)}</p>")
    
    return stocks

# 添加获取微妙选股列表的函数
def get_weimiao_stocks():
    """读取微妙选股列表"""
    # Linux环境下的文件路径
    weimiao_file = '/var/www/cgi-bin/weimiao.txt'
    
    stocks = []
    try:
        if os.path.exists(weimiao_file):
            with open(weimiao_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        parts = line.split(' ', 1)
                        if len(parts) >= 2:
                            stock_code = parts[0]
                            stock_name = parts[1]
                            # 去除可能的前缀（SH/SZ），因为数据库中可能只存储数字代码
                            if stock_code.startswith('SH'):
                                stock_code = stock_code[2:]
                            elif stock_code.startswith('SZ'):
                                stock_code = stock_code[2:]
                            stocks.append((stock_code, stock_name))
    except Exception as e:
        print(f"<p style='color:red'>读取微妙选股文件错误: {str(e)}</p>")
    
    return stocks

def display_results(sort_column=None, sort_order='ASC', industry=None, days_gt=None, pe_pct_lt=None, ystz_gt=None, sjltz_gt=None, pct_gt=None, dde_net_gt=None, dde_all_gt=None, amount_gt=None, holder_lt=None, optional_stocks=None, weimiao_stocks=None):
    """显示查询结果"""
    try:
        # 获取最近日期
        latest_date = get_latest_date()
        if not latest_date:
            print("<p>无法获取最近日期</p>")
            return

        conn = get_db_connection()
        with conn.cursor() as cursor:
            # 优化holder数据获取，一次获取多个记录
            query = ""
            query += "SELECT d.record_date, d.stock_code, d.stock_name, d.close, d.pct, d.dde_net, d.dde_net_all as dde_all, d.amount, d.rank, d.conti_day AS days, p.pe_pct, f.ystz, f.sjltz, "
            query += "       h.holder1, h.holder2, h.holder3, "
            query += "       z.industry "
            query += "FROM iwencai_dde_table d "
            query += "LEFT JOIN ( "
            query += "    SELECT stock_code, industry, record_date, "
            query += "           ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY record_date DESC) as rn "
            query += "    FROM eastmoney_zlpm_table "
            query += "    WHERE record_date <= %s::date "
            query += " ) z ON d.stock_code = z.stock_code AND z.rn = 1 "
            query += "LEFT JOIN ( "
            query += "    SELECT stock_code, pe_pct, record_date, "
            query += "           ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY record_date DESC) as rn "
            query += "    FROM iwencai_pe_table "
            query += "    WHERE record_date <= %s::date "
            query += " ) p ON d.stock_code = p.stock_code AND p.rn = 1 "
            query += "LEFT JOIN ( "
            query += "    SELECT stock_code, ystz, sjltz, record_date, "
            query += "           ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY record_date DESC) as rn "
            query += "    FROM eastmoney_fina_table "
            query += "    WHERE record_date <= %s::date "
            query += " ) f ON d.stock_code = f.stock_code AND f.rn = 1 "
            query += "LEFT JOIN ( "
            query += "    SELECT stock_code, "
            query += "           MAX(CASE WHEN rn=1 THEN holder_num_ratio END) as holder1, "
            query += "           MAX(CASE WHEN rn=2 THEN holder_num_ratio END) as holder2, "
            query += "           MAX(CASE WHEN rn=3 THEN holder_num_ratio END) as holder3 "
            query += "    FROM ( "
            query += "        SELECT stock_code, holder_num_ratio, record_date, "
            query += "               ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY record_date DESC) as rn "
            query += "        FROM eastmoney_holder_table "
            query += "        WHERE record_date <= %s::date "
            query += "    ) h_sub "
            query += "    GROUP BY stock_code "
            query += " ) h ON d.stock_code = h.stock_code "
            query += "WHERE d.record_date = %s::date "

            # 添加行业筛选条件
            params = [latest_date, latest_date, latest_date, latest_date, latest_date]
            if industry:
                query += "AND z.industry = %s "
                params.append(industry)

            # 添加自选股筛选条件
            if optional_stocks:
                stock_codes = [stock[0] for stock in optional_stocks]
                if stock_codes:
                    # 创建占位符
                    placeholders = ', '.join(['%s'] * len(stock_codes))
                    query += f"AND d.stock_code IN ({placeholders}) "
                    params.extend(stock_codes)

            # 添加微妙选股筛选条件
            if weimiao_stocks:
                stock_codes = [stock[0] for stock in weimiao_stocks]
                if stock_codes:
                    # 创建占位符
                    placeholders = ', '.join(['%s'] * len(stock_codes))
                    query += f"AND d.stock_code IN ({placeholders}) "
                    params.extend(stock_codes)

            # 添加自定义筛选条件
            if days_gt is not None and days_gt.strip():
                try:
                    days_value = float(days_gt)
                    query += "AND d.conti_day > %s "
                    params.append(days_value)
                except ValueError:
                    print("<p style='color:red'>days必须是数字</p>")

            if pe_pct_lt is not None and pe_pct_lt.strip():
                try:
                    pe_value = float(pe_pct_lt)
                    query += "AND p.pe_pct < %s "
                    params.append(pe_value)
                except ValueError:
                    print("<p style='color:red'>pe_pct必须是数字</p>")

            if ystz_gt is not None and ystz_gt.strip():
                try:
                    ystz_value = float(ystz_gt)
                    query += "AND f.ystz > %s "
                    params.append(ystz_value)
                except ValueError:
                    print("<p style='color:red'>ystz必须是数字</p>")

            if sjltz_gt is not None and sjltz_gt.strip():
                try:
                    sjltz_value = float(sjltz_gt)
                    query += "AND f.sjltz > %s "
                    params.append(sjltz_value)
                except ValueError:
                    print("<p style='color:red'>sjltz必须是数字</p>")

            # 添加新的筛选条件
            if pct_gt is not None and pct_gt.strip():
                try:
                    pct_value = float(pct_gt)
                    query += "AND d.pct > %s "
                    params.append(pct_value)
                except ValueError:
                    print("<p style='color:red'>pct必须是数字</p>")

            if dde_net_gt is not None and dde_net_gt.strip():
                try:
                    dde_net_value = float(dde_net_gt)
                    query += "AND d.dde_net > %s "
                    params.append(dde_net_value)
                except ValueError:
                    print("<p style='color:red'>dde_net必须是数字</p>")

            if dde_all_gt is not None and dde_all_gt.strip():
                try:
                    dde_all_value = float(dde_all_gt)
                    query += "AND d.dde_net_all > %s "
                    params.append(dde_all_value)
                except ValueError:
                    print("<p style='color:red'>dde_all必须是数字</p>")

            if amount_gt is not None and amount_gt.strip():
                try:
                    amount_value = float(amount_gt)
                    query += "AND d.amount > %s "
                    params.append(amount_value)
                except ValueError:
                    print("<p style='color:red'>amount必须是数字</p>")

            if holder_lt is not None and holder_lt.strip():
                try:
                    holder_value = float(holder_lt)
                    # holder1, holder2, holder3中只要有一个小于holder_value即可
                    query += "AND (h.holder1 < %s OR h.holder2 < %s OR h.holder3 < %s) "
                    params.extend([holder_value, holder_value, holder_value])
                except ValueError:
                    print("<p style='color:red'>holder必须是数字</p>")

            # 处理排序逻辑
            if sort_column:
                valid_columns = ['rank', 'stock_code', 'stock_name', 'close', 'pct', 'dde_net', 'dde_all', 'amount', 'industry', 'pe_pct', 'ystz', 'sjltz', 'days', 'holder1']
                if sort_column in valid_columns:
                    query += f"ORDER BY {sort_column} {sort_order}, stock_code "
                else:
                    query += "ORDER BY rank ASC, stock_code "
            else:
                query += "ORDER BY rank ASC, stock_code "
        
            query += " LIMIT 500"

            cursor.execute(query, params)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            if not results:
                print(f"<p>未找到 {latest_date} 的数据</p>")
                return

            # 显示当前筛选的标题
            if optional_stocks:
                print(f"<div class='header'><h2>我的自选股 - {latest_date}</h2></div>")
            elif weimiao_stocks:
                print(f"<div class='header'><h2>微淼选股 - {latest_date}</h2></div>")
            elif industry:
                print(f"<div class='header'><h2>行业: {industry} - {latest_date}</h2></div>")
            else:
                print(f"<div class='header'><h2>当日DDE数据与行业查询 - {latest_date}</h2></div>")

            # 生成表头
            print("<div class='table-container'><table><tr>")
            skip_holder = False
            for i, col in enumerate(columns):
                if skip_holder:
                    skip_holder = False
                    continue
                
                # 处理holder1, holder2, holder3，合并为一个holder列
                if col == 'holder1':
                    # 为holder列添加排序链接
                    new_order = 'DESC' if (sort_column == 'holder1' and sort_order == 'ASC') else 'ASC'
                    # 构建查询字符串，包含所有过滤参数
                    query_string = f"sort_column=holder1&sort_order={new_order}"
                    if industry:
                        query_string += f"&industry={industry}"
                    if days_gt:
                        query_string += f"&days_gt={days_gt}"
                    if pe_pct_lt:
                        query_string += f"&pe_pct_lt={pe_pct_lt}"
                    if ystz_gt:
                        query_string += f"&ystz_gt={ystz_gt}"
                    if sjltz_gt:
                        query_string += f"&sjltz_gt={sjltz_gt}"
                    # 添加新的筛选参数到查询字符串
                    if pct_gt:
                        query_string += f"&pct_gt={pct_gt}"
                    if dde_net_gt:
                        query_string += f"&dde_net_gt={dde_net_gt}"
                    if dde_all_gt:
                        query_string += f"&dde_all_gt={dde_all_gt}"
                    if amount_gt:
                        query_string += f"&amount_gt={amount_gt}"
                    if holder_lt:
                        query_string += f"&holder_lt={holder_lt}"
                    if optional_stocks:
                        query_string += "&my_optional=1"
                    # 添加微淼选股参数
                    if weimiao_stocks:
                        query_string += "&weimiao=1"
                    print(f"<th><a class='sort-link' href='?{query_string}'>holder {'↑' if new_order == 'DESC' else '↓'}</a></th>")
                elif col == 'holder2':
                    # 跳过holder2
                    continue
                elif col == 'holder3':
                    # 跳过holder3
                    continue
                else:
                    # 为其他可排序字段添加排序链接
                    if col in ['rank', 'pct', 'dde_net', 'dde_all', 'amount', 'industry', 'pe_pct', 'ystz', 'sjltz', 'days']:
                        new_order = 'DESC' if (sort_column == col and sort_order == 'ASC') else 'ASC'
                        # 构建查询字符串，包含所有过滤参数
                        query_string = f"sort_column={col}&sort_order={new_order}"
                        if industry:
                            query_string += f"&industry={industry}"
                        if days_gt:
                            query_string += f"&days_gt={days_gt}"
                        if pe_pct_lt:
                            query_string += f"&pe_pct_lt={pe_pct_lt}"
                        if ystz_gt:
                            query_string += f"&ystz_gt={ystz_gt}"
                        if sjltz_gt:
                            query_string += f"&sjltz_gt={sjltz_gt}"
                        # 添加新的筛选参数到查询字符串
                        if pct_gt:
                            query_string += f"&pct_gt={pct_gt}"
                        if dde_net_gt:
                            query_string += f"&dde_net_gt={dde_net_gt}"
                        if dde_all_gt:
                            query_string += f"&dde_all_gt={dde_all_gt}"
                        if amount_gt:
                            query_string += f"&amount_gt={amount_gt}"
                        if holder_lt:
                            query_string += f"&holder_lt={holder_lt}"
                        if optional_stocks:
                            query_string += "&my_optional=1"
                        # 添加微淼选股参数
                        if weimiao_stocks:
                            query_string += "&weimiao=1"
                        print(f"<th><a class='sort-link' href='?{query_string}'>{col} {'↑' if new_order == 'DESC' else '↓'}</a></th>")
                    else:
                        print(f"<th>{col}</th>")
            print("</tr>")

            # 输出数据
            html_buffer = []
            for row in results:
                html_buffer.append("<tr>")

                tmp_code = None
                tmp_name = None
                tmp_date = None
                for i, value in enumerate(row):
                    tmp_column = columns[i]
                    if tmp_column == 'record_date':
                        tmp_date = value
                        # 创建可点击的日期链接
                        html_buffer.append(f"<td><a href='javascript:void(0);' class='date-link' data-code='{row[columns.index('stock_code')]}' data-name='{row[columns.index('stock_name')]}'>{value}</a></td>")
                    elif tmp_column == "stock_code":
                        tmp_code = value
                        html_buffer.append(f"<td><a class='stock-link' href='iwencai_dde.cgi?stock_code={tmp_code}' target='_blank'>{tmp_code}</a></td>")
                    elif tmp_column == "stock_name":
                        tmp_name = value
                        if tmp_code and tmp_code[0] == "6":
                            html_buffer.append(f"<td><a class='stock-link' href='https://xueqiu.com/S/SH{tmp_code}' target='_blank'>{tmp_name}</a></td>")
                        elif tmp_code:
                            html_buffer.append(f"<td><a class='stock-link' href='https://xueqiu.com/S/SZ{tmp_code}' target='_blank'>{tmp_name}</a></td>")
                        else:
                            html_buffer.append(f"<td>{value if value is not None else ''}</td>")
                    elif "dde" in tmp_column or "amount" in tmp_column or "dde_net_all" in tmp_column:
                        if value and (value > 100*1000*1000 or value < (-1) * 100*1000*1000):
                            value = value / (100*1000*1000)
                            html_buffer.append(f"<td>{value:.2f}亿</td>")
                        elif value and (value > 10*1000 or value < (-1) * 10*1000):
                            value = value / (10*1000)
                            html_buffer.append(f"<td>{value:.2f}万</td>")
                        else:
                            html_buffer.append(f"<td>{value if value is not None else ''}</td>")
                    elif tmp_column == "pct":
                        if value is not None:
                            color = 'red' if value > 0 else 'green'
                            html_buffer.append(f"<td style='color:{color}'>{value:.2f}</td>")
                        else:
                            html_buffer.append(f"<td></td>")
                    elif tmp_column == "holder1":
                        # 处理holder1, holder2, holder3的值
                        holder1 = value
                        holder2 = row[i+1] if i+1 < len(row) else None
                        holder3 = row[i+2] if i+2 < len(row) else None

                        # 格式化holder值（全部去绝对值，根据原始值正负显示颜色）并添加超级链接
                        holder_parts = []

                        # 处理holder1
                        if holder1 is not None:
                            color = 'red' if holder1 > 0 else 'green'
                            # 生成雪球股东研究链接
                            if tmp_code and tmp_code[0] == "6":
                                holder_link = f"https://xueqiu.com/snowman/S/SH{tmp_code}/detail#/GDRS"
                            elif tmp_code:
                                holder_link = f"https://xueqiu.com/snowman/S/SZ{tmp_code}/detail#/GDRS"
                            else:
                                holder_link = "#"
                            holder_parts.append(f"<a href='{holder_link}' target='_blank' style='text-decoration: none;'><span style='color:{color}'>{abs(holder1):.2f}</span></a>")
                        else:
                            holder_parts.append("-")

                        # 添加holder1和holder2之间的连字符
                        holder_parts.append("-")

                        # 处理holder2
                        if holder2 is not None:
                            color = 'red' if holder2 > 0 else 'green'
                            # 生成雪球股东研究链接
                            if tmp_code and tmp_code[0] == "6":
                                holder_link = f"https://xueqiu.com/snowman/S/SH{tmp_code}/detail#/GDRS"
                            elif tmp_code:
                                holder_link = f"https://xueqiu.com/snowman/S/SZ{tmp_code}/detail#/GDRS"
                            else:
                                holder_link = "#"
                            holder_parts.append(f"<a href='{holder_link}' target='_blank' style='text-decoration: none;'><span style='color:{color}'>{abs(holder2):.2f}</span></a>")
                        else:
                            holder_parts.append("-")

                        # 添加holder2和holder3之间的连字符
                        holder_parts.append("-")

                        # 处理holder3
                        if holder3 is not None:
                            color = 'red' if holder3 > 0 else 'green'
                            # 生成雪球股东研究链接
                            if tmp_code and tmp_code[0] == "6":
                                holder_link = f"https://xueqiu.com/snowman/S/SH{tmp_code}/detail#/GDRS"
                            elif tmp_code:
                                holder_link = f"https://xueqiu.com/snowman/S/SZ{tmp_code}/detail#/GDRS"
                            else:
                                holder_link = "#"
                            holder_parts.append(f"<a href='{holder_link}' target='_blank' style='text-decoration: none;'><span style='color:{color}'>{abs(holder3):.2f}</span></a>")
                        else:
                            holder_parts.append("-")

                        holder_str = "".join(holder_parts)
                        html_buffer.append(f"<td>{holder_str}</td>")
                        continue
                    elif tmp_column == "holder2":
                        continue
                    elif tmp_column == "holder3":
                        continue
                    elif tmp_column == "pe_pct":
                        if value is not None:
                            html_buffer.append(f"<td><a class='stock-link' href='https://iwencai.com/unifiedwap/result?w=?{tmp_code}pe' target='_blank'>{value:.2f}</a></td>")
                        else:
                            html_buffer.append(f"<td></td>")
                    elif tmp_column == "industry":
                        if value is not None:
                            html_buffer.append(f"<td><a class='industry-link' href='?industry={value}'>{value}</a></td>")
                        else:
                            html_buffer.append(f"<td></td>")
                    elif tmp_column in ["ystz"]:
                        if value is not None:
                            if tmp_code[0] == "6":
                                html_buffer.append(f"<td><a class='stock-link' href='https://xueqiu.com/snowman/S/SH{tmp_code}/detail#/ZYCWZB' target='_blank'>{value:.2f}</a></td>")
                            else:
                                html_buffer.append(f"<td><a class='stock-link' href='https://xueqiu.com/snowman/S/SZ{tmp_code}/detail#/ZYCWZB' target='_blank'>{value:.2f}</a></td>")
                        else:
                            html_buffer.append(f"<td></td>")
                    elif tmp_column in ["sjltz"]:
                            html_buffer.append(f"<td><a class='stock-link' href='../sina_html/sina_{tmp_code}.html' target='_blank'>{value:.2f}</a></td>")
                    else:
                        html_buffer.append(f"<td>{value if value is not None else ''}</td>")

                html_buffer.append("</tr>")

            #添加这行代码以输出html_buffer中的内容
            print(''.join(html_buffer))
            print("</table></div>")
            print(f"<p>共找到 {len(results)} 条记录</p>")

    except Exception as e:
        print(f"<p style='color:red'>数据库错误: {str(e)}</p>")
    finally:
        if 'conn' in locals():
            conn.close()


def main():
    generate_html_header()

    form = cgi.FieldStorage()
    sort_column = form.getvalue('sort_column', '')
    sort_order = form.getvalue('sort_order', 'ASC').upper()
    if sort_order not in ['ASC', 'DESC']:
        sort_order = 'ASC'

    industry = form.getvalue('industry', '')
    overview = form.getvalue('overview', '')

    # 获取自选股参数
    my_optional = form.getvalue('my_optional', '')
    # 获取微妙选股参数
    weimiao = form.getvalue('weimiao', '')

    # 获取筛选参数
    days_gt = form.getvalue('days_gt', '')
    pe_pct_lt = form.getvalue('pe_pct_lt', '')
    ystz_gt = form.getvalue('ystz_gt', '')
    sjltz_gt = form.getvalue('sjltz_gt', '')
    # 确保正确初始化所有新的筛选参数
    pct_gt = form.getvalue('pct_gt', '')
    dde_net_gt = form.getvalue('dde_net_gt', '')
    dde_all_gt = form.getvalue('dde_all_gt', '')
    amount_gt = form.getvalue('amount_gt', '')
    holder_lt = form.getvalue('holder_lt', '')

    # 初始化自选股列表
    optional_stocks = None
    if my_optional:
        optional_stocks = get_my_optional_stocks()
        if not optional_stocks:
            print("<p style='color:red'>未找到自选股数据或自选股文件格式不正确</p>")
    
    # 初始化微妙选股列表
    weimiao_stocks = None
    if weimiao:
        weimiao_stocks = get_weimiao_stocks()
        if not weimiao_stocks:
            print("<p style='color:red'>未找到微妙选股数据或微妙选股文件格式不正确</p>")

    # 添加导航链接
    print("<div class='header'>")
    print("<a href='iwencai_dde.cgi'>DDE全字段查询</a> |")
    print("<a href='?'>当日DDE数据</a> | ")
    print("<a href='?overview=1'>行业概览统计</a> | ")
    print("<a href='?my_optional=1'>我的自选</a> |")
    print("<a href='?weimiao=1'>微淼选股</a>")
    print("</div>")

    # 添加筛选表单
    print("<div class='filter-form' style='margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px;'>")
    print("<form method='get'>")
    print(f"<input type='hidden' name='overview' value='{overview}'>")
    print(f"<input type='hidden' name='industry' value='{industry}'>")
    if my_optional:
        print("<input type='hidden' name='my_optional' value='1'>")
    if weimiao:
        print("<input type='hidden' name='weimiao' value='1'>")
    print("<table border='0'>")
    print("<tr>")
    print("<td>days &gt; </td>")
    print("<td><input type='text' name='days_gt' value='{}' placeholder='0'></td>".format(form.getvalue('days_gt', '')))
    print("<td>pe_pct &lt; </td>")
    print("<td><input type='text' name='pe_pct_lt' value='{}' placeholder='0'></td>".format(form.getvalue('pe_pct_lt', '')))
    #print("</tr>")
    #print("<tr>")
    print("<td>ystz &gt; </td>")
    print("<td><input type='text' name='ystz_gt' value='{}' placeholder='0'></td>".format(form.getvalue('ystz_gt', '')))
    print("<td>sjltz &gt; </td>")
    print("<td><input type='text' name='sjltz_gt' value='{}' placeholder='0'></td>".format(form.getvalue('sjltz_gt', '')))
    #print("</tr>")
    #print("<tr>")
    print("<td>pct &gt; </td>")
    print("<td><input type='text' name='pct_gt' value='{}' placeholder='0'></td>".format(form.getvalue('pct_gt', '')))

    print("</tr>")
    print("<tr>")
    
    print("<td>dde_net &gt; </td>")
    print("<td><input type='text' name='dde_net_gt' value='{}' placeholder='0'></td>".format(form.getvalue('dde_net_gt', '')))
    #print("</tr>")
    #print("<tr>")
    print("<td>dde_all &gt; </td>")
    print("<td><input type='text' name='dde_all_gt' value='{}' placeholder='0'></td>".format(form.getvalue('dde_all_gt', '')))
    print("<td>amount &gt; </td>")
    print("<td><input type='text' name='amount_gt' value='{}' placeholder='0'></td>".format(form.getvalue('amount_gt', '')))
    #print("</tr>")
    #print("<tr>")
    print("<td>holder &lt; </td>")
    print("<td><input type='text' name='holder_lt' value='{}' placeholder='0'></td>".format(form.getvalue('holder_lt', '')))
    print("<td colspan='2'></td>")
    print("</tr>")
    print("<tr>")
    
    print("<td colspan='4'><input type='submit' value='应用筛选'> <input type='button' value='重 置' onclick='location.href=\"?overview={}&industry={}\"'></td>".format(overview, industry))
    print("</tr>")
    print("</table>")
    print("</form>")
    print("</div>")

    if overview:
        display_industry_overview(sort_column, sort_order)
    else:
        display_results(sort_column, sort_order, industry, days_gt, pe_pct_lt, ystz_gt, sjltz_gt, pct_gt, dde_net_gt, dde_all_gt, amount_gt, holder_lt, optional_stocks, weimiao_stocks)

    generate_html_footer()

# 在main函数结束时关闭连接
if __name__ == '__main__':
    try:
        main()
    finally:
        if 'DB_CONN' in globals() and DB_CONN and not DB_CONN.closed:
            DB_CONN.close()



'''
wencai_dde_table的record_date和stock_code创建索引
CREATE INDEX idx_iwencai_dde_record_date ON iwencai_dde_table(record_date);
CREATE INDEX idx_iwencai_dde_stock_code ON iwencai_dde_table(stock_code);

-- 为eastmoney_zlpm_table的stock_code和record_date创建索引
CREATE INDEX idx_eastmoney_zlpm_stock_code ON eastmoney_zlpm_table(stock_code);
CREATE INDEX idx_eastmoney_zlpm_record_date ON eastmoney_zlpm_table(record_date);

-- 为eastmoney_holder_table的stock_code和record_date创建索引
CREATE INDEX idx_eastmoney_holder_stock_code ON eastmoney_holder_table(stock_code);
CREATE INDEX idx_eastmoney_holder_record_date ON eastmoney_holder_table(record_date);

'''
