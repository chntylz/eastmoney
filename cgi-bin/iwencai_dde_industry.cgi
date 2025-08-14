#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import cgi
import cgitb
import psycopg2
from datetime import datetime
import pandas as pd

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

def get_db_connection():
    """获取数据库连接"""
    return psycopg2.connect(**DB_CONFIG)


def generate_html_header():
    """生成HTML头部"""
    print("Content-Type: text/html; charset=utf-8\n\n")
    print("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>股票DDE数据与行业查询</title>
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
            .sort-link { text-decoration: none; color: inherit; }
            .industry-link { text-decoration: none; color: #0066cc; }
            .industry-link:hover { text-decoration: underline; }
            .header { margin-bottom: 20px; }
        </style>
    </head>
    <body>
    """)


def generate_html_footer():
    """生成HTML底部"""
    print("""
    </body>
    </html>
    """)


def get_latest_date():
    """获取iwencai_dde_table表中最近的日期"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(record_date) FROM iwencai_dde_table")
            latest_date = cursor.fetchone()[0]
            return latest_date
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
            query += "       AVG(d.pct) as avg_pct, AVG(d.dde_net) as avg_dde_net, "
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
            valid_columns = ['stock_count', 'avg_pct', 'avg_dde_net', 'avg_pe_pct', 'avg_ystz', 'avg_sjltz']
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
                            print(f"<td style='color:{color}'>{value:.2f}%</td>")
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
                            print(f"<td>{value:.2f}%</td>")
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

def display_results(sort_column=None, sort_order='ASC', industry=None):
    """显示查询结果"""
    try:
        # 获取最近日期
        latest_date = get_latest_date()
        if not latest_date:
            print("<p>无法获取最近日期</p>")
            return

        conn = get_db_connection()
        with conn.cursor() as cursor:
            # 构建查询，关联三张表
            query = ""
            query += "SELECT d.*, z.industry, p.pe_pct, f.ystz, f.sjltz "
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
            query += "WHERE d.record_date = %s::date "

            # 添加行业筛选条件
            params = [latest_date, latest_date, latest_date, latest_date]
            if industry:
                query += "AND z.industry = %s "
                params.append(industry)

            # 处理排序逻辑
            if sort_column:
                valid_columns = ['rank', 'stock_code', 'stock_name', 'close', 'pct', 'dde_net', 'amount', 'industry', 'pe_pct', 'ystz', 'sjltz', 'conti_day']
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

            # 显示当前筛选的行业
            if industry:
                print(f"<div class='header'><h2>行业: {industry} - {latest_date}</h2></div>")
            else:
                print(f"<div class='header'><h2>股票DDE数据与行业查询 - {latest_date}</h2></div>")

            # 生成表头
            print("<div class='table-container'><table><tr>")
            for col in columns:
                # 为可排序字段添加排序链接
                if col in ['rank', 'pct', 'dde_net', 'amount', 'industry', 'pe_pct', 'ystz', 'sjltz', 'conti_day']:
                    new_order = 'DESC' if (sort_column == col and sort_order == 'ASC') else 'ASC'
                    if industry:
                        print(f"<th><a class='sort-link' href='?sort_column={col}&sort_order={new_order}&industry={industry}'>{col} {'↑' if new_order == 'DESC' else '↓'}</a></th>")
                    else:
                        print(f"<th><a class='sort-link' href='?sort_column={col}&sort_order={new_order}'>{col} {'↑' if new_order == 'DESC' else '↓'}</a></th>")
                else:
                    print(f"<th>{col}</th>")
            print("</tr>")

            # 输出数据
            for row in results:
                print("<tr>")

                tmp_code = None
                tmp_name = None
                for i, value in enumerate(row):
                    tmp_column = columns[i]
                    if tmp_column == "stock_code":
                        tmp_code = value
                        print(f"<td><a class='stock-link' href='iwencai_dde.cgi?stock_code={tmp_code}' target='_blank'>{tmp_code}</a></td>")
                    elif tmp_column == "stock_name":
                        tmp_name = value
                        if tmp_code and tmp_code[0] == "6":
                            print(f"<td><a class='stock-link' href='https://xueqiu.com/S/SH{tmp_code}' target='_blank'>{tmp_name}</a></td>")
                        elif tmp_code:
                            print(f"<td><a class='stock-link' href='https://xueqiu.com/S/SZ{tmp_code}' target='_blank'>{tmp_name}</a></td>")
                        else:
                            print(f"<td>{value if value is not None else ''}</td>")
                    elif "dde" in tmp_column or "amount" in tmp_column:
                        if value and (value > 100*1000*1000 or value < (-1) * 100*1000*1000):
                            value = value / (100*1000*1000)
                            print(f"<td>{value:.2f}亿</td>")
                        elif value and (value > 10*1000 or value < (-1) * 10*1000):
                            value = value / (10*1000)
                            print(f"<td>{value:.2f}万</td>")
                        else:
                            print(f"<td>{value if value is not None else ''}</td>")
                    elif tmp_column == "pct":
                        if value is not None:
                            color = 'red' if value > 0 else 'green'
                            print(f"<td style='color:{color}'>{value:.2f}%</td>")
                        else:
                            print(f"<td></td>")
                    elif tmp_column == "pe_pct":
                        if value is not None:
                            print(f"<td>{value:.2f}%</td>")
                        else:
                            print(f"<td></td>")
                    elif tmp_column == "industry":
                        if value is not None:
                            print(f"<td><a class='industry-link' href='?industry={value}'>{value}</a></td>")
                        else:
                            print(f"<td></td>")
                    elif tmp_column in ["ystz", "sjltz"]:
                        if value is not None:
                            if tmp_code[0] == "6":
                                print(f"<td><a class='stock-link' href='https://xueqiu.com/snowman/S/SH{tmp_code}/detail#/ZYCWZB' target='_blank'>{value:.2f}%</a></td>")
                            else:
                                print(f"<td><a class='stock-link' href='https://xueqiu.com/snowman/S/SZ{tmp_code}/detail#/ZYCWZB' target='_blank'>{value:.2f}%</a></td>")
                        else:
                            print(f"<td></td>")
                    else:
                        print(f"<td>{value if value is not None else ''}</td>")

                print("</tr>")

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

    # 添加导航链接
    print("<div class='header'>")
    print("<a href='?'>股票DDE数据</a> | ")
    print("<a href='?overview=1'>行业概览统计</a>")
    print("</div>")

    if overview:
        display_industry_overview(sort_column, sort_order)
    else:
        display_results(sort_column, sort_order, industry)

    generate_html_footer()

if __name__ == '__main__':
    main()
