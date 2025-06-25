#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import cgi
import cgitb
import psycopg2
from datetime import datetime

# 启用错误显示
cgitb.enable()

# 数据库配置（需修改为实际值）

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

def get_table_columns():
    """获取表字段信息"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'iwencai_dde_table'
                ORDER BY ordinal_position
            """)
            return cursor.fetchall()
    except Exception as e:
        print(f"<p style='color:red'>获取表结构错误: {str(e)}</p>")
        return []
    finally:
        if 'conn' in locals():
            conn.close()

def generate_html_form(start_date='', end_date='', stock_code=''):
    """生成查询表单"""
    print(f"""
    <h2>股票数据全字段查询</h2>
    <form method="post">
        <label>开始日期: <input type="date" name="start_date" value="{start_date}" required></label>
        <label>结束日期: <input type="date" name="end_date" value="{end_date}" required></label>
        <label>股票代码: <input type="text" name="stock_code" value="{stock_code}" placeholder="可留空查询全部"></label>
        <input type="submit" value="查询">
    </form>
    <hr>
    """)

def display_results(start_date, end_date, stock_code):
    """显示查询结果"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # 获取表结构用于动态生成表头
            columns = get_table_columns()
            if not columns:
                return
                
            # 构建动态查询条件
            query = "SELECT * FROM iwencai_dde_table WHERE record_date BETWEEN %s AND %s"
            params = [start_date, end_date]
            
            if stock_code:
                query += " AND stock_code = %s"
                params.append(stock_code)
                
            query += " ORDER BY record_date DESC, stock_code LIMIT 500"
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            if not results:
                print("<p>未找到匹配数据</p>")
                return
            
            # 动态生成表头
            print("<div class='table-container'><table><tr>")
            for col in columns:
                print(f"<th>{col[0]}</th>")
            print("</tr>")
            
            # 输出数据
            for row in results:
                print("<tr>")
                for value in row:
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
    print("Content-Type: text/html; charset=utf-8\n\n")
    print("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>股票全字段查询</title>
        <meta charset="UTF-8">
        <style>
            body { font-family: Arial; margin: 20px; }
            form { margin-bottom: 20px; background: #f5f5f5; padding: 15px; border-radius: 5px; }
            label { margin-right: 15px; }
            input[type="date"], input[type="text"] { padding: 5px; }
            input[type="submit"] { padding: 5px 15px; background: #4CAF50; color: white; border: none; border-radius: 3px; }
            .table-container { max-height: 600px; overflow: auto; margin-top: 20px; }
            table { border-collapse: collapse; width: 100%; }
            th, td { padding: 8px 12px; border: 1px solid #ddd; text-align: left; }
            th { background-color: #4CAF50; color: white; position: sticky; top: 0; }
            tr:nth-child(even) { background-color: #f2f2f2; }
            tr:hover { background-color: #ddd; }
        </style>
    </head>
    <body>
    """)
    
    form = cgi.FieldStorage()
    start_date = form.getvalue('start_date', '')
    end_date = form.getvalue('end_date', '')
    stock_code = form.getvalue('stock_code', '').upper()
    
    generate_html_form(start_date, end_date, stock_code)
    
    if start_date and end_date:
        display_results(start_date, end_date, stock_code)
    
    print("""
    </body>
    </html>
    """)

if __name__ == '__main__':
    main()

