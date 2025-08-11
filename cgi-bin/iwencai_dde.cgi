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
    """获取表字段信息，包含添加的pe字段"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'iwencai_dde_table'
                ORDER BY ordinal_position
            """)
            columns = cursor.fetchall()
            # 添加pe字段到列列表
            columns.append(('pe_pct', 'float'))
            return columns
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
        <label>开始日期: <input type="date" name="start_date" value="{start_date}" placeholder="可留空查询全部"></label>
        <label>结束日期: <input type="date" name="end_date" value="{end_date}" placeholder="可留空查询全部"></label>
        <label>股票代码: <input type="text" name="stock_code" value="{stock_code}" placeholder="可留空查询全部"></label>
        <input type="submit" value="查询">
    </form>
    <hr>
    """)

def display_results(start_date, end_date, stock_code, sort_column=None, sort_order='ASC'):
    """显示查询结果"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # 获取表结构用于动态生成表头
            columns = get_table_columns()
            if not columns:
                return
                
            # 构建动态查询条件
            # 构建主查询，包含HData_iwencai_pe表中最近的pe值
            query = """
                SELECT d.*, p.pe_pct AS pe
                FROM iwencai_dde_table d
                LEFT JOIN (
                    SELECT stock_code, pe_pct, record_date,
                           ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY record_date DESC) as rn
                    FROM iwencai_pe_table
                ) p ON d.stock_code = p.stock_code AND p.rn = 1
            """

            params = []
            if start_date:
                query += " WHERE d.record_date BETWEEN %s::date AND %s::date"
                params = [start_date, end_date]
            
            if stock_code :
                if start_date:
                    if stock_code.isdigit():
                       query += " AND d.stock_code = %s"
                    else:
                        query += " AND d.stock_name = %s"

                    params.append(stock_code)
                else:
                    if stock_code.isdigit():
                       query += " WHERE d.stock_code = %s"
                    else:
                        query += " WHERE d.stock_name = %s"
                        
                    params.append(stock_code)
            else:
                pass
                
            # 处理排序逻辑
            if sort_column:
                # 确保排序字段是表中存在的列
                valid_columns = [col[0] for col in columns]
                if sort_column in valid_columns:
                    query += f" ORDER BY {sort_column} {sort_order}, record_date DESC, stock_code"
                else:
                    query += " ORDER BY record_date DESC, rank ASC, stock_code"
            else:
                query += " ORDER BY record_date DESC, rank ASC, stock_code"
            query += " LIMIT 500"

            print(query)
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            if not results:
                print("<p>未找到匹配数据</p>")
                return
            
            # 动态生成表头
            print("<div class='table-container'><table><tr>")
            for col in columns:
                col_name = col[0]
                # 为 dde_net 和 conti_day 添加排序链接
                if col_name in ['dde_net', 'conti_day', 'pct', 'pe_pct']:
                    # 切换排序方向
                    new_order = 'DESC' if (sort_column == col_name and sort_order == 'ASC') else 'ASC'
                    print(f"<th><a href='?start_date={start_date}&end_date={end_date}&stock_code={stock_code}&sort_column={col_name}&sort_order={new_order}'>{col_name} ({'↑' if new_order == 'DESC' else '↓'})</a></th>")
                else:
                    print(f"<th>{col_name}</th>")
            print("</tr>")
            
            # 输出数据
            for row in results:
                print("<tr>")

                tmp_code = None
                tmp_name = None
                for i, value in enumerate(row):
                    tmp_column = columns[i][0] 
                    if tmp_column == "stock_code":  # 判断是否为股票代码列  xueqiu link
                        tmp_code = value
                        print(f"<td><a class='stock-link' href='iwencai_dde.cgi?stock_code={tmp_code}' target='_blank'>{tmp_code}</a></td>")

                    elif ("dde" in tmp_column or "amount" in tmp_column):  # 判断是否包含dde
                        if value > 100*1000*1000 or value < (-1) * 100*1000*1000 :  #亿
                            value = value / (100*1000*1000)
                            print(f"<td>{value if value is not None else ''}亿</td>")
                        elif value > 10*1000 or value < (-1) * 10*1000:  #万
                            value = value / (10*1000)
                            print(f"<td>{value if value is not None else ''}万</td>")
                        else:
                            print(f"<td>{value if value is not None else ''}</td>")

                    elif tmp_column == "stock_name":  # 判断是否为股票代码列
                        tmp_name = value
                        if tmp_code[0] == "6":
                            print(f"<td><a class='stock-link' href='https://xueqiu.com/S/SH{tmp_code}' target='_blank'>{tmp_name}</a></td>")
                        else:
                            print(f"<td><a class='stock-link' href='https://xueqiu.com/S/SZ{tmp_code}' target='_blank'>{tmp_name}</a></td>")
                    
                    elif tmp_column == "pe_pct":  # 判断是否为股票代码列  iwencai_pe link
                        print(f"<td><a class='stock-link' href='https://iwencai.com/unifiedwap/result?w=?{tmp_code}pe' target='_blank'>{value}</a></td>")
                        
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
    
    #if start_date and end_date:
    # 获取排序参数
    sort_column = form.getvalue('sort_column', '')
    sort_order = form.getvalue('sort_order', 'ASC').upper()
    if sort_order not in ['ASC', 'DESC']:
        sort_order = 'ASC'
    display_results(start_date, end_date, stock_code, sort_column, sort_order)
    
    print("""
    </body>
    </html>
    """)

if __name__ == '__main__':
    main()

