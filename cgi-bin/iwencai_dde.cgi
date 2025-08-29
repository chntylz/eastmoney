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
    """定义表字段信息，包含添加的pe、industry和holder字段"""
    # 直接返回我们查询中使用的列
    return [
        ('record_date', 'date'),
        ('stock_code', 'text'),
        ('stock_name', 'text'),
        ('close', 'float'),
        ('pct', 'float'),
        ('dde_net', 'float'),
        ('dde_all', 'float'),  # 添加 dde_all 字段
        ('amount', 'float'),
        ('rank', 'integer'),
        ('days', 'integer'),
        ('pe', 'float'),  # 与SQL查询中的别名匹配
        ('ystz', 'float'),
        ('sjltz', 'float'),
        ('holder1', 'float'),
        ('holder2', 'float'),
        ('holder3', 'float'),
        ('industry', 'text')
    ]

def generate_html_form(start_date='', end_date='', stock_code=''):
    """生成查询表单"""
    print(f"""
    <a href='iwencai_dde.cgi'>DDE全字段查询</a> | 
    <a href='iwencai_dde_industry.cgi?'>当日DDE数据 </a> |
    <a href='iwencai_dde_industry.cgi?overview=1'>行业概览统计</a> |
    <a href='iwencai_dde_industry.cgi?my_optional=1'>我的自选</a>
 
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
                SELECT d.record_date, d.stock_code, d.stock_name, d.close, d.pct, d.dde_net, d.dde_net_all AS dde_all, d.amount, d.rank, d.conti_day AS days, p.pe_pct AS pe, f.ystz, f.sjltz, h1.holder1, h2.holder2, h3.holder3, z.industry 
                FROM iwencai_dde_table d
                LEFT JOIN (
                    SELECT stock_code, pe_pct, record_date,
                           ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY record_date DESC) as rn
                    FROM iwencai_pe_table
                ) p ON d.stock_code = p.stock_code AND p.rn = 1
                LEFT JOIN (
                    SELECT stock_code, ystz, sjltz, record_date,
                           ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY record_date DESC) as rn
                    FROM eastmoney_fina_table
                ) f ON d.stock_code = f.stock_code AND f.rn = 1
                LEFT JOIN (
                    SELECT stock_code, industry, record_date,
                           ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY record_date DESC) as rn
                    FROM eastmoney_zlpm_table
                ) z ON d.stock_code = z.stock_code AND z.rn = 1
                LEFT JOIN (
                     SELECT * FROM (
                         SELECT stock_code, holder_num_ratio AS holder1, record_date,
                                ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY record_date DESC) as rn
                         FROM eastmoney_holder_table
                     ) sub WHERE sub.rn = 1
                 ) h1 ON d.stock_code = h1.stock_code AND h1.rn = 1
                LEFT JOIN (
                     SELECT * FROM (
                         SELECT stock_code, holder_num_ratio AS holder2, record_date,
                                ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY record_date DESC) as rn
                         FROM eastmoney_holder_table
                     ) sub WHERE sub.rn = 2
                 ) h2 ON d.stock_code = h2.stock_code AND h2.rn = 2
                LEFT JOIN (
                     SELECT * FROM (
                         SELECT stock_code, holder_num_ratio AS holder3, record_date,
                                ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY record_date DESC) as rn
                         FROM eastmoney_holder_table
                     ) sub WHERE sub.rn = 3
                 ) h3 ON d.stock_code = h3.stock_code AND h3.rn = 3
            """

            # 初始化参数列表
            params = []

            # 构建查询条件
            where_clauses = []

            # 添加iwencai_dde_table的日期条件
            if start_date:
                where_clauses.append("d.record_date BETWEEN %s::date AND %s::date")
                params.extend([start_date, end_date])

            # 添加股票代码条件
            if stock_code:
                if stock_code.isdigit():
                    where_clauses.append("d.stock_code = %s")
                else:
                    where_clauses.append("d.stock_name = %s")
                params.append(stock_code)

            # 组合WHERE子句
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
            else:
                # 如果没有其他条件，我们需要确保z.record_date的条件被应用
                today = datetime.now().strftime('%Y-%m-%d')
                query += " WHERE z.record_date <= %s::date"
                params.append(today)
            

                
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

            #print(query)
            
            #print(f"<p>修复后的查询: {query}</p>")
            #print(f"<p>修复后的参数: {params}</p>")
            #print(f"<p>占位符数量: {query.count('%s')}</p>")
            #print(f"<p>参数数量: {len(params)}</p>")
            cursor.execute(query, params)
            results = cursor.fetchall()
            #print(f"<p>查询结果行数: {len(results)}</p>")
            #print(f"<p>查询结果列数: {len(results[0]) if results else 0}</p>")
            #print(f"<p>列定义数: {len(columns)}</p>")
            if results and len(results) > 0:
                #print(f"<p>第一行数据: {results[0]}</p>")
                pass
            
            if not results:
                print("<p>未找到匹配数据</p>")
                return
            
            # 动态生成表头
            print("<div class='table-container'><table><tr>")
            for col in columns:
                col_name = col[0]

                if col_name == "holder2":
                    continue
                elif col_name == "holder3":
                    continue

                # 为 dde_net 和 days 添加排序链接
                if col_name in ['rank', 'dde_net', 'dde_all', 'amount', 'days', 'pct', 'pe', 'ystz', 'sjltz', 'industry']:
                    # 切换排序方向
                    new_order = 'DESC' if (sort_column == col_name and sort_order == 'ASC') else 'ASC'
                    print(f"<th><a class='sort-link' href='?start_date={start_date}&end_date={end_date}&stock_code={stock_code}&sort_column={col_name}&sort_order={new_order}'>{col_name} {'↑' if new_order == 'DESC' else '↓'}</a></th>")
                else:
                    print(f"<th>{col_name}</th>")
            print("</tr>")
            
            # 输出数据
            for row in results:
                print("<tr>")

                tmp_code = None
                tmp_name = None
                for i, value in enumerate(row):
                    if i < len(columns):
                        tmp_column = columns[i][0]
                    else:
                        tmp_column = f"unknown_column_{i}"
                        print(f"<p style='color:red'>警告: 结果列索引{i}超出了columns列表范围</p>") 
                    if tmp_column == "stock_code":  # 判断是否为股票代码列  xueqiu link
                        tmp_code = value
                        print(f"<td><a class='stock-link' href='iwencai_dde.cgi?stock_code={tmp_code}' target='_blank'>{tmp_code}</a></td>")

                    elif ("dde" in tmp_column or "amount" in tmp_column or "dde_all" in tmp_column):  # 判断是否包含dde
                        
                            if value is not None:
                                if value > 100*1000*1000 or value < (-1) * 100*1000*1000 :  #亿
                                    value = value / (100*1000*1000)
                                    print(f"<td>{value}亿</td>")
                                elif value > 10*1000 or value < (-1) * 10*1000:  #万
                                    value = value / (10*1000)
                                    print(f"<td>{value}万</td>")
                                else:
                                    print(f"<td>{value}</td>")
                            else:
                                print(f"<td></td>")

                    elif tmp_column == "pct":  # 为pct字段添加颜色显示逻辑
                        if value is not None:
                            color = 'red' if value > 0 else 'green'
                            print(f"<td><span style='color:{color}'>{value:.2f}</span></td>")
                        else:
                            print(f"<td></td>")

                    elif tmp_column == "stock_name":  # 判断是否为股票代码列
                        tmp_name = value
                        if tmp_code[0] == "6":
                            print(f"<td><a class='stock-link' href='https://xueqiu.com/S/SH{tmp_code}' target='_blank'>{tmp_name}</a></td>")
                        else:
                            print(f"<td><a class='stock-link' href='https://xueqiu.com/S/SZ{tmp_code}' target='_blank'>{tmp_name}</a></td>")
                    
                    elif tmp_column == "pe":  # 判断是否为股票代码列  iwencai_pe link
                        print(f"<td><a class='stock-link' href='https://iwencai.com/unifiedwap/result?w=?{tmp_code}pe' target='_blank'>{value}</a></td>")
                        
                    elif tmp_column == "industry":
                        if value is not None:
                            print(f"<td><a class='industry-link' href='iwencai_dde_industry.cgi?industry={value}' target='_blank'>{value}</a></td>")
                        else:
                            print(f"<td></td>")
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

                            # 合并holder部分并输出
                            print(f"<td>{''.join(holder_parts)}</td>")
                            # 跳过已处理的holder2和holder3列
                            i += 2
                    elif tmp_column in ["holder2", "holder3"]:
                        # 跳过这些列，因为已经在holder1处理时一起处理了
                        pass
                    elif tmp_column in ["ystz", "sjltz"] :  # 判断是否为股票代码列  iwencai_pe link
                        if tmp_code[0] == "6":
                            print(f"<td><a class='stock-link' href='https://xueqiu.com/snowman/S/SH{tmp_code}/detail#/ZYCWZB' target='_blank'>{round(value, 2) if value is not None else ''}</a></td>")
                        else:
                            print(f"<td><a class='stock-link' href='https://xueqiu.com/snowman/S/SZ{tmp_code}/detail#/ZYCWZB' target='_blank'>{round(value, 2) if value is not None else ''}</a></td>")

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
        <title>DDE全字段查询</title>
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
            .stock-link { text-decoration: none; color: inherit; }
            .sort-link { text-decoration: none; color: inherit; }
            .industry-link { text-decoration: none; color: #0066cc; }
            .industry-link:hover { text-decoration: underline; }
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

