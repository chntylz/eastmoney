#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import cgi
import os
import platform

# 检测操作系统
current_os = platform.system()
print(f'当前操作系统: {current_os}')

# 设置文件路径
# 根据操作系统选择合适的路径格式
if current_os == 'Windows':
    OPTIONAL_FILE = r'C:\Users\aaron\Documents\eastmoney\cgi-bin\my_optional.txt'
else:
    OPTIONAL_FILE = '/var/www/cgi-bin/my_optional.txt'

print(f'目标文件路径: {OPTIONAL_FILE}')

# 获取表单数据
form = cgi.FieldStorage()
stock_code = form.getvalue('stock_code', '')
stock_name = form.getvalue('stock_name', '')


if stock_code[0:1] == '6':
    stock_code = 'SH' + stock_code
else:
    stock_code = 'SZ' + stock_code


# 设置响应类型
print('Content-Type: text/plain; charset=utf-8\n')

if not stock_code or not stock_name:
    print('错误：缺少股票代码或名称')
else:
    try:
        # 检查目录是否存在
        dir_path = os.path.dirname(OPTIONAL_FILE)
        if not os.path.exists(dir_path):
            print(f'目录不存在，尝试创建: {dir_path}')
            os.makedirs(dir_path, exist_ok=True)
            print(f'目录创建成功: {dir_path}')
        else:
            print(f'目录存在: {dir_path}')

        # 检查文件是否存在，如果不存在则创建
        if not os.path.exists(OPTIONAL_FILE):
            print(f'文件不存在，尝试创建: {OPTIONAL_FILE}')
            with open(OPTIONAL_FILE, 'w', encoding='utf-8') as f:
                f.write('股票代码,股票名称\n')
            print(f'文件创建成功: {OPTIONAL_FILE}')
        else:
            print(f'文件已存在: {OPTIONAL_FILE}')

        # 读取现有内容，检查是否已存在该股票
        existing_stocks = set()
        try:
            with open(OPTIONAL_FILE, 'r', encoding='utf-8') as f:
                print(f'成功打开文件进行读取: {OPTIONAL_FILE}')
                next(f)  # 跳过标题行
                for line in f:
                    if line.strip():
                        try:
                            code, _ = line.strip().split(',', 1)
                            existing_stocks.add(code)
                        except ValueError:
                            print(f'警告：行格式不正确，跳过: {line.strip()}')
            print(f'读取完成，已存在的股票代码数量: {len(existing_stocks)}')
        except Exception as e:
            print(f'读取文件错误: {str(e)}')
            print(f'错误类型: {type(e).__name__}')

        # 如果股票不存在，则添加
        if stock_code not in existing_stocks:
            try:
                with open(OPTIONAL_FILE, 'a', encoding='utf-8') as f:
                    f.write(f'{stock_code} {stock_name}\n')
                print(f'成功添加 {stock_name}({stock_code}) 到自选股')
                print(f'文件内容已更新: {OPTIONAL_FILE}')
            except Exception as e:
                print(f'写入文件错误: {str(e)}')
                print(f'错误类型: {type(e).__name__}')
        else:
            print(f'{stock_name}({stock_code}) 已在自选股中')
    except Exception as e:
        print(f'添加自选股失败: {str(e)}')
        print(f'错误类型: {type(e).__name__}')
