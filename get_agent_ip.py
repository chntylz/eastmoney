import os
import re
import time
import pandas as pd
import requests
import concurrent.futures
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from comm_selenium import get_browser
from requests.exceptions import ConnectionError, RequestException, Timeout


# ==================================================================== #

# 可以在线运行python的网站: https://repl.it/languages
# 依赖:
# requests==2.21.0
# You can run python from the website https://repl.it/languages
# requirements:
# requests==2.21.0

# 保存代理的文件名
# Save proxies file name
CHECKED_PROXY = 'unchecked_proxy_spysone'

url = 'http://spys.one/en/socks-proxy-list/'

url = 'http://spys.one/en/http-proxy-list/'

# url = 'http://spys.one/en/anonymous-proxy-list/'

# 设置代理配置:
# Proxy Information Settings:

# xpp = '5' 对应网页上Per page的值,即500. 0:30, 1:50, 2:100, 3:200, 4:300, 5:500
# xpp = '5' Set the Per page value to be 500. 0:30, 1:50, 2:100, 3:200, 4:300, 5:500
xpp = '5'

# xf1 = '4' 对应ANM的值,当前为HIA. 0:All, 1:ANM&HIA, 2:NOA, 3:ANM, 4:HIA
# xf1 = '4' Set the ANM value to be HIA. 0:All, 1:ANM&HIA, 2:NOA, 3:ANM, 4:HIA
xf1 = '4'

# xf2 = '0' 对应SSL的值. 0:All, 1:SSL+, 2:SSL-
# xf2 = '0' Set the SSL value to be All. 0:All, 1:SSL+, 2:SSL-
xf2 = '0'

# xf4 = '0' 对应Port的值. 0:All, 1:3128, 2:8080,3:80
# xf4 = '0' Set the Port value to be All. 0:All, 1:3128, 2:8080,3:80
xf4 = '0'

# xf5 = '2' 对应Type的值. 0:All, 1:HTTP, 2:SOCKS
# xf5 = '2' Set the Type value to be SOCKS. 0:All, 1:HTTP, 2:SOCKS
xf5 = '2'

# ==================================================================== #


unchecked = []
file_path_unchecked = '{0}/{1}.txt'.format(os.getcwd(), CHECKED_PROXY)

# GET HTML [Per page|ANM|SSL|Port|Type]
def get_index(xpp, xf1, xf2, xf4, xf5):
    header = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.2 Safari/605.1.15'}
    data = {
        'xpp': xpp,
        'xf1': xf1,
        'xf2': xf2,
        'xf4': xf4,
        'xf5': xf5
    }
    print('Getting the website...')
    try:
        rsp = requests.post(url=url, headers=header, data=data)
        if rsp.status_code == 200:
            print('Success.')
            html = rsp.text
            return html
        else:
            exit('Can not get the website.')
    except ConnectionError:
        exit('Please run your proxy app and try again.')

def get_proxy_info(html):
    pattern = re.compile('onmouseout.*?spy14>(.*?)<s.*?write.*?nt>\"\+(.*?)\)</scr.*?\/en\/(.*?)-', re.S)
    infos = re.findall(pattern, html)
    return infos

def parse_proxy_info(html, infos):
    print('Get {} proxies.'.format(len(infos)))
    print('Start to get proxy details...')
    port_word = re.findall('\+\(([a-z0-9^]+)\)+', html)
    # DECRYPT PORT VALUE
    port_passwd = {}
    portcode = (re.findall('table><script type="text/javascript">(.*)</script>', html))[0].split(';')
    for i in portcode:
        ii = re.findall('\w+=\d+', i)
        for i in ii:
            kv = i.split('=')
            if len(kv[1]) == 1:
                k = kv[0]
                v = kv[1]
                port_passwd[k] = v
            else:
                pass
    # GET PROXY INFO
    for i in infos:
        proxies_info = {
            'ip': i[0],
            'port': i[1],
            'protocol': i[2]
        }
        port_word = re.findall('\((\w+)\^', proxies_info.get('port'))
        port_digital = ''
        for i in port_word:
            port_digital += port_passwd[i]
        test_it = '{0}:{1}'.format(proxies_info.get('ip'), port_digital)
        unchecked.append(test_it)


def check_proxy(proxy, timeout=5):
    """检查代理IP是否可用
    
    参数:
        proxy (str): 格式为 "ip:port" 的代理地址
        timeout (int): 请求超时时间（秒）
    
    返回:
        bool: 代理可用返回True，否则返回False
    """
    test_url = 'http://www.baidu.com'  # 用百度作为测试网站
    proxies = {
        'http': f'http://{proxy}',
        'https': f'http://{proxy}'
    }
    
    try:
        response = requests.get(test_url, proxies=proxies, timeout=timeout)
        if response.status_code == 200:
            print(f"代理 {proxy} 可用")
            return True
        else:
            print(f"代理 {proxy} 不可用，状态码: {response.status_code}")
            return False
    except (ConnectionError, RequestException, Timeout) as e:
        print(f"代理 {proxy} 不可用，错误: {e.__class__.__name__}")
        return False

def check_proxies(proxy_list, max_workers=10):
    """并发检查多个代理IP的可用性
    
    参数:
        proxy_list (list): 代理地址列表，每个元素格式为 "ip:port"
        max_workers (int): 最大并发检查数量
    
    返回:
        list: 可用的代理地址列表
    """
    valid_proxies = []
    total = len(proxy_list)
    
    print(f"开始检查 {total} 个代理的可用性...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有代理检查任务
        future_to_proxy = {executor.submit(check_proxy, proxy): proxy for proxy in proxy_list}
        
        # 收集结果
        for i, future in enumerate(concurrent.futures.as_completed(future_to_proxy)):
            proxy = future_to_proxy[future]
            try:
                if future.result():
                    valid_proxies.append(proxy)
            except Exception as e:
                print(f"检查代理 {proxy} 时发生异常: {e}")
            
            # 显示进度
            print(f"进度: {i+1}/{total} ({(i+1)/total*100:.1f}%)", end='\r')
    
    print(f"\n检查完成，共找到 {len(valid_proxies)} 个可用代理")
    return valid_proxies

def main():
    html = get_index(xpp, xf1, xf2, xf4, xf5)
    infos = get_proxy_info(html)
    parse_proxy_info(html, infos)
    with open(file_path_unchecked,'a+') as f:
        f.write(time.strftime(">>>%Y-%m-%d %H:%M:%S", time.localtime()) + '\n')
        for proxy in unchecked:
            f.write(proxy + '\n')
    print('Save to {}.'.format(file_path_unchecked))
    
    # 检查代理可用性
    check_proxies_option = input("是否检查代理可用性？(y/n): ").strip().lower()
    if check_proxies_option == 'y':
        # 检查代理可用性
        valid_proxies = check_proxies(unchecked)
        
        # 保存可用代理到文件
        if valid_proxies:
            valid_file = 'valid_proxies.txt'
            with open(valid_file, 'w', encoding='utf-8') as f:
                f.write('\n'.join(valid_proxies))
            print(f"已将 {len(valid_proxies)} 个可用代理保存到 {valid_file}")
            
            # 计算可用率
            valid_rate = len(valid_proxies) / len(unchecked) * 100 if unchecked else 0
            print(f"代理可用率: {valid_rate:.2f}% ({len(valid_proxies)}/{len(unchecked)})")
        else:
            print("未找到可用代理")
    print('Done.')


if __name__ == '__main__':
    main()