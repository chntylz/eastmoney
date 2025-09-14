from get_sina_fina_data import *
from HData_iwencai_dde import *

from selenium.webdriver.common.action_chains import ActionChains


hdata_dde=HData_iwencai_dde("usr","usr")


def parse_table(soup):
    """统一表格解析逻辑"""
    result = []
    for tbody in soup.find_all('tbody'):
        if not tbody.find('tr'):
            continue
        table_data = []
        for tr in tbody.find_all('tr'):
            row_data = [td.text.strip() for td in tr.find_all('td')]
            table_data.append(row_data)
        result.append(table_data)
    return result


#按照group_col 计算target_col连续大于0的天数
# 修改后的函数：计算target_col连续大于0或者连续小于0的天数，小于0用负数表示
def count_continous_positive(df, group_col, target_col):
    # 按股票代码分组后处理每组数据
    def calculate_group_continuous_days(group_df):
        # 创建符号列：1表示正，-1表示负，0表示零
        sign = group_df[target_col].apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
        
        # 创建连续相同符号的分组
        # 当符号变化时，分组键增加
        group_key = (sign != sign.shift()).cumsum()
        
        # 计算每个分组内的连续天数，并乘以符号值
        # 对于正数值：连续天数从1开始计数
        # 对于负数值：连续天数从-1开始计数
        # 对于零值：保持为0
        continuous_days = group_df.groupby(group_key).cumcount() + 1
        result = continuous_days * sign
        
        return result
    
    # 对每个分组应用计算函数
    return df.groupby(group_col).apply(calculate_group_continuous_days).reset_index(level=0, drop=True)


# 新增函数：按照group_col 计算target_col连续大于0的天数内的总和
# 修改后的函数：计算连续大于0或者连续小于0的天数内的总和
def calculate_continuous_sum(df, group_col, target_col):
    # 按股票代码分组后处理每组数据
    def calculate_group_continuous_sum(group_df):
        # 创建分组键：每次符号变化时，分组键增加
        # 先确定当前值的符号（1表示正，-1表示负，0单独处理）
        sign = group_df[target_col].apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
        
        # 创建连续相同符号的分组
        # 1. 当当前值为0时，创建一个新的分组
        # 2. 当符号变化时，创建一个新的分组
        group_key = (sign != sign.shift()).cumsum()
        
        # 根据符号和分组键计算连续总和
        result = group_df[target_col].groupby(group_key).cumsum()
        return result
    
    # 对每个分组应用计算函数
    return df.groupby(group_col).apply(calculate_group_continuous_sum).reset_index(level=0, drop=True)


#如果当天不是星期五，返回最近的星期五
def adjust_weekend_date():
    today = datetime.date.today()
    if today.weekday() >= 5:  # 5=Saturday, 6=Sunday
        # 计算最近的星期五
        delta = today.weekday() - 4  # 4=Friday
        return today - datetime.timedelta(days=delta)
    return today



def scrapy_pages(url):


    #open selenium browser
    #browser = get_browser(1)

    # 获取当前时间的分钟数
    current_minute = datetime.datetime.now().minute

    my_proxy = 0
    # 根据分钟数设置proxy值
    if 0 <= current_minute <= 30:
        my_proxy = 0
    else:
        my_proxy = 1

    # 输出结果（可选）
    my_dbg(f"当前分钟数: {current_minute}, my_proxy值: {my_proxy}")

    browser = get_browser(headless=False, proxy=my_proxy)

    """单页爬取逻辑"""
    df=pd.DataFrame()
    try:
        browser.get(url)
        WebDriverWait(browser, 10).until(
        EC.presence_of_element_located((By.TAG_NAME, "tbody"))
        )

        # 点击触发下拉框展开
        dropdown = browser.find_element(By.CLASS_NAME, "drop-down-box")
        dropdown.click()
        # 定位所有选项并点击"显示100条/页"
        options = WebDriverWait(browser, 10).until(
                EC.visibility_of_all_elements_located((By.CSS_SELECTOR, "div.drop-down-box > div > ul > li"))
            )
        for option in options:
            if "显示100条/页" in option.text:
                option.click()
                break

        time.sleep(random.randint(1, 3))

        soup = BeautifulSoup(browser.page_source, 'html.parser')
        result = parse_table(soup)
        df=pd.DataFrame(result[0])
        my_dbg(df)
    except Exception as e:
        my_dbg(f"页面爬取失败: {e}")
        return None
    finally:
        pass
    
    retry_times = 0
    pages = 52
    loop = pages  # actually is 1+ 52
    while (loop):
        try:
            browser.find_element('xpath', "//a[text()='下页' and not(contains(@class,'disabled'))]").click()
        except Exception as e:
            my_dbg(f'loop:{loop}, error:{e}')
            browser.refresh()
            loop = pages
            retry_times = retry_times + 1
            if retry_times > 10:
                break
            continue
        finally:
            pass
                          
           
        time.sleep(random.randint(1, 5))

        #pull to bottom - 使用JavaScript滚动到页面底部
        try:
            browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.randint(1, 3))
        except Exception as e:
            my_dbg(f'Error scrolling to bottom: {e}')
        finally:
            pass

        html_doc=browser.page_source
        soup = BeautifulSoup(html_doc, 'html.parser')
        result = parse_table(soup)   
        tmp_df=pd.DataFrame(result[0])
        my_dbg(f'loop:{loop}, tmp_df:{tmp_df}')
        df = pd.concat([df, tmp_df])

        loop = loop - 1


    #close selenium browser
    close_broser(browser)

    #handle dataFrame
    df = df.reset_index(drop=True)

    #delete space column
    del df[1]

    # 处理数据中的逗号
    df = df.replace(',','', regex=True)  # 移除所有逗号

    df = df.replace('--','0', regex=True)  # '--' -> 0

    cols = ['rank', 'stock_code', 'stock_name', 'close', 'pct', 'zlkp_rank', \
           'dde_buy', 'dde_sell', 'amount', 'zlkp_pct']

    cols = ['rank', 'stock_code', 'stock_name', 'close', 'pct', 'zlkp_pct', 'zlkp_rank', \
           'dde_buy', 'dde_sell', 'amount']
    df.columns = cols

    #去重
    df = df.drop_duplicates(subset=['stock_code'], keep='first')

    cols_final = ['rank', 'stock_code', 'stock_name', 'close', 'pct', 'zlkp_pct', 'zlkp_rank', \
           'dde_buy', 'dde_sell', 'amount']
    df = df[cols_final]

    #insert record_date
    #df.insert(0, 'record_date', time.strftime("%Y-%m-%d", time.localtime()), allow_duplicates=False)
    valid_date = adjust_weekend_date()
    df.insert(0, 'record_date', valid_date.strftime("%Y-%m-%d") , allow_duplicates=False)
    
     
    #亿->10**8, 万->10**4
    df['dde_buy']  = df['dde_buy'].apply(lambda x: money_unit_transfer(x))
    df['dde_sell']  = df['dde_sell'].apply(lambda x: money_unit_transfer(x))
    df['amount']  = df['amount'].apply(lambda x: money_unit_transfer(x))

    df['dde_net'] = df['dde_buy'] - df['dde_sell']

    df['conti_day'] = 0
    df['dde_net_all'] = 0

    #save
    df.to_csv('./csv/' + time.strftime("%Y-%m-%d", time.localtime()) + '_dde.csv', encoding='gbk')

    return df


def check_table():
    table_exist = hdata_dde.table_is_exist() 
    my_dbg('table_exist=%d' % table_exist)
    if table_exist:
        #hdata_dde.db_hdata_iwencai_create()
        my_dbg('table already exist, recreate')
    else:
        hdata_dde.db_hdata_iwencai_create()
        my_dbg('table not exist, create')



if __name__ == '__main__':

    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


    url_5000='https://www.iwencai.com/unifiedwap/result?typed=0&preParams=&ts=1&f=1&qs=webclient_btcl_nczg&selfsectsn=&querytype=&searchfilter=&tid=stockpick&w=%E4%B8%BB%E5%8A%9B%E6%8E%A7%E7%9B%98%E5%89%8D5000%E7%9A%84%E8%82%A1%E7%A5%A8'

    url_20='https://www.iwencai.com/unifiedwap/result?typed=0&preParams=&ts=1&f=1&qs=webclient_btcl_nczg&selfsectsn=&querytype=&searchfilter=&tid=stockpick&w=%E4%B8%BB%E5%8A%9B%E6%8E%A7%E7%9B%98%E5%89%8D2%E7%9A%84%E8%82%A1%E7%A5%A8'
    url_10='https://www.iwencai.com/unifiedwap/result?typed=0&preParams=&ts=1&f=1&qs=webclient_btcl_nczg&selfsectsn=&querytype=&searchfilter=&tid=stockpick&w=%E4%B8%BB%E5%8A%9B%E6%8E%A7%E7%9B%98%E5%89%8D10%E7%9A%84%E8%82%A1%E7%A5%A8'

    url_dde = 'https://www.iwencai.com/unifiedwap/result?w=dde%E5%A4%A7%E5%8D%95%E5%87%80%E9%A2%9D%E5%89%8D5000%E7%9A%84%E8%82%A1%E7%A5%A8&querytype=stock'


    #主力控盘前5000的股票
    #https://www.iwencai.com/unifiedwap/result?w=%E4%B8%BB%E5%8A%9B%E6%8E%A7%E7%9B%98%E5%89%8D5000%E7%9A%84%E8%82%A1%E7%A5%A8&querytype=stock&addSign=1750524259294
        
    url='https://www.iwencai.com/unifiedwap/result?w=主力控盘比例前6000的股票&querytype=stock'
    my_dbg(url)

    dde_df = scrapy_pages(url)
    my_dbg(dde_df)

    if len(dde_df) > 4000:
        #check table exist
        check_table()
        
        valid_date = adjust_weekend_date()
        hdata_dde.delete_data_from_hdata(
            start_date=valid_date.strftime("%Y-%m-%d"),
            end_date=valid_date.strftime("%Y-%m-%d")
            )
        hdata_dde.copy_from_stringio(dde_df)

        #计算20天内dde_net连续大于0的天数
        s_date = valid_date - datetime.timedelta(days=20)
        e_date = valid_date
        df_20day = hdata_dde.get_data_from_hdata(
            start_date=s_date.strftime("%Y-%m-%d"),
            end_date=e_date.strftime("%Y-%m-%d")
            )

        # 计算连续天数和总和
        df_conti_day = count_continous_positive(df_20day, 'stock_code', 'dde_net')
        df_conti_sum = calculate_continuous_sum(df_20day, 'stock_code', 'dde_net')
        df_20day['conti_day'] = df_conti_day
        df_20day['dde_net_all'] = df_conti_sum
        df_today = df_20day[df_20day['record_date'] == valid_date.strftime("%Y-%m-%d")] 
        df_today = df_today.reset_index(drop=True)

        hdata_dde.delete_data_from_hdata(
            start_date=valid_date.strftime("%Y-%m-%d"),
            end_date=valid_date.strftime("%Y-%m-%d")
            )
        hdata_dde.copy_from_stringio(df_today)

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    my_dbg("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    my_dbg("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))

'''
from get_iwencai_selenium_dde import *

df = pd.read_csv('./csv/2025-08-07_dde.csv',encoding='gbk', index_col=[0], dtype={'stock_code':str})
df = df.drop_duplicates(subset=['stock_code', 'record_date'], keep='first')
df = df.reset_index(drop=True)

cols_final = ['record_date', 'rank', 'stock_code', 'stock_name', 'close', 'pct',
   'zlkp_rank', 'dde_buy', 'dde_sell', 'amount', 'zlkp_pct']
df = df[cols_final]     

   
hdata_dde.copy_from_stringio(df)

'''
