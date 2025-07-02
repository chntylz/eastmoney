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
   

def scrapy_pages(url):


    #open selenium browser
    browser = get_browser()

    """单页爬取逻辑"""
    df=pd.DataFrame()
    try:
        browser.get(url)
        WebDriverWait(browser, 10).until(
        EC.presence_of_element_located((By.TAG_NAME, "tbody"))
        )
        soup = BeautifulSoup(browser.page_source, 'html.parser')
        result = parse_table(soup)
        df=pd.DataFrame(result[0])
        my_dbg(df)
    except Exception as e:
        my_dbg(f"页面爬取失败: {e}")
        return None
    finally:
        pass
    

    loop = 108  # actually is 1+108    
    while (loop):
        try:
            browser.find_element('xpath', "//a[text()='下页' and not(contains(@class,'disabled'))]").click()
        except Exception as e:
            my_dbg(f'loop:{loop}, error:{e}')
            browser.refresh()
            loop = 108
            continue
        finally:
            pass
                          
           
        time.sleep(random.randint(1, 3))

        #pull to bottom
        try:
            actions = ActionChains(browser)
            actions.move_by_offset(0,100).perform()
            time.sleep(random.randint(1, 3))
        except Exception as e:
            my_dbg(f'Error action move{e}')
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

    cols = ['rank', 'stock_code', 'stock_name', 'close', 'pct', 'zlkp_pct', 'zlkp_rank', \
           'dde_buy', 'dde_sell', 'amount']
    df.columns = cols

    #去重
    df = df.drop_duplicates(subset=['stock_code'], keep='first')

    #insert record_date
    df.insert(0, 'record_date', time.strftime("%Y-%m-%d", time.localtime()), allow_duplicates=False)
     
    #亿->10**8, 万->10**4
    df['dde_buy']  = df['dde_buy'].apply(lambda x: money_unit_transfer(x))
    df['dde_sell']  = df['dde_sell'].apply(lambda x: money_unit_transfer(x))
    df['amount']  = df['amount'].apply(lambda x: money_unit_transfer(x))

    df['dde_net'] = df['dde_buy'] - df['dde_sell']

    
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

    dde_df = scrapy_pages(url)
    my_dbg(dde_df)

    if len(dde_df) > 1000:
        #check table exist
        check_table()

        hdata_dde.delete_data_from_hdata(
                start_date=datetime.datetime.now().date().strftime("%Y-%m-%d"),
                end_date=datetime.datetime.now().date().strftime("%Y-%m-%d")
                )
        hdata_dde.copy_from_stringio(dde_df)
 
    
    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    my_dbg("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    my_dbg("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))

