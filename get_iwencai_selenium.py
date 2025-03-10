#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from selenium.webdriver.common.action_chains import ActionChains


from get_daily_zlje import *


debug = 0
debug = 1
debug = 0

global_first_time = True

#https://medium.com/@dharmendradiwaker12/extracting-data-from-canvas-line-charts-with-selenium-python-and-javascript-2cc931104264
def scrape_canvas_data(driver):
    """
    Scrapes tooltip data from a canvas on a given page URL.

    Parameters:
    url (str): The URL of the webpage containing the canvas element.
    hover_steps (int): The number of steps to move horizontally across the canvas to hover and extract data.
    
    Returns:
    list: A list of dictionaries containing date and transaction data.
    """
    try:
        hover_steps=20
        # Locate the canvas element
        canvas = driver.find_element(By.XPATH, "//canvas[@data-zr-dom-id='zr_0']")

        # Scroll into view of the canvas to ensure it's fully visible
        driver.execute_script("arguments[0].scrollIntoView(true);", canvas)
        #time.sleep(1)  # Wait for the canvas to load

        # Get canvas dimensions using JavaScript
        canvas_width = driver.execute_script("return arguments[0].width;", canvas)
        canvas_height = driver.execute_script("return arguments[0].height;", canvas)

        # Calculate x_offset_step based on the number of hover steps
        x_offset_step = canvas_width // hover_steps
        x_offset_step = 1

        # List to store the extracted data
        extracted_data = {}

        # Hover over the canvas in small steps
        #for x_offset in range( 0, canvas_width, x_offset_step):
        for x_offset in range( canvas_width, canvas_width + 1, x_offset_step):
            try:
                # Set y_offset to somewhere in the middle of the canvas
                y_offset = canvas_height // 2

                # JavaScript to simulate the hover action
                hover_script = f"""
                    var event = new MouseEvent('mousemove', {{
                        clientX: arguments[0].getBoundingClientRect().left + {x_offset}, 
                        clientY: arguments[0].getBoundingClientRect().top + {y_offset}, 
                        bubbles: true
                    }});
                    arguments[0].dispatchEvent(event);
                """

                # Execute the hover action at the current x_offset
                driver.execute_script(hover_script, canvas)
                time.sleep(1)  # Wait briefly to let the tooltip appear

                # Check for tooltip and extract data
                try:
                    date_element = WebDriverWait(driver, 30).until(
                        EC.visibility_of_element_located((By.XPATH, "//div[contains(@style, 'display: inline-block; margin-right: 10px; height:17px; line-height:17px;color: #262626')]"))
                    )
                    transaction_element = driver.find_element(By.XPATH, "//span[contains(@style, 'color: #262626;line-height:17px;height:17px;')]")

                    
                    # Extract data and store it in the list
                    date_value = date_element.text
                    transaction_value = transaction_element.text
                    extracted_data[date_value] = float(transaction_value)
                    if debug:
                        print('date_value:%s, transaction_value:%s' % (date_value, transaction_value)  )
                    
                except:
                    # No tooltip found at this point, continue to the next
                    pass
            except Exception as outer_exception:
                # Handle any other issues like JavaScript or element issues
                print(f"Error at x_offset: {x_offset}, {outer_exception}")
    except:
        pass

    stock_date = ''
    pe_pct = 0
    try:
        for key, value in extracted_data.items():
            stock_date = key
            pe_pct =  value
            if debug:
                print(f"{key}: {value}")  
                print(stock_date, pe_pct)  
    except Exception as e:
        if debug:
            print("%s pe failed" % stock_code)
            print(e)
        pass

    return stock_date, pe_pct


def get_browser_real():

    browser = None
    
    # 添加无头headlesss
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument(
            'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'\
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36')


    chrome_options.add_argument("disable-infobars");
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])

    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--ignore-certificate-errors')
    #chrome_options.add_argument("blink-settings=imagesEnabled=false")  #image disable

     
    chrome_options.add_argument('--disk-cache-dir=/dev/shm  --disk-cache-size=4096000000')


    try:
        browser = webdriver.Chrome(executable_path='/usr/bin/chromedriver',
            chrome_options=chrome_options)
    except:
        time.sleep(60)
        try:
            browser = webdriver.Chrome(executable_path='/usr/bin/chromedriver',
                chrome_options=chrome_options)
        except:
            pass
    finally:
        if browser is None:
            try:
                time.sleep(60)
                browser = webdriver.Chrome(executable_path='/usr/bin/chromedriver',
                    chrome_options=chrome_options)
            except:
                pass

    #browser.maximize_window()  # 最大化窗口
    #wait = WebDriverWait(browser, 10)
    with open('./stealth.min.js') as f:
        js = f.read()
    browser.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": js
        })
        
    return browser
    

def get_iwencai_pe(driver, stock_code):
    url='https://iwencai.com/unifiedwap/result?w='+ stock_code + 'pe'
    if debug:
        print(url)

    driver.get(url)
    time.sleep(1)

    global global_first_time  # 声明要修改全局变量
    if global_first_time: 
        try:
            driver.find_element(By.ID, "details-button").click()
        except Exception as e:
            if debug:
                #print(e)
                pass

        time.sleep(1)

        try:
            driver.find_element(By.ID, "proceed-link").click()
        except Exception as e:
            if debug:
                #print(e)
                pass

        global_first_time = False


    stock_date, pe = scrape_canvas_data(driver)
    
    if debug:
        print(stock_code, stock_date, pe)

    return stock_code, stock_date, pe



if __name__ == '__main__':

    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    stock_df = get_latest_zlje_from_db()
    print(stock_df.head(5))
    stock_df_len = len(stock_df)

    driver = get_browser_real()

    with open('./pe.txt','w') as f:
        f.write( 'stock_code, stock_date, pe_pct\n')

    for i in range(stock_df_len):
        stock_code = stock_df.stock_code[i]
        if stock_code[0] == '9':
            continue
        if debug:
            print(stock_code)

        stock_code, stock_date, pe_pct = get_iwencai_pe(driver, stock_code)

        if pe_pct == 0:
            stock_code, stock_date, pe_pct = get_iwencai_pe(driver, stock_code)

        with open('./pe.txt','a') as f:
            f.write('%s, %s, %s\n' % (stock_code, stock_date, pe_pct))

    driver.quit()


    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("main_company.py t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
    
    
