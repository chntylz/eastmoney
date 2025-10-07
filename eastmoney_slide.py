#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by aneasystone on 2018/3/2
import random

from comm_selenium import *

from PIL import Image, ImageChops
from numpy import array
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait as Wait
from selenium.webdriver.support import expected_conditions as Expect
import requests, io, re
import easing
import time
from file_interface import my_dbg

'''
https://www.aneasystone.com/archives/2018/03/python-selenium-geetest-crack.html

输出的验证码为JSON格式，其中大图片是将原图裁剪成横向10份纵向2分共20张图片随机混淆拼接而成的，原图通过在前端移位还原，混淆信息带在JSON上
https://github.com/geffzhang/Geetest


破解极验三代滑动验证，成功率百分之百（二）：分析图片得到滑动距离
https://zhuanlan.zhihu.com/p/569492595


Python爬虫入门教程 58-100 python爬虫高级技术之验证码篇4-极验证识别技术之一
https://developer.aliyun.com/article/705957



https://github.com/1046517444/SliderCracker-1/commit/dabb91ea8b1783d1afb6e14f1df384e2786c1c4e#diff-99ad9999b01aa83906f30952b2c61bfb5e8207528a86f4b4cd2110ec967c0f51


full
https://smartvcode2.eastmoney.com/02/resources/e02b_160/2/1e/1e7e08b4ebe7e83ab59ec113256dc7b8/1e7e08b4ebe7e83ab59ec113256dc7b8.jpg

slice
https://smartvcode2.eastmoney.com/02/resources/e02b_160/2/1e/1e7e08b4ebe7e83ab59ec113256dc7b8/slice/2378b30a.png
'''

def convert_css_to_offset(px):
    ps = px.replace('px', '').split(' ')
    x = -int(ps[0])
    y = -int(ps[1])
    return x, y, x + 10, y + 58


def convert_index_to_offset(index):
    row = int(index / 26)
    col = index % 26
    x = col * 10
    y = row * 58
    return x, y, x + 10, y + 58


def get_slider_offset_from_diff_image(diff):
    im = array(diff)
    width, height = diff.size
    diff = []
    for i in range(height):
        for j in range(width):
            # black is not only (0,0,0)
            if im[i, j, 0] > 15 or im[i, j, 1] > 15 or im[i, j, 1] > 15:
                diff.append(j)
                break
    return min(diff)


def get_slider_offset(image_url, image_url_bg, css):
    image_file = io.BytesIO(requests.get(image_url).content)
    im = Image.open(image_file)
    image_file_bg = io.BytesIO(requests.get(image_url_bg).content)
    im_bg = Image.open(image_file_bg)
    # im.show()
    # im_bg.show()

    # 10*58 26/row => background image size = 260*116
    captcha = Image.new('RGB', (260, 116))
    captcha_bg = Image.new('RGB', (260, 116))
    for i, px in enumerate(css):
        offset = convert_css_to_offset(px)
        region = im.crop(offset)
        region_bg = im_bg.crop(offset)
        offset = convert_index_to_offset(i)
        captcha.paste(region, offset)
        captcha_bg.paste(region_bg, offset)
    diff = ImageChops.difference(captcha, captcha_bg)
    # captcha.show()
    # captcha_bg.show()
    # diff.show()
    diff.save('./diff.png')
    return get_slider_offset_from_diff_image(diff)


def get_image_css(images):
    css = []
    for image in images:
        position = get_image_position_from_style(image.get_attribute("style"))
        css.append(position)
    return css


def get_image_url_from_style(style):
    match = re.match('background-image: url\("(.*?)"\); background-position: (.*?);', style)
    return match.group(1)


def get_image_position_from_style(style):
    match = re.match('background-image: url\("(.*?)"\); background-position: (.*?);', style)
    return match.group(2)


def get_slice_offset(slice):
    style = slice.get_attribute("style")
    match = re.search('background-image: url\("(.*?)"\);', style)
    url = match.group(1)
    image_file = io.BytesIO(requests.get(url).content)
    im = Image.open(image_file)
    im.save('./slice.png')
    return get_slider_offset_from_diff_image(im)


# refer: https://ask.hellobi.com/blog/cuiqingcai/9796
def get_track(distance):
    track = []
    current = 0
    mid = distance * 4 / 5
    t = 0.2
    v = 0

    while current < distance:
        if current < mid:
            a = 2
        else:
            a = -3
        v0 = v
        v = v0 + a * t
        move = v0 * t + 1 / 2 * a * t * t
        current += move
        track.append(round(move))
    return track


def fake_drag(browser, knob, offset):
    # seconds = random.uniform(2, 6)
    # print(seconds)
    # samples = int(seconds*10)
    # diffs = sorted(random.sample(range(0, offset), samples-1))
    # diffs.insert(0, 0)
    # diffs.append(offset)
    # ActionChains(browser).click_and_hold(knob).perform()
    # for i in range(samples):
    #     ActionChains(browser).pause(seconds/samples).move_by_offset(diffs[i+1]-diffs[i], 0).perform()
    # ActionChains(browser).release().perform()

    # tracks = get_track(offset)
    tracks = get_track(offset)
    #offsets, tracks = easing.get_tracks(offset, 12, 'ease_out_expo')
    #print(offsets)
    ActionChains(browser).click_and_hold(knob).perform()
    for x in tracks:
        ActionChains(browser).move_by_offset(x, 0).perform()
    ActionChains(browser).pause(0.5).release().perform()

    return


def do_crack(browser):
    slice = browser.find_element_by_class_name("em_slice")
    slice_offset = get_slice_offset(slice)
    print(slice_offset)

    images = browser.find_elements_by_class_name("em_cut_fullbg_slice")
    images_bg = browser.find_elements_by_class_name("em_cut_bg_slice")
    image_url = get_image_url_from_style(images[0].get_attribute("style"))
    image_url_bg = get_image_url_from_style(images_bg[0].get_attribute("style"))
    css = get_image_css(images)
    offset = get_slider_offset(image_url, image_url_bg, css)
    print(offset)

    knob = browser.find_element_by_class_name("em_slider_knob")
    # ActionChains(browser).drag_and_drop_by_offset(knob, offset - slice_offset, 0).perform()
    try:
        fake_drag(browser, knob, offset - slice_offset)
    except Exception as e:
        my_dbg(e)
        fake_drag(browser, knob, offset - slice_offset)
    finally:
        pass

    return

def do_slide():
    browser = get_browser()
    browser.get('https://eastmoney.com')
    browser.refresh()

    try:
        # 定位元素（使用src属性精准匹配）
        close_button = WebDriverWait(browser, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "img[src='https://emcharts.dfcfw.com/fullscreengg/ic_close.png']"))
        )

        # 确保元素可点击（滚动到视图并等待可交互）
        browser.execute_script("arguments[0].scrollIntoViewIfNeeded();", close_button)
        
        # 尝试点击（如果失败则用JS直接执行点击事件）
        try:
            WebDriverWait(browser, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "img[src*='ic_close.png']"))).click()
        except ElementClickInterceptedException:
            browser.execute_script("arguments[0].click();", close_button)
            
        print("点击成功！")

    except Exception as e:
        print("操作失败:", str(e))
    finally:
        pass


    try:
         # 等待iframe加载并切换到该iframe
        iframe = WebDriverWait(browser, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "popwscps_d_iframe"))
                        )

        # 切换到iframe内部
        browser.switch_to.frame(iframe)
        time.sleep(3)

        browser.find_element('xpath', '//div[contains(@class, "em_slider_knob")]').click()
        my_dbg('test')

        retry=10
        while (retry > 0):
            do_crack(browser)
            my_dbg(f'retry {retry}')
            retry = retry - 1
    except Exception as e:
        my_dbg(e)
        my_dbg('success!')
    finally:
        pass

    browser.close()
    browser.quit()


if __name__ == '__main__':
    do_slide()
