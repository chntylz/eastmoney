#!/usr/bin/env python  
# -*- coding: utf-8 -*-


import time
import random
import threadpool

name_list = []

def sayhello(str):
    #print("Hello ", str )
    time.sleep(1)

def my_normal():
    for i in range(len(name_list)):
        sayhello(name_list[i])

def my_threadpool(my_size):
    pool = threadpool.ThreadPool(my_size) 
    requests = threadpool.makeRequests(sayhello, name_list) 
    [pool.putRequest(req) for req in requests] 
    pool.wait() 


if __name__ == '__main__':
    name_list = [random.randint(1,10) for i in range(4500)]
    name_list = range(1,4000)
    print(name_list)

    for i in range(100, 5000, 500):
        start_time = time.time()
        my_threadpool(i)
        end_time = time.time()
        print( 'i=%d, %d second' % (i, end_time-start_time))
