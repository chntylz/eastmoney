import queue
import threading
 
import time

max=100000
# 创建一个队列对象，把数组值放进去
q = queue.Queue(maxsize=max)
for i in range(max):
    q.put(i)
 
# 定义实际操作
def do_something(i):
    print(i)
 
# 从队列中取出值，并调用实际操作
def f(queue):
    while not queue.empty():
        i = queue.get()
        do_something(i)
 
t1 = time.time()
# 起10个线程，线程target去执行从队列中取值并进行操作的动作
threads = []
for t in range(4):
    thread = threading.Thread(target=f, args=(q,))
    threads.append(thread)
    thread.start()
 
for t in threads:
    t.join()
                        
t2 = time.time()
print("t2-t1=%s"%(t2-t1)) 
