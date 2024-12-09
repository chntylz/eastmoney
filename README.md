# eastmoney


2023-04-24
get jigou data, sort by delta-share,  generate graph by plt

![image](https://github.com/chntylz/eastmoney/assets/9045397/5338e401-3152-4f25-b372-303825190be0)

![image](https://github.com/chntylz/eastmoney/assets/9045397/7c6feea3-a25e-4234-9b1b-02edc44d4c52)


![image](https://github.com/chntylz/eastmoney/assets/9045397/dba51cfc-4a03-4775-a8d2-a5ba5a525e1a)

![image](https://github.com/chntylz/eastmoney/assets/9045397/f67f4271-29ae-442a-bc56-3bf81dd8322b)



2024-12-07  
vncserver and plot conflict, it should stop vncserver

aaron@raspberry:~/eastmoney$ vi generate_picture.py
aaron@raspberry:~/eastmoney$ python3  generate_picture.py

(process:3390339): Gdk-ERROR **: 21:32:19.769: XInput2 support not found on display
Trace/breakpoint trap (core dumped)
aaron@raspberry:~/eastmoney$ vncserver -kill :1
Killing Xtightvnc process ID 2498888
aaron@raspberry:~/eastmoney$ python3  generate_picture.py
nowdate is 2024-12-07
<function plot_stock_picture at 0xffffa4d74040>: 2024-12-06, 000001, 000001
<function plot_stock_picture at 0xffffa4d74040>: 2024-12-06, 000002, 000002
<function plot_stock_picture at 0xffffa4d74040>: 2024-12-06, 000004, 000004
<function plot_stock_picture at 0xffffa4d74040>: 2024-12-06, 000006, 000006



todo:
add pe_ttm
1. zig
2. pe  处于低位 30%一下



hangye bankuai
https://data.eastmoney.com/bkzj/hy.html
https://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112307780615049826384_1733732028771&pn=1&pz=500&po=1&np=1&fields=f12%2Cf13%2Cf14%2Cf62&fid=f62&fs=m%3A90%2Bt%3A2&ut=b2884a393a59ad64002292a3e90d46a5&_=1733732028772

https://push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery35104994650970608425_1733742729185&secid=90.BK1030&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&klt=101&fqt=1&beg=0&end=20500101&smplmt=460&lmt=1000000&_=1733742729186
