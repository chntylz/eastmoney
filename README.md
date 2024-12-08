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

