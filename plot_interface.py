#https://www.cnblogs.com/atanisi/p/8530693.html
# -*- coding: utf-8 -*-
import numpy as np
import matplotlib.pyplot as plt

import datetime as datetime
from HData_eastmoney_day import *
from HData_xq_holder import *

hdata_holder=HData_xq_holder("usr","usr")
hdata=HData_eastmoney_day("usr","usr")

nowdate=datetime.datetime.now().date()
nowcode='300045'
detail_info = hdata.get_data_from_hdata(stock_code=nowcode,end_date=nowdate.strftime("%Y-%m-%d"), limit=20)
detail_info = detail_info.head(5)

holder_df = hdata_holder.get_data_from_hdata(stock_code = nowcode)
holder_df = holder_df.sort_values('record_date', ascending=1)
holder_df = holder_df.reset_index(drop=True)
holder_df = holder_df.head(5)


#x = np.linspace(0,10)
#y = np.linspace(0,10)

x = detail_info['record_date'].to_numpy()
y = detail_info['close'].to_numpy()


print('x=%d, y=%d'  % (len(x), len(y)))
print(x)
print(y)
xx = holder_df['record_date'].to_numpy()
zz = holder_df['holder_num'].to_numpy()
print('xx=%d, zz=%d'  % (len(xx), len(zz)))
print(xx)
print(zz)

fig = plt.figure()
ax = fig.add_subplot(111)
ax.plot(x,y, '-', label = 'Quantity 1')

ax2 = ax.twinx()
ax2.plot(xx,zz, '-r', label = 'Quantity 2')
fig.legend(loc=1, bbox_to_anchor=(1,1), bbox_transform=ax.transAxes)

ax.set_xlabel("x [units]")
ax.set_ylabel(r"Quantity 1")
ax2.set_ylabel(r"Quantity 2")

plt.savefig('0.png')

