#https://www.cnblogs.com/atanisi/p/8530693.html
# -*- coding: utf-8 -*-
import numpy as np
import matplotlib.pyplot as plt

import datetime as datetime
from HData_eastmoney_day import *
from HData_eastmoney_holder import *

hdata_holder=HData_eastmoney_holder("usr","usr")
hdata=HData_eastmoney_day("usr","usr")

nowdate=datetime.datetime.now().date()
nowcode='300045'
detail_info = hdata.get_data_from_hdata(stock_code=nowcode,
        end_date=nowdate.strftime("%Y-%m-%d"), limit=500)
#detail_info = detail_info.head(5)

holder_df = hdata_holder.get_data_from_hdata(stock_code = nowcode)
holder_df = holder_df.sort_values('record_date', ascending=1)
holder_df = holder_df.reset_index(drop=True)
#holder_df = holder_df.head(5)


aa=detail_info
bb=holder_df

df = aa.append(bb, ignore_index=True, sort=False)
df = df.sort_values('record_date')
df = df.reset_index(drop=True)
df = df.fillna(0)
df_len = len(df)


i = 0
tmp_c = 0
tmp_h = 0

#find the first valid data
while ( i < df_len):
    if(df.close[i]):  
        tmp_c = df.close[i]
        break
    i = i + 1

i = 0
while ( i < df_len):
    if(df.holder_num[i]):
        tmp_h = df.holder_num[i]
        break
    i = i + 1

#set 0 with valid data
while ( i < df_len):
    if(df.close[i] == 0):  
        #df.close[i] = tmp_c       
        df.loc[i,'close'] = tmp_c

    if(df.holder_num[i] == 0):  
        #df.holder_num[i] = tmp_h
        df.loc[i,'holder_num'] = tmp_h

    if(df.close[i]):    
        tmp_c = df.close[i]        

    if(df.holder_num[i]):
        tmp_h = df.holder_num[i]

    i = i + 1



x = df['record_date'].to_numpy()
y = df['close'].to_numpy()
z = df['holder_num'].to_numpy()
z = z/10000.0

print('len(x)=%d, len(y)=%d, len(z)=%d'  % (len(x), len(y), len(z)))
print(x)
print(y)
print(z)

fig = plt.figure(figsize=(24, 30),dpi=240)
ax = fig.add_subplot(111)
ax.plot(x,y, '-', label = 'close')

ax.set_xticks(range(0, len(df.index), 20))
#ax.set_xticklabels(df.index[::20],rotation=45)
ax.set_xticklabels(df['record_date'][::20],rotation=45)

ax2 = ax.twinx()
ax2.plot(x,z, '-r', label = 'holder_num')
fig.legend(loc=1, bbox_to_anchor=(1,1), bbox_transform=ax.transAxes)

ax.set_xlabel(" %s x [units]" % nowcode)
ax.set_ylabel(r"close")
ax2.set_ylabel(r"holder_num(w)")

plt.savefig('0.png')

