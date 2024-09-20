#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2
import pandas as pd
import time

import os
import numpy as np
from io import StringIO


debug = 0
'''
income
['报表日期'                                             ['record_date',         
'一、营业总收入'                                        'biztotinco',
'营业收入'                                              'bizinco',
'二、营业总成本'                                        'biztotcost',
'营业成本'                                              'bizcost',
'营业税金及附加'                                        'biztax',
'销售费用'                                              'salesexpe',
'管理费用'                                              'manaexpe',
'财务费用'                                              'finexpe',
 '研发费用'                                             'deveexpe',
'资产减值损失'                                          'asseimpaloss',
'公允价值变动收益'                                      'valuechgloss',
'投资收益'                                              'inveinco',
'其中:对联营企业和合营企业的投资收益'                   'assoinveprof',
'汇兑收益'                                              'exchggain',
'三、营业利润'                                          'perprofit',
 '加:营业外收入'                                        'nonoreve',
'减：营业外支出'                                        'nonoexpe',
'其中：非流动资产处置损失'                              'noncassetsdisl',
'四、利润总额'                                          'totprofit',
'减：所得税费用'                                        'incotaxexpe',
'五、净利润'                                            'netprofit',
 '归属于母公司所有者的净利润'                           'parenetp',
'少数股东损益'                                          'minysharrigh',
'基本每股收益(元/股)'                                   'basiceps',
'稀释每股收益(元/股)'                                   'dilutedeps',
'七、其他综合收 益'                                     'othercompinco',
 '八、综合收益总额'                                     'compincoamt',
'归属于母公司所有者的综合收益总额'                      'parecompincoamt',
'归属于少数股东的综合收益总额'                          'minysharincoamt',
'002261'                                                'stock_code',
'拓维信息']                                             'stock_name'
'''

sina_clos = " record_date, biztotinco, bizinco, biztotcost, bizcost, biztax, \
             salesexpe, manaexpe, finexpe, deveexpe, asseimpaloss, valuechgloss, \
             inveinco, assoinveprof, exchggain, perprofit, nonoreve, nonoexpe, \
             noncassetsdisl, totprofit, incotaxexpe, netprofit, parenetp, minysharrigh, \
             basiceps, dilutedeps, othercompinco, compincoamt, parecompincoamt, minysharincoamt,\
             stock_code, stock_name "

class HData_sina_income(object):
    def __init__(self,user,password):
        # self.aaa = aaa
        self.sina_income_table=[]
        self.user=user
        self.password=password

        self.conn=None
        self.cur=None

    
    def db_connect(self):
        self.conn = psycopg2.connect(database="usr", \
                                user=self.user, password=self.password, host="127.0.0.1",\
                                port="5432")
        self.cur = self.conn.cursor()

    def db_disconnect(self):

        self.conn.close()

    def table_is_exist(self):
        self.db_connect()
        self.cur.execute("select count(*) from pg_class where relname = 'sina_income_table' ;")
        ans=self.cur.fetchall()
        #print(list(ans[0])[0])
        if list(ans[0])[0]:
            self.conn.commit()
            self.db_disconnect()
            return True
        else:
            self.conn.commit()
            self.db_disconnect()
            return False

        pass




    def db_hdata_sina_create(self):

        self.db_connect()

        #ystz  float,  yingyeshouru tongbi
        #sjltz  float,  jinglirun tongbi
        # 创建stocks表
        self.cur.execute('''
            drop table if exists sina_income_table;
            create table sina_income_table(
                record_date    date,
                biztotinco    float,
                bizinco    float,
                biztotcost    float,
                bizcost    float,
                biztax    float,
                salesexpe    float,
                manaexpe    float,
                finexpe    float,
                deveexpe    float,
                asseimpaloss    float,
                valuechgloss    float,
                inveinco    float,
                assoinveprof    float,
                exchggain    float,
                perprofit    float,
                nonoreve    float,
                nonoexpe    float,
                noncassetsdisl    float,
                totprofit    float,
                incotaxexpe    float,
                netprofit    float,
                parenetp    float,
                minysharrigh    float,
                basiceps    float,
                dilutedeps    float,
                othercompinco    float,
                compincoamt    float,
                parecompincoamt    float,
                minysharincoamt    float,
                stock_code    varchar,
                stock_name    varchar
                                
            );
            alter table sina_income_table add primary key(stock_code,record_date);
            ''')
        self.conn.commit()
        self.db_disconnect()

        print("db_sina_income_table_create finish")
        pass

    def copy_from_stringio(self, df):
        """
        Here we are going save the dataframe in memory
        and use copy_from() to copy it to the table
        """
        # save dataframe to an in memory buffer
        buffer = StringIO()
        #df.to_csv(buffer, index_label='id', header=False)
        df.to_csv(buffer, index=0, header=False)
        buffer.seek(0)

        self.db_connect()
        try:
            self.cur.copy_from(buffer, table='sina_income_table', sep=",")
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.conn.rollback()
            self.db_disconnect()
            return 1
        
        #print("copy_from_stringio() done")
        self.db_disconnect()



    def db_get_maxdate_of_stock(self,stock_code):#获取某支股票的最晚日期

        self.db_connect()
        self.cur.execute("select max(record_date) from sina_income_table \
                where stock_code=\'" + stock_code+ "\' ;")
        ans=self.cur.fetchall()
        if(len(ans)==0):
            self.conn.commit()
            self.db_disconnect()
            return None
        else:
            self.conn.commit()
            self.db_disconnect()
            return ans[0][0]

        pass

    def insert_all_stock_data(self, data):
        #data format: record_date , stock_code , open , close , high , low  , volume ,  amount  , \
        #        p_change 
        #data format: ['timestamp', 'symbol', 'open', 'close', 'high', 'low', 'volume', 'amount', \
        #       'percent', 'chg', 'turnoverrate', 'pe', 'pb', 'ps', 'pcf', 'market_capital', \
        #       'hk_volume', 'hk_pct', 'hk_net', 'is_quad', 'is_zig', 'is_quad']
        self.db_connect()
        t1=time.time()

        if debug:
            print('insert_all_stock_data()')
        if data is None:
            print("None")
        else:
            length = len(data)
            sql_cmd = ""
            each_num = 1000
            for i in range(0,length):
                if debug:
                    print (i)

                #str_temp+="\'"+stock_code+"\'"+","
                #str_temp+="\'"+data.index[i]+"\'"
                #str_temp+="\'"+data.index[i].strftime("%Y-%m-%d")+"\'"

                #df.shape[0]  size of df
                #df.shape[1]  size of df.colunms

                str_temp= "\'" + str(data.iloc[i,0]) +  "\'"    #timestamp must be string
                for j in range(1,data.shape[1]):
                    str_temp+=",\'"+str(data.iloc[i,j]) + "\'"      #stock_code must be string

                sql_cmd += "("+str_temp+")"
                if i == 0:
                    sql_cmd += ","
                elif i % each_num == 0 or i == (length -1):
                    pass
                else:
                    sql_cmd += ","

                if i % each_num == 0 and i != 0:
                    if debug:
                        print(sql_cmd)
                    if(sql_cmd != ""):
                        income_cmd = "insert into sina_income_table ("\
                                + sina_cols + \
                                " ) values "+sql_cmd+";"
                        if debug:
                            print(income_cmd)
                        self.cur.execute(income_cmd)
                        self.conn.commit()
                        sql_cmd = ""

            if debug:
                print(sql_cmd)
            if(sql_cmd != ""):
                income_cmd = "insert into sina_income_table ("\
                        + sina_cols + \
                        " ) values "+sql_cmd+";"
                if debug:
                    print(income_cmd)
                self.cur.execute(income_cmd)
                self.conn.commit()

        if debug:
            print(time.time()-t1)
            print('insert_all_stock_data(\\)')

        self.db_disconnect()

    def insert_all_stock_data_2(self, data):
        self.db_connect()
        t0 = t1 = t2 = t3 = t4 = t5 = time.time()

        if debug:
            print('insert_all_stock_data()')

        if data is None:
            print("None")
        else:
            length = len(data)
            sql_cmd = []
            each_num = 1000
            for i in range(length):
                t1 = time.time()
                if debug:
                    print (i)

                #str_temp+="\'"+stock_code+"\'"+","
                #str_temp+="\'"+data.index[i]+"\'"
                #str_temp+="\'"+data.index[i].strftime("%Y-%m-%d")+"\'"

                str_temp=[]
                str_temp.append('\'')
                str_temp.append(str(data.iloc[i,0]))
                str_temp.append('\'')

                #data.values.tolist()[0]
                for j in range(1,data.shape[1]):
                    str_temp.append(',\'')
                    str_temp.append(str(data.iloc[i,j]))
                    str_temp.append('\'')

                sql_cmd.append('(')
                sql_cmd.extend(str_temp)
                sql_cmd.append(')')
                if i == 0:
                    sql_cmd.append(",")
                elif i % each_num == 0 or i == (length -1):
                    pass
                else:
                    sql_cmd.append(",")

                if i % each_num == 0 and i != 0:
                    if debug:
                        print(sql_cmd)
                        print("--------------------------------------")
                        t=''.join(sql_cmd)
                        print(t)
                    if len(sql_cmd):
                        income_sql = [] 
                        income_sql.append("insert into sina_income_table (")
                        income_sql.append(sina_cols)
                        income_sql.append( " ) values ")
                        income_sql.append(''.join(sql_cmd))
                        income_sql.append( "  on conflict (stock_code, record_date) do nothing ; ")
                        #print(''.join(income_sql))
                        sql_cmd = []
                        if debug:
                            print(income_sql)
                        t2 = time.time()
                        self.cur.execute(''.join(income_sql))
                        t3 = time.time()
                        self.conn.commit()
                        t4 = time.time()
                        print(t1, t2, t3, t4, t5)
                t5 = time.time()
            if debug:
                print(t5-t1)
                print(sql_cmd)
                print(sql_cmd)
                print("--------------------------------------")
                t=''.join(sql_cmd)
                print(t)

            if len(sql_cmd):
                income_sql = []
                income_sql.append("insert into sina_income_table (")
                income_sql.append(sina_cols)
                income_sql.append( " ) values ")
                income_sql.append(''.join(sql_cmd))
                income_sql.append( "  on conflict (stock_code, record_date) do nothing; ")
                #print(''.join(income_sql))
                sql_cmd = []
                self.cur.execute(''.join(income_sql))
                self.conn.commit()


        if debug:
            print(time.time()-t0)
            print('insert_all_stock_data(\\)')

        print('insert_all_stock_data(\\) %s records are updated successfully' % len(data))

        self.db_disconnect()

    #https://developer.aliyun.com/article/74419
    #PostgreSQL数据库如果不存在则插入，存在则更新
    def insert_all_stock_data_3(self, data):
        self.db_connect()
        t0 = t1 = t2 = t3 = t4 = t5 = time.time()

        if debug:
            print('insert_all_stock_data_3()')

        if data is None:
            print("None")
        else:
            data.to_sql(name='sina_income_table', con=self.conn, if_exists = 'replace', index=False)
            pass

        if debug:
            print(time.time()-t0)
            print('insert_all_stock_data_3(\\)')

        self.db_disconnect()

    #fix bug: delete zero when the stock is closed
    def delete_amount_is_zero(self):
        self.db_connect()
        sql_temp="delete from sina_income_table where amount = 0;"
        self.cur.execute(sql_temp)

        self.conn.commit()
        self.db_disconnect()
        pass

    def get_data_from_hdata(self, stock_code=None, 
                        start_date=None, 
                        end_date=None,
                        limit=0):#将数据库中的数据读取并转为dataframe格式返回
        self.db_connect()
        
        and_flag = False


        sql_temp = "select"  
        sql_temp += sina_cols 
        sql_temp += "from ( "

        sql_temp += "select"
        sql_temp += sina_cols
        sql_temp += "from sina_income_table"

        if stock_code is None and start_date is None and end_date is None:
            pass
        else:
            sql_temp += " where "

        if stock_code is None:
            pass
        else:
            sql_temp += " stock_code="+"\'"+stock_code+"\'"                       
            and_flag |= True

        if start_date is None:
            pass
        else:
            if and_flag:
                sql_temp += " and record_date >="+"\'"+start_date+"\'"                       
            else:
                sql_temp += " record_date >="+"\'"+start_date+"\'"                       
            
            and_flag |= True


        if end_date is None:
            pass
        else:
            if and_flag:
                sql_temp += " and record_date <="+"\'"+end_date+"\'"                       
            else:
                sql_temp += " record_date <="+"\'"+end_date+"\'"                       


        sql_temp += " order by record_date desc "                 

        if limit == 0:
            pass
        else:
            sql_temp += " LIMIT "+"\'"+str(limit)+"\'" 

        sql_temp +=" ) as tbl order by record_date asc"

        sql_temp += ";"

        if debug:
            print("get_data_from_hdata, sql_temp:%s" % sql_temp)



        #select * from (select * from hdata_hsgt_table where stock_code='000922' order by record_date desc LIMIT 5) as tbl order by record_date asc;
        self.cur.execute(sql_temp)
        rows = self.cur.fetchall()

        self.conn.commit()
        self.db_disconnect()

        dataframe_cols=[tuple[0] for tuple in self.cur.description]#列名和数据库列一致
        df = pd.DataFrame(rows, columns=dataframe_cols)
        df['record_date'] = df['record_date'].apply(lambda x: x.strftime('%Y-%m-%d'))        

        if debug:
            print(type(df))
            print(df.head(2))
    
        return df
        pass
 
    def delete_data_from_hdata(self, stock_code=None, 
                        start_date=None, 
                        end_date=None,
                        ):
        self.db_connect()
        
        and_flag = False

        sql_temp = "delete from sina_income_table"

        if stock_code is None and start_date is None and end_date is None:
            self.db_disconnect()
            pass
            return
        else:
            sql_temp += " where "

        if stock_code is None:
            pass
        else:
            sql_temp += " stock_code="+"\'"+stock_code+"\'"                       
            and_flag |= True

        if start_date is None:
            pass
        else:
            if and_flag:
                sql_temp += " and record_date >="+"\'"+start_date+"\'"                       
            else:
                sql_temp += " record_date >="+"\'"+start_date+"\'"                       
            
            and_flag |= True


        if end_date is None:
            pass
        else:
            if and_flag:
                sql_temp += " and record_date <="+"\'"+end_date+"\'"                       
            else:
                sql_temp += " record_date <="+"\'"+end_date+"\'"                       

        sql_temp += ";"

        print("delete_data_from_hdata, sql_temp:%s" % sql_temp)

        self.cur.execute(sql_temp)
        self.conn.commit()
        self.db_disconnect()
        pass
 
        
'''
'''
