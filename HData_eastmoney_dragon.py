#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2
import pandas as pd
import time

import os
import numpy as np
from io import StringIO





debug = 0
#debug = 1

class HData_eastmoney_dragon(object):
    def __init__(self,user,password):
        # self.aaa = aaa
        self.eastmoney_dragon_table=[]
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
        self.cur.execute("select count(*) from pg_class where relname = 'eastmoney_dragon_table' ;")
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




    def db_hdata_xq_create(self):

        self.db_connect()

        # 创建stocks表
        self.cur.execute('''
            drop table if exists eastmoney_dragon_table;
            create table eastmoney_dragon_table(
	    SSL float,
	    RChange3c float,
	    stock_name varchar,
	    RChange10DO float,
	    AGSZBHXS float,
	    RChange10DC float,
	    TurNover float,
	    Statistic float,
	    CPrice float,
	    stock_code varchar,
	    BSL float,
	    RChange5DC float,
	    PBRate float,
	    RChange20DO float,
	    RChange2DC float,
	    record_date date,
	    SMoney float,
	    TurnRate float,
	    RChange6M float,
	    PBuy float,
	    RChange30DC float,
	    MEMO float,
	    BMoney float,
	    Dchratio float,
	    RChange15DO float,
	    RChange15DC float,
	    RChange3DO float,
	    RChange2DO float,
	    RChange1M float,
	    CTypeDes varchar,
	    RChange5DO float,
	    RChange1DO float,
	    RChange30DO float,
	    RChange1DC float,
	    Ntransac float,
	    RID float,
	    RChange1Y float,
	    Chgradio float,
	    RChange3DC float,
	    RChange20DC float
                );
            alter table eastmoney_dragon_table add primary key(stock_code,record_date);
            ''')
        self.conn.commit()
        self.db_disconnect()

        print("db_eastmoney_dragon_table_create finish")
        pass

    def copy_from_stringio(self, df):
        """
        Here we are going save the dataframe in memory
        and use copy_from() to copy it to the table
        """
        # save dataframe to an in memory buffer
        buffer = StringIO()
        #df.to_csv(buffer, index_label='id', header=False)
        if debug:
            df.to_csv('./test.csv', encoding='utf-8')

        df.to_csv(buffer, index=0, header=False)
        buffer.seek(0)

        self.db_connect()
        try:
            self.cur.copy_from(buffer, table='eastmoney_dragon_table', sep=",")
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
        self.cur.execute("select max(record_date) from eastmoney_dragon_table \
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

    def get_data_from_hdata(self, stock_code=None, 
                        start_date=None, 
                        end_date=None,
                        limit=0):#将数据库中的数据读取并转为dataframe格式返回
        self.db_connect()
        
        and_flag = False


        sql_temp = "select * from ( "

        sql_temp += "select * from eastmoney_dragon_table"

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

        sql_temp = "delete from eastmoney_dragon_table"

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

        if debug:
            print("delete_data_from_hdata, sql_temp:%s" % sql_temp)

        self.cur.execute(sql_temp)
        self.conn.commit()
        self.db_disconnect()
        pass
 

#alter table eastmoney_dragon_table add  "up_days" int not null default 0;


#update eastmoney_dragon_table set is_zig=0 where record_date = '2018-10-08' and stock_code = '002732';

#UPDATE eastmoney_dragon_table SET is_zig = tmp.is_zig, is_quad=tmp.is_quad FROM    (VALUES ( DATE  '2018-06-13', '600647', 11, 1), ( DATE  '2018-06-12', '600647', 12, 1) ) AS tmp (record_date, stock_code, is_zig, is_quad ) WHERE eastmoney_dragon_table.record_date = tmp.record_date and eastmoney_dragon_table.stock_code = tmp.stock_code;
#select * from eastmoney_dragon_table where stock_code ='600647' and (record_date='2018-06-13' or record_date='2018-06-12' ); 
