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

eastmoney_cols = " record_date, stock_code, stock_name, open, close, high, low,\
        volume, amount, amplitude, percent, chg, turnoverrate,\
        pre_close, pe, pb, mkt_cap, circulation_mkt, zlje, \
        is_peach , is_zig , is_quad ,\
        is_macd, is_2d3pct, is_up_days, is_cup_tea, is_duck_head, is_cross3line, is_d_volume "


class HData_eastmoney_day(object):
    def __init__(self,user,password):
        # self.aaa = aaa
        self.eastmoney_d_table=[]
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
        self.cur.execute("select count(*) from pg_class where relname = 'eastmoney_d_table' ;")
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




    def db_hdata_eastmoney_create(self):

        self.db_connect()

        # 创建stocks表
        self.cur.execute('''
            drop table if exists eastmoney_d_table;
            create table eastmoney_d_table(
		record_date date, 
		stock_code varchar, 
		stock_name varchar, 
		open float, 
		close float, 
		high float, 
		low float, 
		volume float, 
		amount float, 
		amplitude float, 
		percent float, 
		chg float, 
		turnoverrate float, 
		pre_close float, 
		pe float, 
		pb float, 
		mkt_cap float, 
		circulation_mkt float, 
		zlje float, 
		is_peach  float, 
		is_zig  float, 
		is_quad  float, 
		is_macd float, 
		is_2d3pct float, 
		is_up_days float, 
		is_cup_tea float, 
		is_duck_head float,
		is_cross3line float, 
	    is_d_volume	float
               );
            alter table eastmoney_d_table add primary key(stock_code,record_date);
            ''')
        self.conn.commit()
        self.db_disconnect()

        print("db_eastmoney_d_table_create finish")
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
            self.cur.copy_from(buffer, table='eastmoney_d_table', sep=",")
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
        self.cur.execute("select max(record_date) from eastmoney_d_table \
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
                        final_cmd = "insert into eastmoney_d_table ("\
                                + eastmoney_cols + \
                                " ) values "+sql_cmd+";"
                        if debug:
                            print(final_cmd)
                        self.cur.execute(final_cmd)
                        self.conn.commit()
                        sql_cmd = ""

            if debug:
                print(sql_cmd)
            if(sql_cmd != ""):
                final_cmd = "insert into eastmoney_d_table ("\
                        + eastmoney_cols + \
                        " ) values "+sql_cmd+";"
                if debug:
                    print(final_cmd)
                self.cur.execute(final_cmd)
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
                        final_sql = [] 
                        final_sql.append("insert into eastmoney_d_table (")
                        final_sql.append(eastmoney_cols)
                        final_sql.append( " ) values ")
                        final_sql.append(''.join(sql_cmd))
                        final_sql.append( " ; ")
                        #print(''.join(final_sql))
                        sql_cmd = []
                        t2 = time.time()
                        self.cur.execute(''.join(final_sql))
                        t3 = time.time()
                        self.conn.commit()
                        t4 = time.time()
                        print(t1, t2, t3, t4, t5)
                t5 = time.time()
                print(t5-t1)
            if debug:
                print(sql_cmd)
                print(sql_cmd)
                print("--------------------------------------")
                t=''.join(sql_cmd)
                print(t)

            if len(sql_cmd):
                final_sql = []
                final_sql.append("insert into eastmoney_d_table (")
                final_sql.append(eastmoney_cols)
                final_sql.append( " ) values ")
                final_sql.append(''.join(sql_cmd))
                final_sql.append( " ; ")
                #print(''.join(final_sql))
                sql_cmd = []
                self.cur.execute(''.join(final_sql))
                self.conn.commit()


        if debug:
            print(time.time()-t0)
            print('insert_all_stock_data(\\)')


        self.db_disconnect()


    def insert_all_stock_data_3(self, data):
        self.db_connect()
        t0 = t1 = t2 = t3 = t4 = t5 = time.time()

        if debug:
            print('insert_all_stock_data_3()')

        if data is None:
            print("None")
        else:
            data.to_sql(name='eastmoney_d_table', con=self.conn, if_exists = 'replace', index=False)
            pass

        if debug:
            print(time.time()-t0)
            print('insert_all_stock_data_3(\\)')

        self.db_disconnect()

    def update_allstock_hdatadate(self, data):

        self.db_connect()

        t1=time.time()

        if debug:
            print(" update_perstock_hdatadate begin")

        if data is None:
            print("None")
        else:
            length = len(data)
            sql_cmd = ""
            sql_head="UPDATE eastmoney_d_table SET is_zig = tmp.is_zig, \
                    is_quad=tmp.is_quad, is_peach=tmp.is_peach FROM ( VALUES "

            sql_tail=" ) AS tmp (record_date, stock_code, is_peach, is_zig, is_quad ) \
                    WHERE eastmoney_d_table.record_date = tmp.record_date \
                    and eastmoney_d_table.stock_code = tmp.stock_code;"

            each_num = 1000
            for i in range(0,length):
                if debug:
                    print (i)

                str_temp = ""
                str_temp+="DATE "+"\'"+str(data.iloc[i,0])+"\'" + ","
                column_size = data.shape[1]

                if debug:
                    print('column_size=%d'% (column_size))
                '''
                for j in range(1, column_size - 1):
                    str_temp+="\'"+str(data.iloc[i,j])+"\'" + ","
                str_temp+="\'"+str(data.iloc[i,column_size-1])+"\'" 
                '''
                str_temp+="\'"+str(data.iloc[i,1])+"\'" + ","
                for j in range(2, column_size - 1):
                    str_temp+=str(data.iloc[i,j])+ ","
                str_temp+=str(data.iloc[i,column_size-1]) 

                sql_cmd= sql_cmd + "("+str_temp+")"

                if i % each_num == 0 or i == (length -1):
                    pass
                else:
                    sql_cmd = sql_cmd+ ","

                if i % each_num == 0:
                    if debug:
                        print(sql_cmd)
                    if(sql_cmd != ""):
                        final_sql=sql_head +sql_cmd+ sql_tail
                        if debug:
                            print('final_sql=%s'%(final_sql))
                            
                        self.cur.execute(final_sql)
                        self.conn.commit()
                        sql_cmd = ""

            if debug:
                print(sql_cmd)
            if(sql_cmd != ""):
                final_sql=sql_head +sql_cmd+ sql_tail
                if debug:
                    print('final_sql=%s'%(final_sql))
                    
                self.cur.execute(final_sql)
                self.conn.commit()
                pass

        if debug:
            print(time.time()-t1)
            print(" insert_perstock_hdatadate finish")

        self.db_disconnect()


    #fix bug: delete zero when the stock is closed
    def delete_amount_is_zero(self):
        self.db_connect()
        sql_temp="delete from eastmoney_d_table where amount = 0;"
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
        sql_temp += eastmoney_cols 
        sql_temp += "from ( "

        sql_temp += "select"
        sql_temp += eastmoney_cols
        sql_temp += "from eastmoney_d_table"

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
 

    def get_latest_data_from_hdata(self):#将数据库中的数据读取并转为dataframe格式返回

        self.db_connect()
        sql_temp = ' select * from eastmoney_d_table where record_date = '\
                + '(select max(record_date) from eastmoney_d_table as tmp_date); '

        if debug:
            print("get_latest_data_from_hdata, sql_temp:%s" % sql_temp)

        self.cur.execute(sql_temp)
        rows = self.cur.fetchall()

        self.conn.commit()
        self.db_disconnect()

        return rows

        dataframe_cols=[tuple[0] for tuple in self.cur.description]#列名和数据库列一致
        df = pd.DataFrame(rows, columns=dataframe_cols)
        df['record_date'] = df['record_date'].apply(lambda x: x.strftime('%Y-%m-%d'))        

        if debug:
            print(type(df))
            print(df.head(2))
    
        return df
 
    def delete_data_from_hdata(self, stock_code=None, 
                        start_date=None, 
                        end_date=None,
                        ):
        self.db_connect()
        
        and_flag = False

        sql_temp = "delete from eastmoney_d_table"

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
 

#alter table eastmoney_d_table add  "up_days" int not null default 0;


#update eastmoney_d_table set is_zig=0 where record_date = '2018-10-08' and stock_code = '002732';

#UPDATE eastmoney_d_table SET is_zig = tmp.is_zig, is_quad=tmp.is_quad FROM    (VALUES ( DATE  '2018-06-13', '600647', 11, 1), ( DATE  '2018-06-12', '600647', 12, 1) ) AS tmp (record_date, stock_code, is_zig, is_quad ) WHERE eastmoney_d_table.record_date = tmp.record_date and eastmoney_d_table.stock_code = tmp.stock_code;
#select * from eastmoney_d_table where stock_code ='600647' and (record_date='2018-06-13' or record_date='2018-06-12' ); 
        
#update eastmoney_d_table set record_date='2021-09-17' where record_date='2021-09-18';
