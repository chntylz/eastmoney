#!/usr/bin/env python  
# -*- coding: utf-8 -*-

from file_interface import *
import psycopg2
import pandas as pd
import time

import os
import numpy as np
from io import StringIO


debug = 0
debug = 1

xq_kday_cols = ' stock_code, current, percent, chg, record_date, volume, amount,\
    market_capital, float_market_capital, turnover_rate, amplitude,\
    open, last_close, high, low, avg_price, trade_volume,\
    side, is_trade, level, trade_session, trade_type,\
    current_year_percent, trade_unique_id, type, bid_appl_seq_num,\
    offer_appl_seq_num, volume_ext, traded_amount_ext,\
    trade_type_v2, yield_to_maturity  '



class HData_xq_kday(object):
    def __init__(self,user,password):
        # self.aaa = aaa
        self.xq_kday_table=[]
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
        self.cur.execute("select count(*) from pg_class where relname = 'xq_kday_table' ;")
        ans=self.cur.fetchall()
        #my_dbg(list(ans[0])[0])
        if list(ans[0])[0]:
            self.conn.commit()
            self.db_disconnect()
            return True
        else:
            self.conn.commit()
            self.db_disconnect()
            return False

        pass




    def db_hdata_xq_kday_create(self):

        self.db_connect()

        # 创建stocks表
        self.cur.execute('''
            drop table if exists xq_kday_table;
            create table xq_kday_table(
                stock_code    varchar,
                current    float,
                percent    float,
                chg    float,
                record_date    date,
                volume    float,
                amount    float,

                market_capital    float,
                float_market_capital    float,
                turnover_rate    float,
                amplitude    float,

                open    float,
                last_close    float,
                high    float,
                low    float,
                avg_price    float,
                trade_volume    float,

                side    float,
                is_trade    boolean,
                level    float,
                trade_session    float,
                trade_type    float,

                current_year_percent    float,
                trade_unique_id    float,
                type    float,
                bid_appl_seq_num    float,

                offer_appl_seq_num    float,
                volume_ext    float,
                traded_amount_ext    float,

                trade_type_v2    float,
                yield_to_maturity  float

                );
            alter table xq_kday_table add primary key(stock_code,record_date);
            ''')
        self.conn.commit()
        self.db_disconnect()

        my_dbg("db_xq_kday_table_create finish")
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
            self.cur.copy_from(buffer, table='xq_kday_table', sep=",")
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            my_dbg("Error: %s" % error)
            self.conn.rollback()
            self.db_disconnect()
            return 1
        
        #my_dbg("copy_from_stringio() done")
        self.db_disconnect()



    def db_get_maxdate_of_stock(self,stock_code):#获取某支股票的最晚日期

        self.db_connect()
        self.cur.execute("select max(record_date) from xq_kday_table \
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
            my_dbg('insert_all_stock_data()')
        if data is None:
            my_dbg("None")
        else:
            length = len(data)
            sql_cmd = ""
            each_num = 1000
            for i in range(0,length):
                if debug:
                    my_dbg (i)

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
                        my_dbg(sql_cmd)
                    if(sql_cmd != ""):
                        final_cmd = "insert into xq_kday_table ("\
                                + xq_kday_cols + \
                                " ) values "+sql_cmd+";"
                        if debug:
                            my_dbg(final_cmd)
                        self.cur.execute(final_cmd)
                        self.conn.commit()
                        sql_cmd = ""

            if debug:
                my_dbg(sql_cmd)
            if(sql_cmd != ""):
                final_cmd = "insert into xq_kday_table ("\
                        + xq_kday_cols + \
                        " ) values "+sql_cmd+";"
                if debug:
                    my_dbg(final_cmd)
                self.cur.execute(final_cmd)
                self.conn.commit()

        if debug:
            my_dbg(time.time()-t1)
            my_dbg('insert_all_stock_data(\\)')

        self.db_disconnect()

    def insert_all_stock_data_2(self, data):
        self.db_connect()
        t0 = t1 = t2 = t3 = t4 = t5 = time.time()

        if debug:
            my_dbg('insert_all_stock_data()')

        if data is None:
            my_dbg("None")
        else:
            length = len(data)
            sql_cmd = []
            each_num = 1000
            for i in range(length):
                t1 = time.time()
                if debug:
                    my_dbg (i)

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
                        my_dbg(sql_cmd)
                        my_dbg("--------------------------------------")
                        t=''.join(sql_cmd)
                        my_dbg(t)
                    if len(sql_cmd):
                        final_sql = [] 
                        final_sql.append("insert into xq_kday_table (")
                        final_sql.append(xq_kday_cols)
                        final_sql.append( " ) values ")
                        final_sql.append(''.join(sql_cmd))
                        final_sql.append( " ; ")
                        #my_dbg(''.join(final_sql))
                        sql_cmd = []
                        t2 = time.time()
                        self.cur.execute(''.join(final_sql))
                        t3 = time.time()
                        self.conn.commit()
                        t4 = time.time()
                        my_dbg(t1, t2, t3, t4, t5)
                t5 = time.time()
                my_dbg(t5-t1)
            if debug:
                my_dbg(sql_cmd)
                my_dbg(sql_cmd)
                my_dbg("--------------------------------------")
                t=''.join(sql_cmd)
                my_dbg(t)

            if len(sql_cmd):
                final_sql = []
                final_sql.append("insert into xq_kday_table (")
                final_sql.append(xq_kday_cols)
                final_sql.append( " ) values ")
                final_sql.append(''.join(sql_cmd))
                final_sql.append( " ; ")
                #my_dbg(''.join(final_sql))
                sql_cmd = []
                self.cur.execute(''.join(final_sql))
                self.conn.commit()


        if debug:
            my_dbg(time.time()-t0)
            my_dbg('insert_all_stock_data(\\)')


        self.db_disconnect()


    def insert_all_stock_data_3(self, data):
        self.db_connect()
        t0 = t1 = t2 = t3 = t4 = t5 = time.time()

        if debug:
            my_dbg('insert_all_stock_data_3()')

        if data is None:
            my_dbg("None")
        else:
            data.to_sql(name='xq_kday_table', con=self.conn, if_exists = 'replace', index=False)
            pass

        if debug:
            my_dbg(time.time()-t0)
            my_dbg('insert_all_stock_data_3(\\)')

        self.db_disconnect()


    #fix bug: delete zero when the stock is closed
    def delete_amount_is_zero(self):
        self.db_connect()
        sql_temp="delete from xq_kday_table where amount = 0;"
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
        sql_temp += xq_kday_cols 
        sql_temp += "from ( "

        sql_temp += "select"
        sql_temp += xq_kday_cols
        sql_temp += "from xq_kday_table"

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
            my_dbg("get_data_from_hdata, sql_temp:%s" % sql_temp)



        #select * from (select * from hdata_hsgt_table where stock_code='000922' order by record_date desc LIMIT 5) as tbl order by record_date asc;
        self.cur.execute(sql_temp)
        rows = self.cur.fetchall()

        self.conn.commit()
        self.db_disconnect()

        dataframe_cols=[tuple[0] for tuple in self.cur.description]#列名和数据库列一致
        df = pd.DataFrame(rows, columns=dataframe_cols)
        df['record_date'] = df['record_date'].apply(lambda x: x.strftime('%Y-%m-%d'))        

        if debug:
            my_dbg(type(df))
            my_dbg(df.head(2))
    
        return df
        pass
 
    def delete_data_from_hdata(self, stock_code=None, 
                        start_date=None, 
                        end_date=None,
                        ):
        self.db_connect()
        
        and_flag = False

        sql_temp = "delete from xq_kday_table"

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

        my_dbg("delete_data_from_hdata, sql_temp:%s" % sql_temp)

        self.cur.execute(sql_temp)
        self.conn.commit()
        self.db_disconnect()
        pass
 

    def get_latest_data_from_hdata(self):#将数据库中的数据读取并转为dataframe格式返回

        self.db_connect()
        sql_temp = ' select * from xq_kday_table where record_date = '\
                + '(select max(record_date) from xq_kday_table as tmp_date); '

        if debug:
            my_dbg("get_latest_data_from_hdata, sql_temp:%s" % sql_temp)

        self.cur.execute(sql_temp)
        rows = self.cur.fetchall()

        self.conn.commit()
        self.db_disconnect()

        #return rows

        dataframe_cols=[tuple[0] for tuple in self.cur.description]#列名和数据库列一致
        df = pd.DataFrame(rows, columns=dataframe_cols)
        df['record_date'] = df['record_date'].apply(lambda x: x.strftime('%Y-%m-%d'))        

        if debug:
            my_dbg(type(df))
            my_dbg(df.head(2))
    
        return df
 



        
