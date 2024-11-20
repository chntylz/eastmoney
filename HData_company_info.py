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

'''
    股票代码,                   stock_code
    股票简称,                   stock_name
    公司名称,                   company_name
    省份,                       area
    城市,                       city
    主营业务收入(202409),       op_income
    净利润(202409),             net_income
    员工人数,                   employee
    上市日期,                   issue_date
    招股书,                     stock_book
    公司财报,                   finiance
    行业分类,                   industry_type
    产品类型,                   product_type
    主营业务                    op_bussiness
'''
company_cols = " stock_code, stock_name, company_name,area, city, op_income, \
                net_income, employee, issue_date, stock_book, finiance, \
                industry_type, product_type, op_bussiness "


class HData_company_info(object):
    def __init__(self,user,password):
        # self.aaa = aaa
        self.company_info_table=[]
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
        self.cur.execute("select count(*) from pg_class where relname = 'company_info_table' ;")
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




    def db_hdata_company_create(self):

        self.db_connect()

        # 创建stocks表
        self.cur.execute('''
            drop table if exists company_info_table;
            create table company_info_table(
                stock_code  varchar,
                stock_name varchar,
                company_name varchar,
                area varchar,
                city  varchar,
                op_income float,
                net_income float, 
                employee float,
                issue_date varchar,
                stock_book varchar,
                finiance varchar,
                industry_type varchar,
                product_type varchar,
                op_bussiness varchar
        );
        alter table company_info_table add primary key(stock_code);
            ''')
        self.conn.commit()
        self.db_disconnect()

        print("db_company_info_table_create finish")
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
            self.cur.copy_from(buffer, table='company_info_table', sep=",")
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.conn.rollback()
            self.db_disconnect()
            return 1
        
        #print("copy_from_stringio() done")
        self.db_disconnect()


    def insert_all_stock_data(self, data):
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
                        final_cmd = "insert into company_info_table ("\
                                + company_cols + \
                                " ) values "+sql_cmd+";"
                        if debug:
                            print(final_cmd)
                        self.cur.execute(final_cmd)
                        self.conn.commit()
                        sql_cmd = ""

            if debug:
                print(sql_cmd)
            if(sql_cmd != ""):
                final_cmd = "insert into company_info_table ("\
                        + company_cols + \
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
                        final_sql.append("insert into company_info_table (")
                        final_sql.append(company_cols)
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
                final_sql.append("insert into company_info_table (")
                final_sql.append(company_cols)
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
            data.to_sql(name='company_info_table', con=self.conn, if_exists = 'replace', index=False)
            pass

        if debug:
            print(time.time()-t0)
            print('insert_all_stock_data_3(\\)')

        self.db_disconnect()

    #fix bug: delete zero when the stock is closed
    def delete_amount_is_zero(self):
        self.db_connect()
        sql_temp="delete from company_info_table where amount = 0;"
        self.cur.execute(sql_temp)

        self.conn.commit()
        self.db_disconnect()
        pass


    def get_data_from_hdata(self, stock_code=None, 
                        limit=0):#将数据库中的数据读取并转为dataframe格式返回
        self.db_connect()
        
        sql_temp += "select"
        sql_temp += company_cols
        sql_temp += "from company_info_table"

        if stock_code is None:
            pass
        else:
            sql_temp += " stock_code="+"\'"+stock_code+"\'"                       

        if limit == 0:
            pass
        else:
            sql_temp += " LIMIT "+"\'"+str(limit)+"\'" 

        sql_temp += ";"

        if debug:
            print("get_data_from_hdata, sql_temp:%s" % sql_temp)

        self.cur.execute(sql_temp)
        rows = self.cur.fetchall()

        self.conn.commit()
        self.db_disconnect()

        dataframe_cols=[tuple[0] for tuple in self.cur.description]#列名和数据库列一致
        df = pd.DataFrame(rows, columns=dataframe_cols)

        if debug:
            print(type(df))
            print(df.head(2))
    
        return df
        pass
 

    def delete_data_from_hdata(self, stock_code=None, 
                        ):
        self.db_connect()

        sql_temp = "delete from company_info_table"

        if stock_code is None :
            self.db_disconnect()
            print("#error: delete_data_from_hdata, please input stock_code!")
            return
        else:
            sql_temp += " where stock_code="+"\'"+stock_code+"\'"                       

        sql_temp += ";"

        print("delete_data_from_hdata, sql_temp:%s" % sql_temp)

        self.cur.execute(sql_temp)
        self.conn.commit()
        self.db_disconnect()
        pass
 


