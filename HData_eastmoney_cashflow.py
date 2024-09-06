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
              security_code ,
              security_name_abbr ,
              industry_name ,
              report_date ,

              netcash_operate ,     经营性现金流量净额
              netcash_operate_ratio ,经营性现金流量净额占比
              sales_services ,销售商品、提供劳务收到的现金

              sales_services_ratio ,销售商品、提供劳务收到的现金占比
              pay_staff_cash ,  支付给职工以及为职工支付的现金
              psc_ratio ,支付给职工以及为职工支付的现金占比
              netcash_invest ,投资活动产生的现金流量净额

              netcash_invest_ratio ,投资活动产生的现金流量净额占比
              receive_invest_income ,取得投资收益收到的现金
              rii_ratio ,取得投资收益收到的现金占比

              construct_long_asset ,购建固定资产、无形资产和其他长期资产支付的现金
              cla_ratio ,购建固定资产、无形资产和其他长期资产支付的现金占比
              netcash_finance ,融资性现金流量净额

              netcash_finance_ratio ,融资性现金流量净额占比
              cce_add ,现金及现金等价物净增加额
              cce_add_ratio ,现金及现金等价物净增加额占比

              customer_deposit_add ,
              cda_ratio ,
              deposit_iofi_other ,
              dio_ratio ,

              loan_advance_add ,
              laa_ratio ,
              receive_interest_commission ,

              ric_ratio ,
              invest_pay_cash ,
              ipc_ratio ,
              begin_cce ,

              begin_cce_ratio ,
              end_cce ,
              end_cce_ratio ,
              receive_origic_premium ,

              rop_ratio ,
              pay_origic_compensate ,
              poc_ratio 


'''


#eastmoney_cols = " security_code, security_name_abbr, industry_name, report_date,\
eastmoney_cols = " stock_code, stock_name, industry_name, record_date,\
    netcash_operate, netcash_operate_ratio, sales_services,\
    sales_services_ratio, pay_staff_cash, psc_ratio, netcash_invest,\
    netcash_invest_ratio, receive_invest_income, rii_ratio,\
    construct_long_asset, cla_ratio, netcash_finance,\
    netcash_finance_ratio, cce_add, cce_add_ratio,\
    customer_deposit_add, cda_ratio, deposit_iofi_other, dio_ratio,\
    loan_advance_add, laa_ratio, receive_interest_commission,\
    ric_ratio, invest_pay_cash, ipc_ratio, begin_cce,\
    begin_cce_ratio, end_cce, end_cce_ratio, receive_origic_premium,\
    rop_ratio, pay_origic_compensate, poc_ratio "

class HData_eastmoney_cashflow(object):
    def __init__(self,user,password):
        # self.aaa = aaa
        self.eastmoney_cashflow_table=[]
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
        self.cur.execute("select count(*) from pg_class where relname = 'eastmoney_cashflow_table' ;")
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

        #ystz  float,  yingyeshouru tongbi
        #sjltz  float,  jinglirun tongbi
        # 创建stocks表
        self.cur.execute('''
            drop table if exists eastmoney_cashflow_table;
            create table eastmoney_cashflow_table(
                stock_code  varchar, 
                stock_name  varchar, 
                industry_name  varchar, 
                record_date  date,

                netcash_operate  float,
                netcash_operate_ratio  float,
                sales_services  float,

                sales_services_ratio  float,
                pay_staff_cash  float,
                psc_ratio  float,
                netcash_invest  float,

                netcash_invest_ratio  float,
                receive_invest_income  float,
                rii_ratio  float,

                construct_long_asset  float,
                cla_ratio  float,
                netcash_finance  float,

                netcash_finance_ratio  float,
                cce_add  float,
                cce_add_ratio  float,

                customer_deposit_add  float,
                cda_ratio  float,
                deposit_iofi_other  float,
                dio_ratio  float,

                loan_advance_add  float,
                laa_ratio  float,
                receive_interest_commission  float,

                ric_ratio  float,
                invest_pay_cash  float,
                ipc_ratio  float,
                begin_cce  float,

                begin_cce_ratio  float,
                end_cce  float,
                end_cce_ratio  float,
                receive_origic_premium  float,

                rop_ratio  float,
                pay_origic_compensate  float,
                poc_ratio float

            );
            alter table eastmoney_cashflow_table add primary key(stock_code,record_date);
            ''')
        self.conn.commit()
        self.db_disconnect()

        print("db_eastmoney_cashflow_table_create finish")
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
            self.cur.copy_from(buffer, table='eastmoney_cashflow_table', sep=",")
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
        self.cur.execute("select max(record_date) from eastmoney_cashflow_table \
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
                        cashflowl_cmd = "insert into eastmoney_cashflow_table ("\
                                + eastmoney_cols + \
                                " ) values "+sql_cmd+";"
                        if debug:
                            print(cashflowl_cmd)
                        self.cur.execute(cashflowl_cmd)
                        self.conn.commit()
                        sql_cmd = ""

            if debug:
                print(sql_cmd)
            if(sql_cmd != ""):
                cashflowl_cmd = "insert into eastmoney_cashflow_table ("\
                        + eastmoney_cols + \
                        " ) values "+sql_cmd+";"
                if debug:
                    print(cashflowl_cmd)
                self.cur.execute(cashflowl_cmd)
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
                        cashflowl_sql = [] 
                        cashflowl_sql.append("insert into eastmoney_cashflow_table (")
                        cashflowl_sql.append(eastmoney_cols)
                        cashflowl_sql.append( " ) values ")
                        cashflowl_sql.append(''.join(sql_cmd))
                        cashflowl_sql.append( "  on conflict (stock_code, record_date) do nothing ; ")
                        #print(''.join(cashflowl_sql))
                        sql_cmd = []
                        if debug:
                            print(cashflowl_sql)
                        t2 = time.time()
                        self.cur.execute(''.join(cashflowl_sql))
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
                cashflowl_sql = []
                cashflowl_sql.append("insert into eastmoney_cashflow_table (")
                cashflowl_sql.append(eastmoney_cols)
                cashflowl_sql.append( " ) values ")
                cashflowl_sql.append(''.join(sql_cmd))
                cashflowl_sql.append( "  on conflict (stock_code, record_date) do nothing; ")
                #print(''.join(cashflowl_sql))
                sql_cmd = []
                self.cur.execute(''.join(cashflowl_sql))
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
            data.to_sql(name='eastmoney_cashflow_table', con=self.conn, if_exists = 'replace', index=False)
            pass

        if debug:
            print(time.time()-t0)
            print('insert_all_stock_data_3(\\)')

        self.db_disconnect()

    #fix bug: delete zero when the stock is closed
    def delete_amount_is_zero(self):
        self.db_connect()
        sql_temp="delete from eastmoney_cashflow_table where amount = 0;"
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
        sql_temp += "from eastmoney_cashflow_table"

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

        sql_temp = "delete from eastmoney_cashflow_table"

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
 




        
