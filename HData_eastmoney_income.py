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
    security_name_abbr,
    industry_name,
    report_date,
    parent_netprofit,      净利润
    total_operate_income,  营业总收入
    total_operate_cost,营业总支出
    toe_ratio,营业总支出同比增长率
    operate_cost,营业支出
    operate_expense,营业支出
    operate_expense_ratio,营业支出同比增长率
    sale_expense,销售费用
    manage_expense,管理费用
    finance_expense,财务费用
    operate_profit,营业利润
    total_profit,利润总额
    income_tax,所得税
    operate_income,
    interest_ni,利息收入
    interest_ni_ratio,利息收入同比增长率
    fee_commission_ni,手续费及佣金收入
    fcn_ratio,手续费及佣金收入同比增长率
    operate_tax_add,
    manage_expense_bank,
    fcn_calculate,
    interest_ni_calculate,
    earned_premium,
    earned_premium_ratio,
    invest_income,
    surrender_value,
    compensate_expense,
    toi_ratio,营业总收入同比增长率
    operate_profit_ratio,营业利润同比增长率
    parent_netprofit_ratio,净利润同比增长率
    deduct_parent_netprofit,扣非归母净利润
    dpn_ratio ,扣非归母净利润同比增长率
              
'''


#eastmoney_cols = " security_code, security_name_abbr, industry_name, report_date,\
eastmoney_cols = " stock_code, stock_name, industry_name, record_date,\
        parent_netprofit, total_operate_income, total_operate_cost,\
        toe_ratio, operate_cost, operate_expense, operate_expense_ratio,\
        sale_expense, manage_expense, finance_expense, operate_profit,\
        total_profit, income_tax, operate_income, interest_ni,\
        interest_ni_ratio, fee_commission_ni, fcn_ratio,\
        operate_tax_add, manage_expense_bank, fcn_calculate,\
        interest_ni_calculate, earned_premium, earned_premium_ratio,\
        invest_income, surrender_value, compensate_expense, toi_ratio,\
        operate_profit_ratio, parent_netprofit_ratio,\
        deduct_parent_netprofit, dpn_ratio "

class HData_eastmoney_income(object):
    def __init__(self,user,password):
        # self.aaa = aaa
        self.eastmoney_income_table=[]
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
        self.cur.execute("select count(*) from pg_class where relname = 'eastmoney_income_table' ;")
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
            drop table if exists eastmoney_income_table;
            create table eastmoney_income_table(
                stock_code  varchar, 
                stock_name  varchar, 
                industry_name  varchar, 
                record_date  date,

                parent_netprofit  float, 
                total_operate_income  float, 
                total_operate_cost float,
                toe_ratio  float, 
                operate_cost  float, 
                operate_expense  float, 
                operate_expense_ratio float,
                sale_expense  float, 
                manage_expense  float, 
                finance_expense  float, 
                operate_profit float,
                total_profit  float, 
                income_tax  float, 
                operate_income  float, 
                interest_ni float,
                interest_ni_ratio  float, 
                fee_commission_ni  float, 
                fcn_ratio float,
                operate_tax_add  float, 
                manage_expense_bank  float, 
                fcn_calculate float,
                interest_ni_calculate  float, 
                earned_premium  float, 
                earned_premium_ratio  float,
                invest_income  float, 
                surrender_value  float, 
                compensate_expense  float, 
                toi_ratio  float,
                operate_profit_ratio  float, 
                parent_netprofit_ratio  float,
                deduct_parent_netprofit  float, 
                dpn_ratio float 


            );
            alter table eastmoney_income_table add primary key(stock_code,record_date);
            ''')
        self.conn.commit()
        self.db_disconnect()

        print("db_eastmoney_income_table_create finish")
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
            self.cur.copy_from(buffer, table='eastmoney_income_table', sep=",")
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
        self.cur.execute("select max(record_date) from eastmoney_income_table \
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
                        incomel_cmd = "insert into eastmoney_income_table ("\
                                + eastmoney_cols + \
                                " ) values "+sql_cmd+";"
                        if debug:
                            print(incomel_cmd)
                        self.cur.execute(incomel_cmd)
                        self.conn.commit()
                        sql_cmd = ""

            if debug:
                print(sql_cmd)
            if(sql_cmd != ""):
                incomel_cmd = "insert into eastmoney_income_table ("\
                        + eastmoney_cols + \
                        " ) values "+sql_cmd+";"
                if debug:
                    print(incomel_cmd)
                self.cur.execute(incomel_cmd)
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
                        incomel_sql = [] 
                        incomel_sql.append("insert into eastmoney_income_table (")
                        incomel_sql.append(eastmoney_cols)
                        incomel_sql.append( " ) values ")
                        incomel_sql.append(''.join(sql_cmd))
                        incomel_sql.append( "  on conflict (stock_code, record_date) do nothing ; ")
                        #print(''.join(incomel_sql))
                        sql_cmd = []
                        if debug:
                            print(incomel_sql)
                        t2 = time.time()
                        self.cur.execute(''.join(incomel_sql))
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
                incomel_sql = []
                incomel_sql.append("insert into eastmoney_income_table (")
                incomel_sql.append(eastmoney_cols)
                incomel_sql.append( " ) values ")
                incomel_sql.append(''.join(sql_cmd))
                incomel_sql.append( "  on conflict (stock_code, record_date) do nothing; ")
                #print(''.join(incomel_sql))
                sql_cmd = []
                self.cur.execute(''.join(incomel_sql))
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
            data.to_sql(name='eastmoney_income_table', con=self.conn, if_exists = 'replace', index=False)
            pass

        if debug:
            print(time.time()-t0)
            print('insert_all_stock_data_3(\\)')

        self.db_disconnect()

    #fix bug: delete zero when the stock is closed
    def delete_amount_is_zero(self):
        self.db_connect()
        sql_temp="delete from eastmoney_income_table where amount = 0;"
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
        sql_temp += "from eastmoney_income_table"

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

        sql_temp = "delete from eastmoney_income_table"

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
 




        
