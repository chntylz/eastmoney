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
      
    'security_code', 
    'security_name_abbr', 
    'industry_name',
    'report_date', 
    'total_assets', 总资产
    'fixed_asset', 固定资产
    'monetaryfunds',货币资金
    'monetaryfunds_ratio', 货币资金同比增长率
    'accounts_rece', 应收账款
    'accounts_rece_ratio',应收账款同比增长率
    'inventory', 存货
    'inventory_ratio', 存货同比增长率
    'total_liabilities', 总负债
    'accounts_payable', 应付账款
    'accounts_payable_ratio', 应付账款同比增长率
    'advance_receivables',预收账款
    'advance_receivables_ratio', 预收账款同比增长率
    'total_equity', 股东权益合计=资产总额-负债总额
    'total_equity_ratio',股东权益同比增长率
    'total_assets_ratio', 总资产同比增长率
    'total_liab_ratio', 总负债同比增长率f
    'current_ratio',  流动比率
    'debt_asset_ratio',   资产负债率=（负债总额÷资产总额）×100%  资产负债率是企业负债总额占企业资产总额的百分比。这个指标反映了在企业的全部资产中由债权人提给的资产所占比重的大小, 反映了债权人向企业提给信贷资金的危机程度, 也反映了企业举债经营的能力

    cash_deposit_pbc,
    cdp_ratio,
    loan_advance,
    loan_advance_ratio,
    available_sale_finasset,
    asf_ratio,
    loan_pbc,
    loan_pbc_ratio,
    accept_deposit,
    accept_deposit_ratio,
    sell_repo_finasset,
    srf_ratio,
    settle_excess_reserve,
    ser_ratio,
    borrow_fund,
    borrow_fund_ratio,
    agent_trade_security,
    ats_ratio,
    premium_rece,
    premium_rece_ratio,
    short_loan,
    short_loan_ratio,
    advance_premium,
    advance_premium_ratio,
               
'''


#eastmoney_cols = " security_code, security_name_abbr, industry_name, report_date,\
eastmoney_cols = " stock_code, stock_name, industry_name, record_date,\
    total_assets, fixed_asset, monetaryfunds, monetaryfunds_ratio,\
    accounts_rece, accounts_rece_ratio, inventory, inventory_ratio,\
    total_liabilities, accounts_payable, accounts_payable_ratio,\
    advance_receivables, advance_receivables_ratio, total_equity,\
    total_equity_ratio, total_assets_ratio, total_liab_ratio, \
    current_ratio, debt_asset_ratio,\
    cash_deposit_pbc,  cdp_ratio, loan_advance, loan_advance_ratio, available_sale_finasset, \
    asf_ratio, loan_pbc, loan_pbc_ratio, accept_deposit, accept_deposit_ratio, sell_repo_finasset, srf_ratio, \
    settle_excess_reserve, ser_ratio, borrow_fund, borrow_fund_ratio, agent_trade_security, \
    ats_ratio, premium_rece, premium_rece_ratio, short_loan, short_loan_ratio, advance_premium,  advance_premium_ratio  " 

class HData_eastmoney_balance(object):
    def __init__(self,user,password):
        # self.aaa = aaa
        self.eastmoney_balance_table=[]
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
        self.cur.execute("select count(*) from pg_class where relname = 'eastmoney_balance_table' ;")
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
            drop table if exists eastmoney_balance_table;
            create table eastmoney_balance_table(
                stock_code  varchar, 
                stock_name  varchar, 
                industry_name  varchar, 
                record_date  date,
                total_assets  float, 
                fixed_asset  float, 
                monetaryfunds  float,
                monetaryfunds_ratio  float,
                accounts_rece  float, 
                accounts_rece_ratio  float, 
                inventory  float, 
                inventory_ratio  float,
                total_liabilities  float, 
                accounts_payable  float, 
                accounts_payable_ratio  float,
                advance_receivables  float, 
                advance_receivables_ratio  float, 
                total_equity  float,
                total_equity_ratio  float, 
                total_assets_ratio  float, 
                total_liab_ratio  float,
                current_ratio  float, 
                debt_asset_ratio  float,

                cash_deposit_pbc    float,
                cdp_ratio    float,
                loan_advance    float,
                loan_advance_ratio    float,
                available_sale_finasset    float,
                asf_ratio    float,
                loan_pbc    float,
                loan_pbc_ratio    float,
                accept_deposit    float,
                accept_deposit_ratio    float,
                sell_repo_finasset    float,
                srf_ratio    float,
                settle_excess_reserve    float,
                ser_ratio    float,
                borrow_fund    float,
                borrow_fund_ratio    float,
                agent_trade_security    float,
                ats_ratio    float,
                premium_rece    float,
                premium_rece_ratio    float,
                short_loan    float,
                short_loan_ratio    float,
                advance_premium    float,
                advance_premium_ratio    float
            );
            alter table eastmoney_balance_table add primary key(stock_code,record_date);
            ''')
        self.conn.commit()
        self.db_disconnect()

        print("db_eastmoney_balance_table_create finish")
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
            self.cur.copy_from(buffer, table='eastmoney_balance_table', sep=",")
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
        self.cur.execute("select max(record_date) from eastmoney_balance_table \
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
                        balancel_cmd = "insert into eastmoney_balance_table ("\
                                + eastmoney_cols + \
                                " ) values "+sql_cmd+";"
                        if debug:
                            print(balancel_cmd)
                        self.cur.execute(balancel_cmd)
                        self.conn.commit()
                        sql_cmd = ""

            if debug:
                print(sql_cmd)
            if(sql_cmd != ""):
                balancel_cmd = "insert into eastmoney_balance_table ("\
                        + eastmoney_cols + \
                        " ) values "+sql_cmd+";"
                if debug:
                    print(balancel_cmd)
                self.cur.execute(balancel_cmd)
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
                        balancel_sql = [] 
                        balancel_sql.append("insert into eastmoney_balance_table (")
                        balancel_sql.append(eastmoney_cols)
                        balancel_sql.append( " ) values ")
                        balancel_sql.append(''.join(sql_cmd))
                        balancel_sql.append( "  on conflict (stock_code, record_date) do nothing ; ")
                        #print(''.join(balancel_sql))
                        sql_cmd = []
                        if debug:
                            print(balancel_sql)
                        t2 = time.time()
                        self.cur.execute(''.join(balancel_sql))
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
                balancel_sql = []
                balancel_sql.append("insert into eastmoney_balance_table (")
                balancel_sql.append(eastmoney_cols)
                balancel_sql.append( " ) values ")
                balancel_sql.append(''.join(sql_cmd))
                balancel_sql.append( "  on conflict (stock_code, record_date) do nothing; ")
                #print(''.join(balancel_sql))
                sql_cmd = []
                self.cur.execute(''.join(balancel_sql))
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
            data.to_sql(name='eastmoney_balance_table', con=self.conn, if_exists = 'replace', index=False)
            pass

        if debug:
            print(time.time()-t0)
            print('insert_all_stock_data_3(\\)')

        self.db_disconnect()

    #fix bug: delete zero when the stock is closed
    def delete_amount_is_zero(self):
        self.db_connect()
        sql_temp="delete from eastmoney_balance_table where amount = 0;"
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
        sql_temp += "from eastmoney_balance_table"

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

        sql_temp = "delete from eastmoney_balance_table"

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
 




        
