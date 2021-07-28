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

class HData_xq_fina(object):
    def __init__(self,user,password):
        # self.aaa = aaa
        self.xq_fina_table=[]
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
        self.cur.execute("select count(*) from pg_class where relname = 'xq_fina_table' ;")
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
            drop table if exists xq_fina_table;
            create table xq_fina_table(
                record_date date,
                stock_code varchar,
                report_name varchar,
                report_date date,
                avg_roe float,
                avg_roe_new float,
                np_per_share float,
                np_per_share_new float,
                operate_cash_flow_ps float,
                operate_cash_flow_ps_new float,
                basic_eps float,
                basic_eps_new float,
                capital_reserve float,
                capital_reserve_new float,
                undistri_profit_ps float,
                undistri_profit_ps_new float,
                net_interest_of_total_assets float,
                net_interest_of_total_assets_new float,
                net_selling_rate float,
                net_selling_rate_new float,
                gross_selling_rate float,
                gross_selling_rate_new float,
                total_revenue float,
                total_revenue_new float,
                operating_income_yoy float,
                operating_income_yoy_new float,
                net_profit_atsopc float,
                net_profit_atsopc_new float,
                net_profit_atsopc_yoy float,
                net_profit_atsopc_yoy_new float,
                net_profit_after_nrgal_atsolc float,
                net_profit_after_nrgal_atsolc_new float,
                np_atsopc_nrgal_yoy float,
                np_atsopc_nrgal_yoy_new float,
                ore_dlt float,
                ore_dlt_new float,
                rop float,
                rop_new float,
                asset_liab_ratio float,
                asset_liab_ratio_new float,
                current_ratio float,
                current_ratio_new float,
                quick_ratio float,
                quick_ratio_new float,
                equity_multiplier float,
                equity_multiplier_new float,
                equity_ratio float,
                equity_ratio_new float,
                holder_equity float,
                holder_equity_new float,
                ncf_from_oa_to_total_liab float,
                ncf_from_oa_to_total_liab_new float,
                inventory_turnover_days float,
                inventory_turnover_days_new float,
                receivable_turnover_days float,
                receivable_turnover_days_new float,
                accounts_payable_turnover_days float,
                accounts_payable_turnover_days_new float,
                cash_cycle float,
                cash_cycle_new float,
                operating_cycle float,
                operating_cycle_new float,
                total_capital_turnover float,
                total_capital_turnover_new float,
                inventory_turnover float,
                inventory_turnover_new float,
                account_receivable_turnover float,
                account_receivable_turnover_new float,
                accounts_payable_turnover float,
                accounts_payable_turnover_new float,
                current_asset_turnover_rate float,
                current_asset_turnover_rate_new float,
                fixed_asset_turnover_ratio  float,
                fixed_asset_turnover_ratio_new  float
                );
            alter table xq_fina_table add primary key(stock_code,record_date);
            ''')
        self.conn.commit()
        self.db_disconnect()

        print("db_xq_fina_table_create finish")
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
            df.to_csv('./test.csv', encoding='gbk')

        df.to_csv(buffer, index=0, header=False)
        buffer.seek(0)

        self.db_connect()
        try:
            self.cur.copy_from(buffer, table='xq_fina_table', sep=",")
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
        self.cur.execute("select max(record_date) from xq_fina_table \
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

        sql_temp += "select * from xq_fina_table"

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

        sql_temp = "delete from xq_fina_table"

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
 

#alter table xq_fina_table add  "up_days" int not null default 0;


#update xq_fina_table set is_zig=0 where record_date = '2018-10-08' and stock_code = '002732';

#UPDATE xq_fina_table SET is_zig = tmp.is_zig, is_quad=tmp.is_quad FROM    (VALUES ( DATE  '2018-06-13', '600647', 11, 1), ( DATE  '2018-06-12', '600647', 12, 1) ) AS tmp (record_date, stock_code, is_zig, is_quad ) WHERE xq_fina_table.record_date = tmp.record_date and xq_fina_table.stock_code = tmp.stock_code;
#select * from xq_fina_table where stock_code ='600647' and (record_date='2018-06-13' or record_date='2018-06-12' ); 
