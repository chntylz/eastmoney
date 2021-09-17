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

class HData_xq_income(object):
    def __init__(self,user,password):
        # self.aaa = aaa
        self.xq_income_table=[]
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
        self.cur.execute("select count(*) from pg_class where relname = 'xq_income_table' ;")
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
            drop table if exists xq_income_table;
            create table xq_income_table(
                record_date date, 
                stock_code varchar, 
                report_name varchar, 
                report_date date, 
                net_profit float, 
                net_profit_new float, 
                net_profit_atsopc float, 
                net_profit_atsopc_new float, 
                total_revenue float, 
                total_revenue_new float, 
                op float, 
                op_new float, 
                income_from_chg_in_fv float, 
                income_from_chg_in_fv_new float, 
                invest_incomes_from_rr float, 
                invest_incomes_from_rr_new float, 
                invest_income float, 
                invest_income_new float, 
                exchg_gain float, 
                exchg_gain_new float, 
                operating_taxes_and_surcharge float, 
                operating_taxes_and_surcharge_new float, 
                asset_impairment_loss float, 
                asset_impairment_loss_new float, 
                non_operating_income float, 
                non_operating_income_new float, 
                non_operating_payout float, 
                non_operating_payout_new float, 
                profit_total_amt float, 
                profit_total_amt_new float, 
                minority_gal float, 
                minority_gal_new float, 
                basic_eps float, 
                basic_eps_new float, 
                dlt_earnings_per_share float, 
                dlt_earnings_per_share_new float, 
                othr_compre_income_atoopc float, 
                othr_compre_income_atoopc_new float, 
                othr_compre_income_atms float, 
                othr_compre_income_atms_new float, 
                total_compre_income float, 
                total_compre_income_new float, 
                total_compre_income_atsopc float, 
                total_compre_income_atsopc_new float, 
                total_compre_income_atms float, 
                total_compre_income_atms_new float, 
                othr_compre_income float, 
                othr_compre_income_new float, 
                net_profit_after_nrgal_atsolc float, 
                net_profit_after_nrgal_atsolc_new float, 
                income_tax_expenses float, 
                income_tax_expenses_new float, 
                credit_impairment_loss float, 
                credit_impairment_loss_new float, 
                interest_net_income float, 
                interest_net_income_new float, 
                interest_income float, 
                interest_income_new float, 
                interest_payout float, 
                interest_payout_new float, 
                commi_net_income float, 
                commi_net_income_new float, 
                fee_and_commi_income float, 
                fee_and_commi_income_new float, 
                charge_and_commi_expenses float, 
                charge_and_commi_expenses_new float, 
                othr_income float, 
                othr_income_new float, 
                operating_payout float, 
                operating_payout_new float, 
                business_and_manage_fee float, 
                business_and_manage_fee_new float, 
                othr_business_costs float, 
                othr_business_costs_new float, 
                revenue float, 
                revenue_new float, 
                operating_costs float, 
                operating_costs_new float, 
                operating_cost float, 
                operating_cost_new float, 
                sales_fee float, 
                sales_fee_new float, 
                manage_fee float, 
                manage_fee_new float, 
                financing_expenses float, 
                financing_expenses_new float, 
                rad_cost float, 
                rad_cost_new float, 
                finance_cost_interest_fee float, 
                finance_cost_interest_fee_new float, 
                finance_cost_interest_income float, 
                finance_cost_interest_income_new float, 
                asset_disposal_income float, 
                asset_disposal_income_new float, 
                other_income float, 
                other_income_new float, 
                noncurrent_assets_dispose_gain float, 
                noncurrent_assets_dispose_gain_new float, 
                noncurrent_asset_disposal_loss float, 
                noncurrent_asset_disposal_loss_new float, 
                net_profit_bi float, 
                net_profit_bi_new float, 
                continous_operating_np float, 
                continous_operating_np_new float, 
                net_income_from_brokerage float, 
                net_income_from_brokerage_new float, 
                net_income_from_invest_banking float, 
                net_income_from_invest_banking_new float, 
                asset_manage_service_charge_ni float, 
                asset_manage_service_charge_ni_new float, 
                earned_premium float, 
                earned_premium_new float, 
                insurance_income float, 
                insurance_income_new float, 
                rein_premium_income float, 
                rein_premium_income_new float, 
                ceded_out_premium float, 
                ceded_out_premium_new float, 
                draw_undueduty_deposit float, 
                draw_undueduty_deposit_new float, 
                refunded_premium float, 
                refunded_premium_new float, 
                compen_payout float, 
                compen_payout_new float, 
                compen_expense float, 
                compen_expense_new float, 
                draw_duty_deposit float, 
                draw_duty_deposit_new float, 
                amortized_deposit_for_duty float, 
                amortized_deposit_for_duty_new float, 
                commi_on_insurance_policy float, 
                commi_on_insurance_policy_new float, 
                rein_expenditure float, 
                rein_expenditure_new float, 
                amortized_rein_expenditure float, 
                amortized_rein_expenditure_new float, 
                operating_total_cost_si float, 
                operating_total_cost_si_new float
                );
            alter table xq_income_table add primary key(stock_code,record_date);
            ''')
        self.conn.commit()
        self.db_disconnect()

        print("db_xq_income_table_create finish")
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
            self.cur.copy_from(buffer, table='xq_income_table', sep=",")
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
        self.cur.execute("select max(record_date) from xq_income_table \
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

        sql_temp += "select * from xq_income_table"

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

        sql_temp = "delete from xq_income_table"

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
 

#alter table xq_income_table add  "up_days" int not null default 0;


#update xq_income_table set is_zig=0 where record_date = '2018-10-08' and stock_code = '002732';

#UPDATE xq_income_table SET is_zig = tmp.is_zig, is_quad=tmp.is_quad FROM    (VALUES ( DATE  '2018-06-13', '600647', 11, 1), ( DATE  '2018-06-12', '600647', 12, 1) ) AS tmp (record_date, stock_code, is_zig, is_quad ) WHERE xq_income_table.record_date = tmp.record_date and xq_income_table.stock_code = tmp.stock_code;
#select * from xq_income_table where stock_code ='600647' and (record_date='2018-06-13' or record_date='2018-06-12' ); 
