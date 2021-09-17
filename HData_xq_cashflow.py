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

class HData_xq_cashflow(object):
    def __init__(self,user,password):
        # self.aaa = aaa
        self.xq_cashflow_table=[]
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
        self.cur.execute("select count(*) from pg_class where relname = 'xq_cashflow_table' ;")
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
            drop table if exists xq_cashflow_table;
            create table xq_cashflow_table(
                record_date date,
                stock_code varchar,
                report_name varchar,
                report_date date,
                ncf_from_oa float, 
                ncf_from_oa_new float, 
                ncf_from_ia float, 
                ncf_from_ia_new float, 
                ncf_from_fa float, 
                ncf_from_fa_new float, 
                cash_received_of_othr_oa float, 
                cash_received_of_othr_oa_new float, 
                sub_total_of_ci_from_oa float, 
                sub_total_of_ci_from_oa_new float, 
                cash_paid_to_employee_etc float, 
                cash_paid_to_employee_etc_new float, 
                payments_of_all_taxes float, 
                payments_of_all_taxes_new float, 
                othrcash_paid_relating_to_oa float, 
                othrcash_paid_relating_to_oa_new float, 
                sub_total_of_cos_from_oa float, 
                sub_total_of_cos_from_oa_new float, 
                cash_received_of_dspsl_invest float, 
                cash_received_of_dspsl_invest_new float, 
                invest_income_cash_received float, 
                invest_income_cash_received_new float, 
                net_cash_of_disposal_assets float, 
                net_cash_of_disposal_assets_new float, 
                net_cash_of_disposal_branch float, 
                net_cash_of_disposal_branch_new float, 
                cash_received_of_othr_ia float, 
                cash_received_of_othr_ia_new float, 
                sub_total_of_ci_from_ia float, 
                sub_total_of_ci_from_ia_new float, 
                invest_paid_cash float, 
                invest_paid_cash_new float, 
                cash_paid_for_assets float, 
                cash_paid_for_assets_new float, 
                othrcash_paid_relating_to_ia float, 
                othrcash_paid_relating_to_ia_new float, 
                sub_total_of_cos_from_ia float, 
                sub_total_of_cos_from_ia_new float, 
                cash_received_of_absorb_invest float, 
                cash_received_of_absorb_invest_new float, 
                cash_received_from_investor float, 
                cash_received_from_investor_new float, 
                cash_received_from_bond_issue float, 
                cash_received_from_bond_issue_new float, 
                cash_received_of_borrowing float, 
                cash_received_of_borrowing_new float, 
                cash_received_of_othr_fa float, 
                cash_received_of_othr_fa_new float, 
                sub_total_of_ci_from_fa float, 
                sub_total_of_ci_from_fa_new float, 
                cash_pay_for_debt float, 
                cash_pay_for_debt_new float, 
                cash_paid_of_distribution float, 
                cash_paid_of_distribution_new float, 
                branch_paid_to_minority_holder float, 
                branch_paid_to_minority_holder_new float, 
                othrcash_paid_relating_to_fa float, 
                othrcash_paid_relating_to_fa_new float, 
                sub_total_of_cos_from_fa float, 
                sub_total_of_cos_from_fa_new float, 
                effect_of_exchange_chg_on_cce float, 
                effect_of_exchange_chg_on_cce_new float, 
                net_increase_in_cce float, 
                net_increase_in_cce_new float, 
                initial_balance_of_cce float, 
                initial_balance_of_cce_new float, 
                final_balance_of_cce float, 
                final_balance_of_cce_new float, 
                deposit_and_interbank_net_add float, 
                deposit_and_interbank_net_add_new float, 
                borrowing_net_add_central_bank float, 
                borrowing_net_add_central_bank_new float, 
                lending_net_add_other_org float, 
                lending_net_add_other_org_new float, 
                cash_received_of_interest_etc float, 
                cash_received_of_interest_etc_new float, 
                loan_and_advance_net_add float, 
                loan_and_advance_net_add_new float, 
                naa_of_cb_and_interbank float, 
                naa_of_cb_and_interbank_new float, 
                cash_paid_for_interests_etc float, 
                cash_paid_for_interests_etc_new float, 
                net_cash_amt_from_branch float, 
                net_cash_amt_from_branch_new float, 
                cash_received_of_sales_service float, 
                cash_received_of_sales_service_new float, 
                refund_of_tax_and_levies float, 
                refund_of_tax_and_levies_new float, 
                goods_buy_and_service_cash_pay float, 
                goods_buy_and_service_cash_pay_new float, 
                naa_of_disposal_fnncl_assets float, 
                naa_of_disposal_fnncl_assets_new float, 
                borrowing_net_increase_amt float, 
                borrowing_net_increase_amt_new float, 
                acting_sec_received_net_cash float, 
                acting_sec_received_net_cash_new float, 
                net_add_in_repur_capital float, 
                net_add_in_repur_capital_new float, 
                cash_paid_for_fees_and_commi float, 
                cash_paid_for_fees_and_commi_new float, 
                ia_cos_si float, 
                ia_cos_si_new float, 
                cash_received_from_orig_ic float, 
                cash_received_from_orig_ic_new float, 
                net_cash_received_from_rein float, 
                net_cash_received_from_rein_new float, 
                naa_assured_saving_and_invest float, 
                naa_assured_saving_and_invest_new float, 
                oa_net_ci_si float, 
                oa_net_ci_si_new float, 
                cash_of_orig_ic_indemnity float, 
                cash_of_orig_ic_indemnity_new float, 
                oa_net_cos_si float, 
                oa_net_cos_si_new float, 
                cash_paid_for_policy_dividends float, 
                cash_paid_for_policy_dividends_new float, 
                net_increase_in_pledge_loans float, 
                net_increase_in_pledge_loans_new float
                );
            alter table xq_cashflow_table add primary key(stock_code,record_date);
            ''')
        self.conn.commit()
        self.db_disconnect()

        print("db_xq_cashflow_table_create finish")
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
            self.cur.copy_from(buffer, table='xq_cashflow_table', sep=",")
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
        self.cur.execute("select max(record_date) from xq_cashflow_table \
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

        sql_temp += "select * from xq_cashflow_table"

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

        sql_temp = "delete from xq_cashflow_table"

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
 

