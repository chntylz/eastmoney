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

class HData_xq_balance(object):
    def __init__(self,user,password):
        # self.aaa = aaa
        self.xq_balance_table=[]
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
        self.cur.execute("select count(*) from pg_class where relname = 'xq_balance_table' ;")
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
            drop table if exists xq_balance_table;
            create table xq_balance_table(
                record_date date,
                stock_code varchar,
                report_name varchar,
                report_date date,
                total_assets float, 
                total_assets_new float, 
                total_liab float, 
                total_liab_new float, 
                asset_liab_ratio float, 
                asset_liab_ratio_new float, 
                total_quity_atsopc float, 
                total_quity_atsopc_new float, 
                tradable_fnncl_assets float, 
                tradable_fnncl_assets_new float, 
                interest_receivable float, 
                interest_receivable_new float, 
                saleable_finacial_assets float, 
                saleable_finacial_assets_new float, 
                held_to_maturity_invest float, 
                held_to_maturity_invest_new float, 
                fixed_asset float, 
                fixed_asset_new float, 
                intangible_assets float, 
                intangible_assets_new float, 
                construction_in_process float, 
                construction_in_process_new float, 
                dt_assets float, 
                dt_assets_new float, 
                tradable_fnncl_liab float, 
                tradable_fnncl_liab_new float, 
                payroll_payable float, 
                payroll_payable_new float, 
                tax_payable float, 
                tax_payable_new float, 
                estimated_liab float, 
                estimated_liab_new float, 
                dt_liab float, 
                dt_liab_new float, 
                bond_payable float, 
                bond_payable_new float, 
                shares float, 
                shares_new float, 
                capital_reserve float, 
                capital_reserve_new float, 
                earned_surplus float, 
                earned_surplus_new float, 
                undstrbtd_profit float, 
                undstrbtd_profit_new float, 
                minority_equity float, 
                minority_equity_new float, 
                total_holders_equity float, 
                total_holders_equity_new float, 
                total_liab_and_holders_equity float, 
                total_liab_and_holders_equity_new float, 
                lt_equity_invest float, 
                lt_equity_invest_new float, 
                derivative_fnncl_liab float, 
                derivative_fnncl_liab_new float, 
                general_risk_provision float, 
                general_risk_provision_new float, 
                frgn_currency_convert_diff float, 
                frgn_currency_convert_diff_new float, 
                goodwill float, 
                goodwill_new float, 
                invest_property float, 
                invest_property_new float, 
                interest_payable float, 
                interest_payable_new float, 
                treasury_stock float, 
                treasury_stock_new float, 
                othr_compre_income float, 
                othr_compre_income_new float, 
                othr_equity_instruments float, 
                othr_equity_instruments_new float, 
                central_bank_cash_and_deposit float, 
                central_bank_cash_and_deposit_new float, 
                interbank_storage float, 
                interbank_storage_new float, 
                precious_metal float, 
                precious_metal_new float, 
                lending_fund float, 
                lending_fund_new float, 
                derivative_fnncl_assets float, 
                derivative_fnncl_assets_new float, 
                buy_resale_fnncl_assets float, 
                buy_resale_fnncl_assets_new float, 
                disbursement_loan_and_advance float, 
                disbursement_loan_and_advance_new float, 
                receivable_invest float, 
                receivable_invest_new float, 
                othr_assets float, 
                othr_assets_new float, 
                asset_si float, 
                asset_si_new float, 
                loan_from_central_bank float, 
                loan_from_central_bank_new float, 
                interbank_deposit_etc float, 
                interbank_deposit_etc_new float, 
                borrowing_funds float, 
                borrowing_funds_new float, 
                fnncl_assets_sold_for_repur float, 
                fnncl_assets_sold_for_repur_new float, 
                savings_absorption float, 
                savings_absorption_new float, 
                bill_payable float, 
                bill_payable_new float, 
                accounts_payable float, 
                accounts_payable_new float, 
                pre_receivable float, 
                pre_receivable_new float, 
                othr_liab float, 
                othr_liab_new float, 
                liab_si float, 
                liab_si_new float, 
                preferred_share float, 
                preferred_share_new float, 
                perpetual_bond float, 
                perpetual_bond_new float, 
                amortized_cost_fnncl_assets float, 
                amortized_cost_fnncl_assets_new float, 
                fv_chg_income_fnncl_assets float, 
                fv_chg_income_fnncl_assets_new float, 
                currency_funds float, 
                currency_funds_new float, 
                bills_receivable float, 
                bills_receivable_new float, 
                account_receivable float, 
                account_receivable_new float, 
                pre_payment float, 
                pre_payment_new float, 
                dividend_receivable float, 
                dividend_receivable_new float, 
                othr_receivables float, 
                othr_receivables_new float, 
                inventory float, 
                inventory_new float, 
                nca_due_within_one_year float, 
                nca_due_within_one_year_new float, 
                othr_current_assets float, 
                othr_current_assets_new float, 
                current_assets_si float, 
                current_assets_si_new float, 
                total_current_assets float, 
                total_current_assets_new float, 
                lt_receivable float, 
                lt_receivable_new float, 
                dev_expenditure float, 
                dev_expenditure_new float, 
                lt_deferred_expense float, 
                lt_deferred_expense_new float, 
                othr_noncurrent_assets float, 
                othr_noncurrent_assets_new float, 
                noncurrent_assets_si float, 
                noncurrent_assets_si_new float, 
                total_noncurrent_assets float, 
                total_noncurrent_assets_new float, 
                st_loan float, 
                st_loan_new float, 
                dividend_payable float, 
                dividend_payable_new float, 
                othr_payables float, 
                othr_payables_new float, 
                noncurrent_liab_due_in1y float, 
                noncurrent_liab_due_in1y_new float, 
                current_liab_si float, 
                current_liab_si_new float, 
                total_current_liab float, 
                total_current_liab_new float, 
                lt_loan float, 
                lt_loan_new float, 
                lt_payable float, 
                lt_payable_new float, 
                special_payable float, 
                special_payable_new float, 
                othr_non_current_liab float, 
                othr_non_current_liab_new float, 
                noncurrent_liab_si float, 
                noncurrent_liab_si_new float, 
                total_noncurrent_liab float, 
                total_noncurrent_liab_new float, 
                salable_financial_assets float, 
                salable_financial_assets_new float, 
                othr_current_liab float, 
                othr_current_liab_new float, 
                ar_and_br float, 
                ar_and_br_new float, 
                contractual_assets float, 
                contractual_assets_new float, 
                bp_and_ap float, 
                bp_and_ap_new float, 
                contract_liabilities float, 
                contract_liabilities_new float, 
                to_sale_asset float, 
                to_sale_asset_new float, 
                other_eq_ins_invest float, 
                other_eq_ins_invest_new float, 
                other_illiquid_fnncl_assets float, 
                other_illiquid_fnncl_assets_new float, 
                fixed_asset_sum float, 
                fixed_asset_sum_new float, 
                fixed_assets_disposal float, 
                fixed_assets_disposal_new float, 
                construction_in_process_sum float, 
                construction_in_process_sum_new float, 
                project_goods_and_material float, 
                project_goods_and_material_new float, 
                productive_biological_assets float, 
                productive_biological_assets_new float, 
                oil_and_gas_asset float, 
                oil_and_gas_asset_new float, 
                to_sale_debt float, 
                to_sale_debt_new float, 
                lt_payable_sum float, 
                lt_payable_sum_new float, 
                noncurrent_liab_di float, 
                noncurrent_liab_di_new float, 
                special_reserve float, 
                special_reserve_new float, 
                customer_fund_deposit float, 
                customer_fund_deposit_new float, 
                settle_reserves float, 
                settle_reserves_new float, 
                customer_provision float, 
                customer_provision_new float, 
                financing_funds float, 
                financing_funds_new float, 
                receivable float, 
                receivable_new float, 
                paid_deposit float, 
                paid_deposit_new float, 
                acting_td_sec float, 
                acting_td_sec_new float, 
                act_underwriting_sec float, 
                act_underwriting_sec_new float, 
                st_financing_payable float, 
                st_financing_payable_new float, 
                accrued_payable float, 
                accrued_payable_new float, 
                td_seat_fee float, 
                td_seat_fee_new float, 
                pledged_loan float, 
                pledged_loan_new float, 
                premium_receivable float, 
                premium_receivable_new float, 
                rein_account_receivable float, 
                rein_account_receivable_new float, 
                rein_contract_reserve float, 
                rein_contract_reserve_new float, 
                assured_pledge_loan float, 
                assured_pledge_loan_new float, 
                fixed_deposit float, 
                fixed_deposit_new float, 
                paid_capital_deposit float, 
                paid_capital_deposit_new float, 
                separate_account float, 
                separate_account_new float, 
                advance_premium float, 
                advance_premium_new float, 
                charge_and_commi_payable float, 
                charge_and_commi_payable_new float, 
                rein_payable float, 
                rein_payable_new float, 
                claim_payable float, 
                claim_payable_new float, 
                dvdnd_payable_for_the_insured float, 
                dvdnd_payable_for_the_insured_new float, 
                assured_saving_and_invest float, 
                assured_saving_and_invest_new float, 
                insurance_contract_reserve float, 
                insurance_contract_reserve_new float, 
                independent_account_liab float, 
                independent_account_liab_new float, 
                rein_undue_liability_reserve float, 
                rein_undue_liability_reserve_new float, 
                receivable_rein_duty_reserve float, 
                receivable_rein_duty_reserve_new float, 
                received_deposit float, 
                received_deposit_new float, 
                unearned_premium_reserve float, 
                unearned_premium_reserve_new float, 
                life_insurance_reserve float, 
                life_insurance_reserve_new float, 
                lt_health_insurance_reserve float, 
                lt_health_insurance_reserve_new float, 
                lt_staff_salary_payable float, 
                lt_staff_salary_payable_new float
                );
            alter table xq_balance_table add primary key(stock_code,record_date);
            ''')
        self.conn.commit()
        self.db_disconnect()

        print("db_xq_balance_table_create finish")
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
            self.cur.copy_from(buffer, table='xq_balance_table', sep=",")
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
        self.cur.execute("select max(record_date) from xq_balance_table \
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

        sql_temp += "select * from xq_balance_table"

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

        sql_temp = "delete from xq_balance_table"

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
 
