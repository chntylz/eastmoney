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
                                        eps_=_earnings_per_share





'报告日期'                           'record_date'
'摊薄每股收益(元)'                    'diluted_eps'
'加权每股收益(元)'                    'weighted_eps'
'每股收益_调整后(元)'                 'eps_adjusted'
'扣除非经常性损益后的每股收益(元)'       'eps_after_deducting_non_recurring_gains_and_losses'

'每股净资产_调整前(元)'                'net_assets_per_share_before_adjustment'
'每股净资产_调整后(元)'                'net_assets_per_share_adjusted'
'每股经营性现金流(元)'                 'operating_cash_flow_per_share'
'每股资本公积金(元)'                  'capital_reserve_per_share'
'每股未分配利润(元)'                  'retained_eps'

'调整后的每股净资产(元)'               'adjusted_net_assets_per_share'
'总资产利润率(%)'                     'total_asset_profit_rate'
'主营业务利润率(%)'                   'main_business_profit_rate'
'总资产净利润率(%)'                   'total_asset_net_profit_rate'
'成本费用利润率(%)'                   'cost_and_expense_profit_rate'

'营业利润率(%)'                      'operating_profit_rate'
'主营业务成本率(%)'                   'main_business_cost_rate'
'销售净利率(%)'                      'net_profit_margin'
'股本报酬率(%)'                      'return_on_equity'
'净资产报酬率(%)'                     'return_on_net_assets'
'资产报酬率(%)'                      'return_on_assets'

'销售毛利率(%)'                      'gross_profit_margin'
'三项费用比重'                        'proportion_of_three_expenses'
'非主营比重'                         'non_main_business_proportion'
'主营利润比重'                        'main_business_profit_proportion'
'股息发放率(%)'                      'dividend_payout_rate'
'投资收益率(%)'                      'investment_return_rate'
'主营业务利润(元)'                    'main_business_profit'

'净资产收益率(%)'                     'net_asset_return_rate'
'加权净资产收益率(%)'                 'weighted_net_asset_return_rate'
'扣除非经常性损益后的净利润(元)'        'net_profit_after_deducting_non_recurring_gains_and_losses'
'主营业务收入增长率(%)'                'main_business_income_growth_rate'
'净利润增长率(%)'                     'net_profit_growth_rate'

'净资产增长率(%)'                     'net_asset_growth_rate'
'总资产增长率(%)'                     'total_asset_growth_rate'
'应收账款周转率(次)'                  'accounts_receivable_turnover_rate_times'
'应收账款周转天数(天)'                 'accounts_receivable_turnover_days_days'
'存货周转天数(天)'                    'inventory_turnover_days_days'
'存货周转率(次)'                      'inventory_turnover_rate_times'

'固定资产周转率(次)'                  'fixed_asset_turnover_rate_times'
'总资产周转率(次)'                    'total_asset_turnover_rate_times'
'总资产周转天数(天)'                  'total_asset_turnover_days_days'
'流动资产周转率(次)'                  'current_asset_turnover_rate_times'
'流动资产周转天数(天)'                 'current_asset_turnover_days_days'

'股东权益周转率(次)'                  'shareholders_equity_turnover_rate_times'
'流动比率'                           'current_ratio'
'速动比率'                           'quick_ratio'
'现金比率(%)'                        'cash_ratio'
'利息支付倍数'                        'interest_coverage_ratio'
'长期债务与营运资金比率(%)'            'long_term_debt_to_working_capital_ratio'

'股东权益比率(%)'                     'shareholders_equity_ratio'
'长期负债比率(%)'                     'long_term_debt_ratio'
'股东权益与固定资产比率(%)'            'shareholders_equity_to_fixed_assets_ratio'
'负债与所有者权益比率(%)'              'liabilities_to_owners_equity_ratio'
'长期资产与长期资金比率(%)'            'long_term_assets_to_long_term_funds_ratio'

'资本化比率(%)'                      'capitalization_ratio'
'固定资产净值率(%)'                   'fixed_asset_net_value_ratio'
'资本固定化比率(%)'                   'capitalization_fix_ratio'
'产权比率(%)'                        'equity_ratio'
'清算价值比率(%)'                     'liquidation_value_ratio'
'固定资产比重(%)'                     'fixed_assets_ratio'

'资产负债率(%)'                      'asset_liability_ratio'
'总资产(元)'                         'total_assets'
'经营现金净流量对销售收入比率(%)'       'net_operating_cash_flow_to_sales_revenue_ratio'
'资产的经营现金流量回报率(%)'           'operating_cash_flow_return_on_assets'

'经营现金净流量与净利润的比率(%)'       'net_operating_cash_flow_to_net_profit_ratio'
'经营现金净流量对负债比率(%)'           'net_operating_cash_flow_to_debt_ratio'
'现金流量比率(%)'                     'cash_flow_ratio'
'短期股票投资(元)'                    'short_term_stock_investment'
'短期债券投资(元)'                    'short_term_bond_investment_yuan'

'短期其它经营性投资(元)'               'short_term_other_operating_investment_yuan'
'长期股票投资(元)'                    'long_term_stock_investment_yuan'
'长期债券投资(元)'                    'long_term_bond_investment_yuan'
'长期其它经营性投资(元)'               'long_term_other_operating_investment_yuan'
'1年以内应收帐款(元)'                 'accounts_receivable_within_1_year_yuan'

'1-2年以内应收帐款(元)'               'accounts_receivable_within_1_2_years_yuan'
'2-3年以内应收帐款(元)'               'accounts_receivable_within_2_3_years_yuan'
'3年以内应收帐款(元)'                 'accounts_receivable_within_3_years_yuan'
'1年以内预付货款(元)'                 'prepayments_within_1_year_yuan'

'1-2年以内预付货款(元)'               'prepayments_within_1_2_years_yuan'
'2-3年以内预付货款(元)'               'prepayments_within_2_3_years_yuan'
'3年以内预付货款(元)'                 'prepayments_within_3_years_yuan'
'1年以内其它应收款(元)'                'other_receivables_within_1_year_yuan'

'1-2年以内其它应收款(元)'              'other_receivables_within_1_2_years_yuan'
'2-3年以内其它应收款(元)'              'other_receivables_within_2_3_years_yuan'
'3年以内其它应收款(元)'                'other_receivables_within_3_years_yuan'
'002261'                                'stock_code' 
'拓维信息']                             'stock_name'

'''

sina_cols = " record_date,stock_code, stock_name,diluted_eps,weighted_eps,eps_adjusted,eps_after_deducting_non_recurring_gains_and_losses,\
             net_assets_per_share_before_adjustment,net_assets_per_share_adjusted,operating_cash_flow_per_share,\
             capital_reserve_per_share,retained_eps,adjusted_net_assets_per_share,total_asset_profit_rate,\
             main_business_profit_rate,total_asset_net_profit_rate,cost_and_expense_profit_rate,\
             operating_profit_rate,main_business_cost_rate,net_profit_margin,return_on_equity,\
             return_on_net_assets,return_on_assets,gross_profit_margin,proportion_of_three_expenses,\
             non_main_business_proportion,main_business_profit_proportion,dividend_payout_rate,\
             investment_return_rate,main_business_profit,net_asset_return_rate,\
             weighted_net_asset_return_rate,net_profit_after_deducting_non_recurring_gains_and_losses,\
             main_business_income_growth_rate,net_profit_growth_rate,net_asset_growth_rate,\
             total_asset_growth_rate,accounts_receivable_turnover_rate_times,accounts_receivable_turnover_days_days,\
             inventory_turnover_days_days,inventory_turnover_rate_times,fixed_asset_turnover_rate_times,\
             total_asset_turnover_rate_times,total_asset_turnover_days_days,current_asset_turnover_rate_times,\
             current_asset_turnover_days_days,shareholders_equity_turnover_rate_times,current_ratio,\
             quick_ratio,cash_ratio,interest_coverage_ratio,long_term_debt_to_working_capital_ratio,\
             shareholders_equity_ratio,long_term_debt_ratio,shareholders_equity_to_fixed_assets_ratio,\
             liabilities_to_owners_equity_ratio,long_term_assets_to_long_term_funds_ratio,\
             capitalization_ratio,fixed_asset_net_value_ratio,capitalization_fix_ratio,equity_ratio,\
             liquidation_value_ratio,fixed_assets_ratio,asset_liability_ratio,total_assets,\
             net_operating_cash_flow_to_sales_revenue_ratio,operating_cash_flow_return_on_assets,\
             net_operating_cash_flow_to_net_profit_ratio,net_operating_cash_flow_to_debt_ratio,\
             cash_flow_ratio,short_term_stock_investment,short_term_bond_investment_yuan,\
             short_term_other_operating_investment_yuan,long_term_stock_investment_yuan,\
             long_term_bond_investment_yuan,long_term_other_operating_investment_yuan,\
             accounts_receivable_within_1_year_yuan,accounts_receivable_within_1_2_years_yuan,\
             accounts_receivable_within_2_3_years_yuan,accounts_receivable_within_3_years_yuan,\
             prepayments_within_1_year_yuan,prepayments_within_1_2_years_yuan,\
             prepayments_within_2_3_years_yuan,prepayments_within_3_years_yuan,\
             other_receivables_within_1_year_yuan,other_receivables_within_1_2_years_yuan,\
             other_receivables_within_2_3_years_yuan,other_receivables_within_3_years_yuan "


class HData_sina_fina(object):
    def __init__(self,user,password):
        # self.aaa = aaa
        self.sina_fina_table=[]
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
        self.cur.execute("select count(*) from pg_class where relname = 'sina_fina_table' ;")
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




    def db_hdata_sina_create(self):

        self.db_connect()

        #ystz  float,  yingyeshouru tongbi
        #sjltz  float,  jinglirun tongbi
        # 创建stocks表
        self.cur.execute('''
            drop table if exists sina_fina_table;
            create table sina_fina_table(
                record_date      date,
                stock_code    varchar,
                stock_name    varchar,
                diluted_eps         float,
                weighted_eps         float,
                eps_adjusted         float,
                eps_after_deducting_non_recurring_gains_and_losses         float,
                net_assets_per_share_before_adjustment         float,
                net_assets_per_share_adjusted         float,
                operating_cash_flow_per_share         float,
                capital_reserve_per_share         float,
                retained_eps         float,
                adjusted_net_assets_per_share         float,
                total_asset_profit_rate         float,
                main_business_profit_rate         float,
                total_asset_net_profit_rate         float,
                cost_and_expense_profit_rate         float,
                operating_profit_rate         float,
                main_business_cost_rate         float,
                net_profit_margin         float,
                return_on_equity         float,
                return_on_net_assets         float,
                return_on_assets         float,
                gross_profit_margin         float,
                proportion_of_three_expenses         float,
                non_main_business_proportion         float,
                main_business_profit_proportion         float,
                dividend_payout_rate         float,
                investment_return_rate         float,
                main_business_profit         float,
                net_asset_return_rate         float,
                weighted_net_asset_return_rate         float,
                net_profit_after_deducting_non_recurring_gains_and_losses         float,
                main_business_income_growth_rate         float,
                net_profit_growth_rate         float,
                net_asset_growth_rate         float,
                total_asset_growth_rate         float,
                accounts_receivable_turnover_rate_times         float,
                accounts_receivable_turnover_days_days         float,
                inventory_turnover_days_days         float,
                inventory_turnover_rate_times         float,
                fixed_asset_turnover_rate_times         float,
                total_asset_turnover_rate_times         float,
                total_asset_turnover_days_days         float,
                current_asset_turnover_rate_times         float,
                current_asset_turnover_days_days         float,
                shareholders_equity_turnover_rate_times         float,
                current_ratio         float,
                quick_ratio         float,
                cash_ratio         float,
                interest_coverage_ratio         float,
                long_term_debt_to_working_capital_ratio         float,
                shareholders_equity_ratio         float,
                long_term_debt_ratio         float,
                shareholders_equity_to_fixed_assets_ratio         float,
                liabilities_to_owners_equity_ratio         float,
                long_term_assets_to_long_term_funds_ratio         float,
                capitalization_ratio         float,
                fixed_asset_net_value_ratio         float,
                capitalization_fix_ratio         float,
                equity_ratio         float,
                liquidation_value_ratio         float,
                fixed_assets_ratio         float,
                asset_liability_ratio         float,
                total_assets         float,
                net_operating_cash_flow_to_sales_revenue_ratio         float,
                operating_cash_flow_return_on_assets         float,
                net_operating_cash_flow_to_net_profit_ratio         float,
                net_operating_cash_flow_to_debt_ratio         float,
                cash_flow_ratio         float,
                short_term_stock_investment         float,
                short_term_bond_investment_yuan         float,
                short_term_other_operating_investment_yuan         float,
                long_term_stock_investment_yuan         float,
                long_term_bond_investment_yuan         float,
                long_term_other_operating_investment_yuan         float,
                accounts_receivable_within_1_year_yuan         float,
                accounts_receivable_within_1_2_years_yuan         float,
                accounts_receivable_within_2_3_years_yuan         float,
                accounts_receivable_within_3_years_yuan         float,
                prepayments_within_1_year_yuan         float,
                prepayments_within_1_2_years_yuan         float,
                prepayments_within_2_3_years_yuan         float,
                prepayments_within_3_years_yuan         float,
                other_receivables_within_1_year_yuan         float,
                other_receivables_within_1_2_years_yuan         float,
                other_receivables_within_2_3_years_yuan         float,
                other_receivables_within_3_years_yuan         float
            );
            alter table sina_fina_table add primary key(stock_code,record_date);
            ''')
        self.conn.commit()
        self.db_disconnect()

        print("db_sina_fina_table_create finish")
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
            self.cur.copy_from(buffer, table='sina_fina_table', sep=",")
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
        self.cur.execute("select max(record_date) from sina_fina_table \
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
                        fina_cmd = "insert into sina_fina_table ("\
                                + sina_cols + \
                                " ) values "+sql_cmd+";"
                        if debug:
                            print(fina_cmd)
                        self.cur.execute(fina_cmd)
                        self.conn.commit()
                        sql_cmd = ""

            if debug:
                print(sql_cmd)
            if(sql_cmd != ""):
                fina_cmd = "insert into sina_fina_table ("\
                        + sina_cols + \
                        " ) values "+sql_cmd+";"
                if debug:
                    print(fina_cmd)
                self.cur.execute(fina_cmd)
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
                        fina_sql = [] 
                        fina_sql.append("insert into sina_fina_table (")
                        fina_sql.append(sina_cols)
                        fina_sql.append( " ) values ")
                        fina_sql.append(''.join(sql_cmd))
                        fina_sql.append( "  on conflict (stock_code, record_date) do nothing ; ")
                        #print(''.join(fina_sql))
                        sql_cmd = []
                        if debug:
                            print(fina_sql)
                        t2 = time.time()
                        self.cur.execute(''.join(fina_sql))
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
                fina_sql = []
                fina_sql.append("insert into sina_fina_table (")
                fina_sql.append(sina_cols)
                fina_sql.append( " ) values ")
                fina_sql.append(''.join(sql_cmd))
                fina_sql.append( "  on conflict (stock_code, record_date) do nothing; ")
                #print(''.join(fina_sql))
                sql_cmd = []
                self.cur.execute(''.join(fina_sql))
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
            data.to_sql(name='sina_fina_table', con=self.conn, if_exists = 'replace', index=False)
            pass

        if debug:
            print(time.time()-t0)
            print('insert_all_stock_data_3(\\)')

        self.db_disconnect()

    #fix bug: delete zero when the stock is closed
    def delete_amount_is_zero(self):
        self.db_connect()
        sql_temp="delete from sina_fina_table where amount = 0;"
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
        sql_temp += sina_cols 
        sql_temp += "from ( "

        sql_temp += "select"
        sql_temp += sina_cols
        sql_temp += "from sina_fina_table"

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

        sql_temp = "delete from sina_fina_table"

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
 
        
'''
'''
