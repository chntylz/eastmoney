#!/#!/usr/bin/env python
# -*- coding: utf-8 -*-


import pandas as pd
import csv
import os
import re
import  datetime
import time

from get_daily_zlje import *

import multiprocessing

from bs4 import BeautifulSoup
import random

from HData_sina_balance import *
from HData_sina_income import *
from HData_sina_cashflow import *
from HData_sina_fina import *

hdata_sina_balance = HData_sina_balance("usr","usr")
hdata_sina_income  = HData_sina_income("usr","usr")
hdata_sina_cashflow= HData_sina_cashflow("usr","usr")
hdata_sina_fina    = HData_sina_fina("usr","usr")

'''
#balance
#https://money.finance.sina.com.cn/corp/go.php/vFD_BalanceSheet/stockid/600660/ctrl/part/displaytype/4.phtml
#https://money.finance.sina.com.cn/corp/go.php/vFD_BalanceSheet/stockid/600660/ctrl/2024/displaytype/4.phtml


#income
#https://money.finance.sina.com.cn/corp/go.php/vFD_ProfitStatement/stockid/600660/ctrl/2024/displaytype/4.phtml

#cashflow
#https://money.finance.sina.com.cn/corp/go.php/vFD_CashFlow/stockid/600660/ctrl/2024/displaytype/4.phtml


#fina
#https://vip.stock.finance.sina.com.cn/corp/go.php/vFD_FinancialGuideLine/stockid/600660/displaytype/4.phtml
#https://money.finance.sina.com.cn/corp/go.php/vFD_FinancialGuideLine/stockid/600660/ctrl/2024/displaytype/4.phtml


'''

debug = 0 
debug = 1 
debug = 0 

update_all = 0

def insert_to_database(df, type_table):

    if len(df) == 0:
        return

    cols = []
    database = ''

    income_cols = [ 'record_date', 'stock_code', 'stock_name', 'biztotinco', 'bizinco', 'biztotcost', 'bizcost', 'biztax', \
                 'salesexpe', 'manaexpe', 'finexpe', 'deveexpe', 'asseimpaloss', 'valuechgloss', \
                 'inveinco', 'assoinveprof', 'exchggain', 'perprofit', 'nonoreve', 'nonoexpe', \
                 'noncassetsdisl', 'totprofit', 'incotaxexpe', 'netprofit', 'parenetp', 'minysharrigh', \
                 'eps', 'basiceps', 'dilutedeps', 'othercompinco', 'compincoamt', 'parecompincoamt', 'minysharincoamt' ]
    
    balance_cols = [ 'record_date', 'stock_code', 'stock_name', 'current_assets', 'curfds', \
    'tradfinasset', 'derifinaasset', 'notesaccorece', \
	'notesrece', 'accorece', 'recfinanc', 'prep', 'otherrecetot', 'interece', 'dividrece', \
	'otherrece', 'purcresaasset', 'inve', 'accheldfors', 'expinoncurrasset', 'prepexpe', \
	'unseg', 'othercurrasse', 'totcurrasset', 'noncurrent_assets', 'lendandloan', 'avaisellasse', 'holdinvedue', \
	'longrece', 'equiinve', 'inveprop', 'consprogtot', 'consprog', 'engimate', 'fixedassecleatot', \
	'fixedassenet', 'fixedasseclea', 'prodasse', 'comasse', 'hydrasset', 'ruseassets', 'intaasset', \
	'deveexpe', 'goodwill', 'logprepexpe', 'defetaxasset', 'othernoncasse', 'totalnoncassets', \
	'totasset', 'current_debt', 'shorttermborr', 'tradfinliab', 'notesaccopaya', 'notespaya', 'accopaya', \
	'advapaym', 'copepoun', 'copeworkersal', 'taxespaya', 'otherpaytot', 'intepaya', 'divipaya', \
	'otherpay', 'accrexpe', 'defereve', 'shorttermbdspaya', 'duenoncliab', 'othercurreliabi', \
	'totalcurrliab', 'noncurrent_debt', 'longborr', 'bdspaya', 'leaseliab', 'lcopeworkersal', 'longpayatot', \
	'longpaya', 'specpaya', 'expenoncliab', 'defeincotaxliab', 'longdefeinco', 'othernoncliabi', \
	'totalnoncliab', 'totliab', 'equity', 'paidincapi', 'capisurp', 'treastk', 'ocl', 'specrese', 'rese', \
    'generiskrese', 'undiprof', 'paresharrigh', 'minysharrigh', 'righaggr', 'totliabsharequi' ]

    cash_cols =  ['record_date', 'stock_code', 'stock_name', 'cashfromop', 'laborgetcash', 'taxrefd', 'receotherbizcash', 'bizcashinfl', 
             'labopayc', 'payworkcash', 'paytax', 'payacticash', 'bizcashoutf', 'mananetr', 
             'cashfrominvent', 'withinvgetcash', 'inveretugetcash', 'fixedassetnetc', 'subsnetc', 'receinvcash', 
             'invcashinfl', 'acquassetcash', 'invpayc', 'subspaynetcash', 'payinvecash', 
             'invcashoutf', 'invnetcashflow', 'cashfromfinancingactivities', 'invrececash', 'subsrececash', 'recefromloan', 
             'issbdrececash', 'recefincash', 'fincashinfl', 'debtpaycash', 'diviprofpaycash', 
             'subspaydivid', 'finrelacash', 'fincashoutf', 'finnetcflow', 'chgexchgchgs', 
             'cashnetr', 'inicashbala', 'finalcashbala', 'cashnote', 'netprofit', 'minysharrigh', 
             'unreinveloss', 'asseimpa', 'assedepr', 'intaasseamor', 'longdefeexpenamor', 
             'prepexpedecr', 'accrexpeincr', 'dispfixedassetloss', 'fixedassescraloss', 
             'valuechgloss', 'defeincoincr', 'estidebts', 'finexpe', 'inveloss', 'defetaxassetdecr', 
             'defetaxliabincr', 'inveredu', 'receredu', 'payaincr', 'unseparachg', 'unfiparachg', 
             'other', 'biznetcflow', 'debtintocapi', 'expiconvbd', 'finfixedasset', 'cashfinalbala', 
             'cashopenbala', 'equfinalbala', 'equopenbala', 'cashneti' ]

    fina_cols = [ 'record_date','stock_code','stock_name','per_indictor', 'diluted_eps','weighted_eps','eps_adjusted','eps_after_deducting_non_recurring_gains_and_losses',\
             'net_assets_per_share_before_adjustment','net_assets_per_share_adjusted','operating_cash_flow_per_share',\
             'capital_reserve_per_share','retained_eps','adjusted_net_assets_per_share','earn_capacity','total_asset_profit_rate',\
             'main_business_profit_rate','total_asset_net_profit_rate','cost_and_expense_profit_rate',\
             'operating_profit_rate','main_business_cost_rate','net_profit_margin','return_on_equity',\
             'return_on_net_assets','return_on_assets','gross_profit_margin','proportion_of_three_expenses',\
             'non_main_business_proportion','main_business_profit_proportion','dividend_payout_rate',\
             'investment_return_rate','main_business_profit','net_asset_return_rate',\
             'weighted_net_asset_return_rate','net_profit_after_deducting_non_recurring_gains_and_losses',\
             'growth_capacity','main_business_income_growth_rate','net_profit_growth_rate','net_asset_growth_rate',\
             'total_asset_growth_rate','op_capacity','accounts_receivable_turnover_rate_times','accounts_receivable_turnover_days_days',\
             'inventory_turnover_days_days','inventory_turnover_rate_times','fixed_asset_turnover_rate_times',\
             'total_asset_turnover_rate_times','total_asset_turnover_days_days','current_asset_turnover_rate_times',\
             'current_asset_turnover_days_days','shareholders_equity_turnover_rate_times','capital_structure','current_ratio',\
             'quick_ratio','cash_ratio','interest_coverage_ratio','long_term_debt_to_working_capital_ratio',\
             'shareholders_equity_ratio','long_term_debt_ratio','shareholders_equity_to_fixed_assets_ratio',\
             'liabilities_to_owners_equity_ratio','long_term_assets_to_long_term_funds_ratio',\
             'capitalization_ratio','fixed_asset_net_value_ratio','capitalization_fix_ratio','equity_ratio',\
             'liquidation_value_ratio','fixed_assets_ratio','asset_liability_ratio','total_assets',\
             'cash_flow','net_operating_cash_flow_to_sales_revenue_ratio','operating_cash_flow_return_on_assets',\
             'net_operating_cash_flow_to_net_profit_ratio','net_operating_cash_flow_to_debt_ratio',\
             'cash_flow_ratio','other_indicator','short_term_stock_investment','short_term_bond_investment_yuan',\
             'short_term_other_operating_investment_yuan','long_term_stock_investment_yuan',\
             'long_term_bond_investment_yuan','long_term_other_operating_investment_yuan',\
             'accounts_receivable_within_1_year_yuan','accounts_receivable_within_1_2_years_yuan',\
             'accounts_receivable_within_2_3_years_yuan','accounts_receivable_within_3_years_yuan',\
             'prepayments_within_1_year_yuan','prepayments_within_1_2_years_yuan',\
             'prepayments_within_2_3_years_yuan','prepayments_within_3_years_yuan',\
             'other_receivables_within_1_year_yuan','other_receivables_within_1_2_years_yuan',\
             'other_receivables_within_2_3_years_yuan','other_receivables_within_3_years_yuan' ]
             


    if type_table == 'balance':
        cols = balance_cols
        database = hdata_sina_balance
    elif type_table == 'income':  
        cols = income_cols
        database = hdata_sina_income
    elif type_table == 'cashflow':  
        cols = cash_cols
        database = hdata_sina_cashflow
    elif type_table == 'fina':  
        cols = fina_cols
        database = hdata_sina_fina
    else:
        print('### type_table:%s is null, return'% type_table)
        return df

    if debug:
        print('cols=%s, df.columns=%s' % (len(cols), len(df.columns)))

    try:
        df.columns = cols
        #str to date format  for database format
        df['record_date']=df['record_date'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date().strftime("%Y%m%d"))
        if update_all == 0:
            df = df.head(1)  #only update the latest item
        database.copy_from_stringio(df)
    except Exception as e:
        print("### error (%s):%s %s" % (e, type_table, df.head(1)))

    return df

def get_sina_data_from_read_html(stock_code, stock_name, type_table, year):
    
    time.sleep(random.randint(5,10)) #add time to avoid sina crawl rules

    table_idx = 0

    if type_table == 'balance':
        table_idx = 13
        url = 'https://money.finance.sina.com.cn/corp/go.php/vFD_BalanceSheet/stockid/' + stock_code + '/ctrl/' + year + '/displaytype/4.phtml'
    elif type_table == 'income':  
        table_idx = 13
        url = 'https://money.finance.sina.com.cn/corp/go.php/vFD_ProfitStatement/stockid/' + stock_code + '/ctrl/' + year + '/displaytype/4.phtml'
    elif type_table == 'cashflow':  
        table_idx = 13
        url = 'https://money.finance.sina.com.cn/corp/go.php/vFD_CashFlow/stockid/' + stock_code + '/ctrl/' + year + '/displaytype/4.phtml'
    elif type_table == 'fina':  
        table_idx = 12
        url = 'https://money.finance.sina.com.cn/corp/go.php/vFD_FinancialGuideLine/stockid/' + stock_code + '/ctrl/' + year + '/displaytype/4.phtml'
    else:
        print('### type_table is null, return')


    tb = pd.read_html(url)
    df=tb[table_idx]
    df=df.T

    df=df.fillna(0)
    df = df[df[0] != 0]   #date is not 0
    df=df[1:]
    df = df.replace('--',0)
    
    if type_table != 'fina':
        #tr is gray
        del df[1]

    df.insert(1, 'stock_name' , stock_name, allow_duplicates=False)
    df.insert(1, 'stock_code' , stock_code, allow_duplicates=False)

    if debug:
        df.to_csv('./sina/' + year + '_' + type_table + '_' + stock_code + '_sina_html.csv', mode='w', encoding='utf_8_sig', header=1, index=0)
        print(df)

    #insert_to_database(df, type_table)

    return df

def get_sina_fina_data(stock_code, stock_name):

    if '银行' in stock_name:
        return


    df_balance  = pd.DataFrame()
    df_income   = pd.DataFrame()
    df_cashflow = pd.DataFrame()
    df_fina     = pd.DataFrame()

    this_year = int(time.strftime("%Y", time.localtime()))
    #get continuous 5 years data
    target_years = 5
    target_years = 1
    if update_all == 0:
        target_years = 5

    for yy in range(target_years):
        year = str(this_year - yy)
        df_balance_tmp  = get_sina_data_from_read_html(stock_code, stock_name, 'balance', year)
        df_balance = pd.concat([df_balance, df_balance_tmp])

        df_income_tmp   = get_sina_data_from_read_html(stock_code, stock_name, 'income', year)
        df_income = pd.concat([df_income, df_income_tmp])

        df_cashflow_tmp = get_sina_data_from_read_html(stock_code, stock_name, 'cashflow', year)
        df_cashflow = pd.concat([df_cashflow, df_cashflow_tmp])

        df_fina_tmp     = get_sina_data_from_read_html(stock_code, stock_name, 'fina', year)
        df_fina = pd.concat([df_fina, df_fina_tmp])

    insert_to_database(df_balance, 'balance')
    insert_to_database(df_income, 'income')
    insert_to_database(df_cashflow, 'cashflow')
    insert_to_database(df_fina, 'fina')
    pass


def worker(data):
    if debug:
        print(data)
    stock_code = data[2]
    stock_name = data[3]

    get_sina_fina_data(stock_code, stock_name)

    return


if __name__ == '__main__':

    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    stock_df=get_daily_zlje2()
    stock_df.to_csv('./sina/zlje.csv', encoding='utf-8-sig', float_format='%.2f')
    stock_df = stock_df.sort_values('f12', ascending=1)
    stock_df = stock_df.reset_index(drop=True)
    print(stock_df.head(5))
    #stock_df=stock_df.head(4)
    #exit()


    data_list = np.array(stock_df)
    data_list = data_list.tolist()

    #only update the latest item
    if update_all:
        hdata_sina_income.db_hdata_sina_create()
        hdata_sina_balance.db_hdata_sina_create()
        hdata_sina_cashflow.db_hdata_sina_create()
        hdata_sina_fina.db_hdata_sina_create()

    processes = 4
    number = len(stock_df)
    with multiprocessing.Pool(int(processes)) as pool:
        pool.map(worker, data_list)

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))


