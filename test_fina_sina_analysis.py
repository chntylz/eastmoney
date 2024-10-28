#!/#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from file_interface import *
import pandas as pd
#keep 0.01 accrucy
pd.set_option('display.float_format',lambda x : '%.2f' % x)


from HData_sina_fina import *
from HData_sina_income import *
from HData_sina_balance  import *
from HData_sina_cashflow import *


debug = 0
debug = 1

hdata_fina     = HData_sina_fina("usr","usr")
hdata_income   = HData_sina_income("usr","usr")
hdata_balance  = HData_sina_balance("usr","usr")
hdata_cashflow = HData_sina_cashflow("usr","usr")


#1-1
def income_analysis_assets(df):
    y_unit=10000*10000
    df_len=len(df)
    #zong zi chan zengzhanglv  > 20%
    #total_asset_growth_rate
    flag = True
    i = 0
    list = []
    list.append([df.stock_name_x[0], '总资产', '总资产增长率', 'result'])
    '''
    list.append([df.stock_name_x[0], 'total_assets', 'total_assets_pct', 'result'])
    '''
    
    biaozhun= '先看总资产 看总资产，判断公司实力及扩张能力 >30%'

    for i in range(df_len):
        if debug:
            print('record_date=%s, i=%d, total_assets=%f, total_asset_growth_rate=%f'\
                    %(df.record_date[i], i, df.total_assets[i]/y_unit, df.total_asset_growth_rate[i] * 100))
        if df.total_asset_growth_rate[i] < 0.2:
            flag = False

        list.append([df.record_date[i], df.total_assets[i]/y_unit, \
                df.total_asset_growth_rate[i] * 100 , df.total_asset_growth_rate[i] >= 0.2])

    list.append([biaozhun, 0, 0, 0])

    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret, flag

#1-2
def income_analysis_liab(df):
    y_unit=10000*10000
    df_len=len(df)
    flag = True
    #zong zi chan fuzhailv  < 60%
    #asset_liability_ratio
    i = 0
    list = []
    list.append([df.stock_name_x[0], '总资产', '总负债', '资产负债率', 'result'])
    '''
    list.append([df.stock_name_x[0], 'total_assets', 'totliab', 'asset_liability_ratio', 'result'])
    '''
    for i in range(df_len):
        if debug:
            print('record_date=%s, i=%d, total_assets=%f, totliab=%f, asset_liability_ratio=%f, '\
                    %(df.record_date[i], i, df.total_assets[i]/y_unit, \
                    df.totliab[i]/y_unit, df.asset_liability_ratio[i]))
        if df.asset_liability_ratio[i] >= 60:
            flag = False

        list.append([df.record_date[i], df.total_assets[i]/y_unit, df.totliab[i]/y_unit,\
                df.asset_liability_ratio[i], df.asset_liability_ratio[i] < 60 ])
    biaozhun='看资产负债率，判断公司的债务风险.    资产负债率大于 60%的公司，债务风险较大需要注意'
    list.append([biaozhun, 0, 0, 0, 0])

    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret, flag

#1-3
def income_analysis_loan(df):
    y_unit=10000*10000
    df_len=len(df)
    flag = True
    #youxifuzhai > huobijijin  
    #fuzhai vs zichan
    i = 0
    list = []
    list.append([df.stock_name_x[0], '货币资金', '短期借款', \
                '应付利息', '一年内到期的非流动负债', \
                '长期借款', '应付债券', '长期应付款', '有息负债总额','result'])
    '''
    list.append([df.stock_name_x[0], 'curfds', 'shorttermborr', \
                'intepaya', 'duenoncliab', \
                'longborr', 'bdspaya', 'longpayatot', 'total_loan','result'])
    '''
    for i in range(df_len):
        if debug:
            print('record_date=%s, i=%d, curfds=%f, shorttermborr=%f, intepaya=%f, \
                duenoncliab=%f, longborr=%f, bdspaya=%f, longpayatot=%f, \
                total_loan=%f '\
                %(df.record_date[i], i, df.curfds[i]/y_unit, df.shorttermborr[i]/y_unit, \
                df.intepaya[i]/y_unit, df.duenoncliab[i]/y_unit, \
                df.longborr[i]/y_unit, df.bdspaya[i]/y_unit, df.longpayatot[i]/y_unit, \
                df.total_loan[i]/y_unit))
        if df.curfds[i] <= df.total_loan[i]:
            flag = False
        list.append([df.record_date[i], df.curfds[i]/y_unit, df.shorttermborr[i]/y_unit, \
                df.intepaya[i]/y_unit, df.duenoncliab[i]/y_unit, \
                df.longborr[i]/y_unit, df.bdspaya[i]/y_unit, df.longpayatot[i]/y_unit, \
                df.total_loan[i]/y_unit, df.curfds[i] > df.total_loan[i] ])

    biaozhun='看有息负债和货币资金，排除偿债风险:\
            有息负债和货币资金主要看两者大小，对于资产负债率大于40%的公司，\
            我们要看货币资金是否大于有息负债。货币资金小于有息负债的公司淘汰\
            货币资金 - 有息负债总额  必须大于0'

    list.append([biaozhun, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret, flag

#1-4
def income_analysis_payable_receivable(df):
    y_unit=10000*10000
    df_len=len(df)
    flag = True

    #
    # yingfuyushou vs yingshouyufu
    i = 0
    list = []
    list.append([df.stock_name_x[0], '总资产', '应付票据', '应付账款', \
            '预收款项', '应付预收合计',\
            '应收票据', '应收账款', '预付款项', '应收预付合计', \
            '应付预收 - 应收预付', '应收账款/总资产', 'result'\
            ])
    ''' 
    list.append([df.stock_name_x[0], 'total_assets', 'notespaya', 'accopaya', \
            'advapaym', 'total_payable',\
            'notesrece', 'accorece', 'prep', 'total_receivable', \
            'payable-receivable', 'reveivable/total_assets', 'result'\
            ])
    ''' 
    for i in range(df_len):
        total_payable =  df.notespaya[i] + df.accopaya[i] + df.advapaym[i]
        total_receivable = df.notesrece[i] + df.accorece[i] + df.prep[i]
        recv_of_total_assets = 0
        if df.total_assets[i] :
            recv_of_total_assets = total_receivable / df.total_assets[i] * 100 
        total_receivable_of_total_assets = 0
        if df.total_assets[i]:
            total_receivable_of_total_assets = total_receivable / df.total_assets[i] * 100 
        if recv_of_total_assets >= 20:
            flag = False
        list.append([df.record_date[i], \
            df.total_assets[i]/y_unit, df.notespaya[i]/y_unit, df.accopaya[i]/y_unit,\
            df.advapaym[i]/y_unit, total_payable/y_unit, \
            df.notesrece[i]/y_unit,df.accorece[i]/y_unit,df.prep[i]/y_unit,\
            total_receivable/y_unit,\
            (total_payable - total_receivable)/y_unit,\
            total_receivable_of_total_assets,\
            recv_of_total_assets < 20
            ])
        biaozhun='看“应收应付”和“预付预收”，判断公司的行业地位:\
                “应收应付”和“预付预收”主要看两点，\
                一是看（应付票据+应付账款+预收款项）与（应收票据+应收账款+预付款项）的大小；\
                二是看应收账款与总资产的比率。\
                （应付票据+应付账款+预收款项）-（应收票据+应收账款+预付款项）小于 0，\
                说明在经营过程中，公司的自有资金会被其他公司无偿占用，这样的公司竞争力相对较弱。\
                在实践中，我们把（应付票据+应付账款+预收款项）-（应收票据+应收账款+预付款项）\
                小于 0 的公司淘汰掉。\
                另外应收账款与总资产的比率大于 20%的公司，\
                说明公司应收账款的规模较大，经营风险自然也较大。'
    list.append([biaozhun, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret, flag

#1-5
def income_analysis_fixed_assets(df):
    y_unit=10000*10000
    df_len=len(df)
    flag = True
    #gudingzichan  < 40%
    #
    i = 0
    list = []
    list.append([df.stock_name_x[0],'固定资产', '在建工程',\
        '工程物资', '固定资产合计', '总资产', \
        '固定资产/总资产', 'result'])
    '''
    list.append([df.stock_name_x[0],'fixedassecleatot', 'consprogtot',\
        'engimate', 'total_fixed', 'total_assets', \
        'total_fixed/total_assets', 'result'])
    '''
    for i in range(df_len):
        total_fixed = df.fixedassecleatot[i] + df.consprogtot[i] \
            + df.engimate[i]
        total_fixed_of_total_assets = 0
        if df.total_assets[i]:
            total_fixed_of_total_assets = total_fixed/df.total_assets[i] * 100
        if total_fixed_of_total_assets >= 40:
            flag = False
        list.append([df.record_date[i], df.fixedassecleatot[i]/y_unit, \
            df.consprogtot[i]/y_unit,\
            df.engimate[i]/y_unit,\
            total_fixed/y_unit, df.total_assets[i]/y_unit, \
            total_fixed_of_total_assets, \
            total_fixed_of_total_assets < 40 \
            ])

    biaozhun='看固定资产，判断公司的轻重:\
            固定资产，只要看一点，那就是（固定资产+在建工程+工程物资）与总资产的比率，\
            （固定资产+在建工程+工程物资）与总资产的比率大于 40%的公司为重资产型公司。\
            重资产型公司保持竞争力的成本比较高，风险比较大，\
            当我们遇到重资产型公司，安全起见还是淘汰。'
    list.append([biaozhun, 0,0,0,0,0,0,0])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret, flag

#1-6
def income_analysis_invest(df):
    y_unit=10000*10000
    df_len=len(df)
    flag = True
    #invest ratio  < 10%
    biaozhun = '看投资类资产，判断公司的专注程度:\
        投资类资产我们只看一点，那就是与公司主业无关的投资类资产与总资产的比率。\
        优秀的公司一定是专注于主业的公司，\
        与主业无关的投资类资产占总资产的比例应当很低才对，最好为 0，\
        在实践中，与主业无关的投资类资产占总资产比率大于 10%的公司不够专注。淘汰。'
    
    i = 0
    list = []
    list.append([df.stock_name_x[0],'以公允价值计量的资产', '可供出售的金融资产',\
        '长期股权投资', '投资性房地产', '总投资', '总资产', \
        '与主业无关的投资类资产占比', 'result'])
    '''
    list.append([df.stock_name_x[0],'valuechgloss_x', 'avaisellasse',\
        'equiinve', 'inveprop', 'total_invest', 'total_assets', \
        'total_fixed/total_assets', 'result'])
    '''

    for i in range(df_len):
        total_invest = df.valuechgloss_x[i] + df.avaisellasse[i] \
            + df.equiinve[i] + df.inveprop[i]
        total_invest_of_total_assets = 0
        if df.total_assets[i]:
            total_invest_of_total_assets = total_invest/df.total_assets[i] * 100

        if total_invest_of_total_assets  >= 10:
            flag = False

        list.append([df.record_date[i], df.valuechgloss_x[i]/y_unit, \
            df.avaisellasse[i]/y_unit,\
            df.equiinve[i]/y_unit, df.inveprop[i]/y_unit, \
            total_invest/y_unit, df.total_assets[i]/y_unit, \
            total_invest_of_total_assets, \
            total_invest_of_total_assets < 10 \
            ])
    list.append([biaozhun, 0, 0,0,0,0,0,0,0])

    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret, flag

#1-7
def income_analysis_net_asset_return_rate(df):
    y_unit=10000*10000
    df_len=len(df)
    flag = True
    # 15% < net_asset_return_rate  < 39%
    biaozhun='看归母净利润，判断公司自有资本的获利能力:\归母净利润主要看两点，一是规模，二是增长率。\
    用“归母净利润”和“归母股东权益”可以计算出公司的净资产收益率，也叫 ROE。 \
    净资产收益率是一个综合性最强的财务比率，是杜邦分析系统的核心。\
    它反映所有者投入资本的获利能力，同时反映企业筹资、投资、运营的效率。\
    一般来说净资产收益率在 15%-39%比较合适。'

    i = 0
    list = []
    list.append([df.stock_name_x[0], '归母净利润', '归母净利润增长率',\
        '归属母公司股东权益合计', '净资产收益率', 'result'])
    '''
    list.append([df.stock_name_x[0], 'parenetp', 'net_profit_growth_rate',\
        'paresharrigh', 'net_asset_return_rate', 'result'])
    '''
    for i in range(df_len):
        net_asset_return_rate = 0
        if  df.paresharrigh[i]:
            net_asset_return_rate = df.parenetp[i] / df.paresharrigh[i] * 100 
        if net_asset_return_rate < 15 :
            flag = False

        list.append([df.record_date[i], df.parenetp[i]/y_unit, \
            df.net_profit_growth_rate[i] * 100, \
            df.paresharrigh[i]/y_unit, \
            net_asset_return_rate, \
            15 < net_asset_return_rate and net_asset_return_rate < 39 \
            ])
    list.append([biaozhun, 0,0,0,0,0])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret, flag

#2-1
def income_analysis_revenue(df):
    y_unit=10000*10000
    df_len=len(df)
    flag = True
    # revenue_yoy > 10%
    # (cash_ratio = cash / revenue) > 100%
    biaozhun='看营业收入，判断公司的行业地位及成长能力:\
            我们通过营业收入的金额和含金量看公司的行业地位；\
            通过营业收入增长率看公司的成长能力。\
            营业收入金额较大且“销售商品、提供劳务收到的现金”与“营业收入”的比率\
            大于 110%的公司行业地位高，产品竞争力强。\
            “营业收入”增长率大于 10%的公司，成长性较好。\
            销售商品、提供劳务收到的现金”与“营业收入”的比率\
            小于 100%的公司、营业收入增长率小于10%的公司淘汰掉。'
    i = 0
    list = []
    list.append([df.stock_name_x[0], '营业收入', '营业收入增长率',\
        '销售商品、提供劳务收到的现金', '现金占比', 'result'])
    '''
    list.append([df.stock_name_x[0], 'bizinco', 'bizinco_yoy',\
        'cash_received_of_sales_service', 'cash_ratio', 'result'])
    '''
    for i in range(df_len):
        cash_ratio  = 0
        if df.bizinco[i]:
            cash_ratio = df.cash_received_of_sales_service[i] / df.bizinco[i] * 100
        condi = df.bizinco_new_x[i] * 100 > 10 and cash_ratio > 100
        if condi is False:
            flag = False
        list.append([df.record_date[i], df.bizinco[i] / y_unit, \
            df.bizinco_new_x[i] * 100, \
            df.cash_received_of_sales_service[i]/y_unit, \
            cash_ratio, \
            condi\
            ])
    list.append([biaozhun,0,0,0,0,0])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret, flag

#2-2
def income_analysis_gross(df):
    y_unit=10000*10000
    df_len=len(df)
    flag = True
    # gross_ratio > 40%
    biaozhun='看毛利率，判断公司产品的竞争力:\
            高毛利率说明公司的产品或服务有很强的竞争力。\
            低毛利率则说明公司的产品或服务竞争力较差。\
            一般来说，毛利率大于 40%的公司都有某种核心竞争力。\
            毛利率小于 40%的公司一般面临的竞争压力都较大，风险也较大。\
            毛利率高的公司，风险相对较小。'
    i = 0
    list = []
    list.append([df.stock_name_x[0], '营业收入', '营业成本',\
        '毛利', '毛利率', 'result'])
    '''
    list.append([df.stock_name_x[0], 'bizinco', 'bizcost',\
        'gross', 'gross_ratio', 'result'])
    '''
    for i in range(df_len):
        gross =  df.bizinco[i] - df.bizcost[i] 
        gross_ratio = 0
        if df.bizinco[i]:
            gross_ratio = gross / df.bizinco[i] * 100
        condi = gross_ratio > 40
        if condi is False:
            flag = False
        list.append([df.record_date[i], df.bizinco[i] / y_unit, \
            df.bizcost[i]/y_unit, \
            gross / y_unit, \
            gross_ratio, \
            condi\
            ])
    list.append([biaozhun, 0,0,0,0,0])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret, flag


#2-3
def income_analysis_costfee(df):
    y_unit=10000*10000
    df_len=len(df)
    flag = True
    # costfee_ratio > 40%
    biaozhun='看费用率，判断公司成本管控能力:\
            毛利率高，费用率低，经营成果才可能好。\
            优秀公司的费用率与毛利率的比率一般小于 40%。\
            费用率与毛利率的比率大于 60%的公司需要注意风险。'
    i = 0
    list = []
    list.append([df.stock_name_x[0], '营业收入', '营业成本',\
        '毛利', '销售费用', '管理费用', '财务费用', '研发费用', '四费合计', \
        '费用率', '毛利率', '费用率/毛利率', 'result'])
    '''
    list.append([df.stock_name_x[0], 'bizinco', 'bizcost',\
        'gross', 'salesexpe', 'manaexpe', 'finexpe', 'deveexpe', 'total_4fee', \
        'costfee_p', 'gross_ratio', 'costfee_ratio', 'result'])
    '''
    for i in range(df_len):
        gross =  df.bizinco[i] - df.bizcost[i] 
        gross_ratio = 0
        if df.bizinco[i]:
            gross_ratio = gross / df.bizinco[i] * 100
        total_4fee = df.salesexpe[i] + df.manaexpe[i] + df.finexpe[i] + df.deveexpe[i]

        costfee_p = 0
        if df.bizinco[i] :
            costfee_p = total_4fee / df.bizinco[i] * 100

        costfee_ratio = 0
        if gross_ratio : 
            costfee_ratio = costfee_p / gross_ratio * 100 

        condi = gross_ratio > 40 and costfee_ratio < 60

        if condi is False:
            flag = False
        list.append([df.record_date[i], df.bizinco[i] / y_unit, \
            df.bizcost[i]/y_unit, \
            gross / y_unit, \
            df.salesexpe[i] / y_unit, \
            df.manaexpe[i]/ y_unit, \
            df.finexpe[i]/ y_unit, \
            df.deveexpe[i]/ y_unit, \
            total_4fee / y_unit, costfee_p, \
            gross_ratio, costfee_ratio,   \
            condi])
    list.append([biaozhun, 0, 0,0,0,0,0,0,0,0,0,0,0])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret, flag

#2-4
def income_analysis_main_profit(df):
    y_unit=10000*10000
    df_len=len(df)
    flag = True
    # profit_ratio > 40%
    biaozhun='看主营利润，判断公司的盈利能力及利润质量:\
            主营利润是一家公司最主要的利润来源，主营利润小于 0 的公司，直接淘汰。\
            毛利率大于 40%的公司，主营利润率至少应该大于 15%。\
            主营利润率小于 15%的公司，淘汰。\
            另外优秀公司的“主营利润”与“利润总额”的比率至少要大于 80%。\
            “主营利润”与“利润总额”的比率小于 80%的公司，要注意。'
    i = 0
    list = []
    list.append([df.stock_name_x[0], '营业收入', '营业成本',\
        '营业税金及附加', \
        '销售费用', '管理费用', '财务费用', '研发费用', '四费合计', \
        '利润总额', \
        '投资收益', '公允价值变动损益', '资产减值', '营业外收入', \
        '营业外支出', '信用减值损失', '其他收益', '资产处置收益', \
        '所有其他收益', '主营利润', \
        '主营利润率', '主营利润/利润总额', 'result'])
 
    '''
    list.append([df.stock_name_x[0], 'bizinco', 'bizcost',\
        'biztax', \
        'salesexpe', 'manaexpe', 'finexpe', 'deveexpe', 'total_4fee', \
        'total_profit', \
        'inveinco', 'valuechgloss_x', 'asseimpaloss', 'nonoreve', \
        'nonoexpe', 'credit_impairment_loss', 'other_income', 'fixedassetnetc', \
        'ocl', 'main_profit', \
        'main_profit_of_bizinco', 'main_profit_of_total_profit', 'result'])
    '''
    for i in range(df_len):
        total_4fee = (df.salesexpe[i] + df.manaexpe[i] + \
                df.finexpe[i] + df.deveexpe[i])
        ocl = (df.inveinco[i] + df.valuechgloss_x[i] - \
                df.asseimpaloss[i]+df.nonoreve[i] - \
                df.nonoexpe[i] - df.credit_impairment_loss[i] + \
                df.other_income[i] + df.fixedassetnetc[i])
        total_profit = df.totprofit[i]
        #main_profit = (total_profit - ocl)  
        main_profit = df.main_business_profit[i]
        main_profit_of_bizinco = df.main_business_profit_rate[i]

        main_profit_of_total_profit = 0
        if total_profit:
            main_profit_of_total_profit = main_profit / total_profit * 100

        condi = main_profit_of_total_profit > 80
        if condi is False:
            flag = False

        list.append([df.record_date[i], df.bizinco[i] / y_unit, \
            df.bizcost[i]/y_unit, \
            df.biztax[i] / y_unit, \
            df.salesexpe[i] / y_unit, \
            df.manaexpe[i]/ y_unit, \
            df.finexpe[i]/ y_unit, \
            df.deveexpe[i]/ y_unit, \
            total_4fee / y_unit, \
            total_profit / y_unit, \
            df.inveinco[i]/ y_unit, df.valuechgloss_x[i]/ y_unit, \
            df.asseimpaloss[i]/ y_unit, df.nonoreve[i]/ y_unit, \
            df.nonoexpe[i]/ y_unit, df.credit_impairment_loss[i]/ y_unit, \
            df.other_income[i]/ y_unit, df.fixedassetnetc[i]/ y_unit,  \
            ocl/ y_unit, main_profit/ y_unit, \
            main_profit_of_bizinco, main_profit_of_total_profit, \
            condi])
    list.append([biaozhun, 0, 0,0,0,0,0,0,0, 0,0,0,0,0,0,0, 0,0,0,0,0,0,0])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret, flag


#2-5
def income_analysis_(df):
    y_unit=10000*10000
    df_len=len(df)
    flag = True
    # ncf_ratio > 100%
    biaozhun='看净利润，判断公司的经营成果及含金量:\
            净利润金额越大越好。净利润小于 0 的公司，直接淘汰掉。\
            优秀的公司不但净利润金额大而且含金量高。\
            优秀公司的“净利润现金比率”会持续的大于 100%。\
            过去 5 年的“净利润现金比率\
            （过去 5 年的“经营活动产生的现金流量净额”总和/过去 5 年的净利润总和*100%）”\
            小于 100%的公司，要注意'
    biaozhun2='现金流量表 cash_flow:\
            看经营活动产生的现金流量净额，判断公司的造血能力经营活动产生的现金流量净额越大，\
            公司的造血能力越强。优秀的公司造血能力都很强大。\
            优秀的公司满足经营活动产生的现金流量净额>\
            固定资产折旧+无形资产摊销+借款利息+现金股利这个条件。\
            “经营活动产生的现金流量净额”持续小于（固定资产折旧和无形资产摊销+借款利息+现金股利）的公司，\
            淘汰'
    i = 0
    list = []
    list.append([df.stock_name_x[0], '经营活动产生的现金流量净额', '经营活动产生的现金流量净额增长率',\
        '净利润', '净利润现金比率', 'result'])
    '''
    list.append([df.stock_name_x[0], 'mananetr', 'mananetr_yoy',\
        '', 'netprofit_of_mananetr', 'result'])
    '''
    total_netprofit = 0
    total_mananetr = 0
    for i in range(df_len):
        total_mananetr += df.mananetr[i] 
        total_netprofit  += df.netprofit[i]
        rate_of_mananetr  = 0
        if df.netprofit[i]:
           rate_of_mananetr  = df.mananetr[i]  * 100/  df.netprofit[i]

        condi = rate_of_mananetr > 100 
        if condi is False:
            flag = False
        list.append([df.record_date[i], \
            df.mananetr[i]/ y_unit, \
            df.mananetr_new[i] * 100, \
            df.netprofit[i] / y_unit,\
            rate_of_mananetr ,\
            condi])
    list.append([biaozhun,biaozhun2,0,0,0,0])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret, flag

#3-1
#cash flow

#3-2
def income_analysis_paid_assets(df):
    y_unit=10000*10000
    df_len=len(df)
    flag = True
    # paid_assets_of_nc  [10% ~ 60%]
    biaozhun='看“购买固定资产、无形资产和其他长期资产支付的现金”，判断公司未来的成长能力:\
            “购买固定资产、无形资产和其他长期资产支付的现金”金额越大，公司未来成长能力越强。\
            成长能力较强的公司，“购买固定资产、无形资产和其他长期资产支付的现金”与\
            “经营活动现金流量净额”比率一般在 10%-60%之间。\
            这个比率连续 2 年高于 100%或低于 10%的公司，淘汰。'
    i = 0
    list = []
    list.append([df.stock_name_x[0], '经营活动产生的现金流量净额',\
            '购建固定资产、无形资产和其他长期资产支付的现金',\
            '处置固定资产、无形资产和其他长期资产收回的现金净额', \
            '购建固定资产、无形资产和其他长期资产支付的现金占比', \
            '处置固定资产、无形资产和其他长期资产收回的现金净额占比', 'result'])
    '''
    list.append([df.stock_name_x[0], 'mananetr', 'acquassetcash',\
        'fixedassetnetc', 'paid_assets_of_ncf','disposal_assets_of_ncf', 'result'])
    '''
    for i in range(df_len):
        paid_assets_of_ncf      = 0
        if df.mananetr[i] :
            paid_assets_of_ncf  = df.acquassetcash[i]  / df.mananetr[i] * 100 

        disposal_assets_of_ncf  = 0
        if df.mananetr[i]: 
            disposal_assets_of_ncf  = df.fixedassetnetc[i] / df.mananetr[i] * 100  

        condi = paid_assets_of_ncf  > 10 and paid_assets_of_ncf < 60 
        if condi is False:
            flag = False
        list.append([df.record_date[i], \
            df.mananetr[i]/ y_unit, \
            df.acquassetcash[i] / y_unit, \
            df.fixedassetnetc[i] / y_unit, \
            paid_assets_of_ncf , \
            disposal_assets_of_ncf, \
            condi])
    list.append([biaozhun,0,0,0,0,0,0])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret, flag

#3-3
#bonus

#3-4
def income_analysis_ncf_of_oa_ia_fa(df):
    y_unit=10000*10000
    df_len=len(df)
    flag = True
    # mananetr > 0
    biaozhun=' 看三大活动现金流量净额的组合类型，选出最佳类型的公司:\
        优秀的公司一般是“正负负”和“正正负”类型。\
        公司经营活动产生的现金流量净额为正，说明公司主业经营赚钱；\
        投资活动产生的现金流量净额为负，说明公司在继续投资，公司处于扩张之中。\
        筹资活动现金流量净额为负，说明公司在还钱或者分红。'
    i = 0
    list = []
    list.append([df.stock_name_x[0], '经营活动产生的现金流量净额', \
        '投资活动产生的现金流量净额', '筹资活动现金流量净额', \
        'result'])
    #list.append([df.stock_name_x[0], 'mananetr', 'invnetcashflow', 'finnetcflow', \
    #    'result'])
    for i in range(df_len):
        condi = df.mananetr[i] > 0 
        if condi is False :
            flag = False
        list.append([df.record_date[i], \
            df.mananetr[i]/ y_unit, \
            df.invnetcashflow[i]/ y_unit, \
            df.finnetcflow[i]/ y_unit, \
            condi])
    list.append([biaozhun, 0,0,0,0])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret, flag


#3-5
def income_analysis_net_increase(df):
    y_unit=10000*10000
    df_len=len(df)
    flag = True 
    # cashnetr > 0
    biaozhun='看“现金及现金等价物的净增加额”，判断公司的稳定性:\
        “现金及现金等价物的净增加额”持续小于 0 的公司，很难稳定持续的保持现有的竞争力。\
        优秀公司的“现金及现金等价物的净增加额”一般都是持续大于 0 的。\
        去掉分红，“现金及现金等价物的净增加额”小于 0 的公司，淘汰。'
    i = 0
    list = []
    list.append([df.stock_name_x[0], '现金及现金等价物净增加额', '期末现金及现金等价物余额', \
        'result'])
    #list.append([df.stock_name_x[0], 'cashnetr', 'finalcashbala', \
    #    'result'])
    for i in range(df_len):
        condi = df.cashnetr[i] > 0 
        if condi is False:
            flag = False
        list.append([df.record_date[i], \
            df.cashnetr[i]/ y_unit, \
            df.finalcashbala[i]/ y_unit, \
            condi])
    list.append([biaozhun, 0,0,0])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret, flag



def fina_data_analysis(df):
    all_df = df
    group_by_stock_code_df=all_df.groupby('stock_code')
    for stock_code, group_df in group_by_stock_code_df:
        if group_df is None:
            print('%s, group_df is None' % stock_code )
            continue

        if len(group_df) < 1:
            print('%s, len(group_df) < 1' % stock_code )
            continue

        ret_df = pd.DataFrame()

        group_df = group_df.reset_index(drop=True)
        if debug:
            print(stock_code)
            print(group_df.head(1))

        '''
        if stock_code != '002475':
            continue
        '''

        ret_df, flag = asset_df, flag_asset = income_analysis_assets(group_df)
        
        liab_df, flag_liab  = income_analysis_liab(group_df)
        ret_df = pd.concat([ret_df, liab_df]) 

        loan_df, flag_loan  = income_analysis_loan(group_df)
        ret_df = pd.concat([ret_df, loan_df]) 

        pay_recv_df, flag_pay_recv = income_analysis_payable_receivable(group_df)
        ret_df = pd.concat([ret_df, pay_recv_df]) 

        fix_assets_df, flag_fix_assets = income_analysis_fixed_assets(group_df)
        ret_df = pd.concat([ret_df, fix_assets_df]) 

        invest_df, flag_invest = income_analysis_invest(group_df)
        ret_df = pd.concat([ret_df, invest_df]) 

        net_asset_return_rate_df, flag_net_asset_return_rate = income_analysis_net_asset_return_rate(group_df)
        ret_df = pd.concat([ret_df, net_asset_return_rate_df]) 

        revenue_df, flag_revnue = income_analysis_revenue(group_df)
        ret_df = pd.concat([ret_df, revenue_df]) 

        gross_df, flag_gross = income_analysis_gross(group_df)
        ret_df = pd.concat([ret_df, gross_df]) 

        costfee_df, flag_costfee = income_analysis_costfee(group_df)
        ret_df = pd.concat([ret_df, costfee_df]) 

        main_profit_df, flag_main_frofit = income_analysis_main_profit(group_df)
        ret_df = pd.concat([ret_df, main_profit_df]) 

        _df, flag_netprofit = income_analysis_netprofit(group_df)
        ret_df = pd.concat([ret_df, _df]) 

        paid_assets_df, flag_paid_asset = income_analysis_paid_assets(group_df)
        ret_df = pd.concat([ret_df, paid_assets_df]) 

        ncf_df, flag_ncf = income_analysis_ncf_of_oa_ia_fa(group_df)
        ret_df = pd.concat([ret_df, ncf_df]) 

        net_increase_df, flag_net_increase = income_analysis_net_increase(group_df)
        ret_df = pd.concat([ret_df, net_increase_df]) 

        ret_df.to_csv('./csv_data/sina_' + stock_code + '_' + ret_df.stock_name_x[0] + '.csv', encoding='gbk')
        
        if flag and flag_net_increase and flag_ncf and flag_paid_asset and flag_ and flag_main_frofit and flag_costfee and flag_gross \
            and flag_revnue and flag_net_asset_return_rate and flag_invest and flag_fix_assets and flag_asset and flag_liab and flag_loan and flag_pay_recv :
            print("################################### %s, %s ################################\n"% (stock_code, stock_name_x))
    
    pass


def get_data_from_fina_income_balance_cashflow():

    df_fina     = hdata_fina.get_data_from_hdata()
    df_income   = hdata_income.get_data_from_hdata()
    df_balance  = hdata_balance.get_data_from_hdata()
    df_cashflow = hdata_cashflow.get_data_from_hdata()

    df_fina = df_fina.sort_values('record_date', ascending=0)
    df_fina = df_fina.reset_index(drop=True)

    df_income = df_income.sort_values('record_date', ascending=0)
    df_income = df_income.reset_index(drop=True)

    df_balance = df_balance.sort_values('record_date', ascending=0)
    df_balance = df_balance.reset_index(drop=True)

    df_cashflow = df_cashflow.sort_values('record_date', ascending=0)
    df_cashflow = df_cashflow.reset_index(drop=True)

    key_day = '12-31'
    df_y_fina       = df_fina[df_fina['record_date'].str.contains(key_day)]
    df_y_income     = df_income[df_income['record_date'].str.contains(key_day)]
    df_y_balance    = df_balance[df_balance['record_date'].str.contains(key_day)]
    df_y_cashflow   = df_cashflow[df_cashflow['record_date'].str.contains(key_day)]

    df_y_fina.iloc[:, :5].head(1)    
    df_y_income.iloc[:, :5].head(1)   
    df_y_balance.iloc[:, :5].head(1)  
    df_y_cashflow.iloc[:, :5].head(1) 

    df_tmp = pd.merge(df_y_fina, df_y_income, how='outer', \
            on=['record_date', 'stock_code'])
    #删除重复的列
    #remove duplicate columns
    df_tmp = df_tmp.T.drop_duplicates().T

    df_tmp = pd.merge(df_tmp, df_y_balance, how='outer', \
            on=['record_date', 'stock_code'])
    #删除重复的列
    #remove duplicate columns
    df_tmp = df_tmp.T.drop_duplicates().T

    df = df_y_income_balance = pd.merge(df_tmp, df_y_cashflow, how='outer', \
            on=['record_date', 'stock_code'])
    #删除重复的列
    #remove duplicate columns
    df = df.T.drop_duplicates().T


    df=df.fillna(0)
    df=round(df,4)
    df['total_loan'] = 0
    df['total_loan']  = df.shorttermborr+ df.intepaya \
        + df.duenoncliab + df.longborr \
        + df.bdspaya + df.longpayatot


    '''
    len(df_y_fina)
    len(df_y_income)  
    len(df_y_balance)
    len(df_y_cashflow)
    len(df_y_income_balance)

    df_y_fina[df_y_fina['stock_code'] == 'SZ002475']
    df_y_income[df_y_income['stock_code'] == 'SZ002475']
    df_y_balance[df_y_balance['stock_code'] == 'SZ002475']
    df_y_cashflow[df_y_cashflow['stock_code'] == 'SZ002475']
    df_y_income_balance[df_y_income_balance['stock_code'] == 'SZ002475']

    df_y_balance[df_y_balance.stock_code=='SH600519'].total_assets
    '''

    return df



if __name__ == '__main__':

    df =  get_data_from_fina_income_balance_cashflow()
    print(df.head(5))
    fina_data_analysis(df)



