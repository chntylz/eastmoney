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
cashflow
'报表日期'                                                             'record_date',                      
'销售商品、提供劳务收到的现金'                                         'laborgetcash',
'收到的税费返还'                                                       'taxrefd',
'收到的其他与经营活动有关的现金'                                       'receotherbizcash',
'经营活动现 金流入小计'                                                'bizcashinfl',
'购买商品、接受劳务支付的现金'                                         'labopayc',
'支付给职工以及为职工支付的现金'                                       'payworkcash',
'支付的各项税费'                                                       'paytax',
'支付的其他与经营活动有关的现金'                                       'payacticash',
'经营活动现金流出小计'                                                 'bizcashoutf',
'经营活动产生的现金流量净额'                                           'mananetr',
'收回投资所收到的现金'                                                 'withinvgetcash',
'取得投资收益所收到的现金'                                             'inveretugetcash',
'处置固定资产、无形资产和其他长期资产所收回的现金净额'                 'fixedassetnetc',
'处置子公司及其他营业单位收到的现金净额'                               'subsnetc',
'收到的 其他与投资活动有关的现金'                                      'receinvcash',
'投资活动现金流入小计'                                                 'invcashinfl',
'购建固定资产、无形资产和其他长期资产所支付的现金'                     'acquassetcash',
'投资所支付的现金'                                                     'invpayc',
'取得子公司及其他营业单位支付的现金净额'                               'subspaynetcash',
'支付的其他与投资活动有关的现金'                                       'payinvecash',
'投资活动现金流出小计'                                                 'invcashoutf',
'投资活动产生的现金流量净额'                                           'invnetcashflow',
'吸收投资收到的现金'                                                   'invrececash',
'其中：子公司吸收少数股东投资收到的现金'                               'subsrececash',
'取得借款收到的现金'                                                   'recefromloan',
'发行债券收到的现金'                                                   'issbdrececash',
'收到其他与筹资活动有关的现金'                                         'recefincash',
'筹资活动现金流入小计'                                                 'fincashinfl',
'偿还债务支付的现金'                                                   'debtpaycash',
'分配股利、利润或偿付利息所支付的现金'                                 'diviprofpaycash',
'其中：子公司支付给少数股东的股利、利润'                               'subspaydivid',
'支付其他与筹资活动有关的现金'                                         'finrelacash',
'筹资活动现金流出小计'                                                 'fincashoutf',
'筹资活动产生的现金流量净额'                                           'finnetcflow',
'四、汇率变动对现金及现金等价物的影响'                                 'chgexchgchgs',
'五、现金及现金等价物净增加额'                                         'cashnetr',
'加:期初现金及现金等价物余额'                                          'inicashbala',
'六、期末现金及现金等价物余额'                                         'finalcashbala',
'净利润'                                                               'netprofit',
'少数股东权益'                                                         'minysharrigh',
'未确认的投资损失'                                                     'unreinveloss',
'资产减值准备'                                                         'asseimpa',
'固定资产折旧、油气资产折耗、生产性物资折旧'                           'assedepr',
'无形资产摊销'                                                         'intaasseamor',
'长期待摊 费用摊销'                                                    'longdefeexpenamor',
'待摊费用的减少'                                                       'prepexpedecr',
'预提费用的增加'                                                       'accrexpeincr',
'处置固定资产、无形资产和其他长期资产的损失'                           'dispfixedassetloss',
'固定资产报废损失'                                                     'fixedassescraloss',
'公允价值变动损失'                                                     'valuechgloss',
'递 延收益增加（减：减少）'                                            'defeincoincr',
'预计负债'                                                             'estidebts',
'财务费用'                                                             'finexpe',
'投资损失'                                                             'inveloss',
'递延所得税资产减少'                                                   'defetaxassetdecr',
'递延所得税负债增加'                                                   'defetaxliabincr',
'存货的减少'                                                           'inveredu',
'经营性应收项目 的减少'                                                'receredu',
'经营性应付项目的增加'                                                 'payaincr',
'已完工尚未结算款的减少(减:增加)'                                      'unseparachg',
'已结算尚未完工款的增加(减:减少)'                                      'unfiparachg',
'其他'                                                                 'other',
'经营 活动产生现金流量净额'                                            'biznetcflow',
'债务转为资本'                                                         'debtintocapi',
'一年内到期的可转换公司债券'                                           'expiconvbd',
'融资租入固定资产'                                                     'finfixedasset',
'现金的期末余额'                                                       'cashfinalbala',
'现金的期初余额'                                                       'cashopenbala',
'现金等价物的期末余额'                                                 'equfinalbala',
'现金等价物的期初余额'                                                 'equopenbala',
'现金及现金等价物的净增加额'                                           'cashneti', 
'002261'                                                               'stock_code',
'拓维信息'                                                             'stock_name'
'''

sina_cols = " record_date, stock_code, stock_name, laborgetcash, taxrefd, receotherbizcash, bizcashinfl, \
             labopayc, payworkcash, paytax, payacticash, bizcashoutf, mananetr, \
             withinvgetcash, inveretugetcash, fixedassetnetc, subsnetc, receinvcash, \
             invcashinfl, acquassetcash, invpayc, subspaynetcash, payinvecash, \
             invcashoutf, invnetcashflow, invrececash, subsrececash, recefromloan, \
             issbdrececash, recefincash, fincashinfl, debtpaycash, diviprofpaycash, \
             subspaydivid, finrelacash, fincashoutf, finnetcflow, chgexchgchgs, \
             cashnetr, inicashbala, finalcashbala, netprofit, minysharrigh, \
             unreinveloss, asseimpa, assedepr, intaasseamor, longdefeexpenamor, \
             prepexpedecr, accrexpeincr, dispfixedassetloss, fixedassescraloss, \
             valuechgloss, defeincoincr, estidebts, finexpe, inveloss, defetaxassetdecr, \
             defetaxliabincr, inveredu, receredu, payaincr, unseparachg, unfiparachg, \
             other, biznetcflow, debtintocapi, expiconvbd, finfixedasset, cashfinalbala, \
             cashopenbala, equfinalbala, equopenbala, cashneti "



class HData_sina_cashflow(object):
    def __init__(self,user,password):
        # self.aaa = aaa
        self.sina_cashflow_table=[]
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
        self.cur.execute("select count(*) from pg_class where relname = 'sina_cashflow_table' ;")
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
            drop table if exists sina_cashflow_table;
            create table sina_cashflow_table(
                record_date    date,
                stock_code    varchar,
                stock_name    varchar,
                laborgetcash    float,
                taxrefd    float,
                receotherbizcash    float,
                bizcashinfl    float,
                labopayc    float,
                payworkcash    float,
                paytax    float,
                payacticash    float,
                bizcashoutf    float,
                mananetr    float,
                withinvgetcash    float,
                inveretugetcash    float,
                fixedassetnetc    float,
                subsnetc    float,
                receinvcash    float,
                invcashinfl    float,
                acquassetcash    float,
                invpayc    float,
                subspaynetcash    float,
                payinvecash    float,
                invcashoutf    float,
                invnetcashflow    float,
                invrececash    float,
                subsrececash    float,
                recefromloan    float,
                issbdrececash    float,
                recefincash    float,
                fincashinfl    float,
                debtpaycash    float,
                diviprofpaycash    float,
                subspaydivid    float,
                finrelacash    float,
                fincashoutf    float,
                finnetcflow    float,
                chgexchgchgs    float,
                cashnetr    float,
                inicashbala    float,
                finalcashbala    float,
                netprofit    float,
                minysharrigh    float,
                unreinveloss    float,
                asseimpa    float,
                assedepr    float,
                intaasseamor    float,
                longdefeexpenamor    float,
                prepexpedecr    float,
                accrexpeincr    float,
                dispfixedassetloss    float,
                fixedassescraloss    float,
                valuechgloss    float,
                defeincoincr    float,
                estidebts    float,
                finexpe    float,
                inveloss    float,
                defetaxassetdecr    float,
                defetaxliabincr    float,
                inveredu    float,
                receredu    float,
                payaincr    float,
                unseparachg    float,
                unfiparachg    float,
                other    float,
                biznetcflow    float,
                debtintocapi    float,
                expiconvbd    float,
                finfixedasset    float,
                cashfinalbala    float,
                cashopenbala    float,
                equfinalbala    float,
                equopenbala    float,
                cashneti      float

            );
            alter table sina_cashflow_table add primary key(stock_code,record_date);
            ''')
        self.conn.commit()
        self.db_disconnect()

        print("db_sina_cashflow_table_create finish")
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
            self.cur.copy_from(buffer, table='sina_cashflow_table', sep=",")
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
        self.cur.execute("select max(record_date) from sina_cashflow_table \
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
                        cashflow_cmd = "insert into sina_cashflow_table ("\
                                + sina_cols + \
                                " ) values "+sql_cmd+";"
                        if debug:
                            print(cashflow_cmd)
                        self.cur.execute(cashflow_cmd)
                        self.conn.commit()
                        sql_cmd = ""

            if debug:
                print(sql_cmd)
            if(sql_cmd != ""):
                cashflow_cmd = "insert into sina_cashflow_table ("\
                        + sina_cols + \
                        " ) values "+sql_cmd+";"
                if debug:
                    print(cashflow_cmd)
                self.cur.execute(cashflow_cmd)
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
                        cashflow_sql = [] 
                        cashflow_sql.append("insert into sina_cashflow_table (")
                        cashflow_sql.append(sina_cols)
                        cashflow_sql.append( " ) values ")
                        cashflow_sql.append(''.join(sql_cmd))
                        cashflow_sql.append( "  on conflict (stock_code, record_date) do nothing ; ")
                        #print(''.join(cashflow_sql))
                        sql_cmd = []
                        if debug:
                            print(cashflow_sql)
                        t2 = time.time()
                        self.cur.execute(''.join(cashflow_sql))
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
                cashflow_sql = []
                cashflow_sql.append("insert into sina_cashflow_table (")
                cashflow_sql.append(sina_cols)
                cashflow_sql.append( " ) values ")
                cashflow_sql.append(''.join(sql_cmd))
                cashflow_sql.append( "  on conflict (stock_code, record_date) do nothing; ")
                #print(''.join(cashflow_sql))
                sql_cmd = []
                self.cur.execute(''.join(cashflow_sql))
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
            data.to_sql(name='sina_cashflow_table', con=self.conn, if_exists = 'replace', index=False)
            pass

        if debug:
            print(time.time()-t0)
            print('insert_all_stock_data_3(\\)')

        self.db_disconnect()

    #fix bug: delete zero when the stock is closed
    def delete_amount_is_zero(self):
        self.db_connect()
        sql_temp="delete from sina_cashflow_table where amount = 0;"
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
        sql_temp += "from sina_cashflow_table"

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

        sql_temp = "delete from sina_cashflow_table"

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
