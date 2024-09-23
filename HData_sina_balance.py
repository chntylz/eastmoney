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
 ['报表日期'                                  'record_date',
 '货币资金'                                  'curfds',
 '交易性金融资产'                            'tradfinasset',
 '衍生金融资产'                              'derifinaasset',
 '应收票据及应收账款'                        'notesaccorece',
 '应收票据'                                  'notesrece',
 '应收账款'                                  'accorece',
 '应收款项融资'                              'recfinanc',
 '预付款项'                                  'prep',
 '其他应收款(合计)'                          'otherrecetot',
 '应收利息'                                  'interece',
 '应收股利'                                  'dividrece',
 '其他应收款'                                'otherrece',
 '买入返售金融资产'                          'purcresaasset',
 '存货'                                      'inve',
 '划分为持有待售的资产'                      'accheldfors',
 '一年内到期的非流动资产'                    'expinoncurrasset',
 '待摊费用'                                  'prepexpe',
 '待处理流动资产损益'                        'unseg',
 '其他流动资产'                              'othercurrasse',
 '流动资产合计'                              'totcurrasset',
 '发放贷款及垫款'                            'lendandloan',
 '可供出售金融资产'                          'avaisellasse',
 '持有至到期投资'                            'holdinvedue',
 '长期应收款'                                'longrece',
 '长期股权投资'                              'equiinve',
 '投资性房地产'                              'inveprop',
 '在建工程(合计)'                            'consprogtot',
 '在建工程'                                  'consprog',
 '工程物资'                                  'engimate',
 '固定资产及清理(合计)'                      'fixedassecleatot',
 '固定资产净额'                              'fixedassenet',
 '固定资产清理'                              'fixedasseclea',
 '生产性生物资产'                            'prodasse',
 '公益性生物资产'                            'comasse',
 '油气资产'                                  'hydrasset',
 '使用 权资产'                               'ruseassets',
 '无形资产'                                  'intaasset',
 '开发支出'                                  'deveexpe',
 '商誉'                                      'goodwill',
 '长期待摊费用'                              'logprepexpe',
 '递延所得税资产'                            'defetaxasset',
 '其他非流动资产'                            'othernoncasse',
 '非流动资产合计'                            'totalnoncassets',
 '资产总计'                                  'totasset',
 '短期借款'                                  'shorttermborr',
 '交易性金融负债'                            'tradfinliab',
 '应付票据及应付账款'                        'notesaccopaya',
 '应付票据'                                  'notespaya',
 '应付账款'                                  'accopaya',
 '预收款项'                                  'advapaym',
 '应付手续费及佣金'                          'copepoun',
 '应付职工薪酬'                              'copeworkersal',
 '应交税费'                                  'taxespaya',
 '其他应付款(合计)'                          'otherpaytot',
 '应付利息'                                  'intepaya',
 '应付股利'                                  'divipaya',
 '其他应付款'                                'otherpay',
 '预提费用'                                  'accrexpe',
 '一年内的递延收益'                          'defereve',
 '应付短期债券'                              'shorttermbdspaya',
 '一年内到期的非流动负债'                    'duenoncliab',
 '其他流动负债'                              'othercurreliabi',
 '流动负债合计'                              'totalcurrliab',
 '长期借款'                                  'longborr',
 '应付债券'                                  'bdspaya',
 '租赁负债'                                  'leaseliab',
 '长期应付职工薪 酬'                         'lcopeworkersal',
 '长期应付款(合计)'                          'longpayatot',
 '长期应付款'                                'longpaya',
 '专项应付款'                                'specpaya',
 '预计非流动负债'                            'expenoncliab',
 '递延所得税负债'                            'defeincotaxliab',
 '长期递延收益'                              'longdefeinco',
 '其他非流 动负债'                           'othernoncliabi',
 '非流动负债合计'                            'totalnoncliab',
 '负债合计'                                  'totliab',
 '实收资本(或股本)'                          'paidincapi',
 '资本公积'                                  'capisurp',
 '减：库存股'                                'treastk',
 '其他综合收益'                              'ocl',
 '专项储备'                                  'specrese',
 '盈余公积'                                  'rese',
 '一般风险准备'                              'generiskrese',
 '未分配利润'                                'undiprof',
 '归属于母公司股东权益合计'                  'paresharrigh',
 '少数股东权益'                              'minysharrigh',
 '所有者权益(或股东权益)合计'                'righaggr',
 '负债和所有者权益(或股东权益)总计'          'totliabsharequi'
 '000158'                                    'stock_code',
 '常山北明']                                 'stock_name'
'''
sina_cols = " record_date, stock_code, stock_name, curfds, tradfinasset, derifinaasset, notesaccorece, \
	notesrece, accorece, recfinanc, prep, otherrecetot, interece, dividrece, \
	otherrece, purcresaasset, inve, accheldfors, expinoncurrasset, prepexpe, \
	unseg, othercurrasse, totcurrasset, lendandloan, avaisellasse, holdinvedue, \
	longrece, equiinve, inveprop, consprogtot, consprog, engimate, fixedassecleatot, \
	fixedassenet, fixedasseclea, prodasse, comasse, hydrasset, ruseassets, intaasset, \
	deveexpe, goodwill, logprepexpe, defetaxasset, othernoncasse, totalnoncassets, \
	totasset, shorttermborr, tradfinliab, notesaccopaya, notespaya, accopaya, \
	advapaym, copepoun, copeworkersal, taxespaya, otherpaytot, intepaya, divipaya, \
	otherpay, accrexpe, defereve, shorttermbdspaya, duenoncliab, othercurreliabi, \
	totalcurrliab, longborr, bdspaya, leaseliab, lcopeworkersal, longpayatot, \
	longpaya, specpaya, expenoncliab, defeincotaxliab, longdefeinco, othernoncliabi, \
	totalnoncliab, totliab, paidincapi, capisurp, treastk, ocl, specrese, rese, \
	generiskrese, undiprof, paresharrigh, minysharrigh, righaggr, totliabsharequi "

class HData_sina_balance(object):
    def __init__(self,user,password):
        # self.aaa = aaa
        self.sina_balance_table=[]
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
        self.cur.execute("select count(*) from pg_class where relname = 'sina_balance_table' ;")
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
            drop table if exists sina_balance_table;
            create table sina_balance_table(
                record_date   date,
                stock_code varchar,
                stock_name varchar,
                curfds   float,
                tradfinasset   float,
                derifinaasset   float,
                notesaccorece   float,
                notesrece   float,
                accorece   float,
                recfinanc   float,
                prep   float,
                otherrecetot   float,
                interece   float,
                dividrece   float,
                otherrece   float,
                purcresaasset   float,
                inve   float,
                accheldfors   float,
                expinoncurrasset   float,
                prepexpe   float,
                unseg   float,
                othercurrasse   float,
                totcurrasset   float,
                lendandloan   float,
                avaisellasse   float,
                holdinvedue   float,
                longrece   float,
                equiinve   float,
                inveprop   float,
                consprogtot   float,
                consprog   float,
                engimate   float,
                fixedassecleatot   float,
                fixedassenet   float,
                fixedasseclea   float,
                prodasse   float,
                comasse   float,
                hydrasset   float,
                ruseassets   float,
                intaasset   float,
                deveexpe   float,
                goodwill   float,
                logprepexpe   float,
                defetaxasset   float,
                othernoncasse   float,
                totalnoncassets   float,
                totasset   float,
                shorttermborr   float,
                tradfinliab   float,
                notesaccopaya   float,
                notespaya   float,
                accopaya   float,
                advapaym   float,
                copepoun   float,
                copeworkersal   float,
                taxespaya   float,
                otherpaytot   float,
                intepaya   float,
                divipaya   float,
                otherpay   float,
                accrexpe   float,
                defereve   float,
                shorttermbdspaya   float,
                duenoncliab   float,
                othercurreliabi   float,
                totalcurrliab   float,
                longborr   float,
                bdspaya   float,
                leaseliab   float,
                lcopeworkersal   float,
                longpayatot   float,
                longpaya   float,
                specpaya   float,
                expenoncliab   float,
                defeincotaxliab   float,
                longdefeinco   float,
                othernoncliabi   float,
                totalnoncliab   float,
                totliab   float,
                paidincapi   float,
                capisurp   float,
                treastk   float,
                ocl   float,
                specrese   float,
                rese   float,
                generiskrese   float,
                undiprof   float,
                paresharrigh   float,
                minysharrigh   float,
                righaggr   float,
                totliabsharequi float

            );
            alter table sina_balance_table add primary key(stock_code,record_date);
            ''')
        self.conn.commit()
        self.db_disconnect()

        print("db_sina_balance_table_create finish")
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
            self.cur.copy_from(buffer, table='sina_balance_table', sep=",")
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
        self.cur.execute("select max(record_date) from sina_balance_table \
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
                        balance_cmd = "insert into sina_balance_table ("\
                                + sina_cols + \
                                " ) values "+sql_cmd+";"
                        if debug:
                            print(balance_cmd)
                        self.cur.execute(balance_cmd)
                        self.conn.commit()
                        sql_cmd = ""

            if debug:
                print(sql_cmd)
            if(sql_cmd != ""):
                balance_cmd = "insert into sina_balance_table ("\
                        + sina_cols + \
                        " ) values "+sql_cmd+";"
                if debug:
                    print(balance_cmd)
                self.cur.execute(balance_cmd)
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
                        balance_sql = [] 
                        balance_sql.append("insert into sina_balance_table (")
                        balance_sql.append(sina_cols)
                        balance_sql.append( " ) values ")
                        balance_sql.append(''.join(sql_cmd))
                        balance_sql.append( "  on conflict (stock_code, record_date) do nothing ; ")
                        #print(''.join(balance_sql))
                        sql_cmd = []
                        if debug:
                            print(balance_sql)
                        t2 = time.time()
                        self.cur.execute(''.join(balance_sql))
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
                balance_sql = []
                balance_sql.append("insert into sina_balance_table (")
                balance_sql.append(sina_cols)
                balance_sql.append( " ) values ")
                balance_sql.append(''.join(sql_cmd))
                balance_sql.append( "  on conflict (stock_code, record_date) do nothing; ")
                #print(''.join(balance_sql))
                sql_cmd = []
                self.cur.execute(''.join(balance_sql))
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
            data.to_sql(name='sina_balance_table', con=self.conn, if_exists = 'replace', index=False)
            pass

        if debug:
            print(time.time()-t0)
            print('insert_all_stock_data_3(\\)')

        self.db_disconnect()

    #fix bug: delete zero when the stock is closed
    def delete_amount_is_zero(self):
        self.db_connect()
        sql_temp="delete from sina_balance_table where amount = 0;"
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
        sql_temp += "from sina_balance_table"

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

        sql_temp = "delete from sina_balance_table"

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
