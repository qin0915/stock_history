import talib
import numpy as np
#from jqdata import *
from talib import MA_Type
from functools import reduce
##from jqlib.technical_analysis import *
##from jqlib.alpha101 import *
import datetime
from datetime import date
from datetime import timedelta
import jqdatasdk as jq
from jqdatasdk import *
import pandas as pd
import requests
from bs4 import BeautifulSoup


auth('18500964880', 'QINbin9999')


def select_ticks (check_date):
    #找出近一个月强势行业
    indus_list = findStrongIndustry(check_date)

    security_list = []
    # 获取所有股票
    securities = get_all_securities(['stock'])

    #tick_list = list(securities.index)

    q = query(
         valuation.code, valuation.pe_ratio,
         valuation.pb_ratio,valuation.market_cap,
         valuation.circulating_market_cap
         ).filter(
         valuation.pe_ratio > 0,
         valuation.pe_ratio < 41,
         valuation.pb_ratio < 3
         )
    fdf = get_fundamentals(q,date = check_date)

    tick_list = fdf['code'].values.tolist()

    print ("获取到所有标：%d"%len(tick_list))

    #tick_list = ['000708.XSHE']

    filter_list = []
    for security in tick_list:
        # 次新股不考虑

        s_days = datetime.datetime.strptime(str(securities.loc[security]['start_date']),"%Y-%m-%d %H:%M:%S")
        c_days = datetime.datetime.strptime(str(check_date), "%Y-%m-%d")
        if (c_days - s_days).days < 200:
            filter_list.append(security)

        # ST股票不考虑
        if securities.loc[security]['display_name'].find('ST') != -1:
            if security not in filter_list :
                filter_list.append(security)

        #创业板科创版不考虑
        if security[0:3] == '300' or security[0:3] == '688':
            if security not in filter_list :
                filter_list.append(security)


    for security in filter_list:
        tick_list.remove (security)


    filter_list = []
    for security in tick_list:
            #不在行业列表中的删除
        dict_indus = get_industry(security, date=None)
        if "sw_l1" in  str(dict_indus) :
            indus_code = dict_indus[security]['sw_l1']['industry_code']
            #不在行业白名单中的去除
            if indus_code not in indus_list:
                if security not in filter_list :
                    filter_list.append(security)
        else:
            if security not in filter_list :
                filter_list.append(security)
    for security in filter_list:
        tick_list.remove (security)


    print ("过滤后剩余：%d"%len(tick_list))

    for security in tick_list:

        tdf = get_price(security,  count = 20, end_date=check_date, frequency='daily', fields = ['open', 'close','high', 'volume', 'high_limit', 'avg', 'pre_close', 'paused'])

        #print('########'+str(tdf['close'].ix[-1])+str(tdf['close'].ix[-2])+str(tdf['close'].ix[-3]))

        #60天内涨幅超过50% 不买入
        if tdf['close'].ix[0]*1.36 < tdf['close'].ix[-2]:
            #print('###60###'+str(tdf['close'].ix[0])+str(tdf['close'].ix[0]))
            continue

        ##价格过高去除
        if tdf['close'].ix[-1] >=500:
            continue

        #前一天涨幅不超过5%
        if tdf['close'].ix[-2] > tdf['close'].ix[-3]*1.05:
            #print('###last###'+str(tdf['close'].ix[-2])+str(tdf['close'].ix[-3]))
            continue

        # 当天没有涨停不考虑
        if tdf['close'].ix[-1] < tdf['close'].ix[-2]*1.098:
            #print('###curr###'+str(tdf['close'].ix[-1])+str(tdf['high_limit'].ix[-1]))
            continue

        security_list.append (security)

    return security_list

 ##选则强势行业
def findStrongIndustry(fdate):
    dt = datetime.datetime.strptime(fdate,'%Y-%m-%d')
    deltime = datetime.timedelta(days=20)
    startdate = dt-deltime
    startdatestr = startdate.strftime('%Y-%m-%d')

    #选择强势行业
    ll = get_industries(name='sw_l1', date=fdate)
    inds = []
    for i in ll.index.tolist():
       if len(get_industry_stocks(i)):
            inds.append(i)

    ind_dict={}
    for code in inds :
        df = finance.run_query(query(finance.SW1_DAILY_PRICE.date,finance.SW1_DAILY_PRICE.open,finance.SW1_DAILY_PRICE.high,finance.SW1_DAILY_PRICE.low,finance.SW1_DAILY_PRICE.close,finance.SW1_DAILY_PRICE.volume
                                ).filter(finance.SW1_DAILY_PRICE.code==code,finance.SW1_DAILY_PRICE.date >= startdate,finance.SW1_DAILY_PRICE.date <= fdate
                                ))
        df["date"] = pd.to_datetime(df["date"])
        df.set_index('date',inplace=True)
        price = np.array(df['close'])
        indsmac = price[-1] / price[0]
        ind_dict[code]=indsmac

    #print(ind_dict)
    industry_ser = pd.Series(ind_dict)
    industry_ser.sort_values(ascending = False, inplace = True)
    #print(industry_ser)
    industry_list = list(industry_ser.index)

    return industry_list[0:3]



start_day ='2021-01-01'
stop_day ='2021-04-06'
sday = datetime.datetime.strptime(start_day,"%Y-%m-%d")
eday = datetime.datetime.strptime(stop_day,"%Y-%m-%d")

count = 0
zcount1 = 0
zcount2 = 0
dcount = 0

while sday <= eday :
    delta = datetime.timedelta(days=14)
    n_date = sday + delta
    dstr1 = sday.strftime('%Y-%m-%d')
    dstr2 = n_date.strftime('%Y-%m-%d')
    lastdayZT = select_ticks(dstr1)
    print('______________________________________')
    print(dstr1)
    for stock in lastdayZT :
        count = count+1
        tdf = get_price(stock,  start_date =dstr1 , end_date=dstr2, frequency='daily', fields = ['open', 'close','high','low'])
        bl = tdf['close'].ix[-1] / tdf['close'].ix[0]
        if  bl >= 1.06 :
            zcount1 += 1
        elif bl>1 :
            zcount2 += 1
        elif bl <=  1 :
            dcount += 1
        print(stock)
        print("############ %d %d %d"%(zcount1,zcount2,dcount))
    sday += datetime.timedelta(days=10)

print(zcount1/count)
print(zcount2/count)
print(dcount/count)


logout()