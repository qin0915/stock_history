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

##factors = ['EPS','PE','PB','ROE','CMV','60ZDF','20HSL','20CJL','RJSZ']
###因子选择
###EPS PE PB ROE CMV  60ZDF(近60个交易日涨跌幅)  20HSL(近20个交易日换手率)  20CJL(近20个交易日成交量增加)  RJSZ(最近一期报表 市值/股东数)

def pefilter():

    return

def pbfilter():

    return

def epsfilter():
    return

def roefilter():

    return

def cmvfilter():

    return

def zdffilter(close_list):

    return

def hslfilter():

    return

def cjlfilter():

    return

def rjszfilter():

    return

def select_ticks (check_date):
    security_list = []
    # 获取所有股票
    securities = get_all_securities(['stock'])

    tick_list = list(securities.index)

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

    print ("过滤后剩余：%d"%len(tick_list))

    tdf = get_price(tick_list,  count = 64, end_date=check_date, frequency='daily', fields = ['close','volume', 'high_limit'])
    close_pb =tdf["close"]
    high_limit_pb = tdf["high_limit"]
    volum_pb = tdf["volume"]

    for security in tick_list:
        closes = close_pb.loc[:,security]
        high_limits = high_limit_pb.loc[:,security]
        volums =volum_pb.loc[:,security]

        # 连续4天涨停
        if closes.ix[-1] == high_limits.ix[-1]:
            if closes.ix[-2] == high_limits.ix[-2]:
                if closes.ix[-3] == high_limits.ix[-3]:
                    if closes.ix[-4] == high_limits.ix[-4]:
                        security_list.append (security)
                        zdf = zdffilter(closes)







                    else:
                        continue
                else:
                    continue
            else:
                continue
        else:
            continue

    print(security_list)
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



start_day ='2021-04-09'
stop_day ='2021-04-06'
sday = datetime.datetime.strptime(start_day,"%Y-%m-%d")
eday = datetime.datetime.strptime(stop_day,"%Y-%m-%d")

dstr1 = sday.strftime('%Y-%m-%d')
todayZTList = select_ticks(dstr1)

##
##
##lastStockList = []
##factors = ['EPS','PE','PB','ROE','CMV','60ZDF','20HSL','20CJL','RJSZ']
###因子选择
###EPS PE PB ROE CMV  60ZDF(近60个交易日涨跌幅)  20HSL(近20个交易日换手率)  20CJL(近20个交易日成交量增加)  RJSZ(最近一期报表 市值/股东数)
###result = pd.DataFrame(columns=['code','name','low1','date1','low2','date2','max','date3'])
####line={'code':code,'name':name,'low1':leftLow,'date1':arryDate[lfIdx],'low2':rightLow,'date2':arryDate[rgIdx],'max':mx,'date3':arryDate[mIdx]}
####result = result.append(line,ignore_index=True)
##
##while sday <= eday :
##    dstr1 = sday.strftime('%Y-%m-%d')
##    todayZTList = select_ticks(dstr1)
##    #过滤掉上一天连续涨停的股票
##    for security in lastStockList:
##        todayZTList.remove (security)
##
####    lastStockList = todayZTList
####    #查询10天前的个因子数据
####    searchdate = sday - datetime.timedelta(days=10)
####    searchdatestr =searchdate.strftime('%Y-%m-%d')
####    #分析参数
####    for stock in todayZTList:
####
####        q = query(
####            income.basic_eps,
####            valuation.pe_ratio,
####            valuation.pb_ratio,
####            income.net_profit/balance.total_owner_equities,
####            valuation.circulating_market_cap
####            ).filter(
####            valuation.code == stock
####        )
####
####        fdf = get_fundamentals(q, date=searchdatestr)
####        print(fdf)
##
##        sday += datetime.timedelta(days=1)
##
##
####print(get_all_factors())


logout()