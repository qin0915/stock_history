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


auth('18500964880', 'QINbin9999')

##取低PE股票
def getStockList1(fdate):
    q = query(
        valuation.code,
        valuation.pe_ratio,
        valuation.pb_ratio
        ).filter(
        valuation.pe_ratio < 40,
        valuation.pe_ratio > 5
       ).order_by(
       valuation.pe_ratio
       )

    fdf = get_fundamentals(q,date = fdate)
    stock_list = fdf['code'].values.tolist()
    ll = len(stock_list)
    stock_list = stock_list[0:int(ll*0.4)]
    return stock_list

##取低PB股票
def getStockList2(fdate,stocklist):
    q = query(
        valuation.code,
        valuation.pe_ratio,
        valuation.pb_ratio
        ).filter(
        valuation.pb_ratio<3,
        valuation.pb_ratio>0,
        valuation.code.in_(stocklist)
       ).order_by(
       valuation.pb_ratio
       )

    fdf = get_fundamentals(q,date = fdate)
    stock_list = fdf['code'].values.tolist()
    ll = len(stock_list)
    stock_list = stock_list[0:int(ll*0.4)]
    return stock_list

##取高增长股票
def getStockList3(fdate,stocklist):
    q = query(
        valuation.code,indicator.inc_net_profit_year_on_year,
        indicator.inc_total_revenue_year_on_year,
        valuation.pe_ratio,
        valuation.pb_ratio

        ).filter(
        indicator.inc_total_revenue_year_on_year > 0.10,
        indicator.inc_net_profit_year_on_year > 0.15,
        indicator.inc_net_profit_year_on_year/indicator.inc_total_revenue_year_on_year < 5,
        valuation.code.in_(stocklist)
       ).order_by(
        indicator.inc_net_profit_year_on_year.desc()
       )
    fdf = get_fundamentals(q,date = fdate)
    stock_list = fdf['code'].values.tolist()
    ll = len(stock_list)
    stock_list = stock_list[0:int(ll*0.4)]
    return stock_list

##取高eps
def getStockList4(fdate,stocklist):
    q = query(
        valuation.code,indicator.inc_net_profit_year_on_year,
        indicator.inc_total_revenue_year_on_year,
        valuation.pe_ratio,
        valuation.pb_ratio,
        indicator.eps
        ).filter(
        indicator.eps >0.15 ,
        valuation.code.in_(stocklist)
       ).order_by(
        indicator.eps.desc()
       )
    fdf = get_fundamentals(q,date = fdate)
    print(fdf)
    stock_list = fdf['code'].values.tolist()
    ll = len(stock_list)
    stock_list = stock_list[0:int(ll*0.6)]
    return stock_list

#### 过滤停牌股票
##def paused_filter(security_list):
##    current_data = jq.get_current_data()
##    security_list = [stock for stock in security_list if not current_data[stock].paused]
##    # 返回结果
##    return security_list
##
##
#### 过滤ST股票
##def st_filter(security_list):
##    current_data = jq.get_current_data()
##    security_list = [stock for stock in security_list if not current_data[stock].is_st]
##    # 返回结果
##    return security_list


_stocks1 = getStockList1('2021-03-08')
_stocks2 = getStockList2('2021-03-08',_stocks1)
_stocks3 = getStockList3('2021-03-08',_stocks2)
_stocks4 = getStockList4('2021-03-08',_stocks3)


print(_stocks4)

logout()