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

##取社保持仓股票
def getStockList(fdate):
    q = query(
        valuation.code, valuation.pe_ratio,
        valuation.pb_ratio,valuation.market_cap,
        valuation.circulating_market_cap
        ).filter(
        valuation.pe_ratio < 60,
        valuation.pe_ratio > 5,
        #indicator.inc_net_profit_annual > 0.30,
        valuation.pb_ratio<3,
        valuation.market_cap.between(20,1000)
       )
    fdf = get_fundamentals(q,date = fdate)

    stock_list = fdf['code'].values.tolist()
    print(len(stock_list))

    return stock_list


##选择MACD3D
def findMacd3DStork(fdate,sk_list):
    buy_stock_list2 = []
    buy_stock_list3 = []
    buy_stock_list4 = []
    for code in sk_list:
        close1 = get_price(code,  count = 120, end_date=fdate, frequency='daily', fields=['close'])
        code_price = np.array(close1['close'])
        #print(code_price)

        code_price=code_price[~np.isnan(code_price)]

        li = len(code_price)
        if li <=30 :
            continue

        li = len(code_price)

        a1 =code_price[10]
        a2 =code_price[int(li/2)]
        a3 =code_price[-1]

        if a1>= a2*1.15 and a2>=a3*1.15:
            macd_tmp = talib.MACD(code_price, fastperiod=12, slowperiod=26, signalperiod=9)
            DIF = macd_tmp[0]
            DEA = macd_tmp[1]
            MACD = macd_tmp[2]

            re = calMacd3Dfun(DIF,DEA)

            if re==4 :
                buy_stock_list4.append(code)

            if re==3 :
                buy_stock_list3.append(code)

            if re==2 :
                buy_stock_list2.append(code)

    return buy_stock_list2,buy_stock_list3,buy_stock_list4

##计算MACD三重底
def calMacd3Dfun(dif,dea):

    dif =  dif[~np.isnan(dif)]
    dea =  dea[~np.isnan(dea)]
    c1 = np.min(dif)    #最小值
    c2 = np.where(dif==c1)[0]     #最小值位置
    start = c2[0]

    last = c1
    num = len(dif)
    result_list = []
    result_list.append(c1)
    for i in range(start,num-4):
        slip = dif[i:i+5]
        slip2 = dea[i:i+5]
        if slip[0]>slip[1] and slip[1]>slip[2] and slip[0]>slip[2]*0.95:
            if slip[3]>slip[2] and slip[4]>slip[3] and slip[4]>slip[2]*0.95:
                if slip[2]<0 and slip[2]>last and slip[2]<slip2[2] :
                    #print(slip[2])
                    result_list.append(slip[2])
                    last = slip[2]


    if len(result_list) > 3 :
##        print(dif2)
##        print(result_list)
        return 4

    if len(result_list) == 3 :
##        print(dif2)
##        print(result_list)
        return 3

    if len(result_list) == 2 :
##        if result_list[-1]== dif2[-3]:
##            print(result_list)
##        print(dif2)
##        print(result_list)
        return 2

    else:
        return 0


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


sb_stocks = getStockList('2021-03-08')

#sb_stocks = ['002065.XSHE']
buy_stocks2,buy_stocks3,buy_stocks4 = findMacd3DStork('2021-03-08',sb_stocks)

print(buy_stocks2)
print(buy_stocks3)
print(buy_stocks4)

logout()