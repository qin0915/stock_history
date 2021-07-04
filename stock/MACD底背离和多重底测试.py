#MACD双底

import talib
import numpy as np
from talib import MA_Type
import datetime
from datetime import date
from datetime import timedelta
import jqdatasdk as jq
from jqdatasdk import *
import pandas as pd
import GetStockFunction as GetStock

auth('18500964880', 'QINbin9999')


##选择MACD3D
def findMacd3DStork(fdate,sk_list):
    buy_stock_list2 = []
    buy_stock_list3 = []

    for code in sk_list:
        close1 = get_price(code,  count = 120, end_date=fdate, frequency='daily', fields=['close'])
        code_price = np.array(close1['close'])

        code_price=code_price[~np.isnan(code_price)]

        li = len(code_price)
        if li <=30 :
            continue
        li = li-30

        a0 =np.mean(code_price[0:5])
        a1 =np.mean(code_price[30:35])
        a2 =np.mean(code_price[30+int(li/2)-2:30+int(li/2)+3])
        a3 =np.mean(code_price[-5:])

        if a0>=a1*1.12 and a1>= a2*1.09 and a2>=a3*1.05:
            print(code)
            macd_tmp = talib.MACD(code_price, fastperiod=12, slowperiod=26, signalperiod=9)
            DIF = macd_tmp[0]
            DEA = macd_tmp[1]
            MACD = macd_tmp[2]

            re = calMacd3Dfun(DIF,DEA)

            if re==3 :
                buy_stock_list3.append(code)
            if re==2 :
                buy_stock_list2.append(code)

    return buy_stock_list2,buy_stock_list3

##计算MACD多重底
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
        if slip[0]>(slip[1]+0.1) and slip[1]>(slip[2]+0.1):
            if slip[3]>(slip[2]+0.1) and slip[4]>(slip[3]+0.1):
                if slip[2]>last :
                    #print(slip[2])
                    result_list.append(slip[2])
                    last = slip[2]

    if len(result_list) >= 3 :
        if abs(result_list[-1])<0.3:
            return 3
        return 0
    elif len(result_list)==2 :
        if abs(result_list[-1])<0.3:
            return 2
        return 0
    else:
        return 0




#sb_stocks = GetStock.getStockListByType('000001.XSHG')

sb_stocks = GetStock.getJjzcStocks(2020,3)

#sb_stocks = ['000813.XSHE']
buy_stocks2,buy_stocks3 = findMacd3DStork('2021-03-12',sb_stocks)

print(buy_stocks2)
print(buy_stocks3)

logout()