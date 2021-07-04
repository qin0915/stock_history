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

##取社保持仓股票
def getSbzcStocks(year,month):
    month = month
    stock_list = []
    if month==1 or month ==2 or month ==3 or month==4 :
        stock_list = getStockList('sbzc',year-1,3)
        return list(stock_list.keys())
    elif month == 5 or month ==6 or month ==7 or month ==8:
        stock_list = getStockList('sbzc',year,1)
        return list(stock_list.keys())
    elif month == 9 or month ==10:
        stock_list = getStockList('sbzc',year,2)
        return list(stock_list.keys())

    elif month == 11 or month ==12:
        stock_list = getStockList('sbzc',year,3)
        return list(stock_list.keys())

def bs4_paraser(html,ye,qr):

    tr_count = 0
    hold_dict = {}
    ####<table class="list_table" id="dataTable">
    soup = BeautifulSoup(html, 'html.parser')
    # 获取数据列表
    dbTable= soup.find('table', attrs={'class': 'list_table'})

    for child in  dbTable.contents:

       if child.name == 'tr' and child.has_attr('class'):
           continue
       if child.name == 'tr' and not child.has_attr('id'):

           tr_count+=1
           td_all = child.find_all('td')
           code = td_all[0].a.string
           bl = float(td_all[5].string)
           add_hold  = float(td_all[6].string)
           all_hlod = float(td_all[4].string)
           add_bl = add_hold/all_hlod
           #选择所有社保持仓的股票列表
           if all_hlod > 0 and add_hold >0:
                if code[0]=='6':
                    hold_dict[code+'.XSHG'] =  bl

                if code[0]=='0':
                    hold_dict[code+'.XSHE'] =  bl

    return hold_dict


def getStockList(ty,y,q):
    ##index.phtml?reportdate=2019&quarter=1&p=2
    stock_list = {}

    #for i in range(1,100):
    for i in range(1,100):
        url = 'http://vip.stock.finance.sina.com.cn/q/go.php/vComStockHold/kind/'+ty+'/index.phtml?reportdate='+str(y)+'&quarter='+str(q)+'&p='+str(i)
        req_obj = requests.get(url)
        hdict  = bs4_paraser(req_obj.text, y, q)
        if len(hdict)>0:
            stock_list.update(hdict)
        else:
            break

    return stock_list

##选择MACD3D
def findMacd3DStork(fdate,sk_list):
    buy_stock_list2 = []
    buy_stock_list3 = []

    for code in sk_list:
        close1 = get_price(code,  count = 120, end_date=fdate, frequency='daily', fields=['close'])
        code_price = np.array(close1['close'])

        #while np.nan in code_price:
        #code_price = code_price.remove(np.nan)

##        print(code_price)
##        code_price = np.r_[code_price,]
##        print(code_price)

        code_price=code_price[~np.isnan(code_price)]

        li = len(code_price)
        if li <=30 :
            continue

        a1 =code_price[10]
        a2 =code_price[int(li/2)]
        a3 =code_price[-1]

        if a1>= a2*1.1 and a2>=a3*1.1:
            macd_tmp = talib.MACD(code_price, fastperiod=12, slowperiod=26, signalperiod=9)
            DIF = macd_tmp[0]
            DEA = macd_tmp[1]
            MACD = macd_tmp[2]

            re = calMacd3Dfun(DIF)

            if re==3 :
                buy_stock_list3.append(code)
            if re==2 :
                buy_stock_list2.append(code)
    return buy_stock_list2,buy_stock_list3

##计算MACD三重底
def calMacd3Dfun(dif):

    dif =  dif[~np.isnan(dif)]
    print(dif)
    last = -100.0
    num = len(dif)
    result_list = []

    for i in range(0,num-4):
        slip = dif[i:i+5]
        if slip[0]>slip[1] and slip[1]>slip[2] and slip[0]>slip[2]*0.95:
            if slip[3]>slip[2] and slip[4]>slip[3] and slip[4]>slip[2]*0.95:
                if slip[2]<0 and slip[2]>last :
                    print(slip[2])
                    result_list.append(slip[2])
                    last = slip[2]

    if len(result_list) >= 3 :
##        if result_list[-1]== dif[-3]:
##            print(result_list)
##            return 3
        return 3
    elif len(result_list)==2 :
##        if result_list[-1]== dif[-3]:
##            print(result_list)
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


sb_stocks = getSbzcStocks(2021,3)

#sb_stocks = ['000813.XSHE']
buy_stocks2,buy_stocks3 = findMacd3DStork('2021-03-08',sb_stocks)

print(buy_stocks2)
print(buy_stocks3)

logout()