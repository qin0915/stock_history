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


##选则强势行业
def findstrongStock(fdate):
    dt = datetime.datetime.strptime(fdate,'%Y-%m-%d')
    deltime = datetime.timedelta(days=360)
    startdate = dt-deltime
    startdatestr = startdate.strftime('%Y-%m-%d')


    #沪深300基准MACKD
    _300MacdTemp  = getstockMacd('000300.XSHG',startdatestr,fdate)
    # print(startdatestr+'#'+fdate+'#')
    # print(_300MacdTemp)
    if len(_300MacdTemp)==0:
        return []
    _300Macd = _300MacdTemp[2]
    _300mac = _300Macd[-1]
    _300del = _300Macd[-1]-_300Macd[-2]
    print(str(_300mac)+' #### '+str(_300del))

    #选择强势行业
    ll = get_industries(name='sw_l1', date=fdate)
    inds = []
    for i in ll.index.tolist():
        if len(get_industry_stocks(i)):
            inds.append(i)

    ind_dict={}
    for code in inds :
       macd_tmp = getindustryMacd(code,startdatestr,fdate)
       if len(macd_tmp)==0:
           continue
       DIF = macd_tmp[0]
       DEA = macd_tmp[1]
       inds_MACD = macd_tmp[2]
       print(code+' ###### '+ str(inds_MACD[-2]) +' ###### '+ str(inds_MACD[-1]))
       indsmac = inds_MACD[-1]
       indsdel = inds_MACD[-1]-inds_MACD[-2]
       print(code+' $$$$$$ '+ str(indsmac) +' $$$$$$ '+ str(indsdel))

       if  indsmac>_300mac :
            ind_dict[code]=indsmac
    print(ind_dict)
    industry_ser = pd.Series(ind_dict)
    industry_ser.sort_values(ascending = False, inplace = True)
    ind_code1 = ''
    ind_code2 = ''
    ind_code3 = ''
    if len(industry_ser)>0 :
        ind_code1 = industry_ser.index[0]
    if len(industry_ser)>1 :
        ind_code2 = industry_ser.index[1]
    if len(industry_ser)>2 :
        ind_code3 = industry_ser.index[2]


##    print(industry_ser)
##    max_code = industry_ser.index[0]
##    max_ind_mac = industry_ser.values[0]
##    print(str(max_code)+' #### '+str(max_ind_mac))
     #选择强势行业中的强势股票
    stock_dic = {}
    if ind_code1!='' :
        stock_list = get_industry_stocks(ind_code1, date=None)
        if ind_code2!='':
            stock_list.extend(get_industry_stocks(ind_code2, date=None))
            if ind_code3!='':
                stock_list.extend(get_industry_stocks(ind_code3, date=None))


        stock_list = kcb_filter(stock_list)
        stock_list = cyb_filter(stock_list)
        stock_list = cxg_filter(stock_list,fdate)
##        stock_list = paused_filter(stock_list)
##        stock_list = st_filter(stock_list)
        print(len(stock_list))
        for sk in stock_list:
            macd_tmp = getstockMacd(sk,startdatestr,fdate)
            if len(macd_tmp)==0:
                continue

            DIF = macd_tmp[0]
            DEA = macd_tmp[1]
            stock_MACD = macd_tmp[2]
            if len(stock_MACD)<2:
                continue
            stockmac = stock_MACD[-1]
            stockmac1 = stock_MACD[-2]
            stockmac2 = stock_MACD[-3]

            d1 = stockmac - stockmac1
            d2 = stockmac1 - stockmac2

            stockdel = stock_MACD[-1]-stock_MACD[-2]

            d1 =  stockmac - stockmac1
            d2 =  stockmac1- stockmac2

            #if stockmac < 0 and stockmac>-2 and DIF[-1]>-2 and DIF[-1]<2 and d1>0.5 and d2>0.5 and DIF[-1] > DIF[-3]:
##            if stockmac < 5 and stockmac>-5 and DIF[-1]>-3 and DIF[-1]<3 :
##                print(sk+'##################')
##                print(DIF[-5:])
##                print(DEA[-5:])
##                print(stock_MACD[-5:])
##                stock_dic[sk] = abs(stockmac)+abs(DIF[-1])

            #if stockmac < 2 and stockmac>-2 and DIF[-1]>-6 and DIF[-1]<6  and (DIF[-1] - DIF[-2])> abs(DIF[-2])*1.1 and (DIF[-2] - DIF[-3])>abs(DIF[-3])*1.1:
            if stockmac < 2 and stockmac>-2 and DIF[-1]>-6 and DIF[-1]<6  and (DIF[-1] - DIF[-2])> abs(DIF[-2])*1.1 :
                stock_dic[sk] = abs(stockmac)+abs(DIF[-1])
                ss = sk + ' # ' + str(stockmac) +' # ' + str(DIF[-1])+' # ' + str(DIF[-2])+' # ' + str(DIF[-3])+' # ' + str(DIF[-4])
                print(ss)
                #print_price(sk,fdate)

    stock_ser = pd.Series(stock_dic)
    stock_ser.sort_values(ascending = True, inplace = True)
    #print(stock_ser)
    stock_list = list(stock_ser.index)

    return stock_list


def day2week(df):
    #period_type = '3D'
    period_type = 'W'
     #进行转换，周线的每个变量都等于那一周中最后一个交易日的变量值
    period_stock_data = df.resample(period_type).last()

    # 周线的open等于那一周中第一个交易日的open
    period_stock_data['open'] = df['open'].resample(period_type).first()
    #周线的high等于那一周中的high的最大值
    period_stock_data['high'] = df['high'].resample(period_type).max()
    #周线的low等于那一周中的low的最大值
    period_stock_data['low'] = df['low'].resample(period_type).min()
    #周线的volume和money等于那一周中volume和money各自的和
    period_stock_data['volume'] = df['volume'].resample(period_type).sum()
    period_stock_data = period_stock_data[period_stock_data['close'].notnull()]
    period_stock_data.reset_index(inplace=True)
    return period_stock_data


def getindustryMacd(code,startdate,enddate):

    df = finance.run_query(query(finance.SW1_DAILY_PRICE.date,finance.SW1_DAILY_PRICE.open,finance.SW1_DAILY_PRICE.high,finance.SW1_DAILY_PRICE.low,finance.SW1_DAILY_PRICE.close,finance.SW1_DAILY_PRICE.volume
                                ).filter(finance.SW1_DAILY_PRICE.code==code,finance.SW1_DAILY_PRICE.date >= startdate,finance.SW1_DAILY_PRICE.date <= enddate
                                ))
    df["date"] = pd.to_datetime(df["date"])
    df.set_index('date',inplace=True)

    df_week = day2week(df)
    #print(df_week)
    price = np.array(df_week['close'])
    price = 100*(price-np.min(price))/(np.max(price)-np.min(price))
    #print(price)
    macd_tmp = talib.MACD(price, fastperiod=12, slowperiod=26, signalperiod=9)
    return macd_tmp

def getstockMacd(code,startdate,enddate):
    df = get_price(code,  start_date = startdate, end_date=enddate, frequency='daily', fq='pre', fields=['close','low','high','open','volume'])

    df_week = day2week(df)

    code_price = np.array(df_week['close'])
    code_price = 100*(code_price-np.min(code_price))/(np.max(code_price)-np.min(code_price))

    cc = np.isnan(code_price).sum()
    if cc <10 :
        macd_tmp = talib.MACD(code_price, fastperiod=12, slowperiod=26, signalperiod=9)
        return macd_tmp
    else:
        return []

def print_price(code , fdate):
    df = get_price(code,  100, end_date=fdate, frequency='daily', fq='pre',fields=['close','low','high','open','volume'])
    df_week = day2week(df)
    code_price = np.array(df_week['close'])
    price = np.array(df['close'])

    print(price[-10:])

## 过滤停牌股票
def kcb_filter(security_list):
    new_list = []
    for s in security_list:
        if s[:3] != '688':
            new_list.append(s)
    return new_list

## 过滤停牌股票
def cyb_filter(security_list):
    new_list = []
    for s in security_list:
        if s[0] != '3':
            new_list.append(s)
    return new_list

##过滤上市不满一年的股票
def cxg_filter(security_list,fdate):
    new_list = []
    for s in security_list:
        s_days = get_security_info(s).start_date
        c_time = datetime.datetime.strptime(fdate, "%Y-%m-%d")
        c_days = datetime.datetime.date(c_time)

        if (c_days - s_days).days >365:
            new_list.append(s)
    return new_list

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


buy_stocks = findstrongStock('2021-01-10')
print(buy_stocks)







logout()