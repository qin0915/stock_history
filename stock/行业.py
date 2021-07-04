import talib
import numpy as np
##from jqdata import *
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

# 均线
def get_ma (close, timeperiod=5):
    return talib.SMA(close, timeperiod)

# 获取均值
def get_avg_price (close, day:int):
    return get_ma (close, day)[-1]

# 获取波动的百分比的标准差
def get_std_percentage(var_list):
    v_array = np.array(var_list)
    #print(v_array)

    # 获取每个单元的涨跌幅百分比
    ratio = (v_array - np.median(v_array))/np.median(v_array)

    # 每个单元的平方，消除负号
    ratio_p2 = ratio * ratio

    # 得到平均涨跌幅的平方
    ratio = ratio_p2.sum()/len(v_array)

    # 开方 得到平均涨跌幅
    return np.sqrt(ratio)

# 均线多头排列
def get_avg_array (avg_list):
    ratio = 0;
    count = len(avg_list)
    print(avg_list)

    for i in range(1, count):
        if avg_list[-i-1] < avg_list[-i]:
            return 0
        ratio += (avg_list[-i-1] - avg_list[-i]) / avg_list[-i]

    return ratio

auth('18500964880', 'QINbin9999')

##check_date = '2021-01-08'
##close1 = get_price('002891.XSHE','2020-03-01','2020-12-30','daily',['close'])['close']
##
### 获取均线矩阵， 不要5日线
##avg_list = np.array([get_ma(close1, 10), get_ma(close1, 20),
##                     get_ma(close1, 30), get_ma(close1, 60)])
##
##print(avg_list.T[-2])
##
##box_ratio = 0
##for i in range(2, 32): # 不考虑当日的影响
##    box_ratio += get_std_percentage(avg_list.T[-i])
##
##print (" box " + str(box_ratio))
##
##array_ratio = 0
##for i in range (1, 6):
##    array_ratio += get_avg_array (avg_list.T[-i])
##print (" array " + str(array_ratio))
##
####avg_score[code] = array_ratio
####score[code] = box_ratio - array_ratio


#start_date

## 过滤停牌股票
def paused_filter(security_list):
    current_data = get_current_data()
    security_list = [stock for stock in security_list if not current_data[stock].paused]
    # 返回结果
    return security_list

## 过滤退市股票
def delisted_filter(context, security_list):
    if g.filter_delisted:
        current_data = get_current_data()
        security_list = [stock for stock in security_list if not (('退' in current_data[stock].name) or ('*' in current_data[stock].name))]
    # 返回结果
    return security_list


## 过滤ST股票
def st_filter(security_list):
    current_data = get_current_data()
    security_list = [stock for stock in security_list if not current_data[stock].is_st]
    # 返回结果
    return security_list

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
        #   value={}
        #   value['代码'] = td_all[0].a.string
        #   value['名称'] = td_all[1].a.string
        #   value['截至日期'] = td_all[2].string
        #   value['家数'] = td_all[3].string
        #   value['持股数'] = td_all[4].string
        #   value['流通占比'] = td_all[5].string
        #   value['增减'] = td_all[6].string
        #   value['持股比例'] = td_all[7].string
        #   value['上期家数'] = td_all[8].string

           code = td_all[0].a.string
           bl = float(td_all[5].string)
           add_hold  = float(td_all[6].string)
           all_hlod = float(td_all[4].string)
           add_bl = add_hold/all_hlod

           #if add_hold > 0 and bl>=g.cgbl and add_bl>=0.1:
           if add_hold > 0 :
                if code[0]=='6':
                    hold_dict[code+'.XSHG'] =  add_bl

                if code[0]=='0':
                    hold_dict[code+'.XSHE'] =  add_bl

    return hold_dict


def getStockList(ty,y,q):
    ##index.phtml?reportdate=2019&quarter=1&p=2
    stock_list = {}

    for i in range(1,100):
        url = 'http://vip.stock.finance.sina.com.cn/q/go.php/vComStockHold/kind/'+ty+'/index.phtml?reportdate='+str(y)+'&quarter='+str(q)+'&p='+str(i)
        req_obj = requests.get(url)
        hdict  = bs4_paraser(req_obj.text, y, q)
        if len(hdict)>0:
            stock_list.update(hdict)
        else:
            break

    return stock_list


def day2week(df):
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
    #print(price)
    macd_tmp = talib.MACD(price, fastperiod=12, slowperiod=26, signalperiod=9)
    return macd_tmp

def getstockMacd(code,startdate,enddate):
    df = get_price(code,  start_date = startdate, end_date=enddate, frequency='daily', fields=['close','low','high','open','volume'])
    df_week = day2week(df)

    code_price = np.array(df_week['close'])
    macd_tmp = talib.MACD(code_price, fastperiod=12, slowperiod=26, signalperiod=20)
    return macd_tmp



##
##get_industries(name='zjw')
ll = get_industries(name='sw_l1', date='2016-01-01')
#print(ll)

inds = []
for i in ll.index.tolist():
    if len(get_industry_stocks(i)):
        inds.append(i)
        #print(i+':'+str(len(get_industry_stocks(i))))
cudate = date.fromisoformat('2021-01-16')
deltime = timedelta(days=360)
startdate = cudate-deltime


_300MacdTemp  = getstockMacd('000300.XSHG',startdate.isoformat(),cudate.isoformat())
_300Macd = _300MacdTemp[2]
print(_300Macd)

inds=['801010','801050']

buyseries = {}

for code in inds :

    macd_tmp = getindustryMacd(code,startdate.isoformat(),cudate.isoformat())


    DIF = macd_tmp[0]
    DEA = macd_tmp[1]
    MACD = macd_tmp[2]
    print(MACD)

    # 判断MACD走向
##    if MACD[-1] > 0 and MACD[-4] < 0:
##        buyseries[code] = MACD[-1]

##ser = pd.Series(buyseries)
##print(ser)
##ser.sort_values(ascending = False, inplace = True)
##print(ser)

stock_list = get_industry_stocks('801010', date=None)

stock_list=['000541.XSHE','600167.XSHG']

for sk in stock_list:
    macd_tmp = getstockMacd(sk,startdate.isoformat(),cudate.isoformat())
    DIF = macd_tmp[0]
    DEA = macd_tmp[1]
    MACD = macd_tmp[2]
    print(MACD)








logout()