import numpy as np
import datetime
from datetime import date
from datetime import timedelta
import jqdatasdk as jq
from jqdatasdk import *
import pandas as pd
import requests
from bs4 import BeautifulSoup

##取基金持仓股票
def getQfiiStocks(year,month):
    month = month
    stock_list = []
    if month==1 or month ==2 or month ==3 or month==4 :
        stock_list = getStockList('qfii',year-1,3)
        return list(stock_list.keys())
    elif month == 5 or month ==6 or month ==7 or month ==8:
        stock_list = getStockList('qfii',year,1)
        return list(stock_list.keys())
    elif month == 9 or month ==10:
        stock_list = getStockList('qfii',year,2)
        return list(stock_list.keys())

    elif month == 11 or month ==12:
        stock_list = getStockList('qfii',year,3)
        return list(stock_list.keys())

##取基金持仓股票
def getJjzcStocks(year,month):
    month = month
    stock_list = []
    if month==1 or month ==2 or month ==3 or month==4 :
        stock_list = getStockList('jjzc',year-1,3)
        return list(stock_list.keys())
    elif month == 5 or month ==6 or month ==7 or month ==8:
        stock_list = getStockList('jjzc',year,1)
        return list(stock_list.keys())
    elif month == 9 or month ==10:
        stock_list = getStockList('jjzc',year,2)
        return list(stock_list.keys())

    elif month == 11 or month ==12:
        stock_list = getStockList('jjzc',year,3)
        return list(stock_list.keys())

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

##获取对应分类的股票,例如'000300.XSHG'
def getStockListByType(t):
    stocks = get_index_stocks(t)
    return stocks



##按条件选股 最小PE 最大PE 年增长率net PB 最小市值cap1 最大市值cpa2 时间
def getStockListByCondition(pe1,pe2,net,pb,cap1,cap2,fdate):
    q = query(
        valuation.code, valuation.pe_ratio,
        valuation.pb_ratio,valuation.market_cap,
        valuation.circulating_market_cap
        ).filter(
        valuation.pe_ratio < pe2,
        valuation.pe_ratio > pe1,
        indicator.inc_net_profit_annual > net,
        valuation.pb_ratio<pb,
        valuation.market_cap.between(cap1,cap2)
       )
    fdf = get_fundamentals(q,date = fdate)

    stock_list = fdf['code'].values.tolist()
    print(len(stock_list))

    return stock_list
