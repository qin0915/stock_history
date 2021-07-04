import math
import talib
import numpy as np
##from jqdata import *
from talib import MA_Type
from functools import reduce
##from jqlib.technical_analysis import *
##from jqlib.alpha101 import *
import datetime
import jqdatasdk as jq
from jqdatasdk import *
import requests
from bs4 import BeautifulSoup


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

           if add_hold > 0 and bl>=1.0 and add_bl>0.11 and code[0:3]!='688' :
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


def get_factors(fdate,bylist):
    q = query(
        valuation.code,indicator.inc_net_profit_year_on_year,
        indicator.inc_total_revenue_year_on_year,
        valuation.pe_ratio,
        valuation.pb_ratio,
        indicator.eps
        ).filter(
        valuation.market_cap>600,
        valuation.code.in_(bylist)
        ).order_by(
        valuation.market_cap
       )
    fdf = get_fundamentals(q,date = fdate)
    stock_list = fdf['code'].values.tolist()
    return stock_list[0:10]


auth('18500964880', 'QINbin9999')

check_date = '2021-05-19'
stockList = getStockList('qfii',2021,1)
security_list = get_factors(check_date,stockList)
print (security_list)



logout()