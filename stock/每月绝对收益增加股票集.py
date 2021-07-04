import pandas as pd
from pandas import Series, DataFrame
import numpy as np
import statsmodels.api as sm
import scipy.stats as scs
import matplotlib.pyplot as plt
import jqdatasdk as jq
from jqdatasdk import *
auth('18500964880', 'QINbin9999')


def getMonthReturn(startdate,enddate):
    sel_stock = DataFrame(columns=['code','return'])
    stock_set = get_index_stocks('000300.XSHG',startdate)
    close1 = get_price('000300.XSHG',startdate,enddate,'daily',['close'])

    ret1 = close1['close'].ix[-1]/close1['close'].ix[0] -1
    print('######################################')
    print(ret1)
    print('######################################')

    for stock in stock_set:
        close2 = get_price(stock, startdate, enddate, 'daily', ['close'])
        ll = len(close2['close'])
        if close2['close'].ix[-1] > close2['close'].ix[0]:
            ret = close2['close'].ix[-1]/close2['close'].ix[0] -1
            ret_mid = close2['close'].ix[int(ll/2)]/close2['close'].ix[0] -1
            if ret > ret_mid and ret > ret1 :
                dic = get_industry('000001.XSHE', date="2018-06-01")

                sel_stock = sel_stock.append({'code':stock,'return':ret},ignore_index=True)

    sel_stock = sel_stock.sort_values(by = 'return',axis = 0,ascending = False)
    sel_stock.set_index(["code"], inplace=True)

    return sel_stock

def getIndustry(stocklist,fdate):
    #rel_stock = DataFrame(columns=['code','return','sw_ll','sw_l2','sw_l3','jq_l1','jq_l2','zjw'])
    industry_stock = stocklist
    for index, row in stocklist.iterrows():
        dic = get_industry(index, date=fdate)
        sub_dic = dic[index]
        try:
            industry_stock.loc[index, 'sw_l1'] = sub_dic['sw_l1']['industry_name']
            industry_stock.loc[index, 'sw_l2'] = sub_dic['sw_l2']['industry_name']
            industry_stock.loc[index, 'sw_l3'] = sub_dic['sw_l3']['industry_name']
##            industry_stock.loc[index, 'jq_l1'] = sub_dic['jq_l1']['industry_name']
##            industry_stock.loc[index, 'jq_l2'] = sub_dic['jq_l2']['industry_name']
            industry_stock.loc[index, 'zjw'] = sub_dic['zjw']['industry_name']
            industry_stock.loc[index, 'int'] = 1
        except ValueError as e:
            print('except:', e)


    return industry_stock


plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus'] = False
fig,ax = plt.subplots(3,4,figsize=(20,15))

yy='2020'
mm=['01','02','03','04','05','06','07','08','09','10','11','12']

for i in range(1,13):
    start = yy+'-'+mm[i-1]+'-01'
    end = start
    if i==12 :
        end =  yy+'-12-31'
    else:
        end =  yy+'-'+mm[i]+'-01'
    slist =getMonthReturn(start,end)
    ilist =getIndustry(slist,start)
    ass = ilist['return'].groupby(ilist['sw_l1']).mean()
    rr = int((i-1)/4)
    cc = (i-1)%4
    tt = 'm'+str(i)
    ass.plot(ax=ax[rr,cc],kind='bar',title=tt,rot=45,grid=True)

plt.subplots_adjust(left=0.05, bottom=0.05, right=0.95, top=0.95, wspace=None, hspace=None)


plt.savefig('month.pdf')
plt.legend()
plt.show()


##dic = get_industry('000001.XSHE', date="2018-06-01")

#print(getMonthReturn('2020-11-01','2020-12-01'))
##sub_dic = dic['000001.XSHE']
##print(sub_dic['sw_l1'])
##print(sub_dic['sw_l2'])
##print(sub_dic['sw_l3'])
##print(sub_dic['zjw'])
##print(sub_dic['jq_l2'])
##print(sub_dic['jq_l1'])


logout()