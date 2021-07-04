import pandas as pd
from pandas import Series, DataFrame
import numpy as np
import statsmodels.api as sm
import scipy.stats as scs
import matplotlib.pyplot as plt
import requests
from bs4 import BeautifulSoup


import jqdatasdk as jq

from jqdatasdk import *
auth('18500964880', 'QINbin9999')

jjteyp = 'jjzc'

factors = ['B/M','EPS','PEG','ROE','ROA','GP/R','P/R','L/A','FAP','CMV']
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
           bl = td_all[5].string
           add  = float(td_all[6].string)
           hold = float(td_all[4].string)
           level = 0
           if hold == add :
                level = 1
           elif add > 0 :
##                tmp = add/(hold-add)
##                if tmp > 1:
##                    level = 3
##                elif tmp > 0.5:
##                    level = 2
##                elif tmp >0.1:
##                    level = 1
                level = 1
           elif add<0 :
##                tmp = add/(hold-add)
##                if tmp < -1:
##                    level = -3
##                elif tmp < -0.5:
##                    level = -2
##                elif tmp < -0.1:
##                    level = -1
                level = -1

           if code[0]=='6':
                    hold_dict[code+'.XSHG'] = float(level)


           if code[0]=='0':
                    hold_dict[code+'.XSHE'] = float(level)

    return hold_dict

def getStockList(y,q):
    ##index.phtml?reportdate=2019&quarter=1&p=2
    stock_list = {}

    #for i in range(1,100):
    for i in range(1,3):
        url = 'http://vip.stock.finance.sina.com.cn/q/go.php/vComStockHold/kind/'+jjteyp+'/index.phtml?reportdate='+str(y)+'&quarter='+str(q)+'&p='+str(i)
        req_obj = requests.get(url)
        hdict  = bs4_paraser(req_obj.text, y, q)
        if len(hdict)>0:
            stock_list.update(hdict)
        else:
            break

    return stock_list


#月初取出因子数值
def get_factors(fdate,factors,stock_set):

    q = query(
        valuation.code,
        balance.total_owner_equities/valuation.market_cap/100000000,
        income.basic_eps,
        valuation.pe_ratio,
        income.net_profit/balance.total_owner_equities,
        income.net_profit/balance.total_assets,
        income.total_profit/income.operating_revenue,
        income.net_profit/income.operating_revenue,
        balance.total_liability/balance.total_assets,
        balance.fixed_assets/balance.total_assets,
        valuation.circulating_market_cap
        ).filter(
        valuation.code.in_(stock_set),
        valuation.circulating_market_cap
    )
    fdf = get_fundamentals(q, date=fdate)
    fdf.index = fdf['code']
    fdf.columns = ['code'] + factors
    return fdf


def caculate_port_monthly_return(port,startdate,enddate,nextdate,CMV):

    close1 = get_price(port, startdate, enddate, 'daily', ['close'])
    close2 = get_price(port, enddate, nextdate, 'daily',['close'])
    weighted_m_return = ((close2['close'].ix[0,:]/close1['close'].ix[0,:]-1)*CMV).sum()/(CMV.ix[port].sum())
    return weighted_m_return

def caculate_benchmark_monthly_return(startdate,enddate,nextdate):

    close1 = get_price(['000001.XSHG'],startdate,enddate,'daily',['close'])['close']
    close2 = get_price(['000001.XSHG'],enddate, nextdate, 'daily',['close'])['close']
    benchmark_return = (close2.ix[0,:]/close1.ix[0,:]-1).sum()
    return benchmark_return


def getStockData():
    #year = [2015,2016,2017,2018,2019,2020]
    year = [2017,2018,2019,2020]
    qrt = [1,2,3,4]  #季度
    result = {}
    last_stock = []
    idx = 0
    for i in range(0,len(year)):
        for q in qrt:
            curdate = ''
            if q == 1:
                curdate = str(year[i])+'-04-01'
            elif q == 2:
                curdate = str(year[i])+'-07-01'
            elif q == 3:
                curdate = str(year[i])+'-10-01'
            elif q == 4:
                if i==len(year)-1:
                   break
                else:
                   curdate = str(year[i+1])+'-01-01'

            stock_list = getStockList(year[i],q)

            stock_code = list(stock_list.keys())
            if len(last_stock)==0 :
                last_stock = stock_code
            else:
                tmp = [x for x in last_stock if x in stock_code]
                last_stock =tmp

            stock_info = get_factors(curdate,factors,stock_code)

##            stock_close = df = get_price(stock_code, count = 5, end_date=curdate, frequency='daily', fields=['close'])

##            jj_holdfee = list(stock_list.values())*stock_close['close'].ix[-1,:]

##            stock_info['jj']=list(stock_list.values())
##
##            stock_info['close']= stock_close['close'].ix[-1,:]

            stock_info['hold']= list(stock_list.values())

            result[idx] = stock_info

            idx += 1

##            print(stock_info)

    return result,last_stock



rs, stock_last = getStockData()

##print(rs['2020q1'])
##print(stock_last)
for (key,value) in rs.items():
    new_df=value.loc[value['code'].isin(stock_last)]
    rs[key]= new_df

print(rs[0])
##print(rs['2020q2'])
##print(rs['2020q3'])

stock_3d = pd.Panel(rs)

ncolumns  = factors + ['hold']
sum_corr = DataFrame(np.zeros(11*11).reshape(11,11),index = ncolumns,columns = ncolumns)

for stock in stock_last:
   monthly = stock_3d[:,stock,:]
   monthly=monthly.T

   monthly = monthly.iloc[:,-11:]

   monthly2=monthly.astype(float)

   yz_corr = monthly2.corr()

   sum_corr = sum_corr+yz_corr

   print(sum_corr)


mean_corr = sum_corr/len(stock_last)
print(mean_corr)


#factors = ['B/M','EPS','PEG','ROE','ROA','GP/R','P/R','L/A','FAP','CMV']
#因为研究模块取fundmental数据默认date为研究日期的前一天。所以要自备时间序列。按月取
##year = ['2009','2010','2011','2012','2013','2014','2015']
##month = ['01','02','03','04','05','06','07','08','09','10','11','12']
##result = {}
##
##for i in range(7*12):
##    startdate = year[i/12] + '-' + month[i%12] + '-01'
##    try:
##        enddate = year[(i+1)/12] + '-' + month[(i+1)%12] + '-01'
##    except IndexError:
##        enddate = '2016-01-01'
##    try:
##        nextdate = year[(i+2)/12] + '-' + month[(i+2)%12] + '-01'
##    except IndexError:
##        if enddate == '2016-01-01':
##            nextdate = '2016-02-01'
##        else:
##            nextdate = '2016-01-01'
##    print('time %s'%startdate)
##    fdf = get_factors(startdate,factors)
##    CMV = fdf['CMV']
##    #5个组合，10个因子
##    df = DataFrame(np.zeros(6*10).reshape(6,10),index = ['port1','port2','port3','port4','port5','benchmark'],columns = factors)
##    for fac in factors:
##        score = fdf[fac].sort_values()
##        port1 = list(score.index)[: len(score)/5]
##        port2 = list(score.index)[ len(score)/5+1: 2*len(score)/5]
##        port3 = list(score.index)[ 2*len(score)/5+1: -2*len(score)/5]
##        port4 = list(score.index)[ -2*len(score)/5+1: -len(score)/5]
##        port5 = list(score.index)[ -len(score)/5+1: ]
##        df.ix['port1',fac] = caculate_port_monthly_return(port1,startdate,enddate,nextdate,CMV)
##        df.ix['port2',fac] = caculate_port_monthly_return(port2,startdate,enddate,nextdate,CMV)
##        df.ix['port3',fac] = caculate_port_monthly_return(port3,startdate,enddate,nextdate,CMV)
##        df.ix['port4',fac] = caculate_port_monthly_return(port4,startdate,enddate,nextdate,CMV)
##        df.ix['port5',fac] = caculate_port_monthly_return(port5,startdate,enddate,nextdate,CMV)
##        df.ix['benchmark',fac] = caculate_benchmark_monthly_return(startdate,enddate,nextdate)
##        print('factor %s'%fac)
##    result[i+1]=df
##
##monthly_return = pd.Panel(result)
##
##
##total_return = {}
##annual_return = {}
##excess_return = {}
##win_prob = {}
##loss_prob = {}
##effect_test = {}
##MinCorr = 0.3
##Minbottom = -0.05
##Mintop = 0.05
##for fac in factors:
##    effect_test[fac] = {}
##    monthly = monthly_return[:,:,fac]
##    total_return[fac] = (monthly+1).T.cumprod().iloc[-1,:]-1
##    annual_return[fac] = (total_return[fac]+1)**(1./6)-1
##    excess_return[fac] = annual_return[fac]- annual_return[fac][-1]
##    #判断因子有效性
##    #1.年化收益与组合序列的相关性 大于 阀值
##    effect_test[fac][1] = annual_return[fac][0:5].corr(Series([1,2,3,4,5],index = annual_return[fac][0:5].index))
##    #2.高收益组合跑赢概率
##    #因子小，收益小，port1是输家组合，port5是赢家组合
##    if total_return[fac][0] < total_return[fac][-2]:
##        loss_excess = monthly.iloc[0,:]-monthly.iloc[-1,:]
##        loss_prob[fac] = loss_excess[loss_excess<0].count()/float(len(loss_excess))
##        win_excess = monthly.iloc[-2,:]-monthly.iloc[-1,:]
##        win_prob[fac] = win_excess[win_excess>0].count()/float(len(win_excess))
##
##        effect_test[fac][3] = [win_prob[fac],loss_prob[fac]]
##
##        #超额收益
##        effect_test[fac][2] = [excess_return[fac][-2]*100,excess_return[fac][0]*100]
##
##    #因子小，收益大，port1是赢家组合，port5是输家组合
##    else:
##        loss_excess = monthly.iloc[-2,:]-monthly.iloc[-1,:]
##        loss_prob[fac] = loss_excess[loss_excess<0].count()/float(len(loss_excess))
##        win_excess = monthly.iloc[0,:]-monthly.iloc[-1,:]
##        win_prob[fac] = win_excess[win_excess>0].count()/float(len(win_excess))
##
##        effect_test[fac][3] = [win_prob[fac],loss_prob[fac]]
##
##        #超额收益
##        effect_test[fac][2] = [excess_return[fac][0]*100,excess_return[fac][-2]*100]
#effect_test[1]记录因子相关性，>0.5或<-0.5合格
#effect_test[2]记录【赢家组合超额收益，输家组合超额收益】
#effect_test[3]记录赢家组合跑赢概率和输家组合跑输概率。【>0.5,>0.4】合格(因实际情况，跑输概率暂时不考虑)
##DataFrame(effect_test)




logout()