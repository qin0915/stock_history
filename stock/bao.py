import pandas as pd
from pandas import Series, DataFrame
import numpy as np
import statsmodels.api as sm
import scipy.stats as scs
import matplotlib.pyplot as plt
import baostock as bs


# 登陆系统
lg = bs.login()


#factors = ['B/M','EPS','PEG','ROE','ROA','GP/R','P/R','L/A','FAP','CMV']
factors = ['roeAvg','npMargin','gpMargin','netProfit','epsTTM','MBRevenue','totalShare','liqaShare']

#月初取出因子数值
def get_factors(y,q,factors):

    # 获取沪深300成分股
    dt = str(y)+'-01-01'
    rs = bs.query_hs300_stocks(dt)

    # 打印结果集
    hs300_stocks = []
    while (rs.error_code == '0') & rs.next():
    # 获取一条记录，将记录合并在一起
        hs300_stocks.append(rs.get_row_data()[1])


    profit_list = []
    for stock in hs300_stocks:

        # 查询季频估值指标盈利能力
        rs_profit = bs.query_profit_data(code=stock, year=y, quarter=q)
        if (rs_profit.error_code == '0') & rs_profit.next():
            profit_list.append(rs_profit.get_row_data())

    fdf = pd.DataFrame(data=profit_list, columns=['code','pubDate','statDate','roeAvg','npMargin','gpMargin','netProfit','epsTTM','MBRevenue','totalShare','liqaShare'])
    fdf.index = fdf['code']

    print(fdf.head())

    return fdf.iloc[:,-8:]


def caculate_port_monthly_return(port,startdate,enddate,nextdate,CMV):
    weight = []
    for p in port:
        close1 = 0
        close2 = 0

        rs = bs.query_history_k_data_plus(p,"date,code,close",start_date=startdate,end_date=enddate,frequency="d", adjustflag="2")
        if (rs.error_code == '0') & rs.next():
            close1 =float(rs.get_row_data()[2])
            print(close1)

        rs = bs.query_history_k_data_plus(p,"date,code,close",start_date=enddate,end_date=nextdate,frequency="d", adjustflag="2")
        if (rs.error_code == '0') & rs.next():
            close2 = float(rs.get_row_data()[2])
            print(close2)

        weight.append(close2/close1-1)


    return np.mean(weight)



def caculate_benchmark_monthly_return(startdate,enddate,nextdate):
    close1 = 0
    close2 = 0

    rs = bs.query_history_k_data_plus('sh.000300',"date,code,close",start_date=startdate,end_date=enddate,frequency="d", adjustflag="2")
    if (rs.error_code == '0') & rs.next():
        close1 = float(rs.get_row_data()[2])
    rs = bs.query_history_k_data_plus('sh.000300',"date,code,close",start_date=enddate,end_date=nextdate,frequency="d", adjustflag="2")
    if (rs.error_code == '0') & rs.next():
        close2 =float( rs.get_row_data()[2])

    benchmark_return = close2/close1-1
    return benchmark_return

#factors = ['B/M','EPS','PEG','ROE','ROA','GP/R','P/R','L/A','FAP','CMV']
#因为研究模块取fundmental数据默认date为研究日期的前一天。所以要自备时间序列。按月取
year = [2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017]
quarter = [1,2,3,4]
result = {}

for i in range(0,10):
    for q in quarter:

        if q == 1 :
            startdate = str(year[i]) + '-01'+ '-01'
            enddate = str(year[i]) + '-04'+ '-01'
            nextdate = str(year[i]) + '-07'+ '-01'
        if q == 2 :
            startdate = str(year[i]) + '-04'+ '-01'
            enddate =str(year[i]) + '-07'+ '-01'
            nextdate = str(year[i]) + '-10'+ '-01'
        if q == 3 :
            startdate =str(year[i]) + '-07'+ '-01'
            enddate = str(year[i]) + '-10'+ '-01'
            nextdate = str(year[i+1]) + '-01'+ '-01'
        if q == 4 :
            startdate = str(year[i]) + '-10'+ '-01'
            enddate = str(year[i+1]) + '-01'+ '-01'
            nextdate = str(year[i+1]) + '-04'+ '-01'


        fdf = get_factors(year[i],q,factors)

        CMV = fdf['MBRevenue']
        #5个组合，10个因子
        df = DataFrame(np.zeros(6*8).reshape(6,8),index = ['port1','port2','port3','port4','port5','benchmark'],columns = factors)
        for fac in factors:
            score = fdf[fac].sort_values()
            port1 = list(score.index)[: int(len(score)/5)]
            port2 = list(score.index)[ int(len(score)/5)+1: int(2*len(score)/5)]
            port3 = list(score.index)[ int(2*len(score)/5)+1: int(-2*len(score)/5)]
            port4 = list(score.index)[ int(-2*len(score)/5)+1: int(-len(score)/5)]
            port5 = list(score.index)[ int(-len(score)/5+1): ]

            df.ix['port1',fac] = caculate_port_monthly_return(port1,startdate,enddate,nextdate,CMV)
            df.ix['port2',fac] = caculate_port_monthly_return(port2,startdate,enddate,nextdate,CMV)
            df.ix['port3',fac] = caculate_port_monthly_return(port3,startdate,enddate,nextdate,CMV)
            df.ix['port4',fac] = caculate_port_monthly_return(port4,startdate,enddate,nextdate,CMV)
            df.ix['port5',fac] = caculate_port_monthly_return(port5,startdate,enddate,nextdate,CMV)
            df.ix['benchmark',fac] = caculate_benchmark_monthly_return(startdate,enddate,nextdate)
            print('factor %s'%fac)
        result[i+1]=df

monthly_return = pd.Panel(result)


total_return = {}
annual_return = {}
excess_return = {}
win_prob = {}
loss_prob = {}
effect_test = {}
MinCorr = 0.3
Minbottom = -0.05
Mintop = 0.05

for fac in factors:
    effect_test[fac] = {}
    monthly = monthly_return[:,:,fac]
    print(monthly)

    total_return[fac] = (monthly+1).T.cumprod().iloc[-1,:]-1
    annual_return[fac] = (total_return[fac]+1)**(1./11)-1
    excess_return[fac] = annual_return[fac]- annual_return[fac][-1]
    #判断因子有效性
    #1.年化收益与组合序列的相关性 大于 阀值
    effect_test[fac][1] = annual_return[fac][0:5].corr(Series([1,2,3,4,5],index = annual_return[fac][0:5].index))
    #2.高收益组合跑赢概率
    #因子小，收益小，port1是输家组合，port5是赢家组合
    if total_return[fac][0] < total_return[fac][-2]:
        loss_excess = monthly.iloc[0,:]-monthly.iloc[-1,:]
        loss_prob[fac] = loss_excess[loss_excess<0].count()/float(len(loss_excess))
        win_excess = monthly.iloc[-2,:]-monthly.iloc[-1,:]
        win_prob[fac] = win_excess[win_excess>0].count()/float(len(win_excess))

        effect_test[fac][3] = [win_prob[fac],loss_prob[fac]]

        #超额收益
        effect_test[fac][2] = [excess_return[fac][-2]*100,excess_return[fac][0]*100]

    #因子小，收益大，port1是赢家组合，port5是输家组合
    else:
        loss_excess = monthly.iloc[-2,:]-monthly.iloc[-1,:]
        loss_prob[fac] = loss_excess[loss_excess<0].count()/float(len(loss_excess))
        win_excess = monthly.iloc[0,:]-monthly.iloc[-1,:]
        win_prob[fac] = win_excess[win_excess>0].count()/float(len(win_excess))

        effect_test[fac][3] = [win_prob[fac],loss_prob[fac]]

        #超额收益
        effect_test[fac][2] = [excess_return[fac][0]*100,excess_return[fac][-2]*100]
#effect_test[1]记录因子相关性，>0.5或<-0.5合格
#effect_test[2]记录【赢家组合超额收益，输家组合超额收益】
#effect_test[3]记录赢家组合跑赢概率和输家组合跑输概率。【>0.5,>0.4】合格(因实际情况，跑输概率暂时不考虑)
##DataFrame(effect_test)
##
effective_factors = ['B/M','PEG','P/R','FAP','CMV']
##DataFrame(total_return).ix[:,effective_factors]

def draw_return_picture(df):
    plt.figure(figsize =(10,4))
    plt.plot((df.T+1).cumprod().ix[:,0], label = 'port1')
    plt.plot((df.T+1).cumprod().ix[:,1], label = 'port2')
    plt.plot((df.T+1).cumprod().ix[:,2], label = 'port3')
    plt.plot((df.T+1).cumprod().ix[:,3], label = 'port4')
    plt.plot((df.T+1).cumprod().ix[:,4], label = 'port5')
    plt.plot((df.T+1).cumprod().ix[:,5], label = 'benchmark')
    plt.xlabel('return of factor %s'%fac)
    plt.legend(loc=0)

for fac in effective_factors:
    draw_return_picture(monthly_return[:,:,fac])





# 登出系统
bs.logout()