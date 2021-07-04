# 克隆自聚宽文章：https://www.joinquant.com/post/32130
# 标题：韶华研究之二，顺势而为，年化140
# 作者：韶华聚鑫

# 导入函数库

import jqdatasdk as jq
from jqdatasdk import *
from six import BytesIO
from jqdatasdk.technical_analysis import *
import pandas as pd
import os
import datetime
from datetime import date

auth('18500964880', 'QINbin9999')

benchmark = '000300.XSHG'
indus_list = ['801010','801080','801120','801140','801150','801210','801710','801750','801760','801780','801790','801880']
buylist=[]
selllist=[]



## 收盘后运行函数,判断买卖信号
def after_close_brain(fdatetime):
    ##0，预设阶段
    #得到今天的日期和数据
    t_time=datetime.datetime.strptime(fdatetime,'%Y-%m-%d')
    today_date =t_time.date()
    print(today_date)
    lastd_date = get_trade_days(end_date=today_date, count=2)[0]
    #all_data = get_current_data()

    ##1，收集bench指数的股票列表，三除，按市值排序
    stocklist = get_index_stocks(benchmark)
##    stocklist = [stockcode for stockcode in stocklist if not all_data[stockcode].paused]
##    stocklist = [stockcode for stockcode in stocklist if not all_data[stockcode].is_st]
##    stocklist = [stockcode for stockcode in stocklist if'退' not in all_data[stockcode].name]

    df_bench_price = get_price(benchmark, count = 2, end_date=today_date, frequency='daily', fields=['close'])
    rise_bench_today = (df_bench_price['close'].values[-1] - df_bench_price['close'].values[-2])/df_bench_price['close'].values[-2]

    ##2，循环遍历指数列表，去除百日次新，通用条件过滤，行业条件过滤后形成买入信号，直接记录到brain和log中

    for stockcode in stocklist:
        if (today_date - get_security_info(stockcode).start_date).days <= 100:
            continue
        stock_name = get_security_info(stockcode).display_name
        dict_indus = get_industry(stockcode, date=None)
        indus_code = dict_indus[stockcode]['sw_l1']['industry_code']
        #不在行业白名单中的去除
        if indus_code not in indus_list:
            continue
        #在舱内的去除
##        if stockcode in context.portfolio.positions:
##            continue
        CCI_today = CCI(stockcode, today_date, N=14, unit = '1d', include_now = True)
        CCI_lastd = CCI(stockcode, lastd_date, N=14, unit = '1d', include_now = True)
        cci_value = CCI_today[stockcode]
        if CCI_today[stockcode] >= -100 or CCI_lastd[stockcode] <= -100:    #去除CCI非下探-100的形态
            continue

        df_price = get_price(stockcode,count=100,end_date=today_date, frequency='daily', fields=['high','low','close']) #先老后新

        q = query(
        valuation.code,
        valuation.pe_ratio,
        valuation.pb_ratio,
        valuation.circulating_market_cap
        ).filter(
        valuation.code == stockcode
        )

        df_value = get_fundamentals(q,date = today_date)
##        df_value = get_valuation(stockcode, count=1, end_date=today_date, fields=['circulating_market_cap','pe_ratio','pb_ratio','turnover_ratio'])#先新后老
        price_T = df_price['close'].values[-1]
        rise_100 = price_T/df_price['close'].values[0]
        volatility_100 = df_price['high'].values.max()/df_price['low'].values.min()
        cir_m = df_value['circulating_market_cap'].values[0]
        pe_ratio = df_value['pe_ratio'].values[0]
        pb_ratio = df_value['pb_ratio'].values[0]

        #通用过滤条件--个股基本面
        if price_T >=300:   #价格过高去除
            continue
        if pe_ratio <0 or pb_ratio<1:   #亏损和破净的去除
            continue

        #通用过滤条件--个股形态面
        if rise_100 <0.73:
            continue

        df_valutaion=finance.run_query(query(finance.SW1_DAILY_PRICE.date,finance.SW1_DAILY_PRICE.code,finance.SW1_DAILY_PRICE.high,finance.SW1_DAILY_PRICE.low
        ).filter(finance.SW1_DAILY_PRICE.code==indus_code,finance.SW1_DAILY_PRICE.date <= today_date).order_by(finance.SW1_DAILY_PRICE.date.desc()).limit(100))

        volat_indus_100 = df_valutaion['high'].values.max()/df_valutaion['low'].values.min()
        volat_StockvsIndus = volatility_100/volat_indus_100
        if volat_indus_100 <1.2 or volat_StockvsIndus <1.1: #行业波澜不兴，和不突出于行业均值的去除
            continue

        CYF_code = CYF(stockcode, check_date=today_date, N = 10, unit = '1d', include_now = True)
        popularity = CYF_code[stockcode]
        #行业过滤条件
        if indus_code == '801010':
            if volat_StockvsIndus <1.3:
                continue
            if popularity <50 or popularity >82:
                continue
        elif indus_code == '801080':
            if volat_StockvsIndus <1.42:
                continue
            if popularity <50 or popularity >82:
                continue
        elif indus_code == '801120':
            if volatility_100 <1.99:
                continue
        elif indus_code == '801140':
            if volat_StockvsIndus <1.23:
                continue
            if popularity <50 or popularity >82:
                continue
        elif indus_code == '801150':
            if volat_StockvsIndus <1.28 or volat_StockvsIndus >1.6:
                continue
            if popularity <50 or popularity >82:
                continue
        elif indus_code == '801210':
            if volatility_100 <1.53:
                continue
        elif indus_code == '801710':
            if volat_StockvsIndus <1.2:
                continue
            if popularity <40 or popularity >70:
                continue
        elif indus_code == '801750':
            if volatility_100 <1.68 or volatility_100 >2:
                continue
            if popularity <58 or popularity >86:
                continue
        elif indus_code == '801760':
            if volat_StockvsIndus <1.27:
                continue
            if popularity <40 or popularity >70:
                continue
        elif indus_code == '801780':
            if volatility_100 <1.5 or volatility_100 >1.8:
                continue
            if popularity >93:
                continue
        elif indus_code == '801790':
            if volatility_100 <1.88 or volatility_100 >2.25:
                continue
            if popularity >93:
                continue
        elif indus_code == '801880':
            if volatility_100 <1.66 or volatility_100 >2:
                continue
        buylist.append(stockcode)
        #多番过滤后的信号即为买入信号，记录到文件中
        write_file('follow_brain.csv', str('%s,buy,%s,%s\n' % (today_date,stockcode,volatility_100)),append = True)
        write_file('follow_log.csv', str('%s,买入信号,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (today_date,stockcode,stock_name,indus_code,price_T,rise_100,volatility_100,volat_indus_100,volat_StockvsIndus,
        cir_m,pe_ratio,pb_ratio,popularity)),append = True)

##    #大盘暴跌卧倒
##    if rise_bench_today < -0.07:
##        return
##    ##3，遍历持仓，给出卖信号
##    for stockcode in context.portfolio.positions:
##        cost = context.portfolio.positions[stockcode].avg_cost
##        price = context.portfolio.positions[stockcode].price
##        value = context.portfolio.positions[stockcode].value
##        intime= context.portfolio.positions[stockcode].init_time
##        ret = price/cost - 1
##        duration=len(get_trade_days(intime,today_date))
##        rise_ratio = ret/duration
##
##        #创板股提高盈速要求
##        if (stockcode[0:3] == '688' or stockcode[0:3] == '300') and today_date >= datetime.date(2020,9,1):
##            rise_ratio = rise_ratio/2
##            ret = ret/2
##
##        if ret < -0.1:
##            selllist.append(stockcode)
##            write_file('follow_brain.csv',str('%s,sell,%s,ZS\n' % (today_date,stockcode)),append = True) #记录在任务清单中，方便明日盘前载入四大list执行
##            write_file('follow_log.csv', str('%s,止损信号,%s,周期:%s,盈利:%s\n' % (today_date,stockcode,duration,ret)),append = True)
##            continue
##        elif ret > 1:
##            selllist.append(stockcode)
##            write_file('follow_brain.csv',str('%s,sell,%s,ZY\n' % (today_date,stockcode)),append = True) #记录在任务清单中，方便明日盘前载入四大list执行
##            write_file('follow_log.csv', str('%s,止盈信号,%s,周期:%s,盈利:%s\n' % (today_date,stockcode,duration,ret)),append = True)
##            continue
##
##        CCI_today = CCI(stockcode, today_date, N=14, unit = '1d', include_now = True)
##        CCI_lastd = CCI(stockcode, lastd_date, N=14, unit = '1d', include_now = True)
##        cci_value = CCI_today[stockcode]
##        if CCI_today[stockcode] > 100:
##            if rise_ratio >0.025:    #上升态势豁免
##                continue
##            else:
##                selllist.append(stockcode)
##                write_file('follow_brain.csv',str('%s,sell,%s,CCI\n' % (today_date,stockcode)),append = True) #记录在任务清单中，方便明日盘前载入四大list执行
##                write_file('follow_log.csv', str('%s,势高信号,%s,周期:%s,盈利:%s\n' % (today_date,stockcode,duration,ret)),append = True)
##                continue
##
##        df_price = get_price(stockcode, count = 10, end_date=today_date, frequency='daily', fields=['close'])
##        close_max = df_price['close'].max()
##        last_price = df_price['close'].values[-1]
##        if last_price/close_max < 0.9 and duration >8:
##            selllist.append(stockcode)
##            write_file('follow_brain.csv',str('%s,sell,%s,DS\n' % (today_date,stockcode)),append = True) #记录在任务清单中，方便明日盘前载入四大list执行
##            write_file('follow_log.csv', str('%s,动损信号,%s,周期:%s,盈利:%s\n' % (today_date,stockcode,duration,ret)),append = True)
##            continue
##
##        if duration >=5 and ret <0:
##            if df_price['close'].values[-1]> df_price['close'].values[-2]:  #当天收阳过
##                continue
##            selllist.append(stockcode)
##            write_file('follow_brain.csv',str('%s,sell,%s,DK\n' % (today_date,stockcode)),append = True) #记录在任务清单中，方便明日盘前载入四大list执行
##            write_file('follow_log.csv', str('%s,短亏信号,%s,周期:%s,盈利:%s\n' % (today_date,stockcode,duration,ret)),append = True)
##            continue
##
##        if duration >=10 and rise_ratio <0.0085:
##            selllist.append(stockcode)
##            write_file('follow_brain.csv',str('%s,sell,%s,DQ\n' % (today_date,stockcode)),append = True) #记录在任务清单中，方便明日盘前载入四大list执行
##            write_file('follow_log.csv', str('%s,到期信号,%s,周期:%s,盈利:%s\n' % (today_date,stockcode,duration,ret)),append = True)
##            continue

    return

after_close_brain('2021-01-06')
print(buylist)

logout()


