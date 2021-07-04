import math
import talib
import numpy as np
from talib import MA_Type
from functools import reduce
import datetime
import baostock as bao
import pandas as pd


#### 登陆系统 ####
lg = bao.login()


# 获取近期n天大于value_list的个数
def get_bigger_than_val_counter (close, n, value_list):
    np_close = np.array (close[-n:])
    np_value = np.array (value_list[-n:])
    np_value = np_value * 0.98 # 运行向下浮动2%

    diff = np_close - np_value
    return sum (diff > 0)

# 均线
def get_ma (close, timeperiod=5):
    return talib.SMA(close, timeperiod)

# 获取均值
def get_avg_price (close, day:int):
    return get_ma (close, day)[-1]

# 获取macd技术线
def get_macd (close):
    diff, dea, macd = talib.MACDEXT(close, fastperiod=12, fastmatype=1, slowperiod=26, slowmatype=1, signalperiod=9, signalmatype=1)
    macd = macd * 2
    return diff, dea, macd

# 获取day最大价格
def get_max_price (high_values, day):
    return high_values[-day:].max()

# 获取最小价格
def get_min_price (low_values, day):
    return low_values[-day:].min()

# 单日成交量 大于该股的前5日所有成交量的2倍
def is_multiple_stocks (volume, days = 5):
    vol = volume[-1]

    for i in range(2, days + 2):
        if volume[-i] * 2 > vol:
            return False

    return True

# 获取波动的百分比的标准差
def get_std_percentage(var_list):
    v_array = np.array(var_list)

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

    for i in range(1, count):
        if avg_list[-i-1] < avg_list[-i]:
            return 0
        ratio += (avg_list[-i-1] - avg_list[-i]) / avg_list[-i]

    return ratio

def get_stocks_score (tick_list, data_frame, pass_time):
    score = {}
    avg_score = {}

    for code in tick_list:
        high = data_frame[data_frame['code'] == code]['high'].values
        low = data_frame[data_frame['code'] == code]['low'].values
        close = data_frame[data_frame['code'] == code]['close'].values

        # 获取均线矩阵， 不要5日线
        avg_list = np.array([get_ma(close, 10), get_ma(close, 20),
                             get_ma(close, 30), get_ma(close, 60)])

        box_ratio = 0
        for i in range(2, 32): # 不考虑当日的影响
            box_ratio += get_std_percentage(avg_list.T[-i])

        #print (str(code)+" box " + str(box_ratio))

        array_ratio = 0
        for i in range (1, 6):
            array_ratio += get_avg_array (avg_list.T[-i])
        #print (str(code)+" array " + str(array_ratio))

        avg_score[code] = array_ratio
        score[code] = box_ratio - array_ratio

    tick_list.clear()
    score = sorted(score.items(), key = lambda kv:(kv[1], kv[0]), reverse=False)
    for key in score:
        sec_info = get_security_info (key[0])
        #print("\t"+str(sec_info.display_name) + ": " +str(key[0]) + " 得分是 " + str(key[1]))
        if key[1]<=0.7 :
            tick_list.append(key[0])
    print(tick_list)

    return tick_list

# 获取过去180天
def select_ticks (check_date,stocklist):

    _eday = datetime.datetime.strptime(check_date,"%Y-%m-%d")
    _sday = _eday - datetime.timedelta(days=120)
    start_date  = _sday.strftime('%Y-%m-%d')

    filter_list = []
    tick_list= stocklist
    count = 0
    print(len(stocklist))

    for security in stocklist:
        count += 1
        print('%s,%d'%(security,count))

        data_list = []
        stock_rs = bao.query_history_k_data_plus(security,"open,high,low,close,volume,turn",start_date=start_date,end_date=check_date,frequency="d", adjustflag="2")
        data_list.clear()
        while (stock_rs.error_code == '0') & stock_rs.next():
            # 获取一条记录，将记录合并在一起
            data_list.append(stock_rs.get_row_data())
        data_frame = pd.DataFrame(data_list, columns=stock_rs.fields)
        try:
##            print('---------------------------------------------------')
##            print(security)
##            print(data_frame)
            data_frame[['open','high','low','close','volume','turn']] = data_frame[['open','high','low','close','volume','turn']]

            #print(data_frame)


            low = data_frame['low'].values.astype(float)
            open = data_frame['open'].values.astype(float)
            high = data_frame['high'].values.astype(float)
            close = data_frame['close'].values.astype(float)
            volume = data_frame['volume'].values.astype(float)
            turn = data_frame['turn'].values.astype(float)



            if turn[-1] <3 or turn[-1]>10 :
                filter_list.append(security)
                continue

            #不能超过近半年最低价的1.5倍
            if (close[-1] -close.min())/close.min() > 0.5 :
                filter_list.append(security)
                continue

            # 近半年有个大涨的去掉
            if (close.max()/close.min()) > 2 :
                filter_list.append(security)
                continue


            # 获取倍量的股票
            if not is_multiple_stocks (volume, 5):
                filter_list.append(security)
                continue

            # 昨日是跌
##            if close[-1] < open[-1]:
##                filter_list.append(security)
##                continue

            # 放量上涨，昨日不能是长上影线, 倍量换手，不能长上影线
            if (close[-1] - open[-1]) < (high[-1] - close[-1]):
                filter_list.append(security)
                continue

            # 倍量当日，开盘价超过5%
            if open[-1] > close[-2] * 1.05:
                filter_list.append(security)
                continue

            # 倍量涨幅低于5%
            if close[-1] < close[-2] * 1.05:
                filter_list.append(security)
                continue

            # 10日有7日股价高于60日均线， 否则不考虑
##            avg60_list = get_ma (close, 60)
##            count = get_bigger_than_val_counter (close, 11, avg60_list)
##            if count < 7:
##                filter_list.append(security)
##                continue

            # 近3日，股价必须高于60日均线
##            count = get_bigger_than_val_counter (close, 4, avg60_list)
##            if count < 3:
##                filter_list.append(security)
##                continue

            # 不得高于30日最低价的1.25
            min30 = get_min_price (close, 30)
            if min30 * 1.25 < close[-1]:
                filter_list.append(security)
                continue

            # 不得高于5日最低价的1.13
            min5 = get_min_price (close, 5)
            if min5 * 1.13 < close[-1]:
                filter_list.append(security)
                continue
        except:
            print('error :' + security)
            filter_list.append(security)
            continue

    print('------------------------------------------------------')
    print(len(tick_list))
    # 剔除前面不满足条件的股票
    for code in filter_list:
        tick_list.remove (code)

    print(len(tick_list))
    return tick_list





check_day ='2021-05-28'
# 获取证券基本资料
rs = bao.query_stock_basic()
# 打印结果集
data_list = []
while (rs.error_code == '0') & rs.next():
    # 获取一条记录，将记录合并在一起
    data_list.append(rs.get_row_data())
result = pd.DataFrame(data_list, columns=rs.fields)

stock_list = []
for index, row in result.iterrows():
    if row['type'] == '1' and row['status'] == '1' :
        code = row['code']

        if row['code_name'].find('ST') != -1:
            continue
        if row['code_name'].find('退') != -1:
            continue

        if code[3]=='3':
            continue
        if code[3:5]=='688':
            continue

        iop_days = row['ipoDate']
        s_days = datetime.datetime.strptime(iop_days, "%Y-%m-%d")
        c_days = datetime.datetime.strptime(check_day, "%Y-%m-%d")
        if (c_days - s_days).days < 365:
            continue
        stock_list.append(code)

#stock_list =['sh.600008', 'sh.600079', 'sh.600081']

##print(stock_list)

security_list = select_ticks ('2021-06-07',stock_list)
print(security_list)

##start_day ='2016-05-01'
##stop_day ='2016-07-01'
##sday = datetime.datetime.strptime(start_day,"%Y-%m-%d")
##eday = datetime.datetime.strptime(stop_day,"%Y-%m-%d")
##
##while sday <= eday :
##    dstr1 = sday.strftime('%Y-%m-%d')
##    security_list = select_ticks (dstr1,stock_list)
##    print (security_list)
##    sday += datetime.timedelta(days=1)


#### 登出系统 ####
bao.logout()

