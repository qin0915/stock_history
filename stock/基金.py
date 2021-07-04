import jqdatasdk as jq
from jqdatasdk import *
auth('18500964880', 'QINbin9999')

def  get_jgcg_sum(date='2019-09-30',stat=2 , code=None,sort_type=4 ,sort_how=-1,max_count = 100):
    """
    数据来源 : http://data.eastmoney.com/zlsj/jj.html?type=zc
              http://f10.eastmoney.com/f10_v2/ShareholderResearch.aspx?code=sz002624
    code : 默认为None , 返还全市场的 , 指定code时返回个股的
    stat(机构类型) :  1-基金(官网有数据);2-QFII;3-社保;4-券商;5-保险;6-信托
    排序 :
        sort_type : 1-标的代码;2-持有家数;3-持有股数;4-持有比例or市值;5-变动数值;6-变动比例
        sort_how :  1-降序;-1-升序
    date : 报告期
    max_count : 最大返回数量
    url中cmd是筛选CGChange字段的参数,需要的话可以自己加参数"""

    import pandas as pd
    import demjson ,requests
    if code :
        jq_code = normalize_code(code)
        if jq_code[-4:] =='XSHG':
            code = 'SH'+jq_code[:6]
        if jq_code[-4:] =='XSHE':
            code = 'SZ'+jq_code[:6]
        url = "http://f10.eastmoney.com/ShareholderResearch/MainPositionsHodlerAjax?date={}&code={}".format(date,code)
        datas = demjson.decode(requests.get(url).text)
        return  pd.DataFrame(datas)

    url = "http://data.eastmoney.com/zlsj/zlsj_list.aspx?type=ajax"
    url += "&st={}&sr={}&p=1&ps={}".format(sort_type,sort_how,  max_count)

    url += "&stat={}&cmd=1&date={}".format(stat,date)

    res = requests.get(url).text.replace("var jsname = " ,"")
    datas = demjson.decode(res)
    return pd.DataFrame(datas['data'])


df = get_jgcg_sum('2019-06-30',code='600011.XSHG',sort_type=4,max_count=5)

print(df)



logout()