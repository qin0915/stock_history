import pandas as pd
import requests
from bs4 import BeautifulSoup


def bs4_paraser(html,ye,qr,ty):
    global all_value
    tb_head = []
    tr_count = 0

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
           value={}
           value['代码'] = td_all[0].a.string
           value['名称'] = td_all[1].a.string
           value['截至日期'] = td_all[2].string
           value['家数'] = td_all[3].string
           value['持股数'] = td_all[4].string
           value['流通占比'] = td_all[5].string
           value['增减'] = td_all[6].string
           value['持股比例'] = td_all[7].string
           value['上期家数'] = td_all[8].string


    if tr_count == 0 :
        return False
    else:
        return True


##index.phtml?reportdate=2019&quarter=1&p=2

for y in ('2013','2014','2015','2016','2017','2018','2019','2020'):
    for q in range(1,4):
        for i in range(1,100):
            url = 'http://vip.stock.finance.sina.com.cn/q/go.php/vComStockHold/kind/qfii/index.phtml?reportdate='+y+'&quarter='+str(q)+'&p='+str(i)
            req_obj = requests.get(url)
            bend = bs4_paraser(req_obj.text,y,q,'qfii')
            if not bend:
                break

