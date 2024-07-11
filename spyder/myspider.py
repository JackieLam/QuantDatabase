import datetime
import os
import re
import threading
import pandas as pd
from bs4 import BeautifulSoup

from utils.basicspyder import BasicSpyder
from utils.logger import Logger, logger_decorator
from typing import List


def divide_lst(lst, n_groups):
    # 每个组应该分配的数量
    group_num = int(len(lst)/n_groups)
    res_lst = []
    for i in range(n_groups-1):
        idx_start = group_num * i
        idx_end = group_num * (i + 1)
        res_lst.append(lst[idx_start: idx_end])
    res_lst.append(lst[group_num*(n_groups-1):])
    return res_lst


class MySpyder(BasicSpyder):
    def __init__(self, maxtries=50, timeout=10):
        super().__init__(maxtries, timeout)
        self.lock = threading.Lock()
        self.data_lst = []

    def get_ccass_hold_detail(self, hkshare_code, ashare_code=None, trade_date=None):
        '''
        Description
        ----------
        获取中央结算系统的持股量

        Parameters
        ----------
        hkshare_code: str. 港股代码.
        ashare_code: str. A股代码. 默认为None, 即返回结果的stock_code为港股代码
        trade_date: str. 指定时间.格式为YYYYMMDD. 默认为None,即昨天
            非交易日也可，获取结果与上个交易日相同

        Return
        ----------
        pandas.DataFrame.
        '''
        if ashare_code is None:
            ashare_code = hkshare_code
        if trade_date is None:
            trade_date = datetime.datetime.today() - datetime.timedelta(days=1)
            trade_date = trade_date.strftime(r'%Y%m%d')
        url = r'https://www3.hkexnews.hk/sdw/search/searchsdw.aspx/'
        shareholdingdate = '/'.join([trade_date[0:4],
                                     trade_date[4:6], trade_date[6:]])
        params = {
            '__EVENTTARGET': 'btnSearch',
            '__EVENTARGUMENT': '',
            'sortBy': 'participantid',
            'sortDirection': 'asc',
            'alertMsg': '',
            'txtShareholdingDate': shareholdingdate,
            'txtStockCode': hkshare_code
        }
        response = self.get(url, params=params)
        htmltext = response.text
        soup = BeautifulSoup(htmltext, 'html.parser')
        content = soup.find("div", attrs={
            "class": "search-details-table-container table-mobile-list-container"})
        if content is None:
            col_lst = ["stock_code", "trade_date", "col_participant_id", "col_participant_name",
                       "col_shareholding", "col_shareholding_percent"]
            return pd.DataFrame(columns=col_lst)
        body_lst = content.find_all('tbody')
        if len(body_lst) == 0:
            col_lst = ["stock_code", "trade_date", "col_participant_id", "col_participant_name",
                       "col_shareholding", "col_shareholding_percent"]
            return pd.DataFrame(columns=col_lst)
        body = body_lst[0]
        lst = body.find_all('tr')
        df = pd.DataFrame()
        for s in lst:
            # id
            part_id = s.find('td', attrs={"class": "col-participant-id"})
            part_id = part_id.find(
                'div', attrs={"class": "mobile-list-body"}).text
            # name
            part_name = s.find('td', attrs={"class": "col-participant-name"})
            part_name = part_name.find(
                'div', attrs={"class": "mobile-list-body"}).text
            # hold
            part_hold = s.find(
                'td', attrs={"class": "col-shareholding text-right"})
            part_hold = part_hold.find(
                'div', attrs={"class": "mobile-list-body"}).text
            part_hold = int("".join(part_hold.split(',')))
            # holding-percent
            part_percent = s.find(
                'td', attrs={"class": "col-shareholding-percent text-right"})
            part_percent = part_percent.find(
                'div', attrs={"class": "mobile-list-body"}).text
            part_percent = float(part_percent.rstrip('%'))
            # dict
            tempdct = {
                "stock_code": [ashare_code],
                "trade_date": [trade_date],
                "col_participant_id": [part_id],
                "col_participant_name": [part_name],
                "col_shareholding": [part_hold],
                "col_shareholding_percent": [part_percent],
            }
            tempdf = pd.DataFrame(tempdct)
            df = pd.concat([df, tempdf], axis=0)
        df = df.reset_index(drop=True)
        return df

    def get_hsgt_stock(self, trade_date=None):
        '''
        Description
        ----------
        获取指定时间的沪深股通股票和对应的A股/港股代码

        Parameters
        ----------
        trade_date: str. 指定时间.格式为YYYYMMDD. 默认为None,即昨天
            非交易日也可，获取结果与上个交易日相同

        Return
        ----------
        pandas.DataFrame. columns是trade_date, hkshare_code, ashare_code, name
        依次为交易日, 港股代码, A股代码, 股票名称
        '''
        if trade_date is None:
            trade_date = datetime.datetime.today() - datetime.timedelta(days=1)
            trade_date = trade_date.strftime(r'%Y%m%d')
        url = f"""https://www3.hkexnews.hk/sdw/search/stocklist.aspx?sortby=stockcode&shareholdingdate={trade_date}"""
        response = self.get(url.replace(" ", "").replace("\n", ""))
        text = response.text
        print(text)
        lst = eval(text)
        df = pd.DataFrame(lst)
        df.columns = ['hkshare_code', 'name']

        def get_ashare_code(x):
            pattern = r'\(A #(.*?)\)'
            lst = re.findall(pattern, x)
            if len(lst) == 0:
                return None
            else:
                s = lst[0]
                if s[0] == '6':
                    return s + '.SH'
                else:
                    return s + '.SZ'

        df['ashare_code'] = df['name'].apply(get_ashare_code)
        df['trade_date'] = trade_date
        df = df[['trade_date', 'hkshare_code', 'ashare_code', 'name']].copy()
        df = df.dropna().reset_index(drop=True)
        return df

    def spyder_main(self, hkshare_code_lst, ashare_code_lst, trade_date):
        df = pd.DataFrame()
        for i in range(len(hkshare_code_lst)):
            hkshare_code = hkshare_code_lst[i]
            ashare_code = ashare_code_lst[i]
            tempdf = self.get_ccass_hold_detail(
                hkshare_code, ashare_code, trade_date)
            print(tempdf)
            df = pd.concat([df, tempdf])
        df = df.reset_index(drop=True)
        self.lock.acquire()
        self.data_lst.append(df)
        self.lock.release()
        return


def main(start_date, end_date, n_threads=10):
    date_lst = pd.date_range(start_date, end_date, freq='D')
    date_lst = [x.strftime(r'%Y%m%d') for x in date_lst]
    print(f'date_lst:{date_lst}')
    if not os.path.exists('ccass_hold_detail'):
        os.mkdir('ccass_hold_detail')

    myspyder = MySpyder(maxtries=50)
    for trade_date in date_lst:
        # stock_code_df = myspyder.get_hsgt_stock(trade_date)  # TODO(cylin): as expected
        # print('get_stock_code_df')
        # hkshare_code_lst_total = stock_code_df['hkshare_code'].tolist()
        # ashare_code_lst_total = stock_code_df['ashare_code'].tolist()
        hkshare_code_lst_total = ['93883']
        ashare_code_lst_total = ['603883.SH']
        print("[DEBUG] hkshare_code_lst_total")
        print(hkshare_code_lst_total)
        print("[DEBUG] ashare_code_lst_total")
        print(ashare_code_lst_total)
        hkshare_code_lst_thread = divide_lst(hkshare_code_lst_total, n_threads)
        ashare_code_lst_thread = divide_lst(ashare_code_lst_total, n_threads)
        iter_zip = zip(hkshare_code_lst_thread, ashare_code_lst_thread)
        # 创建线程并启动
        print('start thread')
        threads = []
        for hkshare_code_lst, ashare_code_lst in iter_zip:
            thread = threading.Thread(target=myspyder.spyder_main,
                                      kwargs={'hkshare_code_lst': hkshare_code_lst,
                                              'ashare_code_lst': ashare_code_lst,
                                              'trade_date': trade_date})
            threads.append(thread)
            thread.start()

        # 等待所有线程完成
        for thread in threads:
            thread.join()

        # 拼接存储
        df = pd.concat(myspyder.data_lst, axis=0)
        df = df.reset_index(drop=True)
        path = f'./ccass_hold_detail/ccass_hold_detail_{trade_date}.csv'
        df.to_csv(path, index=False)
        myspyder.data_lst = []
        print(f'finished: {trade_date}')


if __name__ == '__main__':
    main(start_date="2023/07/12", end_date="2024/07/09", n_threads=10)
