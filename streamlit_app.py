import os
import random
import string
from datetime import datetime

import numpy as np
import pandas as pd
import requests
import streamlit as st
from requests.adapters import HTTPAdapter
from scipy.stats import rankdata
from urllib3.util import Retry

import plotly.express as px
import plotly.graph_objs as go
import plotly.io as pio


class Creat_Card:
    def __init__(self):
        self.current_path = os.path.dirname(__file__)
        self.session = requests.session()
        self.session.mount('http://',
                           HTTPAdapter(max_retries=Retry(total=10, allowed_methods=frozenset(['GET', 'POST']),
                                                         status_forcelist=[500, 502, 503, 504])))
        self.session.mount('https://',
                           HTTPAdapter(max_retries=Retry(total=10, allowed_methods=frozenset(['GET', 'POST']),
                                                         status_forcelist=[500, 502, 503, 504])))
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, '
                          'like Gecko) Chrome/46.0.2490.76 Mobile Safari/537.36',
        }
        self.status = ''

    def index_data(self, index):
        if index == '931409':
            df = pd.read_excel(self.current_path + '/special_index/931409.xlsx')
        else:
            url = 'https://fundztapi.eastmoney.com/FundSpecialApiNew/FundMSIndexTrend'
            params = dict(
                IndexCode=index,  # 核心字段
                product='EFund',
                ServerVersion='6.5.7',
                DeviceOS='Android10',
                deviceid=r'c1e24b9d17fa03scb8e52a410663ed73||iemi_tluafed_me',  # 核心字段
                version='6.5.7',  # 核心字段
                ctoken='kffredru-auc9kufnhqfn8rjfdjkqcac.2',  # 核心字段
                AppVersion='6.5.7',
                marketchannel='HuaweiApps',
                MobileKey='c1e24b9d17fa03dcb8e52a410663ed73||iemi_tluafed_me',
                ISROE='false',  # 切换PE与ROE
                Version='6.5.7',
                UserId='564db2c0777d40f98edf2196a5cb763e',
                UserAgent='android',
                utoken='udk16-nnnqnhue6-q6fcd8u8fk-e-cqhee6denb2.2',
                plat='Android',  # 核心字段
                passportid='7063094240932552',
                customerNo='564db2c0777d40f98edf2196a5cb763b'
            )
            res = self.session.get(url=url, params=params, headers=self.headers)
            d = res.json()['Datas']
            df = pd.DataFrame(d, columns=['DATE', 'PE'])
            df['PE'] = df['PE'].replace('', np.nan).replace('--', np.nan)
            df = df.dropna(subset=['PE'], axis=0)
        return df

    def index_hist_data(self, index: str) -> pd.DataFrame:
        url = 'https://fundcomapi.tiantianfunds.com/mm/FundIndex/FundIndexPrice'
        data = {
            'product': 'EFund',
            'INDEXCODE': index,
            'passportctoken': ''.join(
                random.sample(string.digits * 10 + string.ascii_letters * 10 + string.punctuation * 1, 171)),
            'passportutoken': ''.join(
                random.sample(string.digits * 10 + string.ascii_letters * 10 + string.punctuation * 1, 331)),
            'deviceid': ''.join(
                random.sample(string.digits * 3 + string.ascii_letters * 3, 30)) + '72%7C%7Ciemi_tluafed_me',
            'userid': ''.join(random.sample(string.digits * 10 + string.ascii_letters * 10, 32)),
            'version': '6.6.5',
            'ctoken': ''.join(random.sample(string.digits * 10 + string.ascii_letters * 10 + string.punctuation * 1, 171)),
            'utoken': ''.join(random.sample(string.digits * 10 + string.ascii_letters * 10 + string.punctuation * 1, 331)),
            'uid': ''.join(random.sample(string.digits * 10 + string.ascii_letters * 10, 32)),
            'plat': 'Android',
            'passportid': ''.join(random.sample(string.digits * 10, 16)),
            'RANGE': 'ln',
        }
        res = requests.post(url=url, data=data)
        df = pd.DataFrame(res.json()['data'])
        df = df.rename(columns={'PDATE': 'DATE', 'PERCENTPRICE': 'CLOSE', 'CHGRT': 'SYL'})
        return df

    def cal_pe100(self, i, series):
        pe100 = (rankdata(series, 'min') - 1) / len(series)
        dic = dict(zip(series, pe100))
        return dic[i]

    def get_pe100(self, pe, index, date, df, period=None):
        # date = pd.to_datetime(date).date()
        if period == '1N':
            min_date = date - pd.to_timedelta(365, unit='days')
        elif period == '2N':
            min_date = date - pd.to_timedelta(750, unit='days')
        elif period == '3N':
            min_date = date - pd.to_timedelta(1095, unit='days')
        elif period == '5N':
            min_date = date - pd.to_timedelta(1826, unit='days')
        else:
            min_date = None
        if min_date is not None:
            if df.DATE.min() > min_date:
                return np.nan
            s = df.loc[(df.DATE <= date) & (df.INDEX == index) & (df.DATE > min_date), 'PE'].to_list()
        else:
            s = df.loc[(df.DATE <= date) & (df.INDEX == index), 'PE'].to_list()
        pe100 = self.cal_pe100(pe, s)
        # print(pe, index, date, pe100)
        return pe100

    def jud_gzlvl(self, pe100):
        if pe100 < 0.15:
            gz = '低位'
        elif pe100 < 0.35:
            gz = '较低'
        elif pe100 < 0.7:
            gz = '适中'
        elif pe100 < 0.85:
            gz = '较高'
        elif pe100 <= 1:
            gz = '高位'
        else:
            gz = np.nan
        return gz

    def get_index_merge(self, index='000300'):
        # self.status = '正在获取指数估值数据...'
        df1 = self.index_data(index)
        if df1.empty is True:
            return
        # self.status = '正在获取指数历史数据...'
        df2 = self.index_hist_data(index)
        if df2 is None:
            return
        # self.status = '正在合并数据...'
        df = df1.merge(df2, left_on='DATE', right_on='DATE', how='left')
        df['INDEX'] = index
        df['_id'] = df.apply(lambda x: pd.to_datetime(x.DATE).strftime('%Y-%m-%d') + '_' + str(index), axis=1)
        df['PE'] = df['PE'].astype('float')
        df['DATE'] = pd.to_datetime(df['DATE'])
        df['PE_1N'] = df.apply(lambda x: self.get_pe100(x.PE, x.INDEX, x.DATE, df, period='1N'), axis=1)
        df['PE_2N'] = df.apply(lambda x: self.get_pe100(x.PE, x.INDEX, x.DATE, df, period='2N'), axis=1)
        df['PE_3N'] = df.apply(lambda x: self.get_pe100(x.PE, x.INDEX, x.DATE, df, period='3N'), axis=1)
        df['PE_5N'] = df.apply(lambda x: self.get_pe100(x.PE, x.INDEX, x.DATE, df, period='5N'), axis=1)
        df['PE_LN'] = df.apply(lambda x: self.get_pe100(x.PE, x.INDEX, x.DATE, df), axis=1)
        df['GZ_1N'] = df['PE_1N'].map(lambda x: self.jud_gzlvl(x))
        df['GZ_2N'] = df['PE_2N'].map(lambda x: self.jud_gzlvl(x))
        df['GZ_3N'] = df['PE_3N'].map(lambda x: self.jud_gzlvl(x))
        df['GZ_5N'] = df['PE_5N'].map(lambda x: self.jud_gzlvl(x))
        df['GZ_LN'] = df['PE_LN'].map(lambda x: self.jud_gzlvl(x))
        return df

    def draw_line_chart(self, index='000300'):
        df = self.get_index_merge(index)
        # print(df)
        trace0 = go.Scatter(x=df["DATE"], y=df["PE"], mode='lines')
        data = [trace0]
        fig_spx = go.Figure(data=data)
        fig_spx.write_image('test.png')
        # print(fig)
        # fig.show()
        # return img

    def streamlit(self):
        st.header('test')
        # st.text(self.status)
        index = st.text_input('输入指数代码', '000905')
        df = self.get_index_merge(index)
        st.dataframe(df)
        # img = self.draw_line_chart(index)
        # st.image(img)

if __name__ == '__main__':
    c = Creat_Card()
    # df = c.index_hist_data('000300')
    df = c.get_index_merge('000300')
    # d = c.draw_card('1304816142', istop=True)
    # c.get_barinfo('010806')
    # c.draw_line_chart()
