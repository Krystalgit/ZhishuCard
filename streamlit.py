import random
import string
from datetime import datetime
import requests
import streamlit as st
import pandas as pd
from PIL import Image, ImageDraw, ImageFont, ImageFilter
import os
import bs4
import re
from bs4 import BeautifulSoup as bs
from io import BytesIO
import numpy as np
from urllib3.util import Retry
from requests.adapters import HTTPAdapter


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

    def streamlit(self):
        st.header('test')
        index = st.text_input('输入指数代码', '000905')
        df = self.index_data(index)

        st.dataframe(df, height=df.shape[0] * 35)


if __name__ == '__main__':
    c = Creat_Card()
    # df = c.get_article_data('1304110194')
    # d = c.draw_card('1304816142', istop=True)
    # c.get_barinfo('010806')
    c.streamlit()