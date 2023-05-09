import os
from datetime import datetime

import numpy as np
import pandas as pd
import requests
import streamlit as st
from requests.adapters import HTTPAdapter
from scipy.stats import rankdata
from urllib3.util import Retry

from snapshot_selenium import snapshot as driver
import pyecharts.options as opts
from pyecharts.charts import Line
from pyecharts.render import make_snapshot


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

    def index_zh_a_hist(self,
                        symbol: str = "000859",
                        period: str = "daily",
                        start_date: str = "19700101",
                        end_date: str = "22220101",
                        ) -> pd.DataFrame:
        """
        东方财富网-中国股票指数-行情数据
        https://quote.eastmoney.com/zz/2.000859.html
        :param symbol: 指数代码
        :type symbol: str
        :param period: choice of {'daily', 'weekly', 'monthly'}
        :type period: str
        :param start_date: 开始日期
        :type start_date: str
        :param end_date: 结束日期
        :type end_date: str
        :return: 行情数据
        :rtype: pandas.DataFrame
        """
        code_id_dict = self.index_code_id_map_em()
        period_dict = {"daily": "101", "weekly": "102", "monthly": "103"}
        url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
        try:
            params = {
                "secid": f"{code_id_dict[symbol]}.{symbol}",
                "ut": "7eea3edcaed734bea9cbfc24409ed989",
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                "klt": period_dict[period],
                "fqt": "0",
                "beg": "0",
                "end": "20500000",
                "_": "1623766962675",
            }
        except KeyError:
            params = {
                "secid": f"1.{symbol}",
                "ut": "7eea3edcaed734bea9cbfc24409ed989",
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                "klt": period_dict[period],
                "fqt": "0",
                "beg": "0",
                "end": "20500000",
                "_": "1623766962675",
            }
            r = requests.get(url, params=params)
            data_json = r.json()
            if data_json["data"] is None:
                params = {
                    "secid": f"0.{symbol}",
                    "ut": "7eea3edcaed734bea9cbfc24409ed989",
                    "fields1": "f1,f2,f3,f4,f5,f6",
                    "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                    "klt": period_dict[period],
                    "fqt": "0",
                    "beg": "0",
                    "end": "20500000",
                    "_": "1623766962675",
                }
                r = requests.get(url, params=params)
                data_json = r.json()
                if data_json["data"] is None:
                    params = {
                        "secid": f"2.{symbol}",
                        "ut": "7eea3edcaed734bea9cbfc24409ed989",
                        "fields1": "f1,f2,f3,f4,f5,f6",
                        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                        "klt": period_dict[period],
                        "fqt": "0",
                        "beg": "0",
                        "end": "20500000",
                        "_": "1623766962675",
                    }
                    r = requests.get(url, params=params)
                    data_json = r.json()
                    if data_json["data"] is None:
                        params = {
                            "secid": f"47.{symbol}",
                            "ut": "7eea3edcaed734bea9cbfc24409ed989",
                            "fields1": "f1,f2,f3,f4,f5,f6",
                            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                            "klt": period_dict[period],
                            "fqt": "0",
                            "beg": "0",
                            "end": "20500000",
                            "_": "1623766962675",
                        }
        r = requests.get(url, params=params)
        data_json = r.json()
        try:
            temp_df = pd.DataFrame(
                [item.split(",") for item in data_json["data"]["klines"]]
            )
        except:
            # 兼容 000859(中证国企一路一带) 和 000861(中证央企创新)
            params = {
                "secid": f"2.{symbol}",
                "ut": "7eea3edcaed734bea9cbfc24409ed989",
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                "klt": period_dict[period],
                "fqt": "0",
                "beg": "0",
                "end": "20500000",
                "_": "1623766962675",
            }
            r = requests.get(url, params=params)
            data_json = r.json()
            temp_df = pd.DataFrame(
                [item.split(",") for item in data_json["data"]["klines"]]
            )
        temp_df.columns = [
            "日期",
            "开盘",
            "收盘",
            "最高",
            "最低",
            "成交量",
            "成交额",
            "振幅",
            "涨跌幅",
            "涨跌额",
            "换手率",
        ]
        temp_df.index = pd.to_datetime(temp_df["日期"])
        temp_df = temp_df[start_date:end_date]
        temp_df.reset_index(inplace=True, drop=True)
        temp_df["开盘"] = pd.to_numeric(temp_df["开盘"])
        temp_df["收盘"] = pd.to_numeric(temp_df["收盘"])
        temp_df["最高"] = pd.to_numeric(temp_df["最高"])
        temp_df["最低"] = pd.to_numeric(temp_df["最低"])
        temp_df["成交量"] = pd.to_numeric(temp_df["成交量"])
        temp_df["成交额"] = pd.to_numeric(temp_df["成交额"])
        temp_df["振幅"] = pd.to_numeric(temp_df["振幅"])
        temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"])
        temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"])
        temp_df["换手率"] = pd.to_numeric(temp_df["换手率"])
        return temp_df

    def index_code_id_map_em(self) -> dict:
        """
        东方财富-股票和市场代码
        http://quote.eastmoney.com/center/gridlist.html#hs_a_board
        :return: 股票和市场代码
        :rtype: dict
        """
        url = "http://80.push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": "1",
            "pz": "10000",
            "po": "1",
            "np": "1",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2",
            "invt": "2",
            "fid": "f3",
            "fs": "m:1 t:2,m:1 t:23",
            "fields": "f12",
            "_": "1623833739532",
        }
        r = requests.get(url, params=params)
        data_json = r.json()
        if not data_json["data"]["diff"]:
            return dict()
        temp_df = pd.DataFrame(data_json["data"]["diff"])
        temp_df["market_id"] = 1
        temp_df.columns = ["sh_code", "sh_id"]
        code_id_dict = dict(zip(temp_df["sh_code"], temp_df["sh_id"]))
        params = {
            "pn": "1",
            "pz": "10000",
            "po": "1",
            "np": "1",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2",
            "invt": "2",
            "fid": "f3",
            "fs": "m:0 t:6,m:0 t:80",
            "fields": "f12",
            "_": "1623833739532",
        }
        r = requests.get(url, params=params)
        data_json = r.json()
        if not data_json["data"]["diff"]:
            return dict()
        temp_df_sz = pd.DataFrame(data_json["data"]["diff"])
        temp_df_sz["sz_id"] = 0
        code_id_dict.update(dict(zip(temp_df_sz["f12"], temp_df_sz["sz_id"])))
        params = {
            "pn": "1",
            "pz": "10000",
            "po": "1",
            "np": "1",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2",
            "invt": "2",
            "fid": "f3",
            "fs": "m:0 t:81 s:2048",
            "fields": "f12",
            "_": "1623833739532",
        }
        r = requests.get(url, params=params)
        data_json = r.json()
        if not data_json["data"]["diff"]:
            return dict()
        temp_df_sz = pd.DataFrame(data_json["data"]["diff"])
        temp_df_sz["bj_id"] = 0
        code_id_dict.update(dict(zip(temp_df_sz["f12"], temp_df_sz["bj_id"])))
        code_id_dict = {
            key: value - 1 if value == 1 else value + 1
            for key, value in code_id_dict.items()
        }
        return code_id_dict

    def get_index_hist(self, index, start_date=None, end_date=None):
        if start_date is None:
            start_date = '19700101'
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        try:
            index_df = self.index_zh_a_hist(symbol=index, period="daily", start_date=start_date, end_date=end_date)
            index_df = index_df.rename(columns={'日期': 'DATE', '开盘': 'OPEN', '收盘': 'CLOSE', '最高': 'HIGH', '最低': 'LOW',
                                                '成交量': 'VOLUME', '成交额': 'AMOUNT', '振幅': 'HIGH-LOW', '涨跌幅': 'SYL',
                                                '涨跌额': 'CLOSE-OPEN', '换手率': 'TURNOVER'})
            return index_df
        except Exception as e:
            print('%s:%s' % (index, e))
            return

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
        self.status = '正在获取指数估值数据...'
        df1 = self.index_data(index)
        if df1.empty is True:
            return
        DATE = pd.to_datetime(df1['DATE'])
        start_date = DATE.min().strftime('%Y-%m-%d')
        # start_date = '19700101'
        end_date = DATE.max().strftime('%Y-%m-%d')
        self.status = '正在获取指数历史数据...'
        df2 = self.get_index_hist(index, start_date, end_date)
        if df2 is None:
            return
        self.status = '正在合并数据...'
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

    def line_chart(self, x_data, y_data) -> Line:
        c = (
            Line()
                .set_global_opts(
                tooltip_opts=opts.TooltipOpts(is_show=False),
                xaxis_opts=opts.AxisOpts(type_="category"),
                yaxis_opts=opts.AxisOpts(
                    type_="value",
                    axistick_opts=opts.AxisTickOpts(is_show=True),
                    splitline_opts=opts.SplitLineOpts(is_show=True),
                ),
            )
                .add_xaxis(xaxis_data=x_data)
                .add_yaxis(
                series_name="",
                y_axis=y_data,
                symbol="emptyCircle",
                is_symbol_show=True,
                label_opts=opts.LabelOpts(is_show=False),
            )
                # .render("basic_line_chart.html")
        )
        return c

    def draw_line_chart(self, index='000300'):
        df = self.get_index_merge(index)
        # print(self.line_chart(df['DATE'], df['PE']))
        img = make_snapshot(driver, self.line_chart(df['DATE'], df['PE']).render(), "line.png")
        return img

    def streamlit(self):
        st.header('test')
        st.text(self.status)
        index = st.text_input('输入指数代码', '000905')
        # df = self.get_index_merge(index)

        img = self.draw_line_chart(index)
        st.image(img)

if __name__ == '__main__':
    c = Creat_Card()
    # df = c.get_article_data('1304110194')
    # d = c.draw_card('1304816142', istop=True)
    # c.get_barinfo('010806')
    c.draw_line_chart()
