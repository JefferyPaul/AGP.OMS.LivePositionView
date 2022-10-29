""""""

import os
from datetime import datetime, date, time
from collections import defaultdict
from typing import List, Dict

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
import matplotlib.dates as mdate
from abc import ABC, abstractmethod
import argparse


class BasePlotter(ABC):
    @classmethod
    @abstractmethod
    def data_from_file(cls, *args):
        pass

    @classmethod
    @abstractmethod
    def plot(cls, *args):
        pass


class StaticLinesPlotter(BasePlotter):
    """
    输入数据格式：
        ColumnName,IndexName,Value
        ColumnName,IndexName,Value
        ColumnName,IndexName,Value
    输出matplotlib折线图：
        x轴：IndexName
        y轴：Value
        项：ColumnName

    应用：
        Trader,Ticker,PositionVolume 持仓图

    """
    @classmethod
    def data_from_file(cls, file) -> pd.DataFrame or None:
        if not os.path.isfile(file):
            return None

        # 读取文件
        with open(file) as f:
            l_lines = f.readlines()
        # 处理数据
        # [{}, {}]
        l_d_data: List[dict] = []
        for line in l_lines:
            line = line.strip()
            if line == '':
                continue
            line_split = line.split(",")
            l_d_data.append({
                "ColumnName": line_split[0],
                "IndexName": line_split[1],
                "Value": float(line_split[2]),
            })

        df = pd.DataFrame(l_d_data)
        df = df.pivot_table(values='Value', index='IndexName', columns='ColumnName', aggfunc='sum')
        df = df.fillna(0)  # 补零
        df = df.loc[(df != 0).any(axis=1), :]  # 去除volume 全是0的 ticker
        return df

    @classmethod
    def plot(
            cls, df,
            column_groupby=''               # 根据哪个列排序，若不输入则按照全部列汇总排序
    ):
        l_column_names = df.columns.to_list()
        l_index_names = df.index.to_list()
        max_value = max(np.max(df))
        min_value = min(np.min(df))

        # 按照 Ticker 量排序
        if column_groupby not in l_column_names:
            l_index_sorted_by_value = list(df.T.sum().sort_values().to_dict().keys())
        else:
            l_index_sorted_by_value = df.loc[:, column_groupby].sort_values().index.to_list()
        df = df.reindex(l_index_sorted_by_value)

        # 画线
        fig, ax = plt.subplots(figsize=(14, 6))
        # 按column 画线
        for _column_name in list(df.columns):
            _l_values = list(df.loc[:, _column_name])
            # ax.scatter(_l_tickers, _l_value, label=_trader, s=2)
            ax.plot(l_index_sorted_by_value, _l_values, label=_column_name, linewidth=0.5)

        # 横纵轴 标签
        # ax.set_xticks(all_tickers)
        ax.set_xticklabels(l_index_names, rotation=90, fontsize=10)
        ax.set_ylim((min_value, max_value), )
        # 图例
        plt.legend(bbox_to_anchor=(1.05, 1.0), loc='upper left')
        plt.tight_layout()
        # 网格
        plt.grid(True, linestyle='--')
        return plt
        #
        # plt.show()


# TODO 时序图


# ==============================================
# StaticLinesPlotter，持仓图
# arg_parser = argparse.ArgumentParser()
# arg_parser.add_argument('-i', '--input', )
# arg_parser.add_argument('--head', default='', help="指定按照某trader的ticker volume排序，若不输入则汇总全部trader的ticker volume然后排序")
# d_args = arg_parser.parse_args()
# INPUT_FILE = d_args.input
# HEAD_TRADER = d_args.head
# assert os.path.isfile(INPUT_FILE)
