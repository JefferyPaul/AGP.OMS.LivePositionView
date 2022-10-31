"""

核心数据存储
{
    AxTitle: [          # 1个图
        {
            "index": ,          # x轴
            "column": ,         # 项。list中含有n个column，则1个图中有n条折线
            "value": ,          # y轴
        },
    ],
}


DataHandler分类
    1, 按照 日期时间 累计数据文件, 以时间为index画图, 按（或不按） column 分图
        RtdTimeSeriesDataHandler
        ./dateA
            /time1.csv          # eg: 每10s一个文件，ticker目标持仓
            /time2.csv
        ./dateB
        eg: 目标持仓信号收集数据
    2, 只有唯一一个数据文件, column 集中在一个图
        RtdAIOFileDataHandler
        ./AxTitle.csv
    3, 只有唯一一个数据文件, 按照 column 分图
        ./Data.csv


"""

import os
import shutil
import sys
from datetime import datetime, date, time
from time import sleep
import threading
import logging
from collections import defaultdict
from typing import List, Dict
from dataclasses import dataclass

from abc import ABC, abstractmethod

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
import matplotlib.dates as mdate

from RtdMonitor.helper.scheduler import ScheduleRunner
from RtdMonitor.helper.simpleLogger import MyLogger


@dataclass
class RtdData:
    column: str
    index: str or datetime or time
    value: str or float


class RtdPlotterBase(ABC):
    def __init__(self, engine):
        self.engine = engine

    @abstractmethod
    def plot(self, *args):
        # 画图
        pass

    @abstractmethod
    def update(self, *args):
        # 更新画图数据
        pass


class RtdDataHandlerBase(ABC):
    def __init__(self, engine):
        self.engine = engine

    @abstractmethod
    def handling(self, *args):
        pass

    @abstractmethod
    def refresh_data(self, *args):
        pass


class RtdMonitorEngine(ScheduleRunner):
    """
    """
    def __init__(
            self,
            running_time: list,  # ScheduleRunner
            rtd_task_interval: int,
            # data_handler: RtdDataHandlerBase,
            # plotter: RtdPlotterBase,
            refresh_data_in_task_start: bool = False,     # 是否在（重新）启动任务时重新刷新 数据
            logger=MyLogger('RtdMonitor'),
    ):
        # 定时任务骑
        super(RtdMonitorEngine, self).__init__(
            running_time=running_time, schedule_checking_interval=rtd_task_interval, logger=logger)
        self.task_interval = int(rtd_task_interval)

        # 作为 data_handler / plotter
        """
        核心数据存储
        {
            AxTitle: [          # 1个图
                {
                    "index": ,          # x轴
                    "column": ,         # 项。list中含有n个column，则1个图中有n条折线
                    "value": ,          # y轴
                }, 
            ],
        }
        """
        self.data: Dict[str, List[RtdData]] = defaultdict(list)
        self.is_data_updated = True
        # 数据处理
        self.data_handler: RtdDataHandlerBase
        # 画图
        self.plotter: RtdPlotterBase

        #
        self._refresh_data_in_task_start = refresh_data_in_task_start
        self._task_processing_thread: None or threading.Thread = None

    def add_data_handler(self, data_handler):
        self.data_handler: RtdDataHandlerBase = data_handler

    def add_plotter(self, plotter):
        self.plotter: RtdPlotterBase = plotter

    def start(self):
        self._scheduler_guard_thread.start()
        self.plotter.plot()

    def _start_task(self):
        if self._refresh_data_in_task_start:
            self.data_handler.refresh_data()
        self._task_processing_thread = threading.Thread(target=self._task_processing_loop)
        self._task_processing_thread.start()

    def _end_task(self):
        # 阻塞,确保仅有一个 线程 在运行
        # 直接用 ScheduleRunner._schedule_in_running 来判断 和 控制，不需要另外 结束
        self.logger.info('正在等待线程结束...')
        if self._task_processing_thread:
            self._task_processing_thread.join()
        self.logger.info('线程已终止!')

    def _task_processing_loop(self):
        while self.schedule_in_running:
            # 数据读取 处理，转换成画图的数据
            self.data_handler.handling()
            # 更新plotter画图数据
            self.plotter.update()
            # 间隔
            sleep(self.task_interval)


# ===========================
# 具体 class

# ===========================


class RtdTimeSeriesDataHandler(RtdDataHandlerBase):
    """
    时间序列数据

    数据文件目录结构:
        ./dateA
            /time1.csv          # eg: 每10s一个文件，ticker目标持仓
            /time2.csv
        ./dateB
    文件数据格式：
        %Y%m%d %H%M%S,ticker,target_volume

    一个ticker 一个图，
        AxTitle => ticker
        index => date time
        column => ticker
        value => target_volume

    读取最新日期中的文件；
    日期更新时 刷新数据
    每次读取所有未读取的数据文件；

    data 中
    不保存所有数据，只保留value发生变化时的数据



    """
    def __init__(
            self,
            engine: RtdMonitorEngine,
            path_root,
            all_in_one_ax_name="",
            refresh_data_in_new_date=True,      # 新日期，重置数据
            only_changed_data=True,     # 仅读取存储，value发生变化的数据，避免时间序列中有过多无用数据
    ):
        super(RtdTimeSeriesDataHandler, self).__init__(engine)

        self.path_root = path_root
        self._refresh_data_in_new_date = refresh_data_in_new_date
        self._only_changed_data = only_changed_data
        self._all_in_one_ax_name = all_in_one_ax_name

        # 初始化
        self._last_reading_file = ''
        self._last_reading_date = ''

    def handling(self):
        self.engine.logger.info('handling new data')
        # [1] 查找最新的文件夹
        _is_new_date = False
        newest_date_folder_name = max([
            i for i in os.listdir(self.path_root)
            if os.path.isdir(os.path.join(self.path_root, i))
        ])
        if newest_date_folder_name != self._last_reading_date:
            _is_new_date = True
        path_newest_date_folder = os.path.join(self.path_root, newest_date_folder_name)

        #
        if self._refresh_data_in_new_date:
            if _is_new_date:
                self.refresh_data()

        # [2] 读取新的文件
        l_new_file_names = sorted([i for i in os.listdir(path_newest_date_folder) if i > os.path.basename(self._last_reading_file)])
        if not l_new_file_names:
            return
        for file_name in l_new_file_names:
            path_file = os.path.join(path_newest_date_folder, file_name)
            try:
                self._read_a_file(path_file)
            except Exception as e:
                self.engine.logger.error(e)
                raise Exception

    def _read_a_file(self, p):
        # [3] 读取新文件
        self._set_data_unupdated()
        with open(p) as f:
            l_lines = f.readlines()
        for line in l_lines:
            line = line.strip()
            if line == '':
                continue
            dt, column_name, value = line.split(',')
            dt = datetime.strptime(dt, '%Y%m%d %H%M%S')
            value = float(value)
            new_data = RtdData(
                index=dt,
                column=column_name,
                value=value,
            )
            # 只在 value 发生变化是添加新数据
            if self._only_changed_data:
                if not self._all_in_one_ax_name:
                    _data_in_column = self.engine.data.get(column_name)
                else:
                    _data_in_column = [
                        i for i in self.engine.data[self._all_in_one_ax_name]
                        if i.column == column_name
                    ]
                if not _data_in_column:
                    # 新增 symbol
                    self._append_new_data(new_data)
                    self._set_data_updated()
                elif value != max(_data_in_column, key=lambda x: x.index).value:
                    # 信号改变, 添加
                    self._append_new_data(new_data)
                    self._set_data_updated()
            else:
                self._append_new_data(new_data)
                self._set_data_updated()

    def _append_new_data(self, data: RtdData):
        if not self._all_in_one_ax_name:
            self.engine.data[data.column].append(data)
        else:
            self.engine.data[self._all_in_one_ax_name].append(data)

    def _set_data_updated(self):
        self.engine.is_data_updated = True

    def _set_data_unupdated(self):
        self.engine.is_data_updated = False

    def refresh_data(self):
        self.engine.data = defaultdict(list)
        self.engine.is_data_updated = True


class RtdTimeSeriesPlotter(RtdPlotterBase):
    """
    1，持续接收画图信息，
    2，实现画图功能，
    3，画图并展示，   plt.show()
    1，持续接收画图信息，并在信息变更后立即重新再画。   plt.draw()
    """
    def __init__(
            self,
            engine,
            nrows, ncols, title,
            add_now_dt_tick=True,
    ):
        super(RtdTimeSeriesPlotter, self).__init__(engine=engine)
        self._add_now_dt_tick = add_now_dt_tick

        self._plotting_thread: None or threading.Thread = None

        # 创建子图
        self.fig, self.axs = plt.subplots(
            nrows, ncols,
            figsize=self._cal_fig_size(nrows, ncols)
        )
        self.fig.subplots_adjust(
            left=0.06, right=0.99,
            bottom=0.06, top=0.95,
            wspace=0.18, hspace=0.3
        )  # 设置子图之间的间距
        self.fig.canvas.set_window_title(title)  # 设置窗口标题

        # 子图字典，key为子图的序号，value为子图句柄
        self._ax_list: List[Axes] = []
        if nrows == 1:
            for i in range(ncols):
                self._ax_list.append(self.axs[i])
        elif ncols == 1:
            for i in range(nrows):
                self._ax_list.append(self.axs[i])
        else:
            for i in range(nrows):
                for j in range(ncols):
                    self._ax_list.append(self.axs[i, j])

    @staticmethod
    def _cal_fig_size(nrows, ncols):
        row_size = 3 * nrows
        col_size = 2 * ncols
        return row_size, col_size

    def plot(self):
        """ 显示曲线  外部调用"""
        plt.show()

    def update(self):
        self.engine.logger.info('plotting new data')
        l_ax_names = list(self.engine.data.keys())
        l_ax_names.sort()
        for n, ax_name in enumerate(l_ax_names):
            ax_data: List[RtdData] = self.engine.data[ax_name]
            ax_data_column_name = list(set([i.column for i in ax_data]))
            #
            self._ax_list[n].clear()  # 清空子图数据
            self._ax_list[n].set_title(ax_name, fontsize=8)

            # 添加曲线
            for column_name in ax_data_column_name:
                ax_data_of_column = [i for i in ax_data if i.column == column_name]
                ax_data_of_column.sort(key=lambda x: x.index)
                x = [i.index for i in ax_data_of_column]
                y = [i.value for i in ax_data_of_column]
                if self._add_now_dt_tick:
                    x.append(datetime.now())
                    y.append(y[-1])
                self._ax_list[n].step(x, y, where="post")  # 绘制最新的数据

            # 设置
            y_of_ax = [i.value for i in ax_data]
            self._ax_list[n].set_yticks(
                self._cal_y_ticks(max(abs(max(y_of_ax)), abs(min(y_of_ax))), 7)
            )
            self._ax_list[n].xaxis.set_major_formatter(mdate.DateFormatter('%H:%M'))
            self._ax_list[n].tick_params(
                labelsize=6,
                labelrotation=15
            )
            plt.draw()

    @staticmethod
    def _cal_y_ticks(_max, n) -> list:
        if _max == 0:
            return [-1, 0, 1]
        if _max > 1:
            num_n = len(str(_max).split('.')[0])
            num_n -= 2
        else:
            num_n = -1
            for s in str(_max).split('.')[-1]:
                if s != 0:
                    break
                num_n -= 1
            num_n -= 1
        _max = round(_max + 10 ** num_n, -num_n)
        return [round(i, -num_n) for i in np.linspace(-_max, _max, n)]


class RtdSingleFileDataHandler(RtdDataHandlerBase):
    """
    2, 只有唯一一个数据文件, column 集中在一个图
        RtdAIOFileDataHandler
        ./AxTitle.csv
    3, 只有唯一一个数据文件, 按照 column 分图
        ./Data.csv
    """
    def __init__(
            self,
            engine: RtdMonitorEngine,
            file_path,
            all_in_one_ax_name="",          # 默认为空，表示按照column name分图，不合并在一张图中
    ):
        super(RtdSingleFileDataHandler, self).__init__(engine=engine)
        self._file_path = file_path
        self._all_in_one_ax_name = all_in_one_ax_name

    def handling(self):
        self.engine.logger.info('handling new data')
        self.refresh_data()
        self._set_data_unupdated()
        with open(self._file_path) as f:
            l_lines = f.readlines()
        for line in l_lines:
            line = line.strip()
            if line == '':
                continue
            index_name, column_name, value = line.split(',')
            value = float(value)
            new_data = RtdData(
                index=index_name,
                column=column_name,
                value=value,
            )
            self._append_new_data(new_data)
        self._set_data_updated()

    def _append_new_data(self, data: RtdData):
        if not self._all_in_one_ax_name:
            self.engine.data[data.column].append(data)
        else:
            self.engine.data[self._all_in_one_ax_name].append(data)

    def _set_data_updated(self):
        self.engine.is_data_updated = True

    def _set_data_unupdated(self):
        self.engine.is_data_updated = False

    def refresh_data(self):
        self.engine.data = defaultdict(list)
        self.engine.is_data_updated = True


class RtdCommonPlotter(RtdPlotterBase):
    """
    1，持续接收画图信息，
    2，实现画图功能，
    3，画图并展示，   plt.show()
    1，持续接收画图信息，并在信息变更后立即重新再画。   plt.draw()
    """
    def __init__(
            self,
            engine,
            nrows, ncols, title,
            sort_by_column_name=''
    ):
        super(RtdCommonPlotter, self).__init__(engine=engine)

        self._plotting_thread: None or threading.Thread = None
        self._sort_by_column_name = sort_by_column_name

        # 创建子图
        self.fig, self.axs = plt.subplots(
            nrows, ncols,
            figsize=self._cal_fig_size(nrows, ncols)
        )
        self.fig.subplots_adjust(
            left=0.06, right=0.99,
            bottom=0.06, top=0.95,
            wspace=0.18, hspace=0.3
        )  # 设置子图之间的间距
        self.fig.canvas.set_window_title(title)  # 设置窗口标题

        # 子图字典，key为子图的序号，value为子图句柄
        self._ax_list: List[Axes] = []
        if nrows == 1 and ncols == 1:
            self._ax_list.append(self.axs)
        elif nrows == 1:
            for i in range(ncols):
                self._ax_list.append(self.axs[i])
        elif ncols == 1:
            for i in range(nrows):
                self._ax_list.append(self.axs[i])
        else:
            for i in range(nrows):
                for j in range(ncols):
                    self._ax_list.append(self.axs[i, j])

    @staticmethod
    def _cal_fig_size(nrows, ncols):
        row_size = 3 * nrows
        col_size = 2 * ncols
        return row_size, col_size

    def plot(self):
        """ 显示曲线  外部调用"""
        plt.show()

    def update(self):
        self.engine.logger.info('plotting new data')
        l_ax_names = list(self.engine.data.keys())
        l_ax_names.sort()
        for n, ax_name in enumerate(l_ax_names):
            #
            self._ax_list[n].clear()  # 清空子图数据
            self._ax_list[n].set_title(ax_name, fontsize=8)

            # 数据处理
            df = pd.DataFrame(self.engine.data[ax_name])
            df = df.pivot_table(values='value', index='index', columns='column', aggfunc='sum')
            df = df.fillna(0)  # 补零
            df = df.loc[(df != 0).any(axis=1), :]  # 去除volume 全是0的 ticker

            l_column_names = df.columns.to_list()
            max_value = max(np.max(df))
            min_value = min(np.min(df))

            # 按照 Ticker 量排序
            if self._sort_by_column_name not in l_column_names:
                l_index_sorted_by_value = list(df.T.sum().sort_values().to_dict().keys())
            else:
                l_index_sorted_by_value = df.loc[:, self._sort_by_column_name].sort_values().index.to_list()
            df = df.reindex(l_index_sorted_by_value)

            # 按column 画线
            for _column_name in list(df.columns):
                _l_values = list(df.loc[:, _column_name])
                # ax.scatter(_l_tickers, _l_value, label=_trader, s=2)
                self._ax_list[n].plot(l_index_sorted_by_value, _l_values, label=_column_name, linewidth=0.5)

            # 横纵轴 标签
            # ax.set_xticks(all_tickers)
            self._ax_list[n].set_xticklabels(l_index_sorted_by_value, rotation=90, fontsize=10)
            self._ax_list[n].set_ylim((min_value, max_value), )
            # 图例
            plt.legend(bbox_to_anchor=(1.05, 1.0), loc='upper left')
            plt.tight_layout()
            # 网格
            plt.grid(True, linestyle='--')
            plt.draw()
