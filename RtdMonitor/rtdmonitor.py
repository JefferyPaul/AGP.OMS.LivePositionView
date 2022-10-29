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
    def plot(self):
        # 画图
        pass

    @abstractmethod
    def update(self):
        # 更新画图数据
        pass


class RtdDataHandlerBase(ABC):
    def __init__(self, engine):
        self.engine = engine

    @abstractmethod
    def handling(self):
        pass


class RtdMonitorEngine(ScheduleRunner):
    """
    """
    def __init__(
            self,
            running_time: list,  # ScheduleRunner
            rtd_task_interval: int,
            data_handler: RtdDataHandlerBase,
            plotter: RtdPlotterBase,
            refresh_data_in_task_start: bool = True,     # 是否在（重新）启动任务时重新刷新 数据
            logger=MyLogger('RtdMonitor'),
    ):
        # 定时任务骑
        super(RtdMonitorEngine, self).__init__(running_time=running_time, schedule_checking_interval=rtd_task_interval)
        self.task_interval = int(rtd_task_interval)

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
        self.data_update = True
        # 数据处理
        self.data_handler: RtdDataHandlerBase = data_handler
        # 画图
        self.plotter: RtdPlotterBase = plotter

        #
        self.logger: logging.Logger = logger

        #
        self._refresh_data_in_task_start = refresh_data_in_task_start
        self._task_processing_thread: None or threading.Thread = None

    def start(self):
        self._scheduler_guard_thread.start()
        self.plotter.plot()

    def _start_task(self):
        if self._refresh_data_in_task_start:
            self._refresh_data()
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

    def _refresh_data(self):
        self.data = defaultdict(list)
        self.data_update = True

    # def _data_to_plot(self):
    #     """
    #     """
    #     self._plotting_symbols = [_[0] for _ in sorted(
    #         self._last_data.items(),
    #         key=lambda x: x[1]['dt'],
    #         reverse=True
    #     )][:self._plot_count]
    #     self._plotting_symbols.sort(key=lambda x: x.lower())
    #
    #     # 更新画图信息
    #     self.logger.info('data to plotter')
    #     for n, symbol in enumerate(self._plotting_symbols):
    #         self.plotter.update_plot(
    #             index=n,
    #             x=[i['dt'] for i in self.data[symbol]],
    #             y=[i['tp'] for i in self.data[symbol]],
    #             symbol=symbol
    #         )


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

    读取最新日期中的文件；日期更新时 刷新数据
    每次读取所有未读取的数据文件；

    data 中
    不保存所有数据，只保留发生变化时的数据


    """
    def __init__(self, engine: RtdMonitorEngine, path_root):
        super(RtdTimeSeriesDataHandler).__init__(engine)

        self.path_root = path_root
        self._last_reading_file = ''
        self._last_reading_date = ''

    def handling(self):
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
        if _is_new_date:
            pass

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
        _data_updated = False
        with open(p) as f:
            l_lines = f.readlines()
        for line in l_lines:
            line = line.strip()
            if line == '':
                continue
            dt, symbol, tp = line.split(',')
            dt = datetime.strptime(dt, '%Y%m%d %H%M%S')
            tp = float(tp)
            new_data = RtdData(
                index=dt,
                column=symbol,
                value=tp,
            )
            if not self.engine.data.get(symbol):
                # 新增 symbol
                self.engine.data[symbol].append(new_data)
                _data_updated = True
            elif tp != self.engine.data[symbol][-1]['value']:
                # 信号改变, 添加
                self.engine.data[symbol].append(new_data)
                self._data_updated = True


class RtdPlotter(RtdPlotterBase):
    """
    1，持续接收画图信息，
    2，实现画图功能，
    3，画图并展示，   plt.show()
    1，持续接收画图信息，并在信息变更后立即重新再画。   plt.draw()
    """
    def __init__(
            self, engine,
            nrows, ncols, title
    ):
        super(RtdPlotterBase).__init__(engine=engine)
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

    def show(self):
        """ 显示曲线  外部调用"""
        plt.show()

    def update_plot(self, index, x, y, symbol):
        """
        更新指定序号的子图
        :param index: 子图序号
        :param x: 横轴数据
        :param y: 纵轴数据
        :return:
        """
        # X轴数据必须和Y轴数据长度一致

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

        if len(x) != len(y):
            ex = ValueError("x and y must have same first dimension")
            raise ex

        self._ax_list[index].clear()  # 清空子图数据

        x.append(datetime.now())
        y.append(y[-1])

        self._ax_list[index].step(x, y, where="post")  # 绘制最新的数据
        self._ax_list[index].set_yticks(
            _cal_y_ticks(max(abs(max(y)), abs(min(y))), 7)
        )
        self._ax_list[index].set_title(symbol, fontsize=8)
        self._ax_list[index].tick_params(
            labelsize=6,
            labelrotation=15
        )
        self._ax_list[index].xaxis.set_major_formatter(mdate.DateFormatter('%H:%M'))

        plt.draw()

