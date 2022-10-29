import os
import shutil
import sys
from datetime import datetime, date, time
from time import sleep
import threading
import logging
from collections import defaultdict
from typing import List, Dict

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
import matplotlib.dates as mdate

from RtdMonitor.helper.scheduler import ScheduleRunner
from RtdMonitor.helper.simpleLogger import MyLogger


class RtdPlotter:
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
        self.engine = engine
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
        """ 显示曲线 """
        # if not self._plotting_thread:
        #     self._plotting_thread = threading.Thread(target=plt.show)
        #     self._plotting_thread.start()
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


class RtdMonitorEngine(ScheduleRunner):
    """
    1，时间控制；[[time(0, 0, 0), time(23, 59, 59)], ]
    2，数据读取并处理
    3，判断需要画什么
    4，将画图信息传送到 RtdPlotter


    data_path 结构要求：
    ./data_path/
        -nameA
        -nameB
            -{time}.csv
            -{time}.csv
    选取[nameA, nameB中名字最大的文件夹]，当最大文件夹更换后，数据会重新初始化

    数据缓存：
    {
        nameA: [{
            'dt': datetime.strptime(_dt, self._dt_pattern),
            'tp': float(_tp)
        }, ],
    }
    """
    def __init__(
            self,
            data_path, checking_interval, nrows, ncols,
            running_time: list,     # ScheduleRunner的参数
            logger=MyLogger('RtdMonitor'),
            dt_pattern='%Y%m%d %H%M%S',
    ):
        super(RtdMonitorEngine, self).__init__(running_time=running_time, schedule_checking_interval=60)

        self._data_path = os.path.abspath(data_path)
        self._checking_interval = int(checking_interval)
        self._nrows = nrows
        self._ncols = ncols
        self._plot_count = nrows * ncols
        self.logger: logging.Logger = logger
        self._dt_pattern = dt_pattern

        assert os.path.isdir(self._data_path)

        self._rtd_thread: None or threading.Thread = None
        self._last_folder_path = ''
        self._last_file = ''          # 记录最新读取的文件
        self.data = defaultdict(list)
        self._last_data = defaultdict(dict)
        self._data_updated = False     #
        self._plotting_symbols = []         # 正在（上一次）展示的是哪一些

        self.plotter = RtdPlotter(
            engine=self, nrows=nrows, ncols=ncols,
            title='Real Time Signal'
        )

    def start(self):
        self._scheduler_guard_thread.start()
        self.plotter.show()

    def _start_task(self):
        """
        :return:
        """
        self._rtd_thread = threading.Thread(target=self._rtd_loop)
        self._rtd_thread.start()

    def _rtd_loop(self):
        self.logger.info('start looping')
        while self.schedule_in_running:
            # 数据读取
            self._read_files_data()
            # 数据处理，转换成画图的数据
            if self._data_updated:
                self._data_to_plot()
                self._data_updated = False
            else:
                self.logger.info('no data changed')
            # 间隔
            sleep(self._checking_interval)

    def _end_task(self):
        # 阻塞,确保仅有一个 线程 在运行
        # 直接用 ScheduleRunner._schedule_in_running 来判断 和 控制，不需要另外 结束
        self.logger.info('正在等待线程结束...')
        if self._rtd_thread:
            self._rtd_thread.join()
        self.logger.info('线程已终止!')

    def _refresh_data(self):
        self.data = defaultdict(list)
        self._last_file = ''
        self._last_data = defaultdict(dict)

    def _read_files_data(self):
        """
        检查 读取文件数据,
        缓存在 self.data 中
        """
        def _read_data(p):
            with open(p) as f:
                l_lines = f.readlines()
            for line in l_lines:
                line = line.strip()
                if line == '':
                    continue
                _dt, _symbol, _tp = line.split(',')
                dt = datetime.strptime(_dt, self._dt_pattern)
                tp = float(_tp)
                _new_data = {'dt': dt, 'tp': tp}
                if not self._last_data.get(_symbol):
                    # 没有 last_data,  初次读取
                    self.data[_symbol].append(_new_data.copy())
                    self._last_data[_symbol] = _new_data.copy()
                    self._data_updated = True
                else:
                    # 只处理与之前的数据 不同的数据
                    if float(_tp) != self._last_data[_symbol]['tp']:
                        self.data[_symbol].append(_new_data.copy())
                        if dt > self._last_data[_symbol]['dt']:
                            self._last_data[_symbol] = _new_data.copy()
                            self._data_updated = True
                            self.logger.info(f'symbol data changed: {_symbol}')

        # 查找最新的文件夹
        newest_folder = max([
            i for i in os.listdir(self._data_path)
            if os.path.isdir(os.path.join(self._data_path, i))
        ])
        if newest_folder != os.path.basename(self._last_folder_path):
            self.logger.info('find new folder, refresh cache data')
            self._refresh_data()
            self._last_folder_path = os.path.join(self._data_path, newest_folder)

        # 只读取新的文件
        l_new_file_names = sorted([i for i in os.listdir(self._last_folder_path) if i > self._last_file])
        if l_new_file_names:
            self._last_file = max(l_new_file_names)
            self.logger.info(f'reading files data, newest files: {self._last_file}')
            for file_name in l_new_file_names:
                path_file = os.path.join(self._last_folder_path, file_name)
                try:
                    _read_data(path_file)
                except Exception as e:
                    self.logger.error(f'reading file error, {path_file}, {e}')
        else:
            self.logger.info('no new data file')

    def _data_to_plot(self):
        """
        """
        self._plotting_symbols = [_[0] for _ in sorted(
            self._last_data.items(),
            key=lambda x: x[1]['dt'],
            reverse=True
        )][:self._plot_count]
        self._plotting_symbols.sort(key=lambda x: x.lower())

        # 更新画图信息
        self.logger.info('data to plotter')
        for n, symbol in enumerate(self._plotting_symbols):
            self._plot(index=n, symbol=symbol)

    def _plot(self, index, symbol):
        self.plotter.update_plot(
            index=index,
            x=[i['dt'] for i in self.data[symbol]],
            y=[i['tp'] for i in self.data[symbol]],
            symbol=symbol
        )
