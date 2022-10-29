import datetime
import time
import abc
import threading

import logging


class ScheduleRunner:
    """
    日内定时任务器。
        一条线程（_scheduler_thread）负责判断是否处于运行时间，并传递到变量中（_schedule_in_running），
        启动任务 _start_schedule
        结束任务 _end_schedule
    """
    def __init__(
            self,
             running_time=[[datetime.time(0, 0, 0), datetime.time(23, 59, 59)], ],
             loop_interval=60 * 1,
             logger=logging.Logger('ScheduleRunner')
         ):
        self._schedule_running_time = running_time
        self._schedule_loop_interval = loop_interval

        self._schedule_in_running = False
        self._scheduler_thread = threading.Thread(target=self._scheduler_guard)
        self.logger = logger

    @abc.abstractmethod
    def start(self):
        self._scheduler_thread.start()

    @abc.abstractmethod
    def _start_schedule(self):
        pass

    @abc.abstractmethod
    def _end_schedule(self):
        pass

    def _scheduler_guard(self):
        print('启动运行...')
        print('等待进入运行时间区间')
        # 初始化，用于检查上一次上传的时间，防止长时间没有上传
        while True:
            time_now = datetime.datetime.now().time()
            is_in_running_time = True in [
                (time_now >= time_range[0]) and (time_now <= time_range[1])
                for time_range in self._schedule_running_time
            ]

            # 运行时间中
            if self._schedule_in_running and is_in_running_time:
                pass
            # 不在运行时间
            elif (not self._schedule_in_running) and (not is_in_running_time):
                pass
            # 开始
            elif (not self._schedule_in_running) and is_in_running_time:
                self._schedule_in_running = True
                self.logger.info('开始运行...')
                self._start_schedule()
            # 结束运行
            else:
                self._schedule_in_running = False
                self.logger.info('暂停运行...')
                self._end_schedule()

            #
            time.sleep(self._schedule_loop_interval)
