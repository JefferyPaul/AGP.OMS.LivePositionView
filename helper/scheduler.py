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
            schedule_checking_interval=60 * 1,
            logger=logging.Logger('ScheduleRunner')
         ):
        self._schedule_running_time = running_time
        self._schedule_checking_interval = schedule_checking_interval

        self.schedule_in_running = False
        self._scheduler_guard_thread = threading.Thread(target=self._scheduler_guard)
        self.logger = logger

    @abc.abstractmethod
    def start(self):
        self._scheduler_guard_thread.start()

    @abc.abstractmethod
    def _start_task(self):
        # 主要用于启动 _task_processing_loop
        # self._task_processing_thread = threading.Thread(target=self._task_processing_loop)
        # self._task_processing_thread.start()
        pass

    @abc.abstractmethod
    def _end_task(self):
        # 主要用于结束 _task_processing_loop
        # 阻塞,确保仅有一个 线程 在运行
        # 用 _schedule_in_running 来判断 和 控制，不需要另外 结束
        # self.logger.info('正在等待线程结束...')
        # if self._task_processing_thread:
        #     self._task_processing_thread.join()
        # self.logger.info('线程已终止!')
        pass

    @abc.abstractmethod
    def _task_processing_loop(self):
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
            if self.schedule_in_running and is_in_running_time:
                pass
            # 不在运行时间
            elif (not self.schedule_in_running) and (not is_in_running_time):
                pass
            # 开始
            elif (not self.schedule_in_running) and is_in_running_time:
                self.schedule_in_running = True
                self.logger.info('开始运行...')
                self._start_task()
            # 结束运行
            else:
                self.schedule_in_running = False
                self.logger.info('暂停运行...')
                self._end_task()

            #
            time.sleep(self._schedule_checking_interval)
