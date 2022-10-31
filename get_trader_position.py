import os
import shutil
import argparse
import sys
from typing import Dict, List
from collections import defaultdict
from time import sleep
from datetime import datetime, timedelta

PATH_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.append(PATH_ROOT)

from pyptools.pyptools_oms.db import OmsDbManagement, TraderPosition

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('-i', '--info_file',)
arg_parser.add_argument('-o', '--output',)
args = arg_parser.parse_args()
INFO_FILE = args.info_file
OUTPUT_ROOT = args.output
if os.path.isdir(OUTPUT_ROOT):
    shutil.rmtree(OUTPUT_ROOT)
    sleep(1)
os.makedirs(OUTPUT_ROOT)


if __name__ == '__main__':
    # 读取oms db信息文件
    l_oms_db_infos: List[dict] = []
    with open(INFO_FILE) as f:
        l_lines = f.readlines()
    for line in l_lines:
        line = line.strip()
        if line == '':
            continue
        line_split = line.split(',')
        try:
            l_oms_db_infos.append({
                "host": line_split[0],
                "user": line_split[1],
                "pwd": line_split[2],
                "db": line_split[3],
            })
        except :
            print(f'{INFO_FILE} 格式错误')
            raise Exception
    # 获取 db position 数据
    # { "Trader": [{"Ticker": , "Volume": , "Price": ,}, {}], }
    d_traders_position = defaultdict(list)
    # 最新持仓更新日期，用于剔除那些旧的trader持仓
    d_traders_position_update_time: Dict[str, datetime] = defaultdict(lambda: datetime(2020, 1, 1))
    for db_info in l_oms_db_infos:
        _l_position: List[TraderPosition] = OmsDbManagement(
            db=db_info['db'],
            host=db_info['host'],
            user=db_info['user'],
            pwd=db_info['pwd'],
        ).query_positions()
        for _p in _l_position:
            _trader = _p.Trader
            _ticker = _p.Ticker
            _l_volume = _p.LongVolume
            _s_volume = _p.ShortVolume
            _l_price = _p.LongPrice
            _s_price = _p.ShortPrice
            _volume = _l_volume - _s_volume
            _price = 0
            if _volume:
                _price = ((_l_volume * _l_price) - (_s_volume * _s_price)) / _volume
            d_traders_position[_trader].append({
                "Ticker": _ticker,
                "Volume": _volume,
                "Price": _price
            })
            # 最新持仓更新日期
            _update_time = _p.UpdateTime
            if _update_time > d_traders_position_update_time[_trader]:
                d_traders_position_update_time[_trader] = _update_time

    dt_now = datetime.now()
    for _trader, _trader_position in d_traders_position.items():
        if dt_now - d_traders_position_update_time[_trader] > timedelta(days=1):
            continue
        output_file = os.path.join(OUTPUT_ROOT, _trader + '.csv')
        l_output_s = [
            ",".join([str(_) for _ in _d_ticker_position.values()])
            for _d_ticker_position in _trader_position
        ]
        with open(output_file, 'w') as f:
            f.writelines('\n'.join(l_output_s))
