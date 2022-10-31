import os
import shutil
import argparse
import sys
from typing import Dict, List
from collections import defaultdict
from datetime import datetime, timedelta
from time import sleep
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker
from urllib import parse

PATH_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.append(PATH_ROOT)

from pyptools.pyptools_qm.db import PnL

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
    # 读取db连接信息
    l_oms_db_infos: List[dict] = []
    with open(INFO_FILE) as f:
        l_lines = f.readlines()
    line = l_lines[0].strip()
    if line == '':
        print(f'{INFO_FILE} 格式错误')
        raise Exception
    line_split = line.split(',')
    try:
        _host = line_split[0]
        _user = line_split[1]
        _pwd = line_split[2]
        _db = line_split[3]
    except :
        print(f'{INFO_FILE} 格式错误')
        raise Exception

    # 连接db
    engine = create_engine(
        f'mssql+pymssql://{str(_user)}:{parse.quote_plus(_pwd)}@{str(_host)}/{str(_db)}',
        echo=False,
        max_overflow=50,  # 超过连接池大小之后，允许最大扩展连接数；
        pool_size=50,  # 连接池的大小
        pool_timeout=600,  # 连接池如果没有连接了，最长的等待时间
        pool_recycle=-1,  # 多久之后对连接池中连接进行一次回收
    )
    # 创建DBSession类
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    # 获取db 数据
    querying_dt = datetime.now() - timedelta(days=3)
    l_pnls: List[PnL] = session.query(PnL).filter(PnL.DataTime > querying_dt).all()
    session.close()
    # 筛选
    d_traders_pnl: Dict[str, PnL] = {}
    for _pnl in l_pnls:
        _trader = _pnl.Trader
        if 'test' in _trader.lower():
            continue
        _datatime = _pnl.DataTime
        if _trader not in d_traders_pnl:
            d_traders_pnl[_trader] = _pnl
        else:
            if _datatime > d_traders_pnl[_trader].DataTime:
                d_traders_pnl[_trader] = _pnl

    # 输出
    for _trader, _pnl in d_traders_pnl.items():
        output_file = os.path.join(OUTPUT_ROOT, _trader + '.csv')
        with open(output_file, 'w') as f:
            f.writelines(str(_pnl.InitX))
