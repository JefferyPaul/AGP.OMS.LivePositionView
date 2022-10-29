import os
import shutil
import argparse
import sys
from typing import Dict, List
from collections import defaultdict

PATH_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.append(PATH_ROOT)

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('-p', '--position',)
arg_parser.add_argument('-i', '--initX',)
arg_parser.add_argument('-t', '--ticker_info', default='')
arg_parser.add_argument('-w', '--white_list', default='')
arg_parser.add_argument('-o', '--output',)
args = arg_parser.parse_args()
PATH_POSITION_ROOT = args.position
PATH_GTI_File = args.ticker_info
PATH_WHILT_LIST_File = args.white_list
PATH_INITX_ROOT = args.initX
PATH_OUTPUT_FILE = args.output
if not os.path.isdir(os.path.dirname(PATH_OUTPUT_FILE)):
    os.makedirs(os.path.dirname(PATH_OUTPUT_FILE))
assert os.path.isdir(PATH_POSITION_ROOT)
assert os.path.isdir(PATH_INITX_ROOT)

from pyptools.common.general_ticker_info import GeneralTickerInfoFile, TickerInfoData
from pyptools.common.object import Product, Ticker


D_TRADER_NAME_MAP = {
    "gz030": "GuoZe",
    "JC": "JunCheng",
    "JHWG10": "TangYin",
    "ZouQian": "ZouWei"
}


def handle_trader_name(name: str):
    if "@" in name:
        name = name.split("@")[1]
    if name in D_TRADER_NAME_MAP.keys():
        name = D_TRADER_NAME_MAP[name]

    return name


if __name__ == '__main__':
    # 读取position
    d_trader_ticker_volume_px = defaultdict(dict)
    for _file_name in os.listdir(PATH_POSITION_ROOT):
        p_trader_position = os.path.join(PATH_POSITION_ROOT, _file_name)
        if not os.path.isfile(p_trader_position):
            continue
        _trader = _file_name.replace('.csv', '')
        with open(p_trader_position) as f:
            l_lines = f.readlines()
        for line in l_lines:
            line = line.strip()
            if line == '':
                continue
            _ticker = line.split(',')[0]
            _volume = float(line.split(',')[1])
            _price = float(line.split(',')[2])
            d_trader_ticker_volume_px[_trader][_ticker] = _volume * _price

    # 读取 general ticker info
    if os.path.isfile(PATH_GTI_File):
        d_ticker_info: Dict[Product, TickerInfoData] = GeneralTickerInfoFile.read(PATH_GTI_File)
        for _trader, _d_trader_data in d_trader_ticker_volume_px.items():
            for _ticker, _volume_px in _d_trader_data.items():
                _product: Product = Ticker.from_name(_ticker).product
                if _product not in d_ticker_info:
                    print(f'GTI文件没有此 product: {str(_product)}')
                    raise KeyError
                _point_value = d_ticker_info[_product].point_value
                _d_trader_data[_ticker] = _volume_px * _point_value

    # 读取initx
    d_trader_initX = dict()
    for _file_name in os.listdir(PATH_INITX_ROOT):
        p_trader_initx = os.path.join(PATH_INITX_ROOT, _file_name)
        if not os.path.isfile(p_trader_initx):
            continue
        _trader = _file_name.replace('.csv', '')
        with open(p_trader_initx) as f:
            l_lines = f.readlines()
        _initx = l_lines[0].strip()
        if _initx == '':
            print(f'{p_trader_initx} 错误')
            raise Exception
        _initx = float(_initx)
        d_trader_initX[_trader] = _initx

    # 相除
    _error = False
    l_trader_ticker_volume_p_initx = []
    for _trader in d_trader_ticker_volume_px.keys():
        if _trader not in d_trader_initX:
            print(f'{_trader} 缺少initX')
            _error = True
            continue
        _initx = d_trader_initX[_trader]
        for _ticker, _position in d_trader_ticker_volume_px[_trader].items():
            l_trader_ticker_volume_p_initx.append([_trader, _ticker, str(_position / _initx)])
    if _error:
        raise Exception

    # trader name 处理
    for _ in l_trader_ticker_volume_p_initx:
        _[0] = handle_trader_name(_[0])

    # 白名单
    if os.path.isfile(PATH_WHILT_LIST_File):
        with open(PATH_WHILT_LIST_File) as f:
            l_lines = f.readlines()
        l_white_list = [_.strip() for _ in l_lines if _.strip()]
        l_trader_ticker_volume_p_initx = [_ for _ in l_trader_ticker_volume_p_initx if _[0] in l_white_list]

    # 输出
    with open(PATH_OUTPUT_FILE, 'w') as f:
        f.writelines('\n'.join([
            ','.join(_)
            for _ in l_trader_ticker_volume_p_initx
        ]))
