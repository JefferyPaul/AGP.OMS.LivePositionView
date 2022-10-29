"""

"""


import os
import sys
from datetime import datetime, date, time
from time import sleep
from collections import defaultdict
from typing import List, Dict

import pandas as pd
import numpy as np
import argparse

PATH_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.append(PATH_ROOT)

from MatplotlibPlotter import StaticLinesPlotter
import matplotlib.pyplot as plt


if __name__ == '__main__':
    ##################
    # StaticLinesPlotter，持仓图
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-i', '--input', )
    arg_parser.add_argument('--head', default='', help="指定按照某trader的ticker volume排序，若不输入则汇总全部trader的ticker volume然后排序")
    d_args = arg_parser.parse_args()
    INPUT_FILE = d_args.input
    HEAD_TRADER = d_args.head
    assert os.path.isfile(INPUT_FILE)

    # while True:
    #     my_plt = StaticLinesPlotter.plot(df=StaticLinesPlotter.data_from_file(INPUT_FILE), column_groupby=HEAD_TRADER)
    #     my_plt.pause(60)
    #     my_plt.close()

    my_plt = StaticLinesPlotter.plot(df=StaticLinesPlotter.data_from_file(INPUT_FILE), column_groupby=HEAD_TRADER)
    my_plt.show()
    # sleep(5)
    # my_plt.close()
