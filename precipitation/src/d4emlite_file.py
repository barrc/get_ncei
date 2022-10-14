import os

import common

def parse_d4em_lite():
    split_data = common.read_basins_file()
    print(split_data[0])

if __name__ == '__main__':
    parse_d4em_lite()
