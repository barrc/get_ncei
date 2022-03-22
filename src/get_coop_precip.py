import csv
import os
import requests
import datetime
import numpy as np
import matplotlib.pyplot as plt


import common


def get_data(station):
    base_url = common.CHPD_BASE_URL + 'access/'

    the_url = base_url + station.station_id + '.csv'

    r = requests.get(the_url)

    out_file = os.path.join(common.DATA_BASE_DIR, 'raw_coop_data', station.station_id + '.csv')
    with open(out_file, 'wb') as file:
        file.write(r.content)


def get_str_date(input_date):
    return str(input_date.year) + '  ' + str(input_date.month) + '  ' + str(input_date.day) + '  '


def get_line(k, v, station_id):
    to_file = ''
    if v == 0:
        return to_file
    else:
        if k.hour == 0:
            temp_date = k - datetime.timedelta(days=1)
            str_date = get_str_date(temp_date)
            str_hour = '24'
        else:
            str_date = get_str_date(k)
            str_hour = str(k.hour)

        to_file += station_id + '           '
        to_file += str_date + str_hour + '  0'
        if len(str_date) + len(str_hour) == 13:
            to_file += '     '
        elif len(str_date) + len(str_hour) == 14:
            to_file += '    '
        elif len(str_date) + len(str_hour) == 15:
            to_file += '   '
        else:
            to_file += '  '
        if v == '-9999':
            # FORNOW -- eventually might fill here instead of
            # writing -9999 to file
            to_file += v
        else:
            to_file += f'{v/100:.3f}'
        to_file += '     \n'

        return to_file


def check_last_records(val):
    partial = 0
    # check last records  TODO what was the_value[-5] ??
    # print(val[-5])
    assert val[-4] == ' '
    # TODO how to handle partial?
    if val[-3] != ' ':  # 'P' is for partial
        partial += 1
    assert val[-2] == ' '
    assert val[-1] == 'C' or val[-1] == ' '


def process_data(station, start_date, end_date, old=False):
    out_file = os.path.join(common.DATA_BASE_DIR, 'raw_coop_data', station.station_id + '.csv')
    with open(out_file, 'rb') as file:
        data = file.readlines()

    header = data.pop(0)

    debug_year_precip = 0
    missing = 0
    partial = 0

    to_file = ''
    previous_date = False

    date_dict = common.get_date_dict('-9999', start_date, end_date)
    for item in data:
        split_item = item.split(b',')
        raw_date = split_item[4].decode().split('-')
        actual_date = datetime.datetime(int(raw_date[0]), int(raw_date[1]),
                                        int(raw_date[2]))

        if actual_date.year == 1994:
            print('debug')

        if actual_date >= start_date and actual_date <= end_date:
            the_value = item.decode().strip('\n').split(',')
            # TODO check flags

            measurement_flag_values = the_value[7:-5:5]
            try:
                for x in measurement_flag_values:
                    assert x == ' ' or x == 'Z' or x == 'g'
            except:
                if x == 'A':
                    print(station.station_id)
                    print(actual_date)
                    print(measurement_flag_values)
            quality_flag_values = the_value[8:-5:5]
            primary_source_flag_values = the_value[9:-5:5]
            secondary_source_flag_values = the_value[10:-5:5]
            precip_values = the_value[6:-5:5]

            check_last_records(the_value)

            try:
                float_precip = [int(x) if x != ' ' and x != '-9999' else '-9999' for x in precip_values]
            except ValueError:
                print(precip_values)

            counter = 0
            for value in float_precip:
                # TODO ASSIGN TO DATE_DICT
                date_dict[actual_date] = value
                # to_file += get_line(value, counter, actual_date, station.station_id)
                # counter += 1
                actual_date = actual_date + datetime.timedelta(hours=1)

    for key, value in date_dict.items():
        to_file += get_line(key, value, station.station_id)

    missing = 0
    counter = 0
    for key, value in date_dict.items():
        if value == '-9999':
            missing += 1
        counter += 1

    missing_percent = missing/counter*100

    if old:
        out_file = os.path.join(common.DATA_BASE_DIR, 'processed_coop_data', station.station_id + '_old.dat')
    else:
        out_file = os.path.join(common.DATA_BASE_DIR, 'processed_coop_data', station.station_id + '.dat')

    with open(out_file, 'w') as file:
        file.write(to_file)

    # with open('coop_percent_missing.csv', 'a') as missing_file:
    #     missing_str = f'{station.station_id},{missing_percent:2f}\n'
    #     missing_file.write(missing_str)


def date_and_cumsum(data, year):
    for item in data:
        if float(item[-1]) < 0:
            item[-1] = 0
    r = [(datetime.datetime(int(item[1]), int(item[2]), int(item[3]),
            int(item[4])), float(item[-1])) for item in data if int(item[1]) == year]

    for item in r:
        if item[-1] < 0:
            print(item)
    x, v = zip(*[(d[0], d[1]) for d in r])
    v = np.array(v).cumsum()  # cumulative sum of y values

    return (x, v)

def plot_cumulative_by_year(data_1, data_2, start_year, end_year):
    for item in data_1:
        if int(item[4]) > 23:
            temp_datetime = datetime.datetime(int(item[1]), int(item[2]), int(item[3]), 0)
            temp_datetime += datetime.timedelta(days=1)

            item[1] = temp_datetime.year
            item[2] = temp_datetime.month
            item[3] = temp_datetime.day
            item[4] = 0
    for item in data_2:
        if int(item[4]) > 23:
            temp_datetime = datetime.datetime(int(item[1]), int(item[2]), int(item[3]), 0)
            temp_datetime += datetime.timedelta(days=1)

            item[1] = temp_datetime.year
            item[2] = temp_datetime.month
            item[3] = temp_datetime.day
            item[4] = 0

    years = list(range(start_year, end_year + 1))

    for year in years:
        x_1, v_1 = date_and_cumsum(data_1, year)
        x_2, v_2 = date_and_cumsum(data_2, year)

        # now plot the results
        fig, ax = plt.subplots(1)

        ax.plot(x_1, v_1, '-o', label='BASINS')
        ax.plot(x_2, v_2, '-o', label='ISD')

        fig.autofmt_xdate()
        plt.title(year)
        ax.legend()
        ax.grid()

        plt.show()


if __name__ == '__main__':
    coop_stations_to_use = common.get_stations('coop_stations_to_use.csv')
    coop_stations_to_use = [x for x in coop_stations_to_use if x.station_id == 'USW00093809']

    for item in coop_stations_to_use:
        if not os.path.exists(os.path.join(
                common.DATA_BASE_DIR, 'raw_coop_data', item.station_id + '.csv')):
            get_data(item)
        process_data(item, item.start_date_to_use, item.end_date_to_use)

