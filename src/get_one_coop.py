import csv
import os
import requests
import datetime
import numpy as np
import matplotlib.pyplot as plt

from dateutil import relativedelta as relativedelta

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


def get_line(val, count, date, station_id):
    to_file = ''
    if val == 0:
        return to_file
    else:
        if count == 0:
            temp_date = date - datetime.timedelta(days=1)
            str_date = get_str_date(temp_date)
            str_hour = '24'
        else:
            str_date = get_str_date(date)
            str_hour = str(count)

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
        if val == -9999:
            # FORNOW -- eventually might fill here instead of
            # writing -9999 to file
            to_file += str(val)
        else:
            to_file += f'{val/100:.3f}'
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


def process_data(station, basins, start_date, end_date):
    out_file = os.path.join(common.DATA_BASE_DIR, 'raw_coop_data', station.station_id + '.csv')
    with open(out_file, 'rb') as file:
        data = file.readlines()

    header = data.pop(0)

    debug_year_precip = 0
    missing = 0
    partial = 0

    to_file = ''
    previous_date = False

    date_dict = common.get_date_dict('9999', start_date, end_date)
    for item in data:
        split_item = item.split(b',')
        raw_date = split_item[4].decode().split('-')
        actual_date = datetime.datetime(int(raw_date[0]), int(raw_date[1]),
                                        int(raw_date[2]))

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

            float_precip = [int(x) for x in precip_values]

            # for item in float_precip:
            #     if item < -1:
            #         pass
            #     else:
            #         debug_year_precip += item

            counter = 0
            for value in float_precip:
                to_file += get_line(value, counter, actual_date, station.station_id)
                counter += 1
                actual_date = actual_date + datetime.timedelta(hours=1)

    out_file = os.path.join(common.DATA_BASE_DIR, 'processed_coop_data', station.station_id + '.dat')
    with open(out_file, 'w') as file:
        file.write(to_file)



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
    coop_stations_to_use = common.get_stations('coop')
    split_basins_data = common.read_basins_file()
    basins_stations = common.make_basins_stations(split_basins_data)

    # get_data(coop_stations_to_use[2]) # 2 -> ALBERTA
    # Alberta is "most" typical -- BASINS goes thru 12/31/2006 and COOP is current

    which_station_id = '332974'  # in BASINS and current
    # which_station_id = '106174'  # in BASINS and not current
    which_station_id = '358717'  # not in BASINS and current
    which_station_id = '214546'
    which_station_id = '018178'  # example where the lat/lon are very different from BASINS to CHPD
    # which_station_id = '352867'
    which_station_id = 'USC00134101'  # Iowa City
    # which_station_id = 'USC00304174'  # Ithaca

    for item in coop_stations_to_use:
        # if not os.path.exists(os.path.join(common.DATA_BASE_DIR, 'raw_coop_data', item.station_id + '.csv')):
        #     get_data(item)
        #     print(item.station_id)
        if item.station_id == 'USC00304174':
            get_data(item)
        s_date = item.get_start_date_to_use(basins_stations)
        e_date = item.get_end_date_to_use(basins_stations)
        s_date = datetime.datetime(1970, 1, 1)
        e_date = datetime.datetime(2006, 12, 31)
        process_data(item, basins_stations, s_date, e_date)
        # exit()

        # coop_filename = os.path.join(common.DATA_BASE_DIR, 'processed_coop_data', item.station_id + '.dat')
        # split_coop_data, coop_years = common.read_precip(s_date, e_date, coop_filename)




    # for comparison
    # s_date = datetime.datetime(1979, 1, 1)
    # e_date = datetime.datetime(2006, 12, 31)
    # process_data(item, basins_stations, s_date, e_date)
    # basins_dir = os.path.join('C:\\', 'Users', 'cbarr02', 'Desktop', 'swcalculator_home', 'data')
    # basins_filename = os.path.join(basins_dir, item.state + item.station_id[-6:] + '.dat')
    # split_basins_data, basins_years = common.read_precip(s_date, e_date, basins_filename)
    # print(coop_years, basins_years)
    # plot_cumulative_by_year(split_basins_data, split_coop_data, s_date.year, e_date.year)