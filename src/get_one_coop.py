import csv
import os
import requests
import datetime
import numpy as np
import matplotlib.pyplot as plt

import common

RAW_DATA_DIR = os.path.join(os.getcwd(), 'src', 'raw_coop_data')
PROCESSED_DATA_DIR = os.path.join(os.getcwd(), 'src', 'processed_coop_data')


def str_date_to_datetime(str_date):
    x = str_date.split(' ')[0].split('-')
    return datetime.datetime(int(x[0]), int(x[1]), int(x[2]))


def get_stations():
    stations = []

    with open(os.path.join('src', 'coop_stations_to_use.csv'), 'r') as file:
        coop_reader = csv.reader(file)
        header = next(coop_reader)
        for row in coop_reader:
            if row[6] == 'True':
                in_basins = True
            else:
                in_basins = False
            if row[7] == 'True':
                break_with_basins = True
            else:
                break_with_basins = False
            stations.append(common.Station(row[0], row[1], row[2],
                            str_date_to_datetime(row[3]),
                            str_date_to_datetime(row[4]), row[5], row[6],
                            in_basins, break_with_basins))

    return stations


def get_data(coop_stations):
    base_url = common.CHPD_BASE_URL + 'access/'

    # for station in coop_stations:
    station = coop_stations
    the_url = base_url + station.station_id + '.csv'

    r = requests.get(the_url)
    print(r.content)
    print(r.status_code)

    out_file = os.path.join(RAW_DATA_DIR, station.station_id + '.csv')
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
    out_file = os.path.join(RAW_DATA_DIR, station.station_id + '.csv')
    with open(out_file, 'rb') as file:
        data = file.readlines()

    header = data.pop(0)

    debug_year_precip = 0
    missing = 0
    partial = 0

    to_file = ''
    previous_date = False

    for item in data:
        split_item = item.split(b',')
        raw_date = split_item[4].decode().split('-')
        actual_date = datetime.datetime(int(raw_date[0]), int(raw_date[1]),
                                        int(raw_date[2]))

        if actual_date.year >= start_date.year:
            if previous_date:
                # If an entire day is missing, it's not included in the .csv
                # Need to flag those as missing explicitly
                days_diff = (actual_date - previous_date).days
                if days_diff > 1:
                    # print(previous_date, actual_date)
                    new_days = [previous_date + datetime.timedelta(n)
                                for n in range(1, days_diff)]
                    float_precip = [-9999 for i in range(0, 24)]
                    # FORNOW -- change when missing data plan determined
                    for new_day in new_days:
                        counter = 0
                        for value in float_precip:
                            to_file += get_line(value, counter, new_day, station.station_id[-6:])
                            counter += 1

            the_value = item.decode().strip('\n').split(',')
            # TODO check flags

            measurement_flag_values = the_value[7:-5:5]
            try:
                for x in measurement_flag_values:
                    assert x == ' ' or x == 'Z' or x == 'g'
            except:
                print(x)
            quality_flag_values = the_value[8:-5:5]
            primary_source_flag_values = the_value[9:-5:5]
            secondary_source_flag_values = the_value[10:-5:5]
            precip_values = the_value[6:-5:5]

            check_last_records(the_value)

            float_precip = [int(x) for x in precip_values]  # -9999?

            for item in float_precip:
                if item < -1:
                    pass
                else:
                    debug_year_precip += item

            counter = 0
            for value in float_precip:
                to_file += get_line(value, counter, actual_date, station.station_id[-6:])
                counter += 1

        previous_date = actual_date

    out_file = os.path.join(PROCESSED_DATA_DIR, station.station_id[-6:] + '.dat')
    with open(out_file, 'w') as file:
        file.write(to_file)

    print(debug_year_precip)
    print(missing)
    print(partial)

def read_precip(start_date, end_date, station_file):

    with open(station_file, 'r') as file:
        data = file.readlines()

    split_data = [item.split() for item in data]
    years = dict.fromkeys(list(range(start_date.year, end_date.year)), 0)
    for item in split_data:
        items_date = datetime.datetime(int(item[1]), int(item[2]), int(item[3]))
        if items_date > start_date:
            if items_date.year in years:
                if item[-1] == '-9999':
                    pass
                else:
                    years[items_date.year] += float(item[-1])

    return (split_data, years)

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
    coop_stations_to_use = get_stations()
    split_basins_data = common.read_basins_file()
    basins_stations = common.make_basins_stations(split_basins_data)
    # print(basins_stations)

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
        if item.station_id == which_station_id:
            # get_data(item)
            # for comparison
            s_date = datetime.datetime(1979, 1, 1)
            e_date = datetime.datetime(2006, 12, 31)
            # s_date = item.get_start_date_to_use(basins_stations)
            # e_date = item.get_end_date_to_use(basins_stations)
            print(s_date, e_date)
            # process_data(item, basins_stations, s_date, e_date)
            coop_filename = os.path.join(PROCESSED_DATA_DIR, item.station_id[-6:] + '.dat')
            split_coop_data, coop_years = read_precip(s_date, e_date, coop_filename)
            basins_dir = os.path.join('C:\\', 'Users', 'cbarr02', 'Desktop', 'swcalculator_home', 'data')
            basins_filename = os.path.join(basins_dir, item.state + item.station_id[-6:] + '.dat')
            split_basins_data, basins_years = read_precip(s_date, e_date, basins_filename)

            print(coop_years, basins_years)
            plot_cumulative_by_year(split_basins_data, split_coop_data, s_date.year, e_date.year)