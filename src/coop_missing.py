import csv
import datetime
import os

import requests

import common


class NLDAS:
    DEGREES_PER_GRID_CELL = 1.0 / 8.0
    WESTMOST_GRID_EDGE = -125.0
    SOUTHMOST_GRID_EDGE = 25.0
    WESTMOST_GRID_CENTER = WESTMOST_GRID_EDGE + DEGREES_PER_GRID_CELL / 2.0
    SOUTHMOST_GRID_CENTER = SOUTHMOST_GRID_EDGE + DEGREES_PER_GRID_CELL / 2.0


def grid_cell_from_lat_lon(lat, lon):
    x = int(round((lon - NLDAS.WESTMOST_GRID_CENTER)
                  / NLDAS.DEGREES_PER_GRID_CELL))
    y = int(round((lat - NLDAS.SOUTHMOST_GRID_CENTER)
                  / NLDAS.DEGREES_PER_GRID_CELL))
    return str(x), str(y)


def get_data(data_type, start_date_str, end_date_str, x_str, y_str):
    precip_url = "https://hydro1.sci.gsfc.nasa.gov/daac-bin/access/" \
                  + "timeseries.cgi?variable=NLDAS:NLDAS_FORA0125_H.002:" \
                  + data_type + "&startDate=" + start_date_str \
                  + "&endDate=" + end_date_str \
                  + "&location=NLDAS:X" + x_str + "-Y" + y_str + "&type=asc2"

    r = requests.get(precip_url)

    return r.content


def process_data(data):
    metadata = {}
    nldas_dict = {}

    split_data = data.decode().split('\n')
    for line in split_data:
        ls = line.strip()
        if "error" in ls.lower():
            print("NLDAS: Error getting precipitation data")
        date_time_data = ls.split()
        if (len(date_time_data) == 3 and len(date_time_data[0]) == 10
                and len(date_time_data[1]) == 3):
            ymd = date_time_data[0].split('-')
            local_date = datetime.datetime(
                             int(ymd[0]), int(ymd[1]), int(ymd[2]),
                             int(date_time_data[1][:-1]))
            nldas_dict[local_date] = float(date_time_data[2])/25.4
        else:
            meta_item = ls.split('=')
            if len(meta_item) == 2:
                metadata[meta_item[0]] = meta_item[1]

    print(metadata)
    # print(dates)
    # print(values)
    print('\n')
    return nldas_dict


def read_data(filename):

    with open(filename, 'r') as file:
        data = file.readlines()

    split_data = [item.split() for item in data]

    return split_data


def get_missing_dates(data_1):
    missing_dates = []
    for x in data_1:
        if x[-1] == '-9999':
            try:
                missing_dates.append(
                    datetime.datetime(int(x[1]), int(x[2]), int(x[3]),
                                      int(x[4])))
            except ValueError:
                missing_dates.append(
                    datetime.datetime(int(x[1]), int(x[2]), int(x[3])) +
                    datetime.timedelta(days=1))

    return missing_dates


def adjust_dates(nldas_data, utc_offset):
    nldas_dict = {}
    for x in nldas_data:
        adjusted_date = x + datetime.timedelta(hours=utc_offset)
        nldas_dict[adjusted_date] = nldas_data[x]

    return nldas_dict


def get_corresponding_nldas(missing_dates, nldas_data):

    first_nldas_date = list(nldas_data.items())[0][0]
    # this will be approximately 1/1/1979, with a different hour depending
    # on the UTC offset
    # for EST, the first available date should be 1979/1/1 19:00

    missing_dict = {}

    for missing_date in missing_dates:
        if missing_date >= first_nldas_date:
            missing_dict[missing_date] = nldas_data[missing_date]

    return missing_dict


def fill_data(missing_dict, coop_data):
    first_nldas_date = list(missing_dict.items())[0][0]

    filled_data = []
    for x in coop_data:
        try:
            local_date = datetime.datetime(
                int(x[1]), int(x[2]), int(x[3]), int(x[4]))
        except ValueError:
            local_date = datetime.datetime(
                int(x[1]), int(x[2]), int(x[3])) + datetime.timedelta(days=1)

        if local_date >= first_nldas_date:
            if x[-1] == '-9999':

                nldas_precip = missing_dict[local_date]
                local_thing = x[0:-1]
                local_thing.append(nldas_precip)
                filled_data.append(local_thing)
            else:
                filled_data.append(x)

    print(filled_data)


if __name__ == '__main__':
    coop_stations_to_use = common.get_stations('coop')
    split_basins_data = common.read_basins_file()
    basins_stations = common.make_basins_stations(split_basins_data)

    short_id = '304174'
    station_id_to_use = 'USC00' + short_id

    # TODO consider adding the UTC offset to the stations. But for now:
    station_inv_file = os.path.join(
        'src', 'HPD_v02r02_stationinv_c20200909.csv')

    with open(station_inv_file, 'r') as csv_file:
        station_inv_reader = csv.reader(csv_file)
        header = next(station_inv_reader)
        for row in station_inv_reader:
            if row[0] == station_id_to_use:
                utc_offset = int(row[8])

    for item in coop_stations_to_use:
        if item.station_id == station_id_to_use:
            station_to_use = item

    coop_filename = os.path.join(
        common.DATA_BASE_DIR, 'processed_coop_data',
        station_id_to_use + '_old.dat')
    basins_filename = os.path.join(
        'C:\\', 'Users', 'cbarr02', 'Desktop', 'swcalculator_home',
        'data', 'NY' + short_id + '.dat')
    # nldas_filename = os.path.join(
    #     'C:\\', 'Users', 'cbarr02', 'Desktop', 'GitHub',
    #     'testing', 'swc', 'precip_qa', 'NY304174_out.txt')

    coop_data = read_data(coop_filename)
    basins_data = read_data(basins_filename)

    x_grid, y_grid = grid_cell_from_lat_lon(
        float(station_to_use.latitude), float(station_to_use.longitude))

    data = get_data('APCPsfc', '1979-01-01T24', '2007-01-01T24', x_grid, y_grid)
    nldas_data = process_data(data)

    # data returned is in UTC
    # for COOP, adjust this by utc_offset
    adjusted_nldas_data = adjust_dates(nldas_data, utc_offset)

    missing_dates = get_missing_dates(coop_data)

    missing_dict = get_corresponding_nldas(missing_dates, adjusted_nldas_data)

    fill_data(missing_dict, coop_data)
