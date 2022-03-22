import csv
import datetime
import os
import random

import matplotlib.pyplot as plt
import requests

import common
import get_coop_precip


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


def get_nldas_data(data_type, start_date_str, end_date_str, x_str, y_str):
    precip_url = "https://hydro1.sci.gsfc.nasa.gov/daac-bin/access/" \
                  + "timeseries.cgi?variable=NLDAS:NLDAS_FORA0125_H.002:" \
                  + data_type + "&startDate=" + start_date_str \
                  + "&endDate=" + end_date_str \
                  + "&location=NLDAS:X" + x_str + "-Y" + y_str + "&type=asc2"

    r = requests.get(precip_url)

    return r.content


def get_datetime_date(str_date):
    split_date = str_date.split('-')
    datetime_date = datetime.datetime(
        int(split_date[0]), int(split_date[1]), int(split_date[2][0:2]),
        int(split_date[2][-2:]))

    return datetime_date

def get_gldas_data(data_type, start_date_str, end_date_str, lat, lon):
    start_date = get_datetime_date(start_date_str)
    end_date = get_datetime_date(end_date_str)

    gldas_21_start_date = datetime.datetime(2000, 1, 1)

    gldas_20 = 'GLDAS2:GLDAS_NOAH025_3H_v2.0:'
    gldas_21 = 'GLDAS2:GLDAS_NOAH025_3H_v2.1:'

    if start_date > gldas_21_start_date:
        conditions = [None, gldas_21]
    elif end_date < gldas_21_start_date:
        conditions = [gldas_20, None]
    else:
        conditions = [gldas_20, gldas_21]

    data = [None, None]
    for (index, condition) in enumerate(conditions):
        if condition:
            if condition[-4:-1] == '2.0':
                start_date_str = '1979-01-01T00'
                end_date_str = '2000-01-01T00'
            elif condition[-4:-1] == '2.1':
                start_date_str = '2000-01-01T00'
                end_date_str = '2007-01-01T00'
            precip_url = "https://hydro1.gesdisc.eosdis.nasa.gov/daac-bin/access/" \
                + "timeseries.cgi?variable=" + condition \
                + data_type + "&startDate=" + start_date_str \
                + "&endDate=" + end_date_str \
                + "&location=GEOM:POINT(" + lon + ",%20" + lat + ")" \
                + "&type=asc2"

            r = requests.get(precip_url)
            data[index] = r.content

    return data



def process_nldas_data(data):
    metadata = {}
    nldas_dict = {}

    split_data = data.decode().split('\n')
    for line in split_data:
        ls = line.strip()
        if "error" in ls.lower():
            raise ValueError("NLDAS: Error getting precipitation data")
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

    return nldas_dict


def process_gldas_data(data):
    metadata = {}
    nldas_dict = {}
    for x in data:

        split_data = x.decode().split('\n')
        for line in split_data:
            ls = line.strip()
            if "error" in ls.lower():
                print(ls.lower())
                raise ValueError("GLDAS: Error getting precipitation data")
            date_time_data = ls.split()
            if (len(date_time_data) == 2 and len(date_time_data[0]) == 19):
                ymd = date_time_data[0].split('-')
                local_date = datetime.datetime(
                                int(ymd[0]), int(ymd[1]), int(ymd[2][0:2]),
                                int(ymd[2][3:5]))
                nldas_dict[local_date] = float(date_time_data[1])/25.4*3600
            else:
                meta_item = ls.split('=')
                if len(meta_item) == 2:
                    metadata[meta_item[0]] = meta_item[1]

    return nldas_dict

def read_data(filename):

    with open(filename, 'r') as file:
        precip_data = file.readlines()

    return [item.split() for item in precip_data]


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


def adjust_dates(nldas, utc):
    nldas_dict = {}
    for x in nldas:
        adjusted_date = x + datetime.timedelta(hours=utc)
        nldas_dict[adjusted_date] = nldas[x]

    return nldas_dict


def get_corresponding_nldas(missing_dates, nldas_data):

    first_nldas_date = list(nldas_data.items())[0][0]
    # this will be approximately 1/1/1979, with a different hour depending
    # on the UTC offset
    # for EST, the first available date should be 1979/1/1 19:00

    missing = {}

    for missing_date in missing_dates:
        if missing_date >= first_nldas_date:
            missing[missing_date] = nldas_data[missing_date]

    return first_nldas_date, missing

def get_corresponding_gldas(missing_dates, gldas_data):

    first_gldas_date = list(gldas_data.items())[0][0]
    # this will be approximately 1/1/1979, with a different hour depending
    # on the UTC offset
    # for EST, the first available date should be 1979/1/1 19:00

    missing = {}

    gldas_keys = list(gldas_data.keys())

    for missing_date in missing_dates:
        if missing_date >= first_gldas_date:
            if missing_date in gldas_keys:
                missing[missing_date] = gldas_data[missing_date]
            else:
                subtracted_date = missing_date - datetime.timedelta(hours = 1)
                if subtracted_date in gldas_keys:
                    missing[missing_date] = gldas_data[subtracted_date]
                else:
                    subtracted_date = missing_date - datetime.timedelta(hours = 2)
                    if subtracted_date in gldas_keys:
                        missing[missing_date] = gldas_data[subtracted_date]
                    else:
                        raise ValueError


    return first_gldas_date, missing


def fill_data(missing, coop_data, first_nldas_date):

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

                nldas_precip = missing[local_date]
                local_thing = x[0:-1]
                local_thing.append(nldas_precip)
                filled_data.append(local_thing)
            else:
                filled_data.append(x)

    return filled_data


def get_dict(input_dict):
    output_dict = {}
    for x in input_dict:
        try:
            local_date = datetime.datetime(
                int(x[1]), int(x[2]), int(x[3]), int(x[4]))
        except ValueError:
            local_date = datetime.datetime(
                int(x[1]), int(x[2]), int(x[3])) + datetime.timedelta(days=1)
        output_dict[local_date] = float(x[-1])

    return output_dict


def compare(coop, missing_dates, basins, first_date, station_name):
    coop_dict = get_dict(coop)
    basins_dict = get_dict(basins)

    coop_plot = []
    basins_plot = []

    for missing_date in missing_dates:
        if missing_date >= first_date:
            coop_plot.append(coop_dict[missing_date])
            try:
                basins_plot.append(basins_dict[missing_date])
            except KeyError:
                basins_plot.append(0)

    plt.figure()
    plt.scatter(basins_plot, coop_plot)
    plt.xlabel('BASINS (in)')
    plt.ylabel('C-HPD (in)')
    plt.title(station_name)
    plt.savefig(os.path.join('src', 'scatter_plots', station_name + '.png'))
    plt.show()


    start_for_calculate = datetime.datetime(int(coop[0][1]), int(coop[0][2]), int(coop[0][3]))
    end_for_calculate = datetime.datetime(int(coop[-1][1]), int(coop[-1][2]), int(coop[-1][3]))
    denom = (end_for_calculate - start_for_calculate).days*24
    percent_diff = (sum(coop_plot) - sum(basins_plot))/(sum(basins_plot))*100
    print(f'{station_name}, {sum(basins_plot):.2f}, {sum(coop_plot):.2f}, {len(missing_dates)}, {len(missing_dates)/denom*100:.2f}, {percent_diff:.2f}')


def get_offset(item):
    station_inv_file = os.path.join(
        'src', 'HPD_v02r02_stationinv_c20200909.csv')

    with open(station_inv_file, 'r') as csv_file:
        station_inv_reader = csv.reader(csv_file)
        header = next(station_inv_reader)
        for row in station_inv_reader:
            if row[0] == item.station_id:
                utc_offset = int(row[8])

    return utc_offset


if __name__ == '__main__':
    coop_stations_to_use = common.get_stations('coop_stations_to_use.csv')
    split_basins_data = common.read_basins_file()
    basins_stations = common.make_basins_stations(split_basins_data)

    # STARTHERE
    with open(os.path.join('src', 'random_coop.csv'), 'r') as file:
        data = file.readlines()

    split_data = [item.strip('\n').split(',') for item in data]
    split_data = [split_data[0]] #TODO remove this; it's for debugging alaska

    for x in split_data:
        for coop in coop_stations_to_use:
            if coop.station_id == x[1]: # formatted as [region, station_id]
                station = coop
                offset = get_offset(station)

        start_date_for_comparison = datetime.datetime(1979, 1, 1)
        end_date_for_comparison = datetime.datetime(2006, 12, 31)
        # get_coop_precip.process_data(station, start_date_for_comparison, end_date_for_comparison, True)

        coop_filename = os.path.join(
            common.DATA_BASE_DIR, 'processed_coop_data',
            station.station_id + '_old.dat')
        basins_filename = os.path.join(
            'C:\\', 'Users', 'cbarr02', 'Desktop', 'swcalculator_home',
            'data', station.state + station.station_id[-6:] + '.dat')
        # nldas_filename = os.path.join(
        #     'C:\\', 'Users', 'cbarr02', 'Desktop', 'GitHub',
        #     'testing', 'swc', 'precip_qa', 'NY304174_out.txt')

        assert os.path.exists(coop_filename)
        assert os.path.exists(basins_filename)

        coop_precip_data = read_data(coop_filename)
        basins_precip_data = read_data(basins_filename)

        if 25 < float(station.latitude) < 53 and -125 < float(station.longitude) < -63:
            fill_type = 'nldas'
        else:
            fill_type = 'gldas'

        if fill_type == 'nldas':

            x_grid, y_grid = grid_cell_from_lat_lon(
                float(station.latitude), float(station.longitude))

            raw_nldas_data = get_nldas_data(
                'APCPsfc', '1979-01-01T24', '2007-01-01T24', x_grid, y_grid)
            nldas_precip_data = process_nldas_data(raw_nldas_data)

            # data returned is in UTC
            # for COOP, adjust this by utc_offset
            adjusted_nldas_data = adjust_dates(nldas_precip_data, offset)

            coop_missing_dates = get_missing_dates(coop_precip_data)

            first_missing_date, missing_dict = get_corresponding_nldas(
                coop_missing_dates, adjusted_nldas_data)

            filled_coop_data = fill_data(
                missing_dict, coop_precip_data, first_missing_date)

        elif fill_type == 'gldas':
            raw_gldas_data = get_gldas_data(
                'Rainf_tavg', '1979-01-01T00', '2006-12-31T00',
                str(station.latitude), str(station.longitude))

            gldas_precip_data = process_gldas_data(raw_gldas_data)

            adjusted_gldas_data = adjust_dates(gldas_precip_data, offset)
            coop_missing_dates = get_missing_dates(coop_precip_data)

            first_missing_date, missing_dict = get_corresponding_gldas(
                coop_missing_dates, adjusted_gldas_data)

            filled_coop_data = fill_data(
                missing_dict, coop_precip_data, first_missing_date)

        out_file = os.path.join(common.DATA_BASE_DIR, 'filled_coop_data', station.station_id + '_old.dat')

        with open(out_file, 'w') as file:
            for item in filled_coop_data:
                file.write(item[0])
                file.write('\t')
                file.write(item[1])
                file.write('\t')
                file.write(item[2])
                file.write('\t')
                file.write(item[3])
                file.write('\t')
                file.write(item[4])
                file.write('\t')
                file.write(item[5])
                file.write('\t')
                file.write(str(item[6]))
                file.write('\n')

        compare(filled_coop_data, coop_missing_dates, basins_precip_data,
                first_missing_date, station.station_name)


