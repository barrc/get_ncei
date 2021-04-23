import csv
import datetime
import os

import requests

import common
import common_fill




def get_gldas_data(data_type, start_date, end_date, lat, lon):

    gldas_21_start_date = datetime.datetime(2000, 1, 1)

    # pad end_date to avoid issues with timezone conversion
    end_date_delta = datetime.timedelta(days=2)

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
                start_date_str = f'{start_date:%Y-%m-%dT00}'
                if end_date > gldas_21_start_date:
                    end_date_str = '2000-01-01T00'
                else:
                    end_date_str = f'{end_date + end_date_delta:%Y-%m-%dT00}'
            elif condition[-4:-1] == '2.1':
                if start_date > gldas_21_start_date:
                    start_date_str = f'{start_date:%Y-%m-%dT00}'
                else:
                    start_date_str = '2000-01-01T00'
                end_date_str = f'{end_date + end_date_delta:%Y-%m-%dT00}'

            precip_url = "https://hydro1.gesdisc.eosdis.nasa.gov/daac-bin/" \
                + "access/timeseries.cgi?variable=" + condition \
                + data_type + "&startDate=" + start_date_str \
                + "&endDate=" + end_date_str \
                + "&location=GEOM:POINT(" + lon + ",%20" + lat + ")" \
                + "&type=asc2"

            r = requests.get(precip_url)
            data[index] = r.content

    return data





def process_gldas_data(data):
    metadata = {}
    gldas_dict = {}

    for x in data:
        if x:
            split_data = x.decode().split('\n')
            for line in split_data:
                ls = line.strip()
                if "error" in ls.lower():
                    raise ValueError("GLDAS: Error getting precipitation data")
                date_time_data = ls.split()
                if (len(date_time_data) == 2 and len(date_time_data[0]) == 19):
                    ymd = date_time_data[0].split('-')
                    local_date = datetime.datetime(
                                    int(ymd[0]), int(ymd[1]), int(ymd[2][0:2]),
                                    int(ymd[2][3:5]))
                    gldas_dict[local_date] = float(date_time_data[1])/25.4*3600
                else:
                    meta_item = ls.split('=')
                    if len(meta_item) == 2:
                        metadata[meta_item[0]] = meta_item[1]

    return gldas_dict


def read_data(filename):

    with open(filename, 'r') as file:
        precip_data = file.readlines()

    return [item.split() for item in precip_data]


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
                subtracted_date = missing_date - datetime.timedelta(hours=1)
                if subtracted_date in gldas_keys:
                    missing[missing_date] = gldas_data[subtracted_date]
                else:
                    subtracted_date = missing_date - datetime.timedelta(
                        hours=2)
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


def write_file(o_file, filled_coop_data):
    with open(o_file, 'w') as file:
        for item in filled_coop_data:
            if type(item[6]) == str:
                str_precip = item[6]
            elif item[6] == 0:
                str_precip = '0.0'
            else:
                str_precip = str(round(item[6], 3))

            if str_precip == '0.0':
                pass
            else:
                to_file = f'{item[0]}\t{item[1]}\t{item[2]}\t{item[3]}\t{item[4]}\t{item[5]}\t{str_precip}\n'
                file.write(to_file)

def nldas_routine(coop_filename, station):
    coop_precip_data = read_data(coop_filename)

    x_grid, y_grid = common_fill.NLDAS.grid_cell_from_lat_lon(
        float(station.latitude), float(station.longitude))

    start_date_str = f'{station.start_date_to_use:%Y-%m-%dT24}'
    end_date_str = f'{station.end_date_to_use + datetime.timedelta(days=1):%Y-%m-%dT24}'

    raw_nldas_data = common_fill.get_nldas_data(
        'APCPsfc', start_date_str, end_date_str, x_grid, y_grid)
    nldas_precip_data = common_fill.process_nldas_data(raw_nldas_data)

    # data returned is in UTC
    # for COOP, adjust this by utc_offset
    adjusted_nldas_data = adjust_dates(nldas_precip_data, offset)

    coop_missing_dates = common_fill.get_missing_dates(
        coop_precip_data, '-9999')

    first_missing_date, missing_dict = get_corresponding_nldas(
        coop_missing_dates, adjusted_nldas_data)

    filled_coop_data = fill_data(
        missing_dict, coop_precip_data, first_missing_date)

    out_file = os.path.join(
        common.DATA_BASE_DIR, 'filled_coop_data', station.station_id + '.dat')

    write_file(out_file, filled_coop_data)


def gldas_routine(coop_filename, station):
    coop_precip_data = read_data(coop_filename)

    raw_gldas_data = get_gldas_data(
        'Rainf_tavg', station.start_date_to_use, station.end_date_to_use,
        str(station.latitude), str(station.longitude))

    try:
        gldas_precip_data = process_gldas_data(raw_gldas_data)
    except ValueError:
        print(station.station_id)

    no_gldas_date = datetime.datetime(2020, 1, 1, 0, 0)
    if no_gldas_date not in list(gldas_precip_data.keys()):
        gldas_precip_data[no_gldas_date] = 0.0
    else:
        print(f'{station.station_id} has data there')

    adjusted_gldas_data = adjust_dates(gldas_precip_data, offset)
    coop_missing_dates = get_missing_dates(coop_precip_data, '-9999')

    first_missing_date, missing_dict = get_corresponding_gldas(
        coop_missing_dates, adjusted_gldas_data)

    filled_coop_data = fill_data(
        missing_dict, coop_precip_data, first_missing_date)

    out_file = os.path.join(
        common.DATA_BASE_DIR, 'filled_coop_data', station.station_id + '.dat')

    write_file(out_data, filled_coop_data)


if __name__ == '__main__':
    coop_stations_to_use = common.get_stations('coop_stations_to_use.csv')

    with open(os.path.join('src', 'coop_stations_to_use.csv'), 'r') as file:
        data = file.readlines()

    split_data = [item.strip('\n').split(',') for item in data]

    header = split_data.pop(0)

    for x in split_data:
        for coop in coop_stations_to_use:
            if coop.station_id == x[0]:
                station_ = coop
                offset = get_offset(station_)

        c_filename = os.path.join(
            common.DATA_BASE_DIR, 'processed_coop_data',
            station_.station_id + '.dat')

        assert os.path.exists(c_filename)

        if 25 < float(station_.latitude) < 53 and -125 < float(station_.longitude) < -63:
            fill_type = 'nldas'
        else:
            fill_type = 'gldas'

        if fill_type == 'nldas':
            try:
                nldas_routine(c_filename, station_)
            except ValueError as e:
                if 'water' in e.args[0]:
                    gldas_routine(c_filename, station_)
                else:
                    pass
                    # probably just an error on NLDAS side; try again later

        elif fill_type == 'gldas':
            gldas_routine(c_filename, station_)

        exit()
