import datetime

import requests
import numpy as np

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


def read_data(filename):

    with open(filename, 'r') as file:
        precip_data = file.readlines()

    return [item.split() for item in precip_data]


def get_nldas_data(data_type, start_date_str, end_date_str, x_str, y_str):
    precip_url = "https://hydro1.sci.gsfc.nasa.gov/daac-bin/access/" \
                  + "timeseries.cgi?variable=NLDAS:NLDAS_FORA0125_H.002:" \
                  + data_type + "&startDate=" + start_date_str \
                  + "&endDate=" + end_date_str \
                  + "&location=NLDAS:X" + x_str + "-Y" + y_str + "&type=asc2"

    r = requests.get(precip_url)

    return r.content

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

            print(precip_url)

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
            raise ValueError(ls.lower())
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
    gldas_dict = {}

    for x in data:
        if x:
            split_data = x.decode().split('\n')
            for line in split_data:
                ls = line.strip()
                if "error" in ls.lower():
                    raise ValueError(ls.lower())
                date_time_data = ls.split()
                if (len(date_time_data) == 2 and len(date_time_data[0]) == 19):
                    ymd = date_time_data[0].split('-')
                    local_date = datetime.datetime(
                                    int(ymd[0]), int(ymd[1]), int(ymd[2][0:2]),
                                    int(ymd[2][3:5]))
                    if date_time_data[1] == '-9999':
                        pass
                    else:
                        gldas_dict[local_date] = float(date_time_data[1])/25.4*3600
                else:
                    meta_item = ls.split('=')
                    if len(meta_item) == 2:
                        metadata[meta_item[0]] = meta_item[1]

    return gldas_dict


def get_missing_dates(data_1, missing_value):
    missing_dates = []
    for x in data_1:
        if x[-1] == missing_value:
            try:
                missing_dates.append(
                    datetime.datetime(int(x[1]), int(x[2]), int(x[3]),
                                      int(x[4])))
            except ValueError:
                missing_dates.append(
                    datetime.datetime(int(x[1]), int(x[2]), int(x[3])) +
                    datetime.timedelta(days=1))

    return missing_dates


def get_corresponding_nldas(missing_dates, nldas_data):

    first_nldas_date = list(nldas_data.items())[0][0]
    # this will be some number of hours after the start_date,
    # with a different hour depending on the UTC offset
    # for EST for a start date of 1979/1/1,  the first available date
    # should be 1979/1/1 19:00

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


def fill_data(missing, coop_data, first_ldas_date, missing_value):

    filled_data = []
    for x in coop_data:
        try:
            local_date = datetime.datetime(
                int(x[1]), int(x[2]), int(x[3]), int(x[4]))
        except ValueError:
            local_date = datetime.datetime(
                int(x[1]), int(x[2]), int(x[3])) + datetime.timedelta(days=1)

        if local_date >= first_ldas_date:
            if x[-1] == missing_value:
                ldas_precip = missing[local_date]
                local_thing = x[0:-1]
                local_thing.append(ldas_precip)
                filled_data.append(local_thing)
            else:
                filled_data.append(x)

    return filled_data

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

def get_ordered_pairs(station):
    latitude = np.arange(-59.875, 89.875, 0.25)
    longitude = np.arange(-179.875, 179.875, 0.25)

    lats, lons = np.meshgrid(latitude, longitude)

    stn_lat = float(station.latitude)
    stn_lon = float(station.longitude)

    abs_lat = np.abs(lats-stn_lat)
    abs_lon = np.abs(lons-stn_lon)

    c = np.maximum(abs_lon, abs_lat)

    x, y = np.where(c == np.min(c))
    grid_lat = lats[x[0], y[0]]
    grid_lon = lons[x[0], y[0]]

    test = np.sort(c, axis=None)[:1000]

    pairs = []
    for a in test:
        x_index, y_index = np.where(c == a)
        for (x_, y_) in zip(x_index, y_index):
            if (x_, y_) not in pairs:
                pairs.append((x_, y_))

    ordered_lat_lons = []
    for pair in pairs:

        grid_lat = lats[0, pair[1]]
        grid_lon = lons[pair[0], 0]

        ordered_lat_lons.append((grid_lat, grid_lon))

    return ordered_lat_lons