import datetime

import requests


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