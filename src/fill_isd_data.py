import csv
import datetime
import os

import requests

import numpy as np

import common
import common_fill

MISSING_VALUE = '9999'


def adjust_dates(ldas, utc):
    ldas_dict = {}
    for x in ldas:
        adjusted_date = x + datetime.timedelta(hours=utc)
        ldas_dict[adjusted_date] = ldas[x]

    return ldas_dict


def nldas_routine(isd_filename, station):
    isd_precip_data = common_fill.read_data(isd_filename)

    x_grid, y_grid = common_fill.NLDAS.grid_cell_from_lat_lon(
        float(station.latitude), float(station.longitude))

    start_date_str = f'{station.start_date_to_use:%Y-%m-%dT24}'
    end_date_str = f'{station.end_date_to_use + datetime.timedelta(days=1):%Y-%m-%dT24}'

    raw_nldas_data = common_fill.get_nldas_data(
        'APCPsfc', start_date_str, end_date_str, x_grid, y_grid)
    nldas_precip_data = common_fill.process_nldas_data(raw_nldas_data)

    isd_missing_dates = common_fill.get_missing_dates(
        isd_precip_data, MISSING_VALUE)

    first_missing_date, missing_dict = common_fill.get_corresponding_nldas(
        isd_missing_dates, nldas_precip_data)

    filled_isd_data = common_fill.fill_data(
        missing_dict, isd_precip_data, first_missing_date, MISSING_VALUE)

    out_file = os.path.join(
        common.DATA_BASE_DIR, 'filled_isd_data', station.station_id + '.dat')

    common_fill.write_file(out_file, filled_isd_data)

def gldas_subset(gldas_precip_data, isd_precip_data, station):
    no_gldas_date = datetime.datetime(2020, 1, 1, 0, 0)
    if no_gldas_date not in list(gldas_precip_data.keys()):
        gldas_precip_data[no_gldas_date] = 0.0

    isd_missing_dates = common_fill.get_missing_dates(
        isd_precip_data, MISSING_VALUE)

    first_missing_date, missing_dict = common_fill.get_corresponding_gldas(
        isd_missing_dates, gldas_precip_data)

    filled_isd_data = common_fill.fill_data(
        missing_dict, isd_precip_data, first_missing_date, MISSING_VALUE)

    out_file = os.path.join(
        common.DATA_BASE_DIR, 'filled_isd_data', station.station_id + '.dat')

    common_fill.write_file(out_file, filled_isd_data)


def gldas_routine(isd_filename, station):
    isd_precip_data = common_fill.read_data(isd_filename)

    raw_gldas_data = common_fill.get_gldas_data(
        'Rainf_tavg', station.start_date_to_use, station.end_date_to_use,
        str(station.latitude), str(station.longitude))

    try:
        gldas_precip_data = common_fill.process_gldas_data(raw_gldas_data)
    except ValueError:
        print(station.station_id)

    if gldas_precip_data:
        gldas_subset(gldas_precip_data, isd_precip_data, station)

    else:
        # try GLDAS routine with next-nearest grid cell
        pairs_to_try = common_fill.get_ordered_pairs(station)
        for a_pair in pairs_to_try:
            raw_gldas_data = common_fill.get_gldas_data(
                'Rainf_tavg', station.start_date_to_use, station.end_date_to_use,
                str(a_pair[0]), str(a_pair[1]))

            gldas_precip_data = common_fill.process_gldas_data(raw_gldas_data)
            if gldas_precip_data:
                gldas_subset(gldas_precip_data, isd_precip_data, station)
                return




if __name__ == '__main__':
    isd_stations_to_use = common.get_stations('isd_herewegoagain.csv') # TODO

    for station_ in isd_stations_to_use:
        filled_filename = os.path.join(
                common.DATA_BASE_DIR, 'filled_isd_data',
                station_.station_id + '.dat')

        if not os.path.exists(filled_filename):
            print(station_.station_id)

            # i_filename = os.path.join(
            #     common.DATA_BASE_DIR, 'processed_isd_data',
            #     station_.station_id + '.dat')

            # assert os.path.exists(i_filename)

            # if 25 < float(station_.latitude) < 53 and -125 < float(station_.longitude) < -63:
            #     fill_type = 'nldas'
            # else:
            #     fill_type = 'gldas'

            # if fill_type == 'nldas':
            #     try:
            #         nldas_routine(i_filename, station_)
            #     except ValueError as e:
            #         if 'water' in e.args[0]:
            #             gldas_routine(i_filename, station_)
            #         else:
            #             pass
            #             # probably just an error on NLDAS side; try again later

            # elif fill_type == 'gldas':
            #     gldas_routine(i_filename, station_)
