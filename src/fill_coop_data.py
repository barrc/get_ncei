import csv
import datetime
import os

import requests

import common
import common_fill


def adjust_dates(ldas, utc):
    ldas_dict = {}
    for x in ldas:
        adjusted_date = x + datetime.timedelta(hours=utc)
        ldas_dict[adjusted_date] = ldas[x]

    return ldas_dict


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


def nldas_routine(coop_filename, station):
    coop_precip_data = common_fill.read_data(coop_filename)

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

    first_missing_date, missing_dict = common_fill.get_corresponding_nldas(
        coop_missing_dates, adjusted_nldas_data)

    filled_coop_data = common_fill.fill_data(
        missing_dict, coop_precip_data, first_missing_date)

    out_file = os.path.join(
        common.DATA_BASE_DIR, 'filled_coop_data', station.station_id + '.dat')

    common_fill.write_file(out_file, filled_coop_data)


def gldas_routine(coop_filename, station):
    coop_precip_data = common_fill.read_data(coop_filename)

    raw_gldas_data = common_fill.get_gldas_data(
        'Rainf_tavg', station.start_date_to_use, station.end_date_to_use,
        str(station.latitude), str(station.longitude))

    try:
        gldas_precip_data = common_fill.process_gldas_data(raw_gldas_data)
    except ValueError:
        print(station.station_id)

    no_gldas_date = datetime.datetime(2020, 1, 1, 0, 0)
    if no_gldas_date not in list(gldas_precip_data.keys()):
        gldas_precip_data[no_gldas_date] = 0.0

    adjusted_gldas_data = adjust_dates(gldas_precip_data, offset)
    coop_missing_dates = common_fill.get_missing_dates(
        coop_precip_data, '-9999')

    first_missing_date, missing_dict = common_fill.get_corresponding_gldas(
        coop_missing_dates, adjusted_gldas_data)

    filled_coop_data = common_fill.fill_data(
        missing_dict, coop_precip_data, first_missing_date)

    out_file = os.path.join(
        common.DATA_BASE_DIR, 'filled_coop_data', station.station_id + '.dat')

    common_fill.write_file(out_file, filled_coop_data)


if __name__ == '__main__':
    coop_stations_to_use = common.get_stations('coop_stations_to_use.csv')

    for coop in coop_stations_to_use:
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

