import csv
import os

import common
import common_fill


MISSING_VALUE = '-9999'


def get_offset(item):
    station_inv_file = os.path.join(os.getcwd(),
        'HPD_v02r02_stationinv_c20200909.csv')

    with open(station_inv_file, 'r') as csv_file:
        station_inv_reader = csv.reader(csv_file)
        header = next(station_inv_reader)
        for row in station_inv_reader:
            if row[0] == item.station_id:
                utc_offset = int(row[8])

    return utc_offset


if __name__ == '__main__':
    coop_stations_to_use = common.get_stations('coop_stations_to_use.csv')

    for station_ in coop_stations_to_use:
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
                common_fill.nldas_routine(c_filename, station_, 'coop', MISSING_VALUE, offset)
            except ValueError as e:
                if 'water' in e.args[0]:
                    common_fill.gldas_routine(c_filename, station_, 'coop', MISSING_VALUE, offset)
                else:
                    pass
                    # probably just an error on NLDAS side; try again later

        elif fill_type == 'gldas':
            common_fill.gldas_routine(c_filename, station_, 'coop', MISSING_VALUE, offset)

