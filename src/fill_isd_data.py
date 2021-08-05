import os

import common
import common_fill


MISSING_VALUE = '9999'


if __name__ == '__main__':
    isd_stations_to_use = common.get_stations('isd_herewegoagain.csv') # TODO

    for station_ in isd_stations_to_use:
        i_filename = os.path.join(
            common.DATA_BASE_DIR, 'processed_isd_data',
            station_.station_id + '.dat')

        assert os.path.exists(i_filename)

        if 25 < float(station_.latitude) < 53 and -125 < float(station_.longitude) < -63:
            fill_type = 'nldas'
        else:
            fill_type = 'gldas'

        if fill_type == 'nldas':
            try:
                common_fill.nldas_routine(i_filename, station_, 'isd', MISSING_VALUE)
            except ValueError as e:
                if 'water' in e.args[0]:
                    common_fill.gldas_routine(i_filename, station_, 'isd', MISSING_VALUE)
                else:
                    pass
                    # probably just an error on NLDAS side; try again later

        elif fill_type == 'gldas':
            common_fill.gldas_routine(i_filename, station_, 'isd', MISSING_VALUE)
