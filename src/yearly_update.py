import csv
import datetime
import os
import shutil

import common
import get_coop_stations
import get_coop_precip
import fill_coop_data
import common_fill
import get_isd_stations


def get_updated_coops(initial_coop_stations, new_coop_stations):
    initial_coop_ids = [x.station_id for x in initial_coop_stations]
    updated_coop_stations = []
    counter = 0
    for new_coop_station in new_coop_stations:
        new_coop_station.start_date_to_use = common.get_start_date_to_use(new_coop_station)
        new_coop_station.end_date_to_use = common.get_end_date_to_use(new_coop_station)

        if new_coop_station.station_id in initial_coop_ids:
            matching_station = [x for x in initial_coop_stations if x.station_id == new_coop_station.station_id][0]
            if new_coop_station.end_date_to_use == matching_station.end_date_to_use: # then there is no new data to get
                counter += 1
                updated_coop_stations.append(new_coop_station)
            else:
                # there may be new data
                if new_coop_station.end_date_to_use.year > matching_station.end_date_to_use.year:
                    updated_coop_stations.append(new_coop_station)
        else:
            if new_coop_station.start_date_to_use >= common.EARLIEST_START_DATE:
                if new_coop_station.end_date_to_use - new_coop_station.start_date_to_use >= common.TEN_YEARS:
                    updated_coop_stations.append(new_coop_station)

    return updated_coop_stations

def combine_old_new(station, match, year):
    if match:
        assert station.station_id == match.station_id
        original_filename = os.path.join(common.DATA_BASE_DIR, 'combined_data', match.station_id + '.dat')
        assert os.path.exists(original_filename)

        with open(original_filename, 'r') as original_file:
            original_data = original_file.readlines()

    new_filename = os.path.join(common.DATA_BASE_DIR, str(year) + '_processed_coop_data', station.station_id + '.dat')
    out_filename = os.path.join(common.DATA_BASE_DIR, str(year) + '_combined_data', station.station_id + '.dat')

    assert os.path.exists(new_filename)

    with open(new_filename, 'r') as new_file:
        new_data = new_file.readlines()

    with open(out_filename, 'w') as out_file:
        if match:
            out_file.writelines(original_data)
        out_file.writelines(new_data)

def copy_unchanged(station, year):
    original_filename = os.path.join(
        common.DATA_BASE_DIR, 'combined_data', station.station_id + '.dat')
    out_filename = os.path.join(
        common.DATA_BASE_DIR, str(year) + '_combined_data', station.station_id + '.dat')

    shutil.copyfile(original_filename, out_filename)

def update_coop_data():
    new_station_inv_file = ('HPD_v02r02_stationinv_c20220320.csv')

    initial_coops = common.get_stations('coop_stations_to_use.csv')
    new_coops = get_coop_stations.read_coop_file(new_station_inv_file)

    updated_coops = get_updated_coops(initial_coops, new_coops)

    counter = 0
    updated_coops = updated_coops[74:]

    for updated_coop in updated_coops:
        print(updated_coop.station_id)
        # maybe download the raw data to a new folder?
        try:
            matching_station = [x for x in initial_coops if x.station_id == updated_coop.station_id][0]
        except:
            matching_station = None
        if matching_station and updated_coop.end_date_to_use == matching_station.end_date_to_use:
            copy_unchanged(updated_coop, YEAR)
        else:
            get_coop_precip.get_data(updated_coop)
            get_coop_precip.process_data(
                updated_coop, updated_coop.start_date_to_use, updated_coop.end_date_to_use,
                old=True) # NAME not in header -- header starts with b'STATION,LATITUDE,LONGITUDE....

            if matching_station:
                updated_coop.start_date_to_use = matching_station.end_date_to_use + datetime.timedelta(days=1)
            get_coop_precip.process_data(
                updated_coop, updated_coop.start_date_to_use, updated_coop.end_date_to_use,
                old=False) # NAME in header -- header starts with b'"STATION","NAME","LATITUDE","LONGITUDE"...

            offset = fill_coop_data.get_offset(updated_coop)
            c_filename = os.path.join(
                common.DATA_BASE_DIR, '2021_processed_coop_data',
                updated_coop.station_id + '.dat')

            assert os.path.exists(c_filename)

            if 25 < float(updated_coop.latitude) < 53 and -125 < float(updated_coop.longitude) < -63:
                fill_type = 'nldas'
            else:
                fill_type = 'gldas'

            if fill_type == 'nldas':
                try:
                    common_fill.nldas_routine(c_filename, updated_coop, 'coop', fill_coop_data.MISSING_VALUE, offset)
                except ValueError as e:
                    if 'water' in e.args[0]:
                        common_fill.gldas_routine(c_filename, updated_coop, 'coop', fill_coop_data.MISSING_VALUE, offset)
                    else:
                        print(f'try again later -- {updated_coop.station_id}')
                        pass
                        # probably just an error on NLDAS side; try again later

            elif fill_type == 'gldas':
                common_fill.gldas_routine(c_filename, updated_coop, 'coop', fill_coop_data.MISSING_VALUE, offset)

            combine_old_new(updated_coop, matching_station, year=YEAR)

def update_isd_data():
    initial_isds = common.get_stations('isd_herewegoagain.csv')

    # get_isd_stations.download_file(YEAR)
    # out_isd_data = get_isd_stations.read_file(YEAR)
    # get_isd_stations.parse_isd_data(out_isd_data, YEAR)

    # wban_basins = get_isd_stations.read_homr_codes()
    # print(wban_basins) # TODO do we need to get new/more homr codes??
    # get_isd_stations.look_at_isd_files(wban_basins)

    stations = []

    # TODO may want to go back and use the function from get_isd_stations, but let's try stuff here for now
    csv_filename = os.path.join('parsed_isd_history_' + str(YEAR) + '.csv')
    assert os.path.exists(csv_filename)

    with open(csv_filename, 'r') as csv_file:
        station_inv_reader = csv.reader(csv_file)
        for row in station_inv_reader:
            if row[4] != '  ': # state
                stations.append(common.Station(row[0] + row[1], row[2],
                                row[4], common.make_date(row[-2]),
                                common.make_date(row[-1]), row[6], row[7],
                                False, False, 'isd', None, None))

    print(len(stations))
    stations_first_pass = []
    # For C-HPD, we can easily determine which stations are in BASINS and apply
    # the four criteria at this point to determine which stations should be included.
    # Because determining if an ISD station is in BASINS requires the HOMR API,
    # we rule out some stations first to minimize API calls.
    for station in stations:
        # All stations with 'BUOY' in the station_name can be eliminated
        if 'BUOY' in station.station_name:
            pass
        # Now, all stations that end before 1/1/2010 can be eliminated
        elif station.end_date < datetime.datetime(2010, 1, 1):
            pass
        # All stations with less than one year of data can be eliminated
        elif (station.end_date - station.start_date).days < 365:
            pass
        # Eliminate stations without lat/lon
        elif station.latitude == '       ' or station.longitude == '       ':
            pass
        else:
            stations_first_pass.append(station)

    print('\n')
    print(len(stations_first_pass))

    # For ISD, we can definitively rule some stations out.
    # We cannot conclusively determine that we will be able to use a station
    # until we have obtained and parsed the precipitation data.
    # Start and end dates can be misleading, some stations have no
    # precipitation data, and some stations have very high percent missing.
    # But we do want to rule stations out first according to our rules.
    stations_to_try = []
    for y in stations_first_pass:
        print(y.station_id)
        # first one, twenty nine palms. didn't work last time. try again?
        try:
            matching_station = [x for x in initial_isds if x.station_id == y.station_id][0]
            print('!!')

        except:
            # TODO check dates, if enough (?) append for trying
            stations_to_try.append(y)



if __name__ == '__main__':
    # new_station_inv_file = get_coop_stations.download_station_inventory_file()
    YEAR = 2021
    # update_coop_data()
    update_isd_data()

    # TODO handle % missing



