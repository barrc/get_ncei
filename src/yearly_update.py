import csv
import datetime
import os
import shutil

import common
import get_coop_stations
import get_coop_precip
import fill_coop_data
import fill_isd_data
import common_fill
import get_isd


def make_directory(year, short_dir):
    try:
        os.mkdir(os.path.join(common.DATA_BASE_DIR, str(year) + short_dir))
    except FileExistsError:
        print('Directory already exists')


def make_directories(year):
    make_directory(year, '_raw_coop_data')
    make_directory(year, '_raw_isd_data')
    make_directory(year, '_processed_coop_data')
    make_directory(year, '_processed_isd_data')
    make_directory(year, '_filled_coop_data')
    make_directory(year, '_filled_isd_data')
    make_directory(year, '_combined_data')


def get_updated_stations(initial_stations, new_stations):
    initial_ids = [x.station_id for x in initial_stations]
    updated_stations = []

    for new_station in new_stations:
        new_station.start_date_to_use = common.get_start_date_to_use(
            new_station)
        new_station.end_date_to_use = common.get_end_date_to_use(
            new_station)

        if new_station.station_id in initial_ids:
            matching_station = get_matching_station(
                initial_stations, new_station)
            matching_station.end_date_to_use = common.get_end_date_to_use(matching_station)
            if new_station.end_date_to_use == matching_station.end_date_to_use:
                updated_stations.append(new_station)
            else:
                # there may be new data
                if (new_station.end_date_to_use.year >
                        matching_station.end_date_to_use.year):
                    updated_stations.append(new_station)
        else:
            if new_station.start_date_to_use >= common.EARLIEST_START_DATE:
                if (new_station.end_date_to_use - new_station.start_date_to_use
                        >= common.TEN_YEARS):
                    updated_stations.append(new_station)

    return updated_stations


def combine_old_new(station, match, year, station_type):
    if match:
        assert station.station_id == match.station_id
        # In future years, change original_filename here to get from 2021_combined_data
        original_filename = os.path.join(
            common.DATA_BASE_DIR, 'combined_data', match.station_id + '.dat')
        try:
            assert os.path.exists(original_filename)
        except AssertionError:
            return False

        with open(original_filename, 'r') as original_file:
            original_data = original_file.readlines()

    new_filename = os.path.join(
        common.DATA_BASE_DIR, str(year) + '_filled_' + station_type + '_data',
        station.station_id + '.dat')
    out_filename = os.path.join(
        common.DATA_BASE_DIR, str(year) + '_combined_data',
        station.station_id + '.dat')

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
        common.DATA_BASE_DIR, str(year) + '_combined_data',
        station.station_id + '.dat')

    if os.path.exists(original_filename):
        shutil.copyfile(original_filename, out_filename)


def copy_filled_combined(station, year):
    original_filename = os.path.join(
        common.DATA_BASE_DIR, str(year) + '_filled_' + station.network + '_data',
        station.station_id + '.dat')

    out_filename = os.path.join(
        common.DATA_BASE_DIR, str(year) + '_combined_data',
        station.station_id + '.dat')

    shutil.copyfile(original_filename, out_filename)

def make_updated_coops():
    new_station_inv_file = get_coop_stations.download_station_inventory_file()

    initial_coops = common.get_stations('coop_stations_to_use.csv')
    new_coops = get_coop_stations.read_coop_file(new_station_inv_file)

    updated_coops = get_updated_stations(initial_coops, new_coops)
    return updated_coops

def update_coop_data(updated_coops):
    initial_coops = common.get_stations('coop_stations_to_use.csv')

    for updated_coop in updated_coops:
        print(updated_coop.station_id)

        if 25 < float(updated_coop.latitude) < 53 and -125 < float(updated_coop.longitude) < -63:
            continue

        try:
            matching_station = get_matching_station(
                initial_coops, updated_coop)
        except IndexError:
            matching_station = None

        if matching_station and updated_coop.end_date_to_use == matching_station.end_date_to_use:
            copy_unchanged(updated_coop, common.CURRENT_END_YEAR)

        else:
            if not os.path.exists(
                             os.path.join(common.DATA_BASE_DIR,
                             str(common.CURRENT_END_YEAR) + '_raw_coop_data',
                             updated_coop.station_id + '.csv')):
                get_coop_precip.get_data(updated_coop)
            # NAME not in header
            # header starts with b'STATION,LATITUDE,LONGITUDE....
            try:
                get_coop_precip.process_data(
                    updated_coop, updated_coop.start_date_to_use,
                    updated_coop.end_date_to_use,
                    old=True)
            except FileNotFoundError:
                pass

            if matching_station:
                # NAME in header
                # header starts with b'"STATION","NAME","LATITUDE","LONGITUDE"...
                get_coop_precip.process_data(
                    updated_coop,
                    matching_station.end_date_to_use + datetime.timedelta(days=1),
                    updated_coop.end_date_to_use,
                    old=False)

            else:
                # NAME in header
                get_coop_precip.process_data(
                    updated_coop, updated_coop.start_date_to_use,
                    updated_coop.end_date_to_use,
                    old=False)

            offset = fill_coop_data.get_offset(updated_coop)
            c_filename = os.path.join(
                common.DATA_BASE_DIR, str(common.CURRENT_END_YEAR) + '_processed_coop_data',
                updated_coop.station_id + '.dat')

            assert os.path.exists(c_filename)

            if 25 < float(updated_coop.latitude) < 53 and -125 < float(updated_coop.longitude) < -63:
                fill_type = 'nldas'
            else:
                fill_type = 'gldas'

            if fill_type == 'nldas':
                try:
                    common_fill.nldas_routine(
                        c_filename, updated_coop, 'coop',
                        fill_coop_data.MISSING_VALUE, offset)
                except ValueError as e:
                    if 'water' in e.args[0]:
                        common_fill.gldas_routine(
                            c_filename, updated_coop, 'coop',
                            fill_coop_data.MISSING_VALUE, offset)
                    else:
                        print(f'try again later -- {updated_coop.station_id}')
                        pass
                        # probably just an error on NLDAS side; try again later

            elif fill_type == 'gldas':
                print('gldas')
                common_fill.gldas_routine(
                    c_filename, updated_coop, 'coop',
                    fill_coop_data.MISSING_VALUE, offset)

            combine_old_new(
                updated_coop, matching_station, common.CURRENT_END_YEAR,
                'coop')

    return updated_coops

def get_matching_station(original, new):
    match = [a for a in original if a.station_id == new.station_id][0]
    return match


def get_some_isds():
    initial_isds = common.get_stations('isd_subset.csv')
    stations = []

    csv_filename = os.path.join('parsed_isd_history_' + str(common.CURRENT_END_YEAR) + '.csv')
    assert os.path.exists(csv_filename)

    with open(csv_filename, 'r') as csv_file:
        station_inv_reader = csv.reader(csv_file)
        for row in station_inv_reader:
            if row[4] != '  ': # state
                stations.append(common.Station(row[0] + row[1], row[2],
                                row[4], common.make_date(row[-2]),
                                common.make_date(row[-1]), row[6], row[7],
                                False, False, 'isd', None, None))


    stations_first_pass = []
    # Rule out some stations
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

    return stations_first_pass

def update_isd_data():
    initial_isds = common.get_stations('isd_subset.csv')
    stations_first_pass = get_some_isds()
    updated_isds = get_updated_stations(initial_isds, stations_first_pass)


    # For ISD, we can definitively rule some stations out.
    # We cannot conclusively determine that we will be able to use a station
    # until we have obtained and parsed the precipitation data.
    # Start and end dates can be misleading, some stations have no
    # precipitation data, and some stations have very high percent missing.
    # But we do want to rule stations out first according to our rules.

    # Rule out stations that had more than 90% missing last time --
    # there is no way they will work
    with open('percent_missing.csv', 'r') as file:
        percent_missing = file.readlines()

    percent_missing_dict = {}
    for item in percent_missing:
        split_item = item.strip('\n').split(',')
        percent_missing_dict[split_item[0]] = float(split_item[1])

    sufficient_data = []

    for y in updated_isds:
        print(y.station_id)

        if y.station_id in percent_missing_dict:
            if percent_missing_dict[y.station_id] > 90.0:
                print('too much missing')
                continue
        try:
            matching_station = get_matching_station(initial_isds, y)
            if (matching_station.start_date_to_use ==
                    matching_station.end_date_to_use):
                matching_station = None
        except IndexError:
            matching_station = None

        if matching_station:
            if matching_station and y.end_date_to_use == matching_station.end_date_to_use:
                copy_unchanged(y, common.CURRENT_END_YEAR)
                continue

            get_isd.get_raw_data(
                y.station_id,
                matching_station.end_date_to_use + datetime.timedelta(days=1),
                y.end_date_to_use, year=common.CURRENT_END_YEAR)
        else:
            get_isd.get_raw_data(
                y.station_id, y.start_date_to_use, y.end_date_to_use,
                year=common.CURRENT_END_YEAR)

        real_start_date, real_end_date = get_isd.get_dates(
            y.station_id, year=common.CURRENT_END_YEAR)

        if real_start_date == real_end_date:
            print('continue')
            continue

        if real_start_date and real_end_date:
            y.start_date_to_use = real_start_date
            y.end_date_to_use = real_end_date
            get_isd.read_raw(
                y, real_start_date, real_end_date,
                year=common.CURRENT_END_YEAR)
            split_isd_data, isd_years = common.read_precip(
                    real_start_date,
                    real_end_date,
                    os.path.join(
                        common.DATA_BASE_DIR,
                        str(common.CURRENT_END_YEAR) + '_processed_isd_data',
                        y.station_id + '.dat'))

            if matching_station:
                original_start_date, original_end_date = get_isd.get_dates(
                    y.station_id)
                original_isd_data, original_isd_years = common.read_precip(
                    original_start_date,
                    original_end_date,
                    os.path.join(
                        common.DATA_BASE_DIR, 'processed_isd_data',
                        y.station_id + '.dat'
                    ))

                combined_isd_data = original_isd_data + split_isd_data

                percent_missing = get_isd.get_percent_missing(
                    combined_isd_data, matching_station.start_date_to_use,
                    y.end_date_to_use)

            else:
                percent_missing = get_isd.get_percent_missing(
                    split_isd_data, real_start_date, real_end_date)
            print(percent_missing)

            # use if < 25% missing data -- communication with Glenn Fernandez 1/5/2021
            if percent_missing < 25.0:
                sufficient_data.append(y)

                i_filename = os.path.join(
                        common.DATA_BASE_DIR, str(common.CURRENT_END_YEAR) + '_processed_isd_data',
                        y.station_id + '.dat')

                assert os.path.exists(i_filename)

                if 25 < float(y.latitude) < 53 and -125 < float(y.longitude) < -63:
                    fill_type = 'nldas'
                else:
                    fill_type = 'gldas'

                if fill_type == 'nldas':
                    try:
                        common_fill.nldas_routine(i_filename, y, 'isd', fill_isd_data.MISSING_VALUE, offset=False)
                    except ValueError as e:
                        if 'water' in e.args[0]:
                            common_fill.gldas_routine(i_filename, y, 'isd', fill_isd_data.MISSING_VALUE, offset=False)
                        else:
                            print(f'try again later -- {y.station_id}')
                            pass
                            # probably just an error on NLDAS side; try again later

                elif fill_type == 'gldas':
                    common_fill.gldas_routine(i_filename, y, 'isd', fill_isd_data.MISSING_VALUE, offset=False)

                if matching_station:
                    old_file = combine_old_new(y, matching_station, common.CURRENT_END_YEAR, 'isd')

                else:
                    copy_filled_combined(y, common.CURRENT_END_YEAR)

    return sufficient_data



def read_data(filename):

    with open(filename, 'r') as file:
        precip_data = file.readlines()

    return [item.split() for item in precip_data]


def calculate_annual_precip(station_id, year):

    filename = os.path.join(
        common.DATA_BASE_DIR,
        str(year) + '_combined_data',
        station_id + '.dat')

    if os.path.exists(filename):

        data = read_data(filename)

        precip_data = {}
        for item in data:
            year = int(item[1])
            if year not in precip_data:
                precip_data[year] = 0
            precip_data[year] += float(item[-1])

        return precip_data

    else:
        return None

def format_date(date):
    """
    Input: date as datetime
    Returns: date formatted for use in D4EM file
    """
    return f"'{date.year}/{date.month:02d}/{date.day:02d}'"


def process(updated_coops, updated_isds):
    new_d4em_processed_data = []
    with open(str(common.CURRENT_END_YEAR - 1) + '_D4EM_PREC_updated.txt', 'r') as file:
        d4em_data = file.readlines()

    split_data = [x.split('\t') for x in d4em_data[1:]]
    new_d4em_processed_data.append(d4em_data[0])

    updated_stations = updated_coops + updated_isds


    for updated_station in updated_stations:
        print(updated_station.station_id)
        try:
            match = [x for x in split_data if x[0] == updated_station.station_id][0]
            match_start = match[8].strip("'").split('/')
            match_start_date = datetime.datetime(int(match_start[0]),
                                int(match_start[1]),
                                int(match_start[2]))

            updated_station.start_date_to_use = match_start_date
        except IndexError:
            match = [updated_station.station_id, 'WdmFinal',
                     updated_station.station_id + '.dat', '1',
                     updated_station.latitude, updated_station.longitude,
                     'OBSERVED', 'PREC',
                     None, None, None, None,
                     updated_station.station_name + '\n']

        if updated_station.network == 'isd':
            if not os.path.exists(
                    os.path.join(common.DATA_BASE_DIR,
                                 str(common.CURRENT_END_YEAR) + '_combined_data',
                                 updated_station.station_id + '.dat')):
                if not os.path.exists(
                        os.path.join(common.DATA_BASE_DIR,
                                 'combined_data', updated_station.station_id + '.dat')):
                    continue

            try:
                real_start_date, real_end_date = get_isd.get_dates(
                                updated_station.station_id, year=common.CURRENT_END_YEAR)
                if real_end_date.month != 12:
                    real_end_date = datetime.datetime(real_end_date.year - 1, 12, 31)
                updated_station.end_date_to_use = real_end_date

            except:
                real_start_date = None
                real_end_date = None

            if real_start_date == real_end_date and real_start_date is not None:
                continue

            else:
                new_filename = os.path.join(
                        common.DATA_BASE_DIR,
                        str(common.CURRENT_END_YEAR) + '_processed_isd_data',
                        updated_station.station_id + '.dat')
                if os.path.exists(new_filename):
                    split_isd_data, isd_years = common.read_precip(
                        real_start_date,
                        real_end_date, new_filename
                        )
                else:
                    split_isd_data = []

            if match:
                try:
                    original_start_date, original_end_date = get_isd.get_dates(
                        updated_station.station_id)
                    if original_end_date.month != 12:
                        original_end_date = datetime.datetime(original_end_date.year - 1, 12, 31)

                    if original_start_date and original_end_date:
                        original_isd_data, original_isd_years = common.read_precip(
                            original_start_date,
                            original_end_date,
                            os.path.join(
                                common.DATA_BASE_DIR, 'processed_isd_data',
                                updated_station.station_id + '.dat'
                            ))
                        if not updated_station.end_date_to_use:
                            updated_station.end_date_to_use = original_end_date
                    else:
                        original_isd_data = []
                except:
                    original_isd_data = []

                combined_isd_data = original_isd_data + split_isd_data

                if original_isd_data and real_end_date:
                    percent_missing = get_isd.get_percent_missing(
                        combined_isd_data, original_start_date,
                        real_end_date)
                elif not real_end_date:
                    percent_missing = get_isd.get_percent_missing(combined_isd_data,
                    original_start_date, original_end_date)
                else:
                    percent_missing = get_isd.get_percent_missing(
                        combined_isd_data, real_start_date,
                        real_end_date
                    )
                print(percent_missing)
                pass

            if percent_missing > 25.0:
                continue


        if not updated_station.end_date_to_use:
            continue
        if not updated_station.start_date_to_use:
            continue
        length_of_record = (
            updated_station.end_date_to_use -
            updated_station.start_date_to_use).days / 365.25

        actual_years = range(
            updated_station.start_date_to_use.year,
            updated_station.end_date_to_use.year + 1)

        out_data = calculate_annual_precip(
            updated_station.station_id,
            common.CURRENT_END_YEAR)

        if out_data:
            xs = list(out_data.values())
            if any(x < 0 for x in xs) or len(out_data.values()) < 10:
                continue

            filtered_out_data = {
                k: v for k, v in out_data.items() if k in actual_years}
            annual_precip = sum(list(filtered_out_data.values()))/length_of_record

            match[4] = str(float(updated_station.latitude))
            match[5] = str(float(updated_station.longitude))
            match[8] = format_date(updated_station.start_date_to_use)
            match[9] = format_date(updated_station.end_date_to_use)
            match[10] = str(round(length_of_record, 2))
            match[-2] = str(round(annual_precip, 2))

            new_d4em_processed_data.append('\t'.join(match))


    with open(str(common.CURRENT_END_YEAR) + '_D4EM_PREC_updated.txt', 'w') as file:
        file.writelines(new_d4em_processed_data)


if __name__ == '__main__':
    make_directories(common.CURRENT_END_YEAR)

    updated_coop_stations = make_updated_coops()

    # we are at the mercy of their server
    # you may need to call this function multiple times but you don't
    # need to start from the beginning. You can start at, for example,
    # the COOP in index position 712 by calling the function
    # with update_coop_data(updated_coop_stations[712:])

    # to get the position of the most recent coop,
    # use:
    # for idx, x in enumerate(updated_coop_stations):
    #     if x.station_id == 'USC00218280':
    #         print(idx)

    update_coop_data(updated_coop_stations)

    updated_isds = update_isd_data()
    first_pass_isds = get_some_isds()

    process(updated_coop_stations, first_pass_isds)



