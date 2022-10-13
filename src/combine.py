import datetime
import os

import common
import get_isd_stations
import check_isd_data

import shutil



BASINS_DIR = os.path.join(
    'C:\\', 'Users', 'cbarr02',
    'OneDrive - Environmental Protection Agency (EPA)',
    'Profile', 'Desktop', 'swcalculator_home', 'old_data')

def combine_coop(station):
    new_filename = os.path.join(common.DATA_BASE_DIR, 'filled_coop_data', station.station_id + '.dat')
    new_data = common.read_precip(None, None, new_filename)
    new_id = new_data[0][0][0]

    basins_filename = os.path.join(BASINS_DIR, station.state + station.station_id[-6:] + '.dat')
    try:
        basins_data = common.read_precip(None, None, basins_filename)
    except:
        return

    basins_id = basins_data[0][0][0]
    assert basins_id == new_id[-6:]

    # what do we do now?
    both_data = []
    for x in basins_data[0]:
        if int(x[1]) >= common.EARLIEST_START_DATE.year:
            x[0] = new_id
            both_data.append(x)

    for y in new_data[0]:
        both_data.append(y)

    return both_data

def combine_isd(station, b_id, b_start_date):
    new_filename = os.path.join(common.DATA_BASE_DIR, 'filled_isd_data', station.station_id + '.dat')
    new_data = common.read_precip(None, None, new_filename)
    try:
        new_id = new_data[0][0][0]
    except:
        return

    basins_filename = os.path.join(BASINS_DIR, station.state + b_id + '.dat')
    try:
        basins_data = common.read_precip(None, None, basins_filename)
    except:
        print(new_id)
        return

    if b_start_date.split('/')[1].strip("'") != '01':
        b_start_year = int(b_start_date.split('/')[0].strip("'")) + 1
    else:
        b_start_year = int(b_start_date.split('/')[0].strip("'"))

    both_data = []
    for x in basins_data[0]:
        if int(x[1]) >= common.EARLIEST_START_DATE.year and int(x[1]) >= b_start_year:
            x[0] = new_id
            both_data.append(x)

    for y in new_data[0]:
        both_data.append(y)

    return both_data


def combine_two_isds(first_isd, second_isd):

    first_isd_filename = os.path.join(common.DATA_BASE_DIR, 'filled_isd_data', first_isd.station_id + '.dat')
    first_isd_data = common.read_precip(None, None, first_isd_filename)
    first_id = first_isd_data[0][0][0]

    second_isd_filename = os.path.join(common.DATA_BASE_DIR, 'filled_isd_data', second_isd.station_id + '.dat')
    second_isd_data = common.read_precip(None, None, second_isd_filename)
    second_id = second_isd_data[0][0][0]

    if first_isd.start_date_to_use.month != 1:
        local_date = first_isd.start_date_to_use
        first_isd.start_date_to_use = datetime.datetime(
            local_date.year + 1, 1, 1
        )

    if first_isd.end_date_to_use > second_isd.start_date_to_use:
        local_date = second_isd.start_date_to_use - datetime.timedelta(days=1)
        first_isd.end_date_to_use = local_date

    both_data = []
    for x in first_isd_data[0]:
        new_local_date = datetime.datetime(int(x[1]), int(x[2]), int(x[3]))
        if (new_local_date.year >= common.EARLIEST_START_DATE.year and
                new_local_date >= first_isd.start_date_to_use and
                new_local_date <= first_isd.end_date_to_use):
            x[0] = second_id
            both_data.append(x)

    for y in second_isd_data[0]:
        both_data.append(y)

    with open('combined_isd_data.csv', 'a') as file:
        to_file = (f'{first_id},{first_station.start_date_to_use},'
                    f'{first_station.end_date_to_use},'
                    f'{second_id},{second_station.start_date_to_use},'
                    f'{second_station.end_date_to_use}\n')
        file.write(to_file)

    return both_data

def write_file(o_file, combined_data):

    with open(o_file, 'w') as file:
        for item in combined_data:
            to_file = f'{item[0]}\t{item[1]}\t{item[2]}\t{item[3]}\t{item[4]}\t{item[5]}\t{item[6]}\n'
            file.write(to_file)


if __name__ == '__main__':
    coop_stations_to_use = common.get_stations('coop_stations_to_use.csv')
    isd_stations_to_use = common.get_stations('isd_subset.csv')

    for station in coop_stations_to_use:
        filename = os.path.join(common.DATA_BASE_DIR, 'combined_data', station.station_id + '.dat')
        if station.in_basins and not station.break_with_basins:
            the_data = combine_coop(station)
            if the_data:
                write_file(filename, the_data)
            else:
                shutil.copyfile(os.path.join(
                    common.DATA_BASE_DIR, 'filled_coop_data', station.station_id + '.dat'), filename)
        else:
            shutil.copyfile(os.path.join(
                common.DATA_BASE_DIR, 'filled_coop_data', station.station_id + '.dat'), filename)


    wban_basins_mapping = get_isd_stations.read_homr_codes()
    prefix_full_ids = check_isd_data.get_stations_with_matches(isd_stations_to_use)
    basins_stuff = common.read_basins_file()

    station_ids_that_match = []
    for item in prefix_full_ids:
        ids = list(item.values())[0]
        station_ids_that_match.append(ids[0])
        station_ids_that_match.append(ids[1])

    for station in isd_stations_to_use:
        if (station.end_date_to_use - station.start_date_to_use).days == 0:
            isd_stations_to_use.remove(station)

    for station in isd_stations_to_use:
        filename = os.path.join(common.DATA_BASE_DIR, 'combined_data', station.station_id + '.dat')

        if not station.station_id in station_ids_that_match:
            if station.in_basins and not station.break_with_basins:
                basins_id = wban_basins_mapping[station.station_id[-5:]]
                relevant_basins = [x for x in basins_stuff if x[0] == basins_id]
                the_data = combine_isd(station, basins_id, relevant_basins[0][8])
                if the_data:
                    write_file(filename, the_data)
                else:
                    shutil.copyfile(os.path.join(
                        common.DATA_BASE_DIR, 'filled_isd_data', station.station_id + '.dat'), filename)
            else:
                shutil.copyfile(os.path.join(
                    common.DATA_BASE_DIR, 'filled_isd_data', station.station_id + '.dat'), filename)

    for item in prefix_full_ids:
        station_a = None
        station_b = None

        things = list(item.values())[0]
        for x in isd_stations_to_use:
            if x.station_id == things[0]:
                station_a = x
            elif x.station_id == things[1]:
                station_b = x

        if station_a and station_b:
            if station_a.start_date_to_use < station_b.start_date_to_use:
                first_station = station_a
                second_station = station_b
            else:
                first_station = station_b
                second_station = station_a

            filename = os.path.join(common.DATA_BASE_DIR, 'combined_data', second_station.station_id + '.dat')

            if (second_station.start_date_to_use - first_station.end_date_to_use).days > 1:
                # no combination but we do need to do something
                    shutil.copyfile(os.path.join(
                        common.DATA_BASE_DIR, 'filled_isd_data', station.station_id + '.dat'), filename)
            else:
                the_data = combine_two_isds(first_station, second_station)
                if the_data:
                    write_file(filename, the_data)
