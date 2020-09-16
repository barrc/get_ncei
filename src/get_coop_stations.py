import csv
import os
import requests
from dataclasses import asdict
from dateutil.relativedelta import relativedelta
from decimal import Decimal

import common

def download_station_inventory_file():
    """
    Downloads the station inventory file for COOP-HPD version 2 from the NOAA

    The station inventory file is updated on an irregular schedule,
    so the function also determines what the URL for the file should be
    Confirms the status_code is 200
    Writes the station inventory file to the src/ directory
    Returns the path to the station inventory file
    """

    station_inv_base_url = common.CHPD_BASE_URL + 'station-inventory/'
    r = requests.get(station_inv_base_url)
    raw_text = r.content.decode()
    start_index = raw_text.find('HPD_')
    end_index = raw_text.find('.csv')
    filename = raw_text[start_index:end_index + 4]

    r_file = requests.get(station_inv_base_url + filename)
    assert r_file.status_code == 200

    station_inv_file = os.path.join('src', filename)

    with open(station_inv_file, 'wb') as file:
        file.write(r_file.content)

    return station_inv_file


def read_coop_file(station_inv_file):
    """
    Reads the C-HPD version 2 station inventory file

    Returns a list of Station objects representing each entry in file
    """

    stations = []

    with open(station_inv_file, 'r') as csv_file:
        station_inv_reader = csv.reader(csv_file)
        header = next(station_inv_reader)
        for row in station_inv_reader:
            por = row[9].split('-')
            stations.append(common.Station(row[0][-6:], row[5],
                                           common.make_date(por[0]),
                                           common.make_date(por[1]),
                                           row[1], row[2], False, False))

    return stations


def check_codes(basins, coops):
    # TODO fix up / document this function
    basins_ids = [item.station_id for item in basins]

    exact_match = []
    close_enough = []
    oh_no = []
    not_in_basins = []

    differences = []
    for item in coops:
        if item.station_id in basins_ids:
            for x in basins:
                if item.station_id == x.station_id:
                    assert item.station_id == x.station_id
                    try:
                        assert item.station_name == x.station_name
                        exact_match.append(item)
                    except:
                        try:
                            assert item.station_name[0:4] == x.station_name[0:4]
                            close_enough.append(item)
                        except:  # TODO
                            oh_no.append(item)
                            # print(item.station_id)
                            # print(item.station_name + ',' + item.latitude + ',' + item.longitude)
                            # print(x.station_name + ',' + x.latitude + ',' + x.longitude)
                            # print(item.station_name, ',', x.station_name)
                            # print(item.latitude, ',', x.latitude)
                            # print(item.longitude, ',', x.longitude)
        else:
            not_in_basins.append(item)
            # print(item.station_name)
            differences.append(item.end_date - item.start_date)

    assert (len(exact_match) + len(close_enough) + len(oh_no) + len(not_in_basins)) == len(coop_stations)

def make_decimal(num, debug=False):
    # if debug:
    #     print(num, round(Decimal(num), 2))
    return round(Decimal(num), 2)

def check_lat_lon(basins, coops):
    basins_ids = [item.station_id for item in basins]

    mismatched = 0
    for item in coops:
        if item.station_id in basins_ids:
            for x in basins:
                if item.station_id == x.station_id:
                    try:
                        assert make_decimal(item.latitude) == make_decimal(x.latitude)
                        assert make_decimal(item.longitude) == make_decimal(x.longitude)
                    except:
                        # print(abs(make_decimal(item.latitude) - make_decimal(x.latitude)))
                        if abs(make_decimal(item.latitude) - make_decimal(x.latitude)) <= Decimal(0.02) and \
                            abs(make_decimal(item.longitude) - make_decimal(x.longitude) <= Decimal(0.02)):
                            pass
                        else:
                            print(item.latitude, item.longitude)
                            print(x.latitude, x.longitude)
                            print(item.station_name, ',' , x.station_name)
                            print(item.station_id)
                            print('\n')
                            mismatched += 1
                        # print(item.latitude, x.latitude, item.longitude, x.longitude)
                        # print(make_decimal(item.latitude, True), make_decimal(x.latitude, True),
                        #         make_decimal(item.longitude, True), make_decimal(x.longitude, True))

                        # return

    print(f'mismatched: {mismatched}')


def assign_in_basins_attribute(basins, coops):
    """
    Assigns in_basins attribute

    Iterates through all C-HPD Stations and determine if the station_id
    maches a station_id in the BASINS dataset

    Writes file break_with_basins to determine which stations have
    a break between the end of the BASINS reporting period and the
    beginning of the C-HPD v2 reporting period

    Returns list of C-HPD stations with in_basins attribute assigned
    """

    basins_ids = [item.station_id for item in basins]
    counter = 0

    header = ("station_id,station_name,BASINS_start_date,BASINS_end_date,"
              "COOP_start_date,COOP_end_date\n")

    with open(os.path.join('src', 'break_with_basins.csv'), 'w') as file:
        file.write(header)
        for item in coops:
            if item.station_id in basins_ids:
                item.in_basins = True
                for x in basins:
                    if item.station_id == x.station_id:
                        if item.start_date > x.end_date:
                            item.break_with_basins = True
                            to_file = item.station_id + ','
                            to_file += item.station_name + ','
                            to_file += (str(x.start_date.month) + '/' +
                                        str(x.start_date.day) + '/' +
                                        str(x.start_date.year) + ',')
                            to_file += (str(x.end_date.month) + '/' +
                                        str(x.end_date.day) + '/' +
                                        str(x.end_date.year) + ',')
                            to_file += (str(item.start_date.month) + '/' +
                                        str(item.start_date.day) + '/' +
                                        str(item.start_date.year) + ',')
                            to_file += (str(item.end_date.month) + '/' +
                                        str(item.end_date.day) + '/' +
                                        str(item.end_date.year))
                            to_file += '\n'
                            file.write(to_file)
                            counter += 1

    message = (f"There are {counter} stations with a gap between the end of "
               "BASINS and the start of the COOP record")
    print(message)

    return coops


def check_years(coops):
    """
    Determines which C-HPD v2 stations to use

    Rules:
    1. If a station is in BASINS and is current, use the station
    2. If a station is in BASINS and is not current, but there is no gap
       between the BASINS end date and the C-HPD v2 start date,  use the
       station as long as there is new data
    3. If a stations is in BASINS and there is a gap between the BASINS
       end date and the C-HPD v2 start date, only use the station if there
       are at least 10 years of data from C-HPD v2
    4. If a station is not in BASINS, use the station if it has at least
       10 consecutive years of data since 1990

    """
    coops_to_use = []

    for item in coops:
        if item.in_basins:
            # rule 2 - if no gap, and if new data, use station
            if not item.break_with_basins:
                if item.end_date.year < 2006:  # end year for BASINS
                    pass
                else:
                    coops_to_use.append(item)
            # rule 3 - if gap, only use if 10 years of data
            else:
                if relativedelta(item.end_date, item.start_date).years >= 10:
                    coops_to_use.append(item)

            # TODO finalize this
            # if item.start_date
            # if item.end_date <= common.CUTOFF_START_DATE:
            #     pass
            # else:
            #     coops_to_use.append(item)
        else:
            # if not in BASINS, use if >= ten years of data from recent past
            if item.station_id == '214546':
                print('debug')
            if relativedelta(item.end_date, item.start_date).years >= 10:
                coops_to_use.append(item)
            else:
                pass

    return coops_to_use


def check_conditions_handled(id, coop_ids, expected_result):
    """
    Checks if station is included in/excluded from coops_to_use as expected
    Returns True if matches expected_result
    """
    if expected_result:
        return id in coop_ids
    else:
        return id not in coop_ids


def write_coop_stations_to_use(dummy_station):

    filename = os.path.join('src', 'coop_stations_to_use.csv')
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(asdict(dummy_station[0]).keys())
        for item in dummy_station:
            writer.writerow(asdict(item).values())


def get_earliest_end_date(coops):
    """
    Prints earliest end date in C-HPD v2
    Currently informational only
    """

    end_dates = [item.end_date for item in coops]
    print(f'The earliest end date for a station is {min(end_dates)}')


def get_latest_start_date(coops):
    """
    Prints latest start date in C-HPD v2
    Currently informational only
    """
    start_dates = [item.start_date for item in coops]
    print(f'The latest start date for a station is {max(start_dates)}')

def check_stations_handled_properly(coops_to_use):

    coops_to_use_ids = [x.station_id for x in coops_to_use]
    # 332974 -- in_basins, current, no break_with_basins --> use
    assert check_conditions_handled('332974', coops_to_use_ids, True)
    # 106174 -- in_basins, not current, no break_with_basins --> use
    assert check_conditions_handled('106174', coops_to_use_ids, True)
    # 121417 -- in_basins, current, break_with_basins, more than 10 years --> use, but won't be appended to BASINS
    assert check_conditions_handled('121417', coops_to_use_ids, True)
    # 059210 -- in_basins, not current, break_with_basins, less than 10 years --> don't use
    assert check_conditions_handled('059210', coops_to_use_ids, False)
    # 358717 -- not in_basins, current, more than 10 years --> use
    assert check_conditions_handled('358717', coops_to_use_ids, True)
    # 212250 -- not in_basins, current, less than 10 years --> don't use --> NOTE this station may become usable in future
    assert check_conditions_handled('212250', coops_to_use_ids, False)
    # 419565 -- not in_basins, not current, more than 10 years --> use
    assert check_conditions_handled('419565', coops_to_use_ids, True)

if __name__ == '__main__':
    # If you have the most recent station inventory file, you can prevent
    # re-downloading that file by commenting out the
    # download_station_inventory_file() line and specifying the filepath
    # directly below

    station_inv = os.path.join('src', 'HPD_v02r02_stationinv_c20200909.csv')
    # station_inv = download_station_inventory_file()

    split_basins_data = common.read_basins_file()
    basins_stations = common.make_basins_stations(split_basins_data)
    coop_stations = read_coop_file(station_inv)

    coop_stations = assign_in_basins_attribute(basins_stations, coop_stations)
    coops_to_use = check_years(coop_stations)

    # TODO come up with a better way of formatting this
    check_stations_handled_properly(coops_to_use)

    write_coop_stations_to_use(coops_to_use)

    # Exploratory functions
    get_earliest_end_date(coop_stations)
    get_latest_start_date(coop_stations)

    # TODO fix this function
    # check_codes(basins_stations, coop_stations)

    # TODO write check_lat_lon function
    check_lat_lon(basins_stations, coop_stations)
