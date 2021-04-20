import csv
import os
from dataclasses import asdict
import datetime
from decimal import Decimal

import requests

import common


def download_station_inventory_file():
    """
    Downloads the station inventory file for COOP-HPD version 2 from NOAA

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
            stations.append(common.Station(row[0], row[5], row[4],
                                           common.make_date(por[0]),
                                           common.make_date(por[1]),
                                           row[1], row[2], False, False,
                                           'coop', None, None))

    return stations


def make_decimal(num):
    return round(Decimal(num), 2)


def check_lat_lon(basins, coops):
    # TODO docstring
    basins_ids = [item.station_id for item in basins]

    mismatched = 0
    for item in coops:
        if item.station_id[-6:] in basins_ids:
            for x in basins:
                if item.station_id[-6:] == x.station_id:
                    try:
                        i_lat = make_decimal(item.latitude)
                        x_lat = make_decimal(x.latitude)
                        i_lon = make_decimal(item.longitude)
                        x_lon = make_decimal(x.longitude)
                        assert i_lat == x_lat
                        assert i_lon == x_lon
                    except AssertionError:
                        if (abs(i_lat - x_lat) <= Decimal(0.02) and
                                abs(i_lon - x_lon) <= Decimal(0.02)):
                            pass
                        else:
                            mismatched += 1

    print(f'mismatched: {mismatched}')


def assign_in_basins_attribute(basins, coops):
    """
    Assigns in_basins attribute

    Iterates through all C-HPD Stations and determine if the station_id
    maches a station_id in the BASINS dataset

    Returns list of C-HPD stations with in_basins and
    break_with_basins attributes assigned
    """

    basins_ids = [item.station_id for item in basins]

    for coop in coops:
        if coop.station_id[-6:] in basins_ids:
            coop.in_basins = True
            for x in basins:
                if coop.station_id[-6:] == x.station_id:
                    if coop.start_date > x.end_date:
                        coop.break_with_basins = True
                        coop.start_date_to_use = common.get_start_date_to_use(coop)
                        coop.end_date_to_use = common.get_end_date_to_use(coop)
                    else:
                        coop.start_date_to_use = (
                            x.end_date + datetime.timedelta(days=1))
                        coop.end_date_to_use = common.get_end_date_to_use(coop)
        else:
            coop.start_date_to_use = common.get_start_date_to_use(coop)
            coop.end_date_to_use = common.get_end_date_to_use(coop)

    return coops





def get_coop_stations_to_use(coops):
    """
    Determines which C-HPD v2 stations to use

    Rules:
    1. If a station is not in BASINS, use the station if it has at least
       10 consecutive years of data since 2000
    2. If a station is in BASINS and there is no gap between the BASINS
       end date and the C-HPD v2 start date, use the station as long as
       there is new data
    3. If a stations is in BASINS and there is a gap between the BASINS
       end date and the C-HPD v2 start date, only use the station if there
       are at least 10 years of data from C-HPD v2

    """
    data = []

    # approximately 10 years
    ten_years = datetime.timedelta(days=3650)

    for item in coops:
        if not item.in_basins:
            # Rule 1
            if item.start_date_to_use >= common.EARLIEST_START_DATE:
                if item.end_date_to_use - item.start_date_to_use >= ten_years:
                   data.append(item)
        else:
            if not item.break_with_basins:
                # Rule 2
                if item.end_date_to_use > item.start_date_to_use:
                    data.append(item)
            else:
                # Rule 3
                if item.end_date_to_use - item.start_date_to_use >= ten_years:
                    data.append(item)

    return data


def check_conditions_handled(id_, coop_ids, expected_result):
    """
    Checks if station is included in/excluded from coops_to_use as expected
    Returns True if matches expected_result
    """
    if expected_result:
        return id_ in coop_ids
    else:
        return id_ not in coop_ids


def write_coop_stations_to_use(stations):
    """
    Writes file containing information about the C-HPD v2 stations
    that will be used
    """

    filename = os.path.join('src', 'coop_stations_to_use.csv')
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(asdict(stations[0]).keys())
        for item in stations:
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


def check_stations_handled_properly(data):
    """
    Tests whether stations are handled properly
    according to values of attributes in_basins and
    break_with_basins as well as if station is current

    No return value
    """

    data_ids = [x.station_id[-6:] for x in data]

    # 103732 -- in_basins, current, no break_with_basins --> use
    assert check_conditions_handled('103732', data_ids, True)

    # 106174 -- in_basins, not current, no break_with_basins --> use
    assert check_conditions_handled('106174', data_ids, True)

    # 121417 -- in_basins, current, break_with_basins, more than 10 years -->
    # use, but won't be appended to BASINS
    assert check_conditions_handled('121417', data_ids, True)

    # 059210 -- in_basins, not current, break_with_basins, less than 10 years
    # --> don't use
    assert check_conditions_handled('059210', data_ids, False)

    # 358717 -- not in_basins, current, more than 10 years --> use
    assert check_conditions_handled('358717', data_ids, True)

    # 212250 -- not in_basins, current, less than 10 years --> don't use -->
    # NOTE this station may become usable in future
    assert check_conditions_handled('212250', data_ids, False)

    # 419565 -- not in_basins, not current, more than 10 years --> use
    assert check_conditions_handled('419565', data_ids, True)


def get_basins_not_in_coop(basins, coops):
    coop_ids = [item.station_id[-6:] for item in coops]
    counter = 0

    filename = os.path.join('src', 'basins_not_in_chpd.csv')
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(asdict(basins[0]).keys())
        for item in basins:
            if item.station_id not in coop_ids:
                writer.writerow(asdict(item).values())


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
    coops_to_use = get_coop_stations_to_use(coop_stations)

    check_stations_handled_properly(coops_to_use)

    write_coop_stations_to_use(coops_to_use)

    # Exploratory functions
    # get_earliest_end_date(coop_stations)
    # get_latest_start_date(coop_stations)

    # check_lat_lon(basins_stations, coop_stations)

    # get_basins_not_in_coop(basins_stations, coop_stations)
