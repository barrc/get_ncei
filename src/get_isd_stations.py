import csv
import datetime
import os

from dataclasses import asdict
from dateutil.relativedelta import relativedelta

import requests

import common


BASEDIR = os.path.join(os.getcwd(), 'src')


def download_file():
    """
    Downloads the station inventory file for ISD from NOAA

    The station inventory file is updated on an irregular schedule,
    Confirms the status_code is 200
    Writes the station inventory file to the src/ directory
    """
    out_file = os.path.join(BASEDIR, 'isd-history.txt')

    r = requests.get('http://www1.ncdc.noaa.gov/pub/data/noaa/isd-history.txt')

    assert r.status_code == 200

    with open(out_file, 'wb') as file:
        file.write(r.content)


def read_file():
    """
    Reads the ISD station inventory file

    Returns a list of tuples representing each entry in file
    """

    with open(os.path.join(BASEDIR, 'isd-history.txt'), 'r',
              encoding='utf-8') as file:
        data = file.readlines()

    opening_lines = data[0:20]
    header = data[20:21]

    isd_data = []
    for item in data[22:]:
        # neither USAF or WBAN are unique identifiers
        usaf = item[0:6]
        wban = item[7:12]
        station_name = item[13:42]
        country = item[43:47]
        state = item[48:50]
        call = item[51:56]
        lat = item[57:64]
        lon = item[65:73]
        elev_m = item[74:81]
        begin = item[82:90]
        end = item[91:].strip('\n')

        local_tup = (usaf, wban, station_name, country, state, call, lat, lon,
                     elev_m, begin, end)

        isd_data.append(local_tup)

    return isd_data


def parse_isd_data(isd_data):

    with open(os.path.join(BASEDIR, 'parsed_isd_history.csv'), 'w',
              encoding='utf-8') as file:
        for item in isd_data:
            if 'US' in item[3]:
                string_to_write = item[0] + ',' + item[1] + ',' + item[2].rstrip() + ',' + \
                    item[3].rstrip() + ',' + item[4] + ',' + item[5] + ',' + \
                    item[6] + ',' + item[7]+ ',' + item[8] + ',' + \
                    item[9] + ',' + item[10] + '\n'
                file.write(string_to_write)

def assign_in_basins_attribute(basins, isds, codes):
    """
    Assigns in_basins attribute

    Iterates through all ISD stations and determine if the station_id
    matches a station_id in the BASINS dataset using the
    WBAN codes from the HOMR API

    Returns list of ISD stations with in_basins and
    break_with_basins attributes assigned
    """

    # basins_ids = [item.station_id for item in basins]

    for isd in isds:
        if isd.station_id[-5:] in codes:
            isd.in_basins = True
            for x in basins:
                if codes[isd.station_id[-5:]] == x.station_id:
                    if isd.start_date > x.end_date:
                        isd.break_with_basins = True

    # Can't do dates here as we do for COOP here because some stations
    # have ID changes and will need to be combined

    return isds


def look_at_isd_files(wban_basins_mapping, basins_stations):

    stations = []

    csv_filename = os.path.join(BASEDIR, 'parsed_isd_history.csv')
    with open(csv_filename, 'r') as csv_file:
        station_inv_reader = csv.reader(csv_file)
        for row in station_inv_reader:
            if row[4] != '  ': # state
                stations.append(common.Station(row[0] + row[1], row[2],
                                row[4], common.make_date(row[-2]),
                                common.make_date(row[-1]), row[6], row[7],
                                False, False, 'isd', None, None))

    stations_first_pass = []
    # For C-HPD, we can easily determine which stations are in BASINS and apply
    # the four criteria at this point to determine which stations should be included.
    # Because determining if an ISD station is in BASINS requires the HOMR API,
    # we rule out some stations first to minimize API calls.
    for station in stations:
        # All stations with 'BUOY' in the station_name can be eliminated
        if 'BUOY' in station.station_name:
            pass
        # All stations that end before 1/1/1990 can be eliminated
        elif station.end_date < datetime.datetime(1990, 1, 1):
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

    # For ISD, we can definitively rule some stations out.
    # We cannot conclusively determine that we will be able to use a station
    # until we have obtained and parsed the precipitation data.
    # Start and end dates can be misleading, some stations have no
    # precipitation data, and some stations have very high percent missing.
    # But we do want to rule stations out first according to our rules.

    # First assign in_basins and break_with_basins
    isd_stations = assign_in_basins_attribute(basins, stations_first_pass, wban_basins_mapping)


    # 2. If a station is in BASINS and there is no gap between the BASINS
    #    end date and the C-HPD v2 start date, use the station as long as
    #    there is new data
    # 3. If a stations is in BASINS and there is a gap between the BASINS
    #    end date and the C-HPD v2 start date, only use the station if there
    #    are at least 10 years of data from C-HPD v2

    # approximately 10 years
    ten_years = datetime.timedelta(days=3650)

    data = []
    for isd in isd_stations:

        # 1. If a station is not in BASINS, use the station if it has at least
        # 10 consecutive years of data since 1990
        if not isd.in_basins:
            # Rule 1
            if isd.end_date - ten_years >= common.EARLIEST_START_DATE:
                if isd.end_date - isd.start_date >= ten_years:
                    data.append(isd)
        else:
            if not isd.break_with_basins:
                # Rule 2

                basins_station = [
                    x for x in basins_stations if x.station_id ==
                    wban_basins_mapping[isd.station_id[-5:]]][0]

                if isd.end_date <= basins_station.end_date:
                    pass
                elif isd.end_date > isd.start_date:
                    data.append(isd)
            else:
                # Rule 3
                if isd.end_date - isd.start_date >= ten_years:
                    data.append(isd)

                #
        if isd.start_date <= common.EARLIEST_START_DATE:
            isd.start_date = common.EARLIEST_START_DATE

    filename = os.path.join('src', 'isd_stations_to_try.csv')
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(asdict(stations[0]).keys())
        for item in data:
            writer.writerow(asdict(item).values())

    return data


def check_years(coops):
    counter = 0
    for item in coops:
        if item.end_date.year >= 2020:
            counter += 1


def read_homr_codes():
    with open('homr_codes.csv', 'r') as file:
        data = file.readlines()

    split_data = [item.strip('\n').split(',') for item in data]

    codes_dict = {}
    # make dict where key is WBAN and value is corresponding BASINS station
    # ignore USAF because not unique identifier (no last digit)
    for item in split_data:
        codes = item[1:-1]
        if codes[1] != 'None':
            codes_dict[codes[1]] = item[0]

    return codes_dict


def read_isd_inventory(station):
    with open(os.path.join('src', 'isd-inventory.csv'), 'r') as file:
        data = file.readlines()

    split_data = [item.strip('\n').split(',') for item in data]
    header = split_data.pop(0)
    print(header)
    print(split_data[0])

    local_usaf = station.station_id[0:6]
    local_wban = station.station_id[6:]
    print(local_usaf, local_wban)
    for x in split_data:
        if x[0] == local_usaf and x[1] == local_wban:
            available = [int(a) for a in x[3:]]
            if any(z > 24*30 for z in available):
                print(available)

if __name__ == '__main__':

    wban_basins = read_homr_codes()

    # download_file()
    out_isd_data = read_file()
    parse_isd_data(out_isd_data)

    # chpd_stations = common.get_stations('coop')

    split_basins_data = common.read_basins_file()
    basins = common.make_basins_stations(split_basins_data)

    isd_stations = look_at_isd_files(wban_basins, basins)
    # print(isd_stations)
    # check_years(isd_stations)




    # test = '72436313803'
    # test = '72219013874'
    # test = '99816999999'
    # for item in isd_stations:
    #     if item.station_id == test:
    #         a_station = item

    # read_isd_inventory(a_station)

