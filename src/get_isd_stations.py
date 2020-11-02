import csv
import collections
import datetime
import os
import requests

from dataclasses import asdict
from dateutil.relativedelta import relativedelta

import common


BASEDIR = os.path.join(os.getcwd(), 'src')


def download_file():
    out_file = os.path.join(BASEDIR, 'isd-history.txt')

    r = requests.get('http://www1.ncdc.noaa.gov/pub/data/noaa/isd-history.txt')
    print(r.status_code)
    with open(out_file, 'wb') as file:
        file.write(r.content)


def read_file():

    with open(os.path.join(BASEDIR, 'isd-history.txt'), 'r',
              encoding='utf-8') as file:
        data = file.readlines()

    opening_lines = data[0:20]
    header = data[20:21]

    isd_data = []
    for item in data[22:]:
        # usaf is not a unique identifier
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


def look_at_isd_files(cutoff_start_year, wban_basins_mapping):

    ids = []
    station_names = []

    stations = []

    csv_filename = os.path.join(BASEDIR, 'parsed_isd_history.csv')
    with open(csv_filename, 'r') as csv_file:
        station_inv_reader = csv.reader(csv_file)
        for row in station_inv_reader:
            ids.append(row[0]+row[1])
            station_names.append(row[2])
            if row[4] != '  ': # state
                stations.append(common.Station(row[0] + row[1], row[2],
                                row[4], common.make_date(row[-2]),
                                common.make_date(row[-1]), row[6], row[7],
                                False, False))

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
        else:
            stations_first_pass.append(station)

    last_five = {}
    for item in stations_first_pass:
        if item.station_id[-5:] == '99999':
            pass
        elif item.station_id[-5:] not in last_five:
            last_five[item.station_id[-5:]] = [item]
        else:
            last_five[item.station_id[-5:]].append(item)

    counter = 0
    those = 0
    stations = []
    for count, item in enumerate(stations_first_pass):
        if item.station_id[-5:] in wban_basins_mapping:
            print(count, item)
            for x in basins_stations:
                if x.station_id == wban_basins_mapping[item.station_id[-5:]]:
                    # 1. If a station is in BASINS and is current, use the station
                    if item.end_date >= common.CUTOFF_END_DATE:
                        item.in_basins = True
                        stations.append(item)
                    # 2. If a station is in BASINS and is not current, but there is no gap
                    #    between the BASINS end date and the ISD start date,  use the
                    #    station as long as there is new data
                    elif item.start_date <= x.end_date and item.end_date >= x.end_date:
                        # example -- 70267526484, BASINS ID 507097
                        item.in_basins = True
                        stations.append(item)
                        print(item.start_date, x.end_date)
                    # 3. If a stations is in BASINS and there is a gap between the BASINS
                    #    end date and the C-HPD v2 start date, only use the station if there
                    #    are at least 10 years of data from C-HPD v2
                    else:
                        if item.end_date >= x.end_date:
                            item.in_basins = True
                            item.break_with_basins = True
                            print('oh no')
                            if relativedelta(item.end_date, item.start_date).years >= 10:
                                stations.append(item)
                    print('BASINS-')
                    print(x)
                    print('\n')

            those += 1
        else:
            # 4. If a station is not in BASINS, use the station if it has at least
            # 10 consecutive years of data since 1990
            if relativedelta(item.end_date, item.start_date).years >= 10:
                if relativedelta(item.end_date, common.EARLIEST_START_DATE).years >= 10:
                    stations.append(item)
                    counter += 1
    print(those)
    print(counter)

    filename = os.path.join('src', 'isd_stations_to_use.csv')
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(asdict(stations[0]).keys())
        for item in stations:
            writer.writerow(asdict(item).values())

    return stations_first_pass


def check_years(coops):
    counter = 0
    for item in coops:
        if item.end_date.year >= 2020:
            counter += 1

    print(counter)
    print(len(coops))


def read_debug(chpd_stations):
    with open(os.path.join('src', 'debug.csv'), 'r') as file:
        data = file.readlines()

    split_data = [item.strip('\n').split(',') for item in data]

    item_last = [item[-1] for item in split_data]
    print(collections.Counter(item_last))

    chpd_ids = [item.station_id[-6:] for item in chpd_stations]
    for item in split_data:

        if item[-1] == ' same':
            # print(item[0])
            if item[0][5] != '0':
                print(item)
        if item[-1] == ' different':
            if item[1] in chpd_ids:
                print('1 - ')
                print(item)
            if item[2] in chpd_ids:
                print('2 - ')
                print(item)


def read_homr_codes():
    with open('homr_codes.csv', 'r') as file:
        data = file.readlines()

    split_data = [item.strip('\n').split(',') for item in data]

    stuff = {}
    for item in split_data:
        codes = item[1:-1]
        if codes[1] != 'None':
            stuff[codes[1]] = item[0]

    return stuff


if __name__ == '__main__':

    wban_basins = read_homr_codes()

    # download_file()
    # out_isd_data = read_file()
    # parse_isd_data(out_isd_data)

    import get_one_coop
    chpd_stations = get_one_coop.get_stations()

    # import common
    split_basins_data = common.read_basins_file()
    basins_stations = common.make_basins_stations(split_basins_data)

    isd_stations = look_at_isd_files(common.CUTOFF_START_DATE.year, wban_basins)
    # print(isd_stations)
    # check_years(isd_stations)

    read_debug(chpd_stations)
