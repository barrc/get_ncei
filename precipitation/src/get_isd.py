
import csv
import datetime
import json
import os
import requests
import statistics
import time

from dataclasses import asdict

import common
import get_isd_stations


#FLD LEN: 3
# LIQUID-PRECIPITATION occurrence identifier
# The identifier that represents an episode of LIQUID-PRECIPITATION.
# DOM: A specific domain comprised of the characters in the ASCII character set.
# AA1 - AA4 An indicator of up to 4 repeating fields of the following items:
# LIQUID-PRECIPITATION period quantity
# LIQUID-PRECIPITATION depth dimension
# LIQUID-PRECIPITATION condition code
# LIQUID-PRECIPITATION quality code

# "01,0000,9,5"

def get_raw_data(isd_station_id, start_date, end_date, year=None):
    start_date_string = f"{start_date.year:04d}-{start_date.month:02d}-{start_date.day:02d}"
    end_date_string = f"{end_date.year:04d}-{end_date.month:02d}-{end_date.day:02d}"

    url = 'https://www.ncei.noaa.gov/access/services/data/v1?dataset=global-hourly&' + \
        'dataTypes=AA1&stations=' + isd_station_id + '&startDate=' + start_date_string + \
        '&endDate=' + end_date_string + '&format=json&options=includeAttributes:false'

    r = requests.get(url)

    try:
        stuff = json.loads(r.content.decode())
    except json.decoder.JSONDecodeError:
        stuff = json.loads(r.content.decode() + ']')

    if year:
        raw_filename = os.path.join(
            common.DATA_BASE_DIR, str(common.CURRENT_END_YEAR) + '_raw_isd_data', isd_station_id + '.json')
    else:
        raw_filename = os.path.join(common.DATA_BASE_DIR, 'raw_isd_data', isd_station_id + '.json')

    out_json = json.dumps(stuff)
    with open(raw_filename, 'w') as file:
        file.write(out_json)


def get_dates(station_id, year=None):
    if year:
        filename = os.path.join(
            common.DATA_BASE_DIR, str(common.CURRENT_END_YEAR) + '_raw_isd_data', station_id + '.json')
    else:
        filename = os.path.join(common.DATA_BASE_DIR, 'raw_isd_data', station_id + '.json')

    with open(filename, 'r') as file:
        data = json.load(file)

    first_date = None
    last_date = None

    for item in data:
        try:
            item['AA1']
            split_aa1 = item['AA1'].split(',')
            if split_aa1[0] == '01':
                the_date = item['DATE'].split('-')
                day_hour = the_date[2].split('T')
                hour_minute = day_hour[1].split(':')
                actual_date = datetime.datetime(int(the_date[0]), int(the_date[1]),
                                                int(day_hour[0]), int(hour_minute[0]),
                                                int(hour_minute[1]))

                rounded_date = actual_date.replace(minute=0)

                if split_aa1[-1] == '5':
                    if not first_date:
                        first_date = rounded_date
                    last_date = rounded_date

        except KeyError:
            pass

    return (first_date, last_date)




def read_raw(station, year=None):

    station_id = station.station_id

    if year:
        filename = os.path.join(common.DATA_BASE_DIR, str(year) + '_raw_isd_data', station_id + '.json')
    else:
        filename = os.path.join(common.DATA_BASE_DIR, 'raw_isd_data', station_id + '.json')

    with open(filename, 'r') as file:
        data = json.load(file)

    date_dict = common.get_date_dict('9999', station.start_date_to_use, station.end_date_to_use)

    for item in data:
        try:
            item['AA1']
            split_aa1 = item['AA1'].split(',')
            if split_aa1[0] == '01':
                the_date = item['DATE'].split('-')
                day_hour = the_date[2].split('T')
                hour_minute = day_hour[1].split(':')
                actual_date = datetime.datetime(int(the_date[0]), int(the_date[1]),
                                                int(day_hour[0]), int(hour_minute[0]),
                                                int(hour_minute[1]))

                rounded_date = actual_date.replace(minute=0)

                # missing data typically are not in the JSON file at all
                # so you can't just do it in the parsing
                # (they say 99 is the value for missing data but I can't find any instances where that's true)

                if split_aa1[-1] == '5': # only records with QA flag = 5 (passed all quality checks) per Glenn Fernandez
                    precip_ = int(split_aa1[1])
                    if precip_ != 0:
                        precip = precip_/254.0
                    else:
                        precip = precip_ # keep 0 as int for later

                    if date_dict[rounded_date] == '9999':
                        date_dict[rounded_date] = precip
                    else:
                        date_dict[rounded_date] += precip

        except KeyError:
            pass

    if year:
        out_filename = os.path.join(common.DATA_BASE_DIR, str(year) + '_processed_isd_data', station_id + '.dat')
    else:
        os.path.join(common.DATA_BASE_DIR, 'processed_isd_data', station_id + '.dat')

    with open(out_filename, 'w') as file:

        to_file = ''
        for key, value in date_dict.items():
            if value != 0:
                if value == '9999':
                    out_value = value
                else:
                    out_value = str(round(value, 2))

                to_file += station_id + '\t' + str(key.year) + '\t' + \
                           str(key.month) + '\t' + str(key.day) + '\t' + \
                           str(key.hour) + '\t0\t' + out_value + '\n'
        file.write(to_file)

    return

def get_percent_missing(split_isd_data, dict_start_date, dict_end_date):
    date_dict = common.get_date_dict('0', dict_start_date, dict_end_date)

    for x in split_isd_data:
        date_dict[datetime.datetime(int(x[1]), int(x[2]), int(x[3]), int(x[4]))] = x[-1]

    missing = 0
    counter = 0
    for key, value in date_dict.items():
        if value == '9999':
            missing += 1
        counter += 1

    return missing/counter*100


if __name__ == '__main__':
    isd_stations_to_use = common.get_stations('isd_stations_to_try.csv')

    split_basins_data = common.read_basins_file()
    basins_stations = common.make_basins_stations(split_basins_data)

    wban_basins = get_isd_stations.read_homr_codes()

    data = []
    for item in isd_stations_to_use:
        if not os.path.exists(os.path.join(
                common.DATA_BASE_DIR, 'raw_isd_data',
                item.station_id + '.json')):
            print(item.station_id)
            get_raw_data(item.station_id, item.start_date, item.end_date)

        else:
            print(item.station_id)
            real_start_date, real_end_date = get_dates(item.station_id)

            if not real_start_date and not real_end_date:
                # all zero
                print(item.station_id)
                with open(os.path.join(
                    common.DATA_BASE_DIR, 'processed_isd_data',
                    item.station_id + '.dat'), 'w') as file:
                        file.write('')
            else:
                # assign real_start_date and real_end_date to station
                item.start_date_to_use = real_start_date
                item.end_date_to_use = real_end_date

                # if start_date is like 1/3 or end date is 12/28, can deal with that when we fill missing data

                read_raw(item)
                split_isd_data, isd_years = common.read_precip(
                    item.start_date_to_use,
                    item.end_date_to_use,
                    os.path.join(
                        common.DATA_BASE_DIR, 'processed_isd_data',
                        item.station_id + '.dat'))

                # get percent missing
                percent_missing = get_percent_missing(
                    split_isd_data, item.start_date_to_use, item.end_date_to_use)

                # use if < 25% missing data -- communication with Glenn Fernandez 1/5/2021
                if percent_missing < 25.0:
                    data.append(item)




    filename = os.path.join('src', 'isd_subset.csv')
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(asdict(data[0]).keys())
        for item in data:
            writer.writerow(asdict(item).values())





