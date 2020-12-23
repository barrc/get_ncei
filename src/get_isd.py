
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

def quick_check(isd_station, start_date, end_date):
    start_date_string = f"{start_date.year:04d}-{start_date.month:02d}-{start_date.day:02d}"
    end_date_string = f"{end_date.year:04d}-{end_date.month:02d}-{end_date.day:02d}"

    url = 'https://www.ncei.noaa.gov/access/services/data/v1?dataset=global-hourly&' + \
        'dataTypes=AA1&stations=' + isd_station + '&startDate=' + start_date_string + \
        '&endDate=' + end_date_string + '&format=json&options=includeAttributes:false'

    r = requests.get(url)

    try:
        stuff = json.loads(r.content.decode())
    except json.decoder.JSONDecodeError:
        stuff = json.loads(r.content.decode() + ']')

    raw_filename = os.path.join(common.DATA_BASE_DIR, 'raw_isd_data', isd_station + '.json')
    out_json = json.dumps(stuff)
    with open(raw_filename, 'w') as file:
        file.write(out_json)


def get_dates(station_id):
    with open(os.path.join(common.DATA_BASE_DIR, 'raw_isd_data', station_id + '.json'), 'r') as file:
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

def get_date_dict(first_date, last_date):

    delta = datetime.timedelta(hours=1)
    date_list = [first_date]

    iterate_date = first_date
    while iterate_date < last_date:
        iterate_date += delta
        date_list.append(iterate_date)

    return {key:'9999' for key in date_list}


def read_raw(station_id, first_date, last_date):
    with open(os.path.join(common.DATA_BASE_DIR, 'raw_isd_data', station_id + '.json'), 'r') as file:
        data = json.load(file)

    date_dict = get_date_dict(first_date, last_date)

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

                if split_aa1[-1] == '5': # TODO add comment
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

    with open(os.path.join(common.DATA_BASE_DIR, 'processed_isd_data', station_id + '.dat'), 'w') as file:

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


if __name__ == '__main__':
    isd_stations_to_use = common.get_stations('isd')

    split_basins_data = common.read_basins_file()
    basins_stations = common.make_basins_stations(split_basins_data)

    wban_basins = get_isd_stations.read_homr_codes()
    for item in isd_stations_to_use:
        # if os.path.exists(os.path.join(common.DATA_BASE_DIR, 'raw_isd_data', item.station_id + '.json')):
        #     pass
        # else:
        # if item.station_id == '91285021504':
            print(item.station_id)
            s_date = item.get_start_date_to_use(basins_stations, wban_basins)
            e_date = item.get_end_date_to_use(basins_stations, wban_basins)
            # quick_check(item.station_id, s_date, e_date)
            real_start_date, real_end_date = get_dates(item.station_id)
            read_raw(item.station_id, real_start_date, real_end_date)
            split_isd_data, isd_years = common.read_precip(s_date, e_date, os.path.join(common.DATA_BASE_DIR, 'processed_isd_data', item.station_id + '.dat'))


        # if all(x==0 for x in isd_years.values()):
        #     with open('all_zero.csv', 'a') as file:
        #         file.write(item.station_id)
        #         file.write('\n')
        # else:
        #     actually_use.append(item)



