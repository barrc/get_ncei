import csv
import json
import os
import collections
import datetime
import time

from dateutil.relativedelta import relativedelta
from dataclasses import asdict

import common
import get_isd_stations # TODO move to common?


def check_all_zero():
    with open('all_zero.csv', 'r') as file:
        raw_data = file.readlines()

    data = [x.strip('\n') for x in raw_data]

    # how many have 99999
    end_in = [x for x in data if x[-5:] == '99999']
    not_end_in = [x for x in data if x[-5:] != '99999']

    return data


def check_data_exists(isd_stations):
    with open('data_exists.csv', 'r') as file:
        raw_data = file.readlines()

    stuff = {}
    ids = []
    for item in raw_data:
        id_ = item[0:11]
        stuff[id_] = json.loads(item[12:])
        ids.append(id_)

    return stuff

    # for x in stuff:
    #     for station in isd_stations:
    #         if station.station_id == x:
    #             print(station.station_name)
    #             print(stuff[x])
    #             print(print(station.start_date, station.end_date))
    #             print('\n')

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
                rounded_date = datetime.datetime(int(the_date[0]), int(the_date[1]),
                                                int(day_hour[0]), int(hour_minute[0]),
                                                0)

                if split_aa1[-1] == '5':
                    if not first_date:
                        first_date = rounded_date
                    last_date = rounded_date

        except KeyError:
            pass

    return (first_date, last_date)

def get_stations_with_matches(station_subset):
    first_groups = []
    second_groups = []
    for station in station_subset:
        first_groups.append(station.station_id[0:6])
        second_groups.append(station.station_id[-5:])

    first_group_counter = collections.Counter(first_groups)
    second_group_counter = collections.Counter(second_groups)

    multiple_first_groups = [x for x in first_group_counter.most_common() if x[1] > 1 and x[0] != '999999']
    multiple_second_groups = [x for x in second_group_counter.most_common() if x[1] > 1 and x[0] != '99999']

    flat_first_groups = [x[0] for x in multiple_first_groups]
    flat_second_groups = [x[0] for x in multiple_second_groups]

    matches = []
    for x in flat_first_groups:
        little_dict = {}
        little_dict[x] = []
        for y in isd_stations_with_some_data:
            if y.station_id[0:6] == x:
                little_dict[x].append(y.station_id)
        matches.append(little_dict)

    for x in flat_second_groups:
        little_dict = {}
        little_dict[x] = []
        for y in isd_stations_with_some_data:
            if y.station_id[-5:] == x:
                little_dict[x].append(y.station_id)
        matches.append(little_dict)

    return matches

def make_real_end_dates_file(all_isd_stations):
    # make file with real start/end dates -- RUN OVERNIGHT
    stations = []
    for item in all_isd_stations:
        if item in isd_stations_with_some_data:
            real_start_date, real_end_date = get_dates(item.station_id)
            item.start_date_to_use = real_start_date
            item.end_date_to_use = real_end_date
            stations.append(item)

    filename = os.path.join('src', 'isd_stations_to_use_with_real_dates.csv')
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(asdict(stations[0]).keys())
        for item in stations:
            writer.writerow(asdict(item).values())


def round_start_date(x):
    if x.start_date_to_use.month == 1:
        x.start_date_to_use = datetime.datetime(x.start_date_to_use.year, 1, 1)
    else:
        x.start_date_to_use = datetime.datetime(x.start_date_to_use.year + 1, 1, 1)

    return x

def round_end_date(x):
    if x.end_date_to_use.month == 12:
        x.end_date_to_use = datetime.datetime(x.end_date_to_use.year, 12, 31)
    else:
        x.end_date_to_use = datetime.datetime(x.end_date_to_use.year - 1, 12, 31)

    return x


def read_percent_missing():
    with open(os.path.join('src', 'percent_missing.csv'), 'r') as file:
        data = file.readlines()

    split_data = [item.strip('\n').split(',') for item in data]
    percent_missing = {x[0]: float(x[1]) for x in split_data}

    return percent_missing

if __name__ == '__main__':
    all_isd_stations = common.get_stations('isd')
    all_zero = check_all_zero()

    isd_stations_with_some_data = [x for x in all_isd_stations if x.station_id not in all_zero]

    split_basins_data = common.read_basins_file()
    basins_stations = common.make_basins_stations(split_basins_data)
    wban_basins = get_isd_stations.read_homr_codes()

    percent_missing = read_percent_missing()

    # if you need to make the real_end_dates file, call this function
    # make_real_end_dates_file(all_isd_stations)

    isd_stations_with_real_dates = common.get_stations('isd', '_with_real_dates')

    isd_stations_to_use = []
    for item in isd_stations_with_real_dates:
        if percent_missing[item.station_id] <= 25.0:
            isd_stations_to_use.append(item)

    prefix_full_ids = get_stations_with_matches(isd_stations_to_use)

    station_ids_that_match = []
    for item in prefix_full_ids:
        ids = list(item.values())[0]
        station_ids_that_match.append(ids[0])
        station_ids_that_match.append(ids[1])

    final_stations = []
    for item in isd_stations_to_use:
        if item.station_id not in station_ids_that_match:
            if not item.in_basins:
                item = round_start_date(item)
                item = round_end_date(item)

                if relativedelta(item.end_date_to_use, item.start_date_to_use).years >= 10:
                    final_stations.append(item)
            else:
                item = round_start_date(item)
                item = round_end_date(item)

                if item.end_date_to_use <= item.start_date_to_use:
                    pass

                else:
                    for x in basins_stations:
                        if x.station_id == wban_basins[item.station_id[-5:]]:
                            if (item.start_date_to_use - x.end_date_to_use).days == 1:
                                final_stations.append(item)
                            else:
                                item.break_with_basins = True
                                if relativedelta(item.end_date_to_use, item.start_date_to_use).years < 10:
                                    pass
                                else:
                                    final_stations.append(item)

    counter = 0
    for x in prefix_full_ids:
        stuff = [station for station in isd_stations_to_use if station.station_id in list(x.values())[0]]
        if stuff[0].start_date_to_use < stuff[1].start_date_to_use:
            earlier_station = stuff[0]
            later_station = stuff[1]
        else:
            earlier_station = stuff[1]
            later_station = stuff[0]

        # the stations in this category fall in four groups
        # 1. no gap; use both stations
        # if (later_station.start_date_to_use - earlier_station.end_date_to_use).days == 1:
        print(earlier_station.start_date_to_use)
        print(earlier_station.end_date_to_use)
        print(later_station.start_date_to_use)
        print(later_station.end_date_to_use)
        print('\n')

        counter += 1

        # 2. one station has only one day of data; other station has ???
        # if (stuff[1].end_date_to_use - stuff[1].start_date_to_use).days < 1:


        # 3. gap between two stations; earlier station has insufficient data

        # 4. overlappers


    print(f'counter -- {counter}')
        # # break

    filename = os.path.join('src', 'isd_stations_to_use_final.csv')
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(asdict(final_stations[0]).keys())
        for item in final_stations:
            writer.writerow(asdict(item).values())

        # if other_start_date == other_end_date:
        #     counter += 1
        # else:
        #     # if other_start_date > end_date:
        #     if start_date.year == other_start_date.year:
        #     #     earlier_station = stuff[0]
        #     #     later_station = stuff[1]
        #     # elif other_start_date < start_date:

        #         print(start_date, end_date)
        #         print(other_start_date, other_end_date)
        #         print('\n')
        #         other_counter += 1

    # print(f'counter: {counter}')
    # print(f'other_counter: {other_counter}')


        # break

    # for station in isd_stations_with_some_data:
    #     if station.station_id in station_ids_that_match:
    #         for item in prefix_full_ids:
    #             if station.station_id in list(item.values())[0]:
    #                 print(item)
    #                 print('\n')
    #         break





    #     print(station.station_id)
    #     #

    #     # print(start_date, end_date)
    #     # print(station.start_date, station.end_date)
    #     print('\n')





    # for i in [i[0] for i in multiple_first_groups]:
    #     # print(i)
    #     matches = [x for x in isd_stations_with_some_data if x.station_id[0:6] == i]
    #     assert len(matches) == 2
    #     # print(matches[0].station_id, matches[1].station_id)
    #     # print(matches[1].end_date_to_use, matches[0].start_date_to_use)

    #     if matches[1].start_date_to_use > matches[1].end_date_to_use:
    #         # print(i)
    #         counter += 1
    #     else:
    #         print(i)
    #         print(matches[0].start_date_to_use, matches[0].end_date_to_use)
    #         print(matches[1].start_date_to_use, matches[1].end_date_to_use)
    #         print('\n')

    # print(f'counter: {counter}')
    # print(f'len: {len(multiple_first_groups)}')

    # for x in isd_stations_with_some_data:
    #     stuff = (x.end_date_to_use - x.start_date_to_use).days/365.0
    #     start_dates.append(x.start_date_to_use)
    #     end_dates.append(x.end_date_to_use)
    #     if x.station_id[0:6] in [i[0] for i in multiple_first_groups]:


    # print(max(start_dates))
    # print(min(end_dates))

    # for x in second_group_counter.most_common():
    #     if x[1] > 1:
    #         print(x)

    # print(max(first_group_counter.values()))

    # isd_data = check_data_exists(isd_stations_to_use)

    # ids_ = [y.station_id for y in isd_stations_with_some_data]
    # short_ids = [id_[0:6] for id_ in ids_]
    # col_ = collections.Counter(short_ids)
    # overlap = [k for k, v in col_.items() if v > 1]
    # # print(overlap)

    # for z in isd_data:
    #     if z[0:6] in overlap:
    #         if z[0:6] == '722720':
    #             print(z)
    #             print(isd_data[z])
    #             print('\n')





    # for x in isd_stations_with_some_data:
    #     values = isd_data[x.station_id].values()
    #     non_zero_values = [x for x in values if x != 0]
    #     if len(non_zero_values) == 1:
    #         print(x.station_id)
    #         print(x.station_id)
    #         print(isd_data[x.station_id])
    #         print(x.start_date, x.end_date)
    #         print('\n')


    # filename = os.path.join('src', 'isd_stations_remove_all_zero.csv')
    # with open(filename, 'w', newline='') as file:
    #     writer = csv.writer(file)
    #     writer.writerow(asdict(isd_stations_with_some_data[0]).keys())
    #     for item in isd_stations_with_some_data:
    #         writer.writerow(asdict(item).values())

