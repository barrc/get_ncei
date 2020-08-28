import csv
import collections
import datetime
import os
import requests

import common
from dataclasses import asdict
from dateutil.relativedelta import relativedelta

def download_station_inventory_file():
    station_inv_base_url = 'http://ncei.noaa.gov/data/coop-hourly-precipitation/v2/station-inventory/'
    r = requests.get(station_inv_base_url)
    raw_text = r.content.decode()
    start_index = raw_text.find('HPD_')
    end_index = raw_text.find('.csv')
    filename = raw_text[start_index:end_index + 4]

    r_file = requests.get(station_inv_base_url + filename)
    print(r_file.status_code)
    print(r_file.content)


def read_coop_file():
    station_inv_file = os.path.join(os.getcwd(), 'src', 'HPD_v02r02_stationinv_c20200723.csv')
    stations = []

    with open(station_inv_file, 'r') as csv_file:
        station_inv_reader = csv.reader(csv_file)
        header = next(station_inv_reader)
        for row in station_inv_reader:
            por = row[9].split('-')
            stations.append(common.Station(row[0][-6:], row[5], 
                                           common.make_date(por[0]), 
                                           common.make_date(por[1]), 
                                           row[1], row[2], False))

    return stations

def check_codes(basins, coops):
    coop_not_in_basins = 0
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
                        except:
                            oh_no.append(item)
                            print(item.station_id)
                            print(item.station_name + ',' + item.latitude + ',' + item.longitude)
                            print(x.station_name + ',' + x.latitude + ',' + x.longitude)
                            # print(item.station_name, ',', x.station_name)
                            # print(item.latitude, ',', x.latitude)
                            # print(item.longitude, ',', x.longitude)
        else:
            not_in_basins.append(item)
            # print(item.station_name)
            differences.append(item.end_date - item.start_date)

    assert (len(exact_match) + len(close_enough) + len(oh_no) + len(not_in_basins)) == len(coop_stations)

def assign_in_basins_attribute(basins, coops):
    basins_ids = [item.station_id for item in basins]

    for item in coops:
        if item.station_id in basins_ids:
            for x in basins:
                if item.station_id == x.station_id:
                    item.in_basins = True

    return coops


def check_years(coops):
    coops_to_use = []

    for item in coops:
        if item.in_basins:
            # TODO finalize this
            if item.end_date <= common.CUTOFF_START_DATE:
                pass
            else:
                coops_to_use.append(item)
        else:
            # if not in BASINS, use if at least ten years of data from some sort of recent past
            if relativedelta(item.end_date, item.start_date).years >= 10:
                coops_to_use.append(item)
            else:
                pass
                
    return coops_to_use
            
    


def write_one(dummy_station):

    with open(os.path.join(os.getcwd(), 'src', 'coop_stations_to_use.csv'), 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(asdict(dummy_station[0]).keys())
        for item in dummy_station:
            writer.writerow(asdict(item).values())

def check_end_date_of_overlap_stations(basins, coops):
    for item in coops:
        for x in basins:
            if item.station_id == x.station_id:
                if x.end_date <= datetime.datetime(2006, 12, 30):
                    if item.start_date >= datetime.datetime(2006, 1, 1):
                        print(item.station_id, item.station_name)
                        print(item.start_date, item.end_date, x.end_date)

def get_earliest_end_date(coops):
    end_dates = [item.end_date for item in coops]

    return min(end_dates)

def get_latest_start_date(coops):
    start_dates = [item.start_date for item in coops]

    return max(start_dates)

if __name__ == '__main__':

    # download_station_inventory_file()

    basins_stations = common.read_basins_file()
    coop_stations = read_coop_file()

    # below function used for exploration; station/code matches deemed acceptable
    # check_codes(basins_stations, coop_stations)

    coop_stations = assign_in_basins_attribute(basins_stations, coop_stations)
    coops_to_use = check_years(coop_stations)

    # get_earliest_end_date(coop_stations)
    # get_latest_start_date(coop_stations)

    # check_end_date_of_overlap_stations(basins_stations, coops_to_use)
    write_one(coops_to_use)