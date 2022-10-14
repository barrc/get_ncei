import csv
import json
import os
import requests

import common


def get_specific_code(identifiers, id_type):
    id_types = [item['idType'] for item in identifiers]

    smo_id = None
    wban_id = None
    if id_type == 'COOP':
        for x in identifiers:
            if x['idType'] == 'WBAN':
                wban_id = x['id']
            elif x['idType'] == 'WMO':
                smo_id = x['id']

        return (smo_id, wban_id)

    elif id_type == 'WMO' or id_type == 'WBAN':
        for x in identifiers:
            if x['idType'] == 'COOP':
                return(x['id'])


def get_codes(station_id, id_type):
    base_url = 'https://www.ncdc.noaa.gov/homr/services/station/search?qid=' + id_type + ':'
    r = requests.get(base_url + station_id)
    try:
        assert r.status_code == 200
    except:
        return None

    data = json.loads(r.content)
    try:
        assert len(data['stationCollection']['stations']) == 1
    except:
        print(data['stationCollection']['stations'])

    out_codes = []
    for x in data['stationCollection']['stations']:
        identifiers = x['identifiers']
        out_codes.append(get_specific_code(identifiers, id_type))

    return out_codes


def quick_check(other_s_id):
    url = 'https://www.ncei.noaa.gov/access/services/data/v1?dataset=global-hourly&stations=' + other_s_id + '&startDate=1970-01-01&endDate=2019-12-31'
    print(url)
    r = requests.get(url)
    # print(r.status_code)
    # print(r.content)


# stationCollection -> stations -> 0 -> identifiers -> idType -> WBAN + WMO

def read_basins_not_in_chpd():
    stations = []
    with open(os.path.join('src', 'basins_not_in_chpd.csv'), 'r') as file:
        coop_reader = csv.reader(file)
        header = next(coop_reader)
        for row in coop_reader:
            if row[6] == 'True':
                in_basins = True
            else:
                in_basins = False
            if row[7] == 'True':
                break_with_basins = True
            else:
                break_with_basins = False
            stations.append(common.Station(row[0], row[1], row[2],
                            common.str_date_to_datetime(row[3]),
                            common.str_date_to_datetime(row[4]), row[5], row[6],
                            in_basins, break_with_basins))

    return stations

if __name__ == '__main__':
    with open('homr_codes.csv', 'w') as file:
        coop_not_in_chpd_stations = read_basins_not_in_chpd()
        for station in coop_not_in_chpd_stations:
            s_id = station.station_id
            print(s_id)
            out_code = get_codes(s_id, 'COOP')
            file.write(s_id)
            file.write(',')
            for x in out_code:
                file.write(str(x[0]))
                file.write(',')
                file.write(str(x[1]))
                file.write(',')
            file.write('\n')

