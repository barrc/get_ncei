import datetime
import json
import os
import requests
import statistics

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
    assert r.status_code == 200

    try:
        stuff = json.loads(r.content.decode())
    except json.decoder.JSONDecodeError:
        stuff = json.loads(r.content.decode() + ']')

    raw_filename = os.path.join(common.DATA_BASE_DIR, 'raw_isd_data', isd_station + '.json')
    out_json = json.dumps(stuff)
    with open(raw_filename, 'w') as file:
        file.write(out_json)


def read_raw(station_id):
    with open(os.path.join(common.DATA_BASE_DIR, 'raw_isd_data', station_id + '.json'), 'r') as file:
        data = json.load(file)

    precips = {}
    precip_total = 0
    with open(os.path.join(common.DATA_BASE_DIR, 'processed_isd_data', station_id + '.dat'), 'w') as file:
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

                    # TODO handle missing data

                    if split_aa1[-1] == '5': # TODO add comment
                        precip_ = int(split_aa1[1])
                        if precip_ == 0:
                            pass
                        else:
                            precip = precip_/254.0
                            to_file = station_id + '\t'
                            to_file += str(rounded_date.year) + '\t'
                            to_file += str(rounded_date.month) + '\t'
                            to_file += str(rounded_date.day) + '\t'
                            to_file += str(rounded_date.hour) + '\t'
                            to_file += '0\t'
                            to_file += str(round(precip, 3)) + '\n'
                            file.write(to_file)
                            precip_total += precip

            except KeyError:
                pass


if __name__ == '__main__':
    isd_stations_to_use = common.get_stations('isd')

    split_basins_data = common.read_basins_file()
    basins_stations = common.make_basins_stations(split_basins_data)

    wban_basins = get_isd_stations.read_homr_codes()
    for item in isd_stations_to_use:
        if os.path.exists(os.path.join(common.DATA_BASE_DIR, 'raw_isd_data', item.station_id + '.json')):
            pass
        elif item.station_id == '72582794190':
            pass # TODO handle this station
        else:
            print(item.station_id)
            s_date = item.get_start_date_to_use(basins_stations, wban_basins)
            e_date = item.get_end_date_to_use(basins_stations, wban_basins)
            quick_check(item.station_id, s_date, e_date)
            read_raw(item.station_id)
            split_isd_data, isd_years = common.read_precip(s_date, e_date, os.path.join(common.DATA_BASE_DIR, 'processed_isd_data', item.station_id + '.dat'))
            print(isd_years)
            if all(x==0 for x in isd_years.values()):
                with open('all_zero.csv', 'a') as file:
                    file.write(item.station_id)
                    file.write('\n')
            else:
                with open('data_exists.csv', 'a') as file:
                    file.write(item.station_id)
                    file.write(',')
                    file.write(json.dumps(isd_years))
                    file.write('\n')

