import datetime
import json
import os
import requests
import statistics

import common

# https://www.ncei.noaa.gov/access/services/data/v1?dataset=global-hourly&dataTypes="TMP,AA1"&stations=72219013874&startDate=2019-01-01&endDate=2019-12-31

# https://www.ncei.noaa.gov/access/services/data/v1?dataset=global-hourly&dataTypes=AA1&stations=72219013874&startDate=2019-01-01&endDate=2019-12-31&format=json&options=includeAttributes:false"


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

def quick_check(isd_station):
    url = 'https://www.ncei.noaa.gov/access/services/data/v1?dataset=global-hourly&' + \
        'dataTypes=AA1&stations=' + isd_station + '&startDate=1973-01-01&endDate=2019-12-31' + \
        '&format=json&options=includeAttributes:false'

    r = requests.get(url)
    assert r.status_code == 200

    stuff = json.loads(r.content.decode())
    print(json.loads(r.content.decode()))

    raw_filename = os.path.join('src', 'raw_isd_data', isd_station + '.json')
    with open(raw_filename, 'w') as file:
        json.dump(stuff, file)


# def get_data(coop_stations):
#     base_url = common.CHPD_BASE_URL + 'access/'

#     # for station in coop_stations:
#     station = coop_stations
#     the_url = base_url + station.station_id + '.csv'

#     r = requests.get(the_url)
#     print(r.content)
#     print(r.status_code)

#     out_file = os.path.join(RAW_DATA_DIR, station.station_id + '.csv')
#     with open(out_file, 'wb') as file:
#         file.write(r.content)

def read_raw(station_id):
    with open(os.path.join('src', 'raw_isd_data', station_id + '.json'), 'r') as file:
        data = json.load(file)

    report_types = [x['REPORT_TYPE'] for x in data]
    print(set(report_types))

    precips = {}
    precip_total = 0
    with open(os.path.join('src', 'processed_isd_data', station_id + '.dat'), 'w') as file:
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
                        precip_ = int(split_aa1[1])
                        if precip_ == 0:
                            pass
                        else:
                            precip = precip_/254.0
                            file.write('13874')
                            file.write('\t')
                            file.write(str(rounded_date.year))
                            file.write('\t')
                            file.write(str(rounded_date.month))
                            file.write('\t')
                            file.write(str(rounded_date.day))
                            file.write('\t')
                            file.write(str(rounded_date.hour))
                            file.write('\t0\t')
                            file.write(str(round(precip, 2)))
                            file.write('\n')
                            precip_total += precip

                    if rounded_date not in precips:
                        if split_aa1[-1] == '5':
                            precips[rounded_date] = [int(split_aa1[1])/254.0]
                    else:
                        print(rounded_date)
                        if split_aa1[-1] == '5':
                            precips[rounded_date].append(int(split_aa1[1])/254.0)
                        print(precips[rounded_date])

            except KeyError:
                pass

    #     new_precips = {}
    #     for item in precips:
    #         if len(precips[item]) != 1:
    #             print(item)
    #         new_precips[item] = statistics.mean(precips[item])

    # precip_sum = 0
    # for item in new_precips:
    #     precip_sum += new_precips[item]

    # print(precip_total)


        #
        # the_date = item['DATE'].split('-')
        # if the_date[0] == '2019' and the_date[1] == '12' and the_date[2][0:2] == '30':
        #     print(item)

    # FM-12 | SYNOP report of surface observation from a fixed land station
    # FM-15 | METAR aviation routine weather report
    # FM-16 | SPECI aviation selected special weather report
    # SY-MT | Synoptic and METAR merged report
    # SOM   | Summary of month report from U.S. ASOS or AWOS station
    # SOD   | Summary of day report from U.S. ASOS or AWOS station


if __name__ == '__main__':
    # thing = common.Station('72219013874')
    # quick_check('72219013874')
    # get_data('99999904236')
    read_raw('72219013874')