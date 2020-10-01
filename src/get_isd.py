import json
import os
import requests

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

def quick_check(other_s_id):
    url = 'https://www.ncei.noaa.gov/access/services/data/v1?dataset=global-hourly&' + \
        'dataTypes=AA1&stations=' + other_s_id + '&startDate=2019-01-01&endDate=2019-12-31' + \
        '&format=json&options=includeAttributes:false'

    r = requests.get(url)
    assert r.status_code == 200

    stuff = json.loads(r.content.decode())
    # print(json.loads(r.content.decode()))

    raw_filename = os.path.join('src', 'raw_isd_data', other_s_id + '.json')
    with open(raw_filename, 'w') as file:
        json.dump(stuff, file)

def read_raw(station_id):
    with open(os.path.join('src', 'raw_isd_data', station_id + '.json'), 'r') as file:
        data = json.load(file)

    report_types = [x['REPORT_TYPE'] for x in data]
    print(set(report_types))

    precips = []
    for item in data:
        try:
            item['AA1']
            split_aa1 = item['AA1'].split(',')
            if split_aa1[0] == '01':
                precips.append(int(item['AA1'].split(',')[1]))
            if split_aa1[1] == '0231':
                print(item)
        except:
            pass

    print(max(precips))


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
    # quick_check('72219013874')
    read_raw('72219013874')