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

def quick_check(isd_station, start_date, end_date):
    start_date_string = f"{start_date.year:04d}-{start_date.month:02d}-{start_date.day:02d}"
    end_date_string = f"{end_date.year:04d}-{end_date.month:02d}-{end_date.day:02d}"

    url = 'https://www.ncei.noaa.gov/access/services/data/v1?dataset=global-hourly&' + \
        'dataTypes=AA1&stations=' + isd_station + '&startDate=' + start_date_string + \
        '&endDate=' + end_date_string + '&format=json&options=includeAttributes:false'

    # # url = 'https://www.ncei.noaa.gov/access/services/data/v1?dataset=global-hourly&' + \
    # #     'dataTypes=AA1&stations=' + isd_station + '&startDate=1970-01-01&endDate=2019-12-31' + \
    # #     '&format=json&options=includeAttributes:false'

    r = requests.get(url)
    assert r.status_code == 200

    try:
        stuff = json.loads(r.content.decode())
    except json.decoder.JSONDecodeError:
        stuff = json.loads(r.content.decode() + ']')

    raw_filename = os.path.join(common.DATA_BASE_DIR, 'raw_isd_data', isd_station + '.json')
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
    with open(os.path.join(common.DATA_BASE_DIR, 'raw_isd_data', station_id + '.json'), 'r') as file:
        data = json.load(file)

    report_types = [x['REPORT_TYPE'] for x in data]

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
                        # print(rounded_date)
                        if split_aa1[-1] == '5':
                            precips[rounded_date].append(int(split_aa1[1])/254.0)
                        # print(precips[rounded_date])

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
    isd_stations_to_use = common.get_stations('isd')




    # ids = ['99826799999','72543094822','72306899999','72320093801','99999923271','72637914845','72036799999','72475499999','72449013993','72531499999','72694024232','99817399999','99821199999','72572024127','72263023034','72253612911','72290023188','99999923272','72494523293','78535011630','72396593209','74505823277','72365623049','72495723213','72207003822','72513014777','99999994290','74793012843','99999992827','99999953152','72642504841','72267399999','72248013957','72281353146','72557014943','72651014944','72535014848','72312003870','72785024157','72439093822','74491514775','72440013995','72429563888','99999914761','72410599999','72492023237','74790013849','72519014771','72214093805','72211012842','99999903868','72341813977','72201292817','72536094830','72456013996','72409514792','72274023160','72356599999','72228693806','72681604110','72244813972','91182022521','72519794794','74781099999','99801199999','72255012912','72407513735','99999913762','72389699999','72256013959','72784624160','72217513860','99999913730','72548094910','72622794790','72646314897','72782594239','72427514894','72450003928','72351013966','72514014778','99800699999','72405399999','72319393807']

    # id_ = '72785024157'
    # id_ = '69019013910' # 19431201,20091231
    # quick_check(id_, datetime.datetime(1970, 1, 1), datetime.datetime(2009, 12, 31))
    # id_ = '69019099999'
    # quick_check('69019099999', datetime.datetime(2000, 1, 1), datetime.datetime(2004, 12, 30))
    # read_raw(id_)

    # id_ = '70197526422'
    # quick_check(id_, datetime.datetime(2006, 1, 1), datetime.datetime(2019, 12, 31))
    # read_raw(id_)
    split_basins_data = common.read_basins_file()
    basins_stations = common.make_basins_stations(split_basins_data)

    # TODO move read_precip to common
    from get_one_coop import read_precip
    from get_isd_stations import read_homr_codes
    wban_basins = read_homr_codes()
    for item in isd_stations_to_use:
        print(item.station_id)
        s_date = item.get_start_date_to_use(basins_stations, wban_basins)
        e_date = item.get_end_date_to_use(basins_stations, wban_basins)
        quick_check(item.station_id, s_date, e_date)
        read_raw(item.station_id)
        split_isd_data, isd_years = read_precip(s_date, e_date, os.path.join(common.DATA_BASE_DIR, 'processed_isd_data', item.station_id + '.dat'))
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




    # # check basins

    # # basins_dir = os.path.join('C:\\', 'Users', 'cbarr02', 'Desktop', 'swcalculator_home', 'data')
    # # basins_filename = os.path.join(basins_dir, 'TX' + '412244' + '.dat')

    # # split_basins_data, basins_years = read_precip(s_date, e_date, basins_filename)
    # # print(basins_years)

    # # from get_one_coop import plot_cumulative_by_year
    # # plot_cumulative_by_year(split_basins_data, split_isd_data, s_date.year, e_date.year)
