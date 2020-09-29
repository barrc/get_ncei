import json
import requests


def get_codes(station_id):
    base_url = 'https://www.ncdc.noaa.gov/homr/services/station/search?qid=COOP:'
    r = requests.get(base_url + station_id)
    assert r.status_code == 200

    data = json.loads(r.content)
    assert len(data['stationCollection']['stations']) == 1

    identifiers = data['stationCollection']['stations'][0]['identifiers']
    for x in identifiers:
        if x['idType'] == 'WBAN':
            wban_id = x['id']
        elif x['idType'] == 'WMO':
            smo_id = x['id']

    return (smo_id + '0' + wban_id)

def quick_check(other_s_id):
    url = 'https://www.ncei.noaa.gov/access/services/data/v1?dataset=global-hourly&stations=' + other_s_id + '&startDate=1970-01-01&endDate=2019-12-31'
    print(url)
    r = requests.get(url)
    # print(r.status_code)
    # print(r.content)


# stationCollection -> stations -> 0 -> identifiers -> idType -> WBAN + WMO

if __name__ == '__main__':
    # s_id = '090451'
    # get_codes(s_id)
    s_id = '457938'
    wban_smo = get_codes(s_id)
    quick_check(wban_smo)