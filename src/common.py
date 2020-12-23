import csv
import datetime
import os
from dataclasses import dataclass

CHPD_BASE_URL = 'http://ncei.noaa.gov/data/coop-hourly-precipitation/v2/'

# plan is to keep this date at 1990
EARLIEST_START_DATE = datetime.datetime(1990, 1, 1)

# set CURRENT_END_YEAR to the year you are in minus 1
CURRENT_END_YEAR = 2019
CURRENT_END_DATE = datetime.datetime(CURRENT_END_YEAR, 12, 31)
CUTOFF_START_DATE = datetime.datetime(CURRENT_END_YEAR - 10 + 1, 1, 1)
CUTOFF_END_DATE = datetime.datetime(CURRENT_END_YEAR, 12, 31)

DATA_BASE_DIR = os.path.join('L:\\', 'Public', 'cbarr02')

@dataclass
class Station:
    station_id: str
    station_name: str
    state: str
    start_date: datetime.datetime
    end_date: datetime.datetime
    latitude: str
    longitude: str
    in_basins: bool
    break_with_basins: bool
    network: str
    start_date_to_use: datetime.datetime
    end_date_to_use: datetime.datetime

    def get_start_date_to_use(self, basins, homr_codes=None):
        if self.in_basins:
            if self.network == 'coop':
                match = [x for x in basins if self.station_id[-6:] == x.station_id]
                assert len(match) == 1
                x = match[0]
            elif self.network == 'isd':
                related_id = homr_codes[self.station_id[-5:]]
                match = [x for x in basins if related_id == x.station_id]
                assert len(match) == 1
                x = match[0]
            if self.start_date <= x.end_date:
                return x.end_date + datetime.timedelta(days=1)
            else:
                return self.start_date

        else:
            if self.start_date <= EARLIEST_START_DATE:
                return EARLIEST_START_DATE
            else:
                return self.start_date

    def get_end_date_to_use(self, basins, homr_codes=None):
        # TODO does it matter if in basins?
        if self.end_date >= CUTOFF_END_DATE:
            return CUTOFF_END_DATE
        elif self.network == 'coop':
            # if coop, use last complete year
            if self.end_date.day == 31 and self.end_date.month == 12:
                return self.end_date
            else:
                return datetime.datetime(self.end_date.year - 1, 12, 31)
        else:
            # for ISD, use end_date, because may be adjacent to another station
            return self.end_date


def make_date(input_date):
    return datetime.datetime(int(input_date[0:4]),
                             int(input_date[4:6]),
                             int(input_date[6:]))


def make_basins_date(input_date):
    list_date = input_date.strip('\'').split('/')
    return datetime.datetime(int(list_date[0]),
                             int(list_date[1]),
                             int(list_date[2]))


def read_basins_file():
    """
    Reads BASINS file
    Returns a list of Station objects representing each entry in file
    """

    basins_file = os.path.join(os.getcwd(), 'src', 'D4EMLite_PREC_Details.txt')

    with open(basins_file, 'r') as file:
        data = file.readlines()

    split_data = [item.strip('\n').split('\t') for item in data]
    header = split_data.pop(0)

    return split_data


def make_basins_stations(data):

    stations = [Station(item[0], item[-1], item[2][0:2].upper(),
                        make_basins_date(item[8]),
                        make_basins_date(item[9]),
                        item[4], item[5], True, True, 'basins', make_basins_date(item[8]), make_basins_date(item[9]))
                for item in data]

    return stations


def str_date_to_datetime(str_date):
    x = str_date.split(' ')[0].split('-')
    return datetime.datetime(int(x[0]), int(x[1]), int(x[2]))


def get_stations(network, subset=False):
    stations = []
    if subset: # TODO get rid of this parameter
        filename = os.path.join('src', 'actually_use_isd_maybe.csv')
    else:
        filename = os.path.join('src', network + '_stations_to_use.csv')


    with open(filename, 'r') as file:
        coop_reader = csv.reader(file)
        header = next(coop_reader)
        for row in coop_reader:
            if row[7] == 'True':
                in_basins = True
            else:
                in_basins = False
            if row[8] == 'True':
                break_with_basins = True
            else:
                break_with_basins = False
            if row[10] and row[11]:
                stations.append(Station(row[0], row[1], row[2],
                            str_date_to_datetime(row[3]),
                            str_date_to_datetime(row[4]), row[5], row[6],
                            in_basins, break_with_basins, row[9],
                            str_date_to_datetime(row[10]),
                            str_date_to_datetime(row[11])))
            else:
                stations.append(Station(row[0], row[1], row[2],
                            str_date_to_datetime(row[3]),
                            str_date_to_datetime(row[4]), row[5], row[6],
                            in_basins, break_with_basins, row[9], None, None))

    return stations


def read_precip(start_date, end_date, station_file):

    with open(station_file, 'r') as file:
        data = file.readlines()

    split_data = [item.split() for item in data]
    years = dict.fromkeys(list(range(start_date.year, end_date.year + 1)), 0)
    for item in split_data:
        items_date = datetime.datetime(int(item[1]), int(item[2]), int(item[3]))
        if items_date > start_date:
            if items_date.year in years:
                if item[-1] == '-9999':
                    pass
                else:
                    years[items_date.year] += float(item[-1])

    return (split_data, years)