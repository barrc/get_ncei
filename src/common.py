import datetime
from collections import namedtuple
from dataclasses import dataclass
import os

CUTOFF_START_YEAR = 2010
CUTOFF_START_DATE = datetime.datetime(CUTOFF_START_YEAR, 1, 1)

CUTOFF_END_YEAR = 2019
CUTOFF_END_DATE = datetime.datetime(CUTOFF_END_YEAR, 12, 31)


@dataclass
class Station:
    station_id: str
    station_name: str
    start_date: datetime.datetime
    end_date: datetime.datetime
    latitude: str
    longitude: str
    in_basins: bool

    def get_dates_to_use(self):
        if self.in_basins:
            print('cat!')

# class Station:
#     def __init__(self, station_id, station_name, start_date, end_date, latitude, longitude):
#         self.station_id = station_id
#         self.station_name = station_name
#         self.start_date = start_date
#         self.end_date = end_date
#         self.latitude = latitude
#         self.longitude = longitude
#         self.in_basins = False

# Station = namedtuple('Station', ['station_id', 'station_name', 'start_date', 
#                                  'end_date', 'latitude', 'longitude'])

def make_date(input_date):
    return datetime.datetime(int(input_date[0:4]), int(input_date[4:6]), int(input_date[6:]))

def make_basins_date(input_date):
    list_date = input_date.strip('\'').split('/')
    return datetime.datetime(int(list_date[0]), int(list_date[1]), int(list_date[2]))



def read_basins_file():
    basins_file = os.path.join(os.getcwd(), 'src', 'D4EMLite_PREC_Details.txt')

    with open(basins_file, 'r') as file:
        data = file.readlines()

    split_data = [item.strip('\n').split('\t') for item in data]
    header = split_data.pop(0)


    stations = [Station(item[0], item[-1], make_basins_date(item[8]), make_basins_date(item[9]), 
                        item[4], item[5], True) for item in split_data]

    return stations