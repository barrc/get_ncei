import datetime
import os

"""
This script was used to determine if stations in the original BASINS
dataset had complete years of missing data
"""

def get_missing_years(p_dir, p_file):
    with open(os.path.join(p_dir, p_file), 'r') as file:
        data = file.readlines()

    split_data = [item.split() for item in data]

    try:
        first_year = int(split_data[0][1])
        last_year = int(split_data[-1][1])
        year_range = list(range(first_year, last_year + 1))
    except:
        year_range = []

    years = [int(x[1]) for x in split_data]
    if len(set(years)) != len(year_range):
        set_years = set(years)
        set_year_range = set(year_range)
        print(precip_file)
        print(set_year_range - set_years)
        return

if __name__ == '__main__':
    precip_dir = os.path.join('C:\\', 'Users', 'cbarr02', 'Desktop', 'swcalculator_home', 'data')
    precip_files = os.listdir(precip_dir)

    for precip_file in precip_files:
        get_missing_years(precip_dir, precip_file)
