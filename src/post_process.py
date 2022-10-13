import os
import datetime

import common
import get_isd_stations


DATA_BASE_DIR = os.path.join('O:\\', 'PRIV', 'CPHEA', 'PESD',
                             'COR', 'Public', 'cbarr02')

DATA_PROCESSING_DIR = os.path.join(
    'C:\\', 'Users', 'cbarr02',
    'OneDrive - Environmental Protection Agency (EPA)', 'Profile', 'Desktop',
    'GitHub', 'Data-Processing-for-SWC-and-SWMM-CAT', 'resources')

def read_data(filename):

    with open(filename, 'r') as file:
        precip_data = file.readlines()

    return [item.split() for item in precip_data]

def calculate_annual_precip(station_id):

    filename = os.path.join(
        DATA_BASE_DIR,
        'combined_data',
        station_id + '.dat')

    if os.path.exists(filename):

        data = read_data(filename)

        precip_data = {}
        for item in data:
            year = int(item[1])
            if year not in precip_data:
                precip_data[year] = 0
            precip_data[year] += float(item[-1])

        return precip_data

    else:
        return None


def read_file():
    d4em_file = os.path.join(DATA_PROCESSING_DIR, 'D4EMLite_PREC_Details.txt')
    with open(d4em_file, 'r') as file:
        data = file.readlines()

    return data

def get_date(str_date):
    local_date = str_date.strip("'").split('/')
    return datetime.datetime(int(local_date[0]),
                             int(local_date[1]),
                             int(local_date[2]))

def read_combined_isd():
    with open('combined_isd_data.csv', 'r') as file:
        data = file.readlines()

    split_data = [item.strip('\n').split(',') for item in data]
    return split_data

if __name__ == '__main__':

    d4em_data = read_file()
    d4em_processed_data = []
    d4em_processed_data.append(d4em_data[0])

    original_basins_data = common.read_basins_file()
    coop_stations = common.get_stations('coop_stations_to_use.csv')
    isd_stations = common.get_stations('isd_subset.csv')
    wban_basins_mapping = get_isd_stations.read_homr_codes()

    combined_isd = read_combined_isd()
    combined_isd_ids = []
    for x in combined_isd:
        combined_isd_ids.append(x[0])
        combined_isd_ids.append(x[3])

    for item in d4em_data[1:]:
        split_item = item.split('\t')
        split_name = split_item[2].split('.')
        local_id = split_name[0][2:]

        matching_station = [x for x in coop_stations if x.station_id == local_id]
        if not matching_station:
            matching_station = [x for x in isd_stations if x.station_id == local_id]

        assert len(matching_station) <= 1

        new_file_name = local_id + '.dat'
        out_data = calculate_annual_precip(split_name[0][2:])

        try:
            assert out_data
        except:
            # print(f'no data for {split_name[0][2:]}')
            continue

        if out_data:
            xs = list(out_data.values())
            if any(x < 0 for x in xs) or len(out_data.values()) < 10:
                pass

            else:
                if matching_station:
                    if matching_station[0].in_basins and not matching_station[0].break_with_basins:
                        if matching_station[0].network == 'coop':
                            the_basins = [x for x in original_basins_data if
                                matching_station[0].station_id[-6:] == x[0]]
                        elif matching_station[0].network == 'isd':
                            basins_id = wban_basins_mapping[matching_station[0].station_id[-5:]]
                            the_basins = [x for x in original_basins_data if x[0] == basins_id]

                        original_basins_start = the_basins[0][8].strip("'")
                        if original_basins_start[5:7] == '01':
                            original_basins_start_date = datetime.datetime(
                                int(original_basins_start[0:4]),
                                int(original_basins_start[5:7]),
                                int(original_basins_start[9:10])
                            )
                        else:
                            original_basins_start_date = datetime.datetime(
                                int(original_basins_start[0:4]) + 1,
                                1, 1)
                        if original_basins_start_date <= common.EARLIEST_START_DATE:
                            true_start_date = common.EARLIEST_START_DATE

                        else:
                            true_start_date = original_basins_start_date
                            # true_start_date = get_date(split_item[8])
                        end_date = get_date(split_item[9])
                        if end_date.month != 12:
                            end_date = datetime.datetime(end_date.year - 1, 12, 31)

                    elif local_id in combined_isd_ids:
                        relevant_row = [y for y in combined_isd if local_id in y][0]
                        true_start_date = datetime.datetime(
                            int(relevant_row[1][0:4]), int(relevant_row[1][5:7]), int(relevant_row[1][8:10]))
                        end_date = datetime.datetime(
                            int(relevant_row[-1][0:4]), int(relevant_row[-1][5:7]), int(relevant_row[-1][8:10]))

                    else:
                        # true_start_date = get_date(split_item[8])
                        # end_date = get_date(split_item[9])
                        if matching_station[0].start_date_to_use.month != 1:
                            true_start_date = datetime.datetime(
                                matching_station[0].start_date_to_use.year + 1, 1, 1)
                        else:
                            true_start_date = matching_station[0].start_date_to_use
                        if matching_station[0].end_date_to_use.month != 12: # TODO why are we not getting here???
                            end_date = datetime.datetime(matching_station[0].end_date_to_use.year - 1, 12, 31)
                        else:
                            end_date = matching_station[0].end_date_to_use

                length_of_record = (end_date - true_start_date).days / 365.25

                if length_of_record < 10:
                    pass

                else:
                    actual_years = range(
                        true_start_date.year, end_date.year + 1)
                    filtered_out_data = {
                        k: v for k, v in out_data.items() if k in actual_years}
                    annual_precip = sum(list(filtered_out_data.values()))/length_of_record

                    split_item[2] = new_file_name
                    split_item[8] = f"'{true_start_date.year}/{true_start_date.month:02d}/{true_start_date.day:02d}'"
                    split_item[9] = f"'{end_date.year}/{end_date.month:02d}/{end_date.day:02d}'"
                    split_item[10] = str(round(length_of_record, 2))
                    split_item[-2] = str(round(annual_precip, 2))
                    d4em_processed_data.append('\t'.join(split_item))

    # Used to create D4EM file for initial update
    with open('2020_D4EM_PREC_updated.txt', 'w') as file:
        for row in d4em_processed_data:
            file.write(str(row))

