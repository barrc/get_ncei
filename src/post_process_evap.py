import os


def read_file():
    d4em_prec_file = os.path.join(os.getcwd(), 'D4EM_PREC_updated.txt')
    with open(d4em_prec_file, 'r') as file:
        data = file.readlines()

    return data

def process_data(data):

    evap_data = []
    for item in data:

        split_item = item.split('\t')
        if split_item[0] == 'StationId':
            pass
        else:
            split_name = split_item[2].split('.')
            local_id = split_name[0]

            split_item[2] = local_id + '.txt'
            split_item[7] = 'PMET'
            split_item[8] = '1990/01/01'
            split_item[9] = '2020/12/31'
            split_item[10] = '30.0' # TODO need to update when new data

        split_item.pop(11)
        evap_data.append('\t'.join(split_item))

    return evap_data

def write_pmet_file(data):
    with open('D4EM_PMET_updated.txt', 'w') as file:
        for row in data:
            file.write(str(row))


if __name__ == '__main__':
    d4em_prec_data = read_file()
    evap_data = process_data(d4em_prec_data)
    write_pmet_file(evap_data)

