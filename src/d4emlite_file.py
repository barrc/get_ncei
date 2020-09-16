import os


def parse_d4em_lite():
    file_path = os.path.join('src', 'D4EMLite_PREC_Details.txt')
    assert os.path.exists(file_path)


if __name__ == '__main__':
    parse_d4em_lite()
