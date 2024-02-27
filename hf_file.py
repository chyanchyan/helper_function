import os
from datetime import datetime as dt
from shutil import copy


def mkdir(path):
    while not os.path.exists(path):
        try:
            os.mkdir(path)
        except FileNotFoundError:
            mkdir(os.path.dirname(path))
            os.mkdir(path)


def snapshot(src_path, dst_folder, auto_timestamp=True, comments=''):
    if os.path.exists(src_path):
        mkdir(dst_folder)
        if auto_timestamp:
            time_str = dt.now().strftime('%Y%m%d_%H%M%S_%f')
        else:
            time_str = ''

        filename_base, ext = os.path.basename(src_path).split('.')

        dst_file_name_base = '_'.join([item for item in [filename_base, time_str, comments] if len(item) > 0])
        dst_file_name = f'{dst_file_name_base}.{ext}'
        dst_path = os.path.join(dst_folder, dst_file_name)

        copy(src=src_path, dst=dst_path)
    else:
        print(f'file: {src_path} is not exist.')