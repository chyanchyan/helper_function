import os
from datetime import datetime as dt
from shutil import copy
if 'helper_function' in __name__.split('.'):
    from .hf_number import is_int
else:
    from hf_number import is_int


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


def get_last_snapshot_timestamp(folder):
    for root, dirs, files in os.walk(folder):
        sn_numbers = [item.split('_') for item in dirs]
        if len(sn_numbers) > 0:
            sn_numbers_int = [
                int(''.join([num for num in sn_number if is_int(num)]))
                for sn_number in sn_numbers
            ]
            latest_sn = dirs[sn_numbers_int.index(max(sn_numbers_int))]
            break
    else:
        print('no database snapshots')
        return

    return latest_sn
