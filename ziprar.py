import os

import zipfile
import rarfile


def rename(pwd: str, file_name=''):
    """压缩包内部文件有中文名, 解压后出现乱码，进行恢复"""

    path = f'{pwd}/{file_name}'
    if os.path.isdir(path):
        for file in os.scandir(path):
            rename(path, file.name)
    new_name = file_name.encode('cp437').decode('gbk')
    os.rename(path, f'{pwd}/{new_name}')


def unzip_unrar(file_path: str, replace_if_exist=False):
    sp = file_path.split('.')
    dir_name = '.'.join(sp[:-1])
    ext = sp[-1]

    try:
        if ext == 'zip':
            file = zipfile.ZipFile(file_path)
        elif ext == 'rar':
            file = rarfile.RarFile(file_path)
        else:
            print('目前不支持该压缩文件格式 %s' % ext)
            return

        # 检查是否存在, 并判断是否覆盖
        if os.path.exists(dir_name) and not replace_if_exist:
            return
        else:
            os.mkdir(dir_name)
            file.extractall(dir_name)
            file.close()

            # 递归修复编码
            rename(dir_name)
    except:
        print(f'{file_path} unzip\\rar fail')
