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


def make_zip(src_dir, dst_path):
    '''
    压缩文件夹下所有文件及文件夹
    默认压缩文件名：文件夹名
    默认压缩文件路径：文件夹上层目录
    '''

    z = zipfile.ZipFile(dst_path, 'w', zipfile.ZIP_DEFLATED)
    for dirpath, dirnames, filenames in os.walk(src_dir):
        fpath = dirpath.replace(src_dir, '')
        fpath = fpath and fpath + os.sep or ''
        for filename in filenames:
            z.write(os.path.join(dirpath, filename), fpath + filename)
    z.close()
    return True

def make_zip_from_pths(src_pths, dst_path):

    z = zipfile.ZipFile(dst_path, 'w', zipfile.ZIP_DEFLATED)
    for pth in src_pths:
        z.write(pth, pth)
    z.close()
    return True
