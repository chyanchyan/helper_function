"""
文件操作辅助函数模块

该模块提供了文件系统操作的辅助函数，包括目录创建、文件快照、时间戳处理等功能。
"""

from datetime import datetime as dt
from shutil import copy
import sys
import os

# 获取当前文件所在目录的绝对路径
current_dir = os.path.dirname(os.path.abspath(__file__))
# 获取父目录路径
parent_dir = os.path.dirname(current_dir)

# 将父目录添加到Python路径中，以便导入mint模块
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from helper_function.hf_number import is_int


def mkdir(path):
    """
    递归创建目录
    
    如果目录不存在，则创建目录。如果父目录不存在，会递归创建父目录。
    
    Args:
        path: 要创建的目录路径
    """
    while not os.path.exists(path):
        try:
            os.mkdir(path)
        except FileNotFoundError:
            # 如果父目录不存在，先创建父目录
            mkdir(os.path.dirname(path))
            os.mkdir(path)


def snapshot(src_path, dst_folder, auto_timestamp=True, comments=''):
    """
    创建文件快照
    
    将源文件复制到目标文件夹，可选择添加时间戳和注释到文件名中。
    
    Args:
        src_path: 源文件路径
        dst_folder: 目标文件夹路径
        auto_timestamp: 是否自动添加时间戳，默认为True
        comments: 文件名注释，默认为空字符串
    """
    if os.path.exists(src_path):
        # 确保目标文件夹存在
        mkdir(dst_folder)
        
        # 生成时间戳
        if auto_timestamp:
            time_str = dt.now().strftime('%Y%m%d_%H%M%S_%f')
        else:
            time_str = ''

        # 分离文件名和扩展名
        filename_base, ext = os.path.basename(src_path).split('.')

        # 构建目标文件名（包含时间戳和注释）
        dst_file_name_base = '_'.join([item for item in [filename_base, time_str, comments] if len(item) > 0])
        dst_file_name = f'{dst_file_name_base}.{ext}'
        dst_path = os.path.join(dst_folder, dst_file_name)

        # 复制文件
        copy(src=src_path, dst=dst_path)
    else:
        print(f'file: {src_path} is not exist.')


def get_last_snapshot_timestamp(folder):
    """
    获取最新的快照时间戳
    
    在指定文件夹中查找最新的快照目录，基于时间戳排序。
    
    Args:
        folder: 要搜索的文件夹路径
        
    Returns:
        str: 最新快照的时间戳字符串，如果没有找到则返回None
    """
    for root, dirs, files in os.walk(folder):
        # 将目录名按_分割，提取数字部分
        sn_numbers = [item.split('_') for item in dirs]
        if len(sn_numbers) > 0:
            # 提取每个目录名中的数字部分并转换为整数
            sn_numbers_int = [
                int(''.join([num for num in sn_number if is_int(num)]))
                for sn_number in sn_numbers
            ]
            # 找到最大的时间戳对应的目录
            latest_sn = dirs[sn_numbers_int.index(max(sn_numbers_int))]
            break
    else:
        print('no database snapshots')
        return

    return latest_sn
