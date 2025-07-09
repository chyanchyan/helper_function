"""
辅助函数模块

该模块包含了各种辅助函数，用于数据处理、文件操作、数据库操作、数学计算等。
提供了项目中常用的工具函数集合。
"""

import sys
import os

# 获取当前文件所在目录的绝对路径
current_dir = os.path.dirname(os.path.abspath(__file__))
# 获取父目录路径
parent_dir = os.path.dirname(current_dir)

# 将父目录添加到Python路径中，以便导入mint模块
if parent_dir not in sys.path:
    sys.path.append(parent_dir)