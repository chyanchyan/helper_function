"""
Excel操作辅助函数模块

该模块提供了Excel文件操作的辅助函数，包括列宽自适应等功能。
"""

import os
import traceback
import pandas as pd
import numpy as np
from openpyxl.utils import get_column_letter
import re

def calculate_length(cell_value):
    """
    计算单元格内容的显示长度
    
    根据字符类型计算不同的长度：
    - 英文或数字字符：长度 * 2
    - 中文字符：长度 * 2.4
    - 其他字符：长度 * 1
    
    Args:
        cell_value: 单元格内容
        
    Returns:
        float: 计算出的显示长度
    """
    length = 0
    for char in cell_value:
        # 判断是否为英文或数字
        if re.match(r'[a-zA-Z0-9]', char):
            length += 2  # 英文或数字字符长度 * 2
        # 判断是否为中文字符
        elif re.match(r'[\u4e00-\u9fff]', char):
            length += 2.4
        else:
            length += 1  # 其他字符按长度 1 计算
    return length


def fit_col_width(ws, df):
    """
    根据DataFrame内容自适应调整Excel工作表的列宽
    
    Args:
        ws: openpyxl工作表对象
        df: pandas DataFrame对象
    """
    # 计算列标题的宽度
    column_widths = (
        df.columns.to_series().apply(calculate_length).values
    )

    # 计算每列数据的最大宽度
    max_widths = (
        df.astype(str).applymap(calculate_length).agg(max).values
    )

    # 处理NaN值并转换为列表
    max_widths = pd.Series(max_widths).fillna(0).to_list()
    # 取列标题宽度和数据最大宽度的较大值
    widths = np.max([column_widths, max_widths], axis=0)

    # 设置每列的宽度，最小宽度为10
    for i, width in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = max(width, 10)
