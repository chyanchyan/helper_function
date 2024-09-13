import os
import traceback
import pandas as pd
import numpy as np
from openpyxl.utils import get_column_letter
import re

def calculate_length(cell_value):
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
    # 自适应列宽
    column_widths = (
        df.columns.to_series().apply(calculate_length).values
    )

    max_widths = (
        df.astype(str).applymap(calculate_length).agg(max).values
    )

    max_widths = pd.Series(max_widths).fillna(0).to_list()
    widths = np.max([column_widths, max_widths], axis=0)

    for i, width in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = max(width, 10)
