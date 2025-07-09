# -*- coding: utf-8 -*-
"""
数组和DataFrame处理工具函数模块

提供各种数组操作、DataFrame转换和时间序列合并等功能。
"""
from typing import List, Union, Optional, Tuple, Any
import numpy as np
import pandas as pd
from functools import reduce
from copy import copy


def value_change_and_change_to_merge(
    a_change: List[Union[float, int]], 
    a_change_to: List[Union[float, int]], 
    init_value: Union[float, int] = 0
) -> Tuple[List[Union[float, int]], List[Union[float, int]]]:
    """
    合并变化值和目标值数组
    
    将变化值数组和目标值数组进行合并处理，处理缺失值并计算累积值。
    
    Args:
        a_change: 变化值数组
        a_change_to: 目标值数组
        init_value: 初始值，默认为0
        
    Returns:
        Tuple[List, List]: 处理后的变化值数组和目标值数组
        
    Raises:
        ValueError: 当数组长度不匹配或存在缺失值时
    """
    if len(a_change) != len(a_change_to):
        raise ValueError(f'change length {len(a_change)} is not equal to change_to length {len(a_change_to)}')

    v0 = copy(init_value)
    re_a_change = []
    re_a_change_to = []
    
    for index, item in enumerate(a_change):
        if pd.isna(item):
            if pd.isna(a_change_to[index]):
                raise ValueError(f'missing value at index {index}')
            else:
                # 如果变化值为NaN，使用目标值计算变化量
                re_a_change.append(a_change_to[index] - v0)
                re_a_change_to.append(a_change_to[index])
                v0 = copy(a_change_to[index])
        else:
            # 正常情况：累加变化值
            v0 += item
            re_a_change.append(item)
            re_a_change_to.append(v0)

    return re_a_change, re_a_change_to



def prod(array: List[Union[float, int]]) -> Union[float, int]:
    """
    计算数组的乘积
    
    Args:
        array: 数值数组
        
    Returns:
        Union[float, int]: 数组元素的乘积
    """
    if not array:
        return 1
    return reduce(lambda x, y: x * y, array)


def get_crop_from_df(
    df: pd.DataFrame,
    anchor_x: int = 0,
    anchor_y: int = 1,
    vertical: bool = False,
    col_offset: int = -1,
    pk_index: Optional[int] = 0
) -> pd.DataFrame:
    """
    从DataFrame中裁剪指定区域的数据
    
    根据锚点位置和偏移量从DataFrame中提取指定区域的数据。
    
    Args:
        df: 源DataFrame
        anchor_x: X轴锚点位置
        anchor_y: Y轴锚点位置
        vertical: 是否垂直处理
        col_offset: 列偏移量
        pk_index: 主键索引位置，None表示不使用主键
        
    Returns:
        pd.DataFrame: 裁剪后的DataFrame
    """
    anchor_x -= 1

    if vertical:
        # 垂直处理时交换坐标
        anchor_x, anchor_y = anchor_y, anchor_x
        df_data = df.values.T
    else:
        df_data = df.values

    # 计算行数
    if pk_index is None:
        row_count = 1
    else:
        row_count = 0
        for r, value in enumerate(df_data[anchor_x:, anchor_y + pk_index]):
            if pd.isna(value):
                row_count = r
                break
            row_count = r + 1

    # 计算列数和列名
    cols = []
    col_count = 0
    for c, col_name in enumerate(df_data[anchor_x + col_offset, anchor_y:]):
        if pd.isna(col_name):
            col_count = c
            break
        cols.append(col_name)
    else:
        col_count = len(df_data[anchor_x + col_offset, anchor_y:])

    # 裁剪数据
    if vertical:
        crop_values = df.iloc[anchor_y: anchor_y + col_count, anchor_x: anchor_x + row_count].T
    else:
        crop_values = df.iloc[anchor_x: anchor_x + row_count, anchor_y: anchor_y + col_count]

    result = pd.DataFrame(crop_values)
    result.columns = cols
    result.reset_index(inplace=True, drop=True)
    return result


def flatten(lst: List[Any]) -> List[Any]:
    """
    扁平化嵌套列表
    
    递归地将嵌套列表转换为单层列表。
    
    Args:
        lst: 要扁平化的列表
        
    Returns:
        List[Any]: 扁平化后的列表
    """
    result = []
    for item in lst:
        if isinstance(item, list):
            result.extend(flatten(item))
        else:
            result.append(item)
    return result


def merge_timeseries(
    dfs: List[pd.DataFrame],
    index: Optional[Union[str, List[str]]],
    axis_cols: List[str],
    mask_original_na: Optional[Any] = None,
    output_axis_col: Optional[str] = None
) -> pd.DataFrame:
    """
    合并多个时间序列DataFrame
    
    将多个DataFrame按时间轴进行外连接合并。
    
    Args:
        dfs: DataFrame列表
        index: 索引列名或列名列表
        axis_cols: 每个DataFrame对应的轴列名
        mask_original_na: 用于填充缺失值的临时值，因为之后需要filldown
        output_axis_col: 输出轴列名，默认为'_axis_col'
        
    Returns:
        pd.DataFrame: 合并后的DataFrame
    """
    # 处理索引参数
    if index is None:
        index_ = []
    elif isinstance(index, str):
        index_ = [index]
    else:
        index_ = list(index)

    if output_axis_col is None:
        output_axis_col = '_axis_col'

    # 复制并预处理DataFrame
    dfs_copy = [df.copy().dropna(how='all') for df in dfs]
    
    for i, df in enumerate(dfs_copy):
        # 重命名轴列
        df.rename(columns={axis_cols[i]: output_axis_col}, inplace=True)

        if mask_original_na is not None:
            df.fillna(mask_original_na, inplace=True)
        df[output_axis_col] = df[output_axis_col].astype('string')

    # 合并DataFrame
    result = reduce(
        lambda x, y: pd.merge(x, y, on=[*index_, output_axis_col], how='outer'),
        dfs_copy
    )

    return result