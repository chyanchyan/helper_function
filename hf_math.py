"""
数学与区间处理辅助函数模块

该模块提供了区间裁剪、数组运算、分组处理等数学相关的辅助函数。
主要功能包括：
1. 网格位置计算
2. 区间覆盖范围计算
3. 日期区间转换
4. 区间裁剪操作
5. 数组区间运算
6. 分组数据处理
7. 时间序列乘积计算
"""

import numpy as np
import pandas as pd
import itertools
from copy import copy, deepcopy
from functools import reduce

import sys
import os
# 获取当前文件所在目录
current_dir = os.path.dirname(os.path.abspath(__file__))
# 获取父目录
parent_dir = os.path.dirname(current_dir)

# 将父目录添加到系统路径中，以便导入同级模块
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# 导入自定义模块
from helper_function.hf_func import profile_line_by_line
from helper_function.hf_array import merge_timeseries

def grid_pos(n, width):
    """
    计算一维序号在二维网格中的行列位置
    
    Args:
        n: 一维序号（从0开始）
        width: 网格宽度（每行的列数）
    Returns:
        tuple: (行号, 列号)，行号和列号都从0开始
    """
    re = n // width, n % width
    return re


def strip_cover(stp_set_1, stp_set_2, freq=1):
    """
    计算两个区间集合的覆盖范围
    
    Args:
        stp_set_1: 区间集合1，元素为(start, end)元组
        stp_set_2: 区间集合2，元素为(start, end)元组
        freq: 步长，默认为1
    Returns:
        range: 覆盖所有区间的范围对象
    """
    # 找到所有区间的最小开始值和最大结束值
    min_st = min([item[0] for item in stp_set_1] + [item[0] for item in stp_set_2])
    max_ed = max([item[1] for item in stp_set_1] + [item[1] for item in stp_set_2])
    # 创建覆盖范围，考虑步长
    rg = range(int(min_st), int(max_ed // freq + ((max_ed % freq) > 0)), freq)

    return rg


def dates_to_sts_exps(dates):
    """
    将日期序列转换为区间序列
    
    Args:
        dates: 日期序列
    Returns:
        list: 区间列表，每个区间为(开始日期, 结束日期)
    """
    ds = list(dates)
    # 将相邻日期配对形成区间
    return list(zip(ds[:-1], ds[1:]))


def crop(p11, p12, p21, p22):
    """
    计算两个区间的交集
    
    Args:
        p11: 第一个区间的开始值
        p12: 第一个区间的结束值
        p21: 第二个区间的开始值
        p22: 第二个区间的结束值
    Returns:
        tuple: (交集开始值, 交集结束值)，如果没有交集则返回(None, None)
    """
    # 处理NaN值，将其转换为None
    if pd.isna(p11):
        p11 = None
    if pd.isna(p12):
        p12 = None
    if pd.isna(p21):
        p21 = None
    if pd.isna(p22):
        p22 = None

    # 如果某个值为None，则使用另一个区间的对应值
    if not p11:
        p11 = copy(p21)
    if not p12:
        p12 = copy(p22)
    if not p21:
        p21 = copy(p11)
    if not p22:
        p22 = copy(p12)

    # 计算交集
    if not p11 and not p21:
        p1 = None
    else:
        p1 = max([p11, p21])  # 交集开始值为两个开始值的最大值

    if not p12 and not p22:
        p2 = None
    else:
        p2 = min([p12, p22])  # 交集结束值为两个结束值的最小值

    # 检查交集是否有效
    if p1 and p2:
        if p1 > p2:
            return p2, p2  # 如果开始值大于结束值，返回空区间

    return p1, p2


def dir_crop(st1, ed1, st2, ed2):
    """
    计算两个有向区间的交集
    
    Args:
        st1: 第一个区间的开始值
        ed1: 第一个区间的结束值
        st2: 第二个区间的开始值
        ed2: 第二个区间的结束值
    Returns:
        tuple: (交集开始值, 交集结束值)，如果没有交集则返回(None, None)
    """
    # 检查两个区间的方向是否一致
    if (st1 - ed1) * (st2 - ed2) < 0:
        return None, None  # 方向不一致，无交集
    else:
        if ed1 >= st1:  # 正向区间
            return max([st1, st2]), min([ed1, ed2])
        else:  # 反向区间
            return min([st1, st2]), max([ed1, ed2])


def array_crop(sts, exps, st, exp):
    """
    计算数组中的区间与指定区间的交集长度
    
    Args:
        sts: 开始值数组
        exps: 结束值数组
        st: 指定区间的开始值
        exp: 指定区间的结束值
    Returns:
        numpy.ndarray: 每个区间与指定区间的交集长度
    """
    # 使用向量化操作计算交集长度
    res = (exps > st) * (  # 确保结束值大于开始值
        (
            (sts <= st) * (  # 区间开始值小于等于指定开始值
                (exps >= exp) * (exp - st) +  # 区间完全包含指定区间
                (exps < exp) * (exps - st)    # 区间部分包含指定区间
            )
        ) + (
            (
                (sts > st) *  # 区间开始值大于指定开始值
                (sts <= exp)  # 区间开始值小于等于指定结束值
            ) * (
                (exps >= exp) * (exp - sts) +  # 区间结束值大于等于指定结束值
                (exps < exp) * (exps - sts)    # 区间结束值小于指定结束值
            )
        )
    )

    return res


def nd_array_crop(sts, exps, sts_exps):
    """
    计算多个区间与多个指定区间的交集长度（多维数组版本）
    
    Args:
        sts: 开始值数组
        exps: 结束值数组
        sts_exps: 指定区间数组，形状为(n, 2)，每行为(start, end)
    Returns:
        numpy.ndarray: 交集长度矩阵，形状为(len(sts), len(sts_exps))
    """
    # 广播指定区间到所需形状
    bc_sts_exps = np.broadcast_to(sts_exps, (len(sts), len(sts_exps), 2)).T

    # 提取上下界
    lower_bound = bc_sts_exps[0, :, :]  # 下界矩阵
    upper_bound = bc_sts_exps[1, :, :]  # 上界矩阵
    # 广播开始值和结束值数组
    a_sts = np.broadcast_to(sts, (len(sts_exps), len(sts)))
    a_exps = np.broadcast_to(exps, (len(sts_exps), len(exps)))

    # 计算交集长度：min(上界, 结束值) - max(下界, 开始值)
    res = np.maximum(
        np.minimum(upper_bound, a_exps) -
        np.maximum(lower_bound, a_sts),
        np.zeros_like(lower_bound)  # 确保结果非负
    ).T

    return res


def array_minus(a, v):
    """
    从数组的累积和中减去指定值，返回调整后的数组
    
    Args:
        a: 输入数组
        v: 要减去的值
    Returns:
        numpy.ndarray: 调整后的数组
    """
    ca = np.cumsum(a)  # 计算累积和
    res = (ca > v) * a  # 保留累积和大于v的元素
    # 在第一个非零元素处添加调整值
    res[(res != 0).argmax()] += ((ca <= v) * a).sum() - v
    return res


@profile_line_by_line
def get_group_last_row_before_line(
        data: pd.DataFrame,
        index: str | list,
        line_col_name: str,
        line_values,
        lines
):
    """
    获取分组数据中每个分组在指定线值之前的最后一行
    
    Args:
        data: 输入数据框
        index: 分组列名或列名列表
        line_col_name: 用于比较的列名
        line_values: 当前行的线值
        lines: 要检查的线值列表
    Returns:
        numpy.ndarray: 布尔矩阵，标识每个分组在每个线值之前的最后一行
    """
    # 确保index是列表格式
    if isinstance(index, str):
        index = [index]
    # 按分组列和线值列排序
    data_ = data.sort_values(index + [line_col_name]).reset_index()
    lines_array = np.array(lines)
    # 创建比较矩阵
    a = line_values < lines_array
    data_[lines] = a
    # 获取每个分组在每个线值之前的最后一行索引
    idxs = [data_[data_[line_col_name] < line].groupby(index).idxmax() for line in lines]
    res = np.zeros(shape=a.shape)
    # 标记结果
    for c, idx in enumerate(idxs):
        res[idx['index'], c] = 1

    return res


def sumprod_timeseries(
        dfs,
        axis_cols,
        value_cols,
        output_col,
        index=None,
        output_axis_col=None
):
    """
    计算多个时间序列的乘积和
    
    Args:
        dfs: 数据框列表
        axis_cols: 轴列名列表
        value_cols: 值列名列表
        output_col: 输出列名
        index: 索引列名或列名列表
        output_axis_col: 输出轴列名
    Returns:
        pandas.DataFrame: 包含乘积和的结果数据框
    """
    # 处理索引参数
    if index is None:
        index_ = []
    elif isinstance(index, str):
        index_ = [index]
    else:
        index_ = list(index)

    # 合并时间序列
    res = merge_timeseries(
        dfs=dfs,
        index=index,
        axis_cols=axis_cols,
        mask_original_na='[na_mask]',
        output_axis_col=output_axis_col
    )

    # 排序并设置索引
    res = res.sort_values([*index_, output_axis_col]).set_index(index_)
    res = res.groupby(index_)
    # 前向填充缺失值
    res = res.ffill().reset_index()

    # 填充数值列为0
    res[[*index_, *value_cols, output_axis_col]] = res[[*index_, *value_cols, output_axis_col]].fillna(0)

    # 恢复原始缺失值标记
    res.replace('[na_mask]', np.nan, inplace=True)

    # 计算乘积
    res[output_col] = np.prod(res[value_cols].values, axis=1)

    return res

