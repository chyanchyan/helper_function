# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import copy
from functools import reduce
from copy import copy


def value_change_and_change_to_merge(a_change, a_change_to, init_value=0):
    if len(a_change) != len(a_change_to):
        print('change length %s is not = change_to length %s' % (len(a_change), len(a_change_to)))
        raise ValueError

    v0 = copy.copy(init_value)
    re_a_change = []
    re_a_change_to = []
    for index, item in enumerate(a_change):
        if pd.isna(item):
            if pd.isna(a_change_to[index]):
                print('missing value at index %s' % str(index))
                raise ValueError
            else:
                re_a_change.append(a_change_to[index] - v0)
                re_a_change_to.append(a_change_to[index])
                v0 = copy.copy(a_change_to[index])
        else:
            v0 += item
            re_a_change.append(item)
            re_a_change_to.append(v0)

    return re_a_change, re_a_change_to


def df_to_list_of_dict(df: pd.DataFrame):
    res = []
    for i, r in df.iterrows():
        res.append(r.to_dict())
    return res


def prod(array):
    return reduce(lambda x, y: x * y, array)


def get_crop_from_df(
        df: pd.DataFrame,
        anchor_x=0,
        anchor_y=1,
        vertical=False,
        col_offset=-1,
        pk_index=0
):
    row_count = 0
    cols = []

    anchor_x -= 1

    if vertical:
        anchor_x, anchor_y = anchor_y, anchor_x
        df_data = df.values.T
    else:
        df_data = df.values

    if pk_index is None:
        row_count = 1
    else:
        for r, value in enumerate(df_data[anchor_x:, anchor_y + pk_index]):
            if pd.isna(value):
                row_count = int(r)
                break
            else:
                row_count = int(r + 1)

    for c, col_name in enumerate(df_data[anchor_x + col_offset, anchor_y:]):
        if pd.isna(col_name):
            col_count = int(c)
            break
        else:
            cols.append(col_name)

    else:
        col_count = len(df_data[anchor_x + col_offset, anchor_y:])

    if vertical:
        crop_values = df.iloc[anchor_y: anchor_y + col_count, anchor_x: anchor_x + row_count].T
    else:
        crop_values = df.iloc[anchor_x: anchor_x + row_count, anchor_y: anchor_y + col_count]

    res = pd.DataFrame(crop_values)
    res.columns = cols
    res.reset_index(inplace=True, drop=True)
    return res


def flatten(l):
    res = []
    for item in l:
        if isinstance(item, list):
            res.extend(flatten(item))
        else:
            res.append(item)

    return res


def merge_timeseries(
        dfs,
        index,
        axis_cols,
        mask_original_na=None,
        output_axis_col=None
):
    if index is None:
        index_ = []
    elif isinstance(index, str):
        index_ = [index]
    else:
        index_ = list(index)

    if output_axis_col is None:
        output_axis_col = '_axis_col'

    dfs_copy = [df.copy().dropna(how='all') for df in dfs]
    for i, df in enumerate(dfs_copy):
        df.rename(columns={axis_cols[i]: output_axis_col}, inplace=True)

        if mask_original_na is not None:
            df.fillna(mask_original_na, inplace=True)
        df[output_axis_col] = df[output_axis_col].astype('string')

    res = reduce(
        lambda x, y: pd.merge(x, y, on=[*index_, output_axis_col], how='outer'),
        dfs_copy
    )


    return res