import numpy as np
import pandas as pd
import itertools
from copy import copy, deepcopy
from functools import reduce

import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

if parent_dir not in sys.path:
    sys.path.append(parent_dir)


from helper_function.hf_func import profile_line_by_line
from helper_function.hf_array import merge_timeseries

def grid_pos(n, width):
    re = n // width, n % width
    return re


def strip_cover(stp_set_1, stp_set_2, freq=1):
    min_st = min([item[0] for item in stp_set_1] + [item[0] for item in stp_set_2])
    max_ed = max([item[1] for item in stp_set_1] + [item[1] for item in stp_set_2])
    rg = range(int(min_st), int(max_ed // freq + ((max_ed % freq) > 0)), freq)

    return rg


def dates_to_sts_exps(dates):
    ds = list(dates)
    return list(zip(ds[:-1], ds[1:]))


def crop(p11, p12, p21, p22):
    if pd.isna(p11):
        p11 = None
    if pd.isna(p12):
        p12 = None
    if pd.isna(p21):
        p21 = None
    if pd.isna(p22):
        p22 = None

    if not p11:
        p11 = copy(p21)
    if not p12:
        p12 = copy(p22)
    if not p21:
        p21 = copy(p11)
    if not p22:
        p22 = copy(p12)

    if not p11 and not p21:
        p1 = None
    else:
        p1 = max([p11, p21])

    if not p12 and not p22:
        p2 = None
    else:
        p2 = min([p12, p22])

    if p1 and p2:
        if p1 > p2:
            return p2, p2

    return p1, p2


def dir_crop(st1, ed1, st2, ed2):
    if (st1 - ed1) * (st2 - ed2) < 0:
        return None, None
    else:
        if ed1 >= st1:
            return max([st1, st2]), min([ed1, ed2])
        else:
            return min([st1, st2]), max([ed1, ed2])


def array_crop(sts, exps, st, exp):
    res = (exps > st) * (
        (
            (sts <= st) * (
                (exps >= exp) * (exp - st) +
                (exps < exp) * (exps - st)
            )
        ) + (
            (
                (sts > st) *
                (sts <= exp)
            ) * (
                (exps >= exp) * (exp - sts) +
                (exps < exp) * (exps - sts)
            )
        )
    )

    return res


def nd_array_crop(sts, exps, sts_exps):
    bc_sts_exps = np.broadcast_to(sts_exps, (len(sts), len(sts_exps), 2)).T

    lower_bound = bc_sts_exps[0, :, :]
    upper_bound = bc_sts_exps[1, :, :]
    a_sts = np.broadcast_to(sts, (len(sts_exps), len(sts)))
    a_exps = np.broadcast_to(exps, (len(sts_exps), len(exps)))

    res = np.maximum(
        np.minimum(upper_bound, a_exps) -
        np.maximum(lower_bound, a_sts),
        np.zeros_like(lower_bound)
    ).T

    return res


def array_minus(a, v):
    ca = np.cumsum(a)
    res = (ca > v) * a
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
    if isinstance(index, str):
        index = [index]
    data_ = data.sort_values(index + [line_col_name]).reset_index()
    lines_array = np.array(lines)
    a = line_values < lines_array
    data_[lines] = a
    idxs = [data_[data_[line_col_name] < line].groupby(index).idxmax() for line in lines]
    res = np.zeros(shape=a.shape)
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
    if index is None:
        index_ = []
    elif isinstance(index, str):
        index_ = [index]
    else:
        index_ = list(index)

    res = merge_timeseries(
        dfs=dfs,
        index=index,
        axis_cols=axis_cols,
        mask_original_na='[na_mask]',
        output_axis_col=output_axis_col
    )

    res = res.sort_values([*index_, output_axis_col]).set_index(index_)
    res = res.groupby(index_)
    res = res.ffill().reset_index()

    res[[*index_, *value_cols, output_axis_col]] = res[[*index_, *value_cols, output_axis_col]].fillna(0)

    res.replace('[na_mask]', np.nan, inplace=True)

    res[output_col] = np.prod(res[value_cols].values, axis=1)

    return res

