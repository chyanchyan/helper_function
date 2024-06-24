import numpy as np
import pandas as pd
import itertools
from copy import copy


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

    res = (exps > bc_sts_exps[0, :, :]).astype(float) * (
        (
            (sts <= bc_sts_exps[0, :, :]).astype(float) * (
                (exps >= bc_sts_exps[1, :, :]).astype(float) * (bc_sts_exps[1, :, :] - bc_sts_exps[0, :, :]) +
                (exps < bc_sts_exps[1, :, :]).astype(float) * (exps - bc_sts_exps[0, :, :])
            )
        ) + (
            (
                (sts > bc_sts_exps[0, :, :]).astype(float) *
                (sts <= bc_sts_exps[1, :, :]).astype(float)
            ) * (
                (exps >= bc_sts_exps[1, :, :]).astype(float) * (bc_sts_exps[1, :, :] - sts) +
                (exps < bc_sts_exps[1, :, :]).astype(float) * (exps - sts)
            )
        )
    )

    return res


def array_minus(a, v):
    ca = np.cumsum(a)
    res = (ca > v) * a
    res[(res != 0).argmax()] += ((ca <= v) * a).sum() - v
    return res


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



def test_grid_pos():
    print(grid_pos(10, 3))


def test_crop():
    print(crop(6, 1, 3, 2))


def test_cartesian():
    l1 = [1, 2, 3, 4]
    l2 = ['a', 'b', 'c']
    l3 = [5, 6, 7]

    for item in itertools.product(l1, l2, l3):
        print(item)


def test_array_minus():
    a = [100]
    v = 50
    print(array_minus(a, v))

    a = [550, 150, 200, 0, 100]
    v = 750
    print(array_minus(a, v))


def test_nd_array_op():
    a = np.array([1, 2, 3, 4, 5])
    b = np.array([6, 7, 8, 9, 10])
    v = np.array([[2, 3], [3, 4], [4, 5], [5, 6]])
    print(nd_array_crop(a, b, v))


if __name__ == '__main__':
    test_nd_array_op()
