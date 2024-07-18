import pandas as pd
from datetime import datetime as dt
from mint.helper_function.hf_math import *
from mint.settings import *

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


def test_nd_array_crop():
    sts = np.array([1, 2, 3, 4, 5])
    exps = np.array([3, 6, 9, 11, 12])
    dates = np.array([2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
    ranges = np.array(list(zip(
        dates[:-1], dates[1:]
    )))

    res = nd_array_crop(
        sts,
        exps,
        ranges
    )
    print(res)


def test_sumprod_timeseries():
    df1 = pd.DataFrame(
        data={
            'n': ['b', 'a', 'a', 'b', 'b'],
            'n2': ['b', 'a', 'a', 'b', 'b'],
            'v1': [1, 2, 3, 4, 5],
            'v2': [1, 2, 3, 4, 5],
            't': [1., 4., 6., 8., 10.]
        }
    )
    df2 = pd.DataFrame(
        data={
            'n': ['a', 'b', 'b'],
            'v2': [1, 2, 3],
            't': [1., 4.5, 5.5]
        }
    )
    df3 = pd.DataFrame(
        data={
            'n': ['a', 'b'],
            'n2': ['a', 'b'],
            'v3': [1, 2],
            't': [1., 4.]
        }
    )

    a = sumprod_timeseries(
        dfs=[df1, df3],
        axis_cols=['t', 't'],
        value_cols=['v1', 'v3'],
        output_col='res',
        index=['n', 'n2']
    )
    print(a)


def test_outer():
    df1 = pd.DataFrame(
        {
            'a': [1, 1, 1, 2, 2, 2, 3, 3, 3],
            'b': [1, 2, 3, 4, 5, 6, 7, 8, 9]
        }
    )
    df2 = pd.DataFrame(
        {
            'a': [1, 1, 2, 2],
            'c': [1, 2, 3, 4]
        }
    )
    print(pd.merge(df1, df2, on='a', how='left'))


if __name__ == '__main__':
    test_sumprod_timeseries()