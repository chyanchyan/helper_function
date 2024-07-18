import json
from typing import Set, List
from datetime import datetime as dt
import numpy as np

import pandas as pd
from pandas._typing import AggFuncType
from collections.abc import Sequence
from copy import deepcopy

import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from helper_function.hf_string import to_json_str


class Infinite:
    def __init__(self, positive=True):
        self.positive = positive

    def __neg__(self):
        return Infinite(positive=not self.positive)

    def __add__(self, other):
        return Infinite(positive=self.positive)

    def __sub__(self, other):
        return Infinite(positive=self.positive)

    def __iadd__(self, other):
        return Infinite(positive=self.positive)

    def __isub__(self, other):
        return Infinite(positive=self.positive)

    def __mul__(self, other):
        return Infinite(positive=self.positive * (other > 0))

    def __gt__(self, other):
        return self.positive

    def __lt__(self, other):
        return not self.positive

    def __ge__(self, other):
        return self.positive

    def __le__(self, other):
        return not self.positive

    def __abs__(self):
        return Infinite(positive=True)

    def __repr__(self):
        return 'inf.'

    def __str__(self):
        return self.__repr__()


class JsonObj:
    def __init__(self, *args, **kwargs):
        pass

    def to_json_obj_raw(
            self,
            include_attrs=(),
            exclude_attrs=()
    ):

        res = dict()
        if len(include_attrs) == 0 or include_attrs == 'all':
            include_attrs = list(
                self.__dir__()[:list(self.__dir__()).index('__module__')]
            )

        include_attrs = sorted(
            list(set(include_attrs) - set(exclude_attrs)),
            key=lambda x: include_attrs.index(x)
        )

        for attr in include_attrs:
            value = eval('self.%s' % attr)
            if isinstance(value, (list, tuple, set)):
                try:
                    res[attr] = []
                except AttributeError:
                    continue

                for v in value:
                    if v:
                        try:
                            res[attr].append(
                                v.to_json_obj_raw(include_attrs=include_attrs)
                            )
                        except AttributeError:
                            res[attr].append(v)

            elif isinstance(value, dict):
                try:
                    res[attr] = dict()
                except AttributeError:
                    continue
                for k, v in value.items():
                    try:
                        res[attr][k] = v.to_json_obj_raw(include_attrs=include_attrs)
                    except AttributeError:
                        res[attr][k] = v

            else:
                try:
                    res[attr] = value.to_json_obj_raw(include_attrs=include_attrs)
                except AttributeError:
                    res[attr] = value

        return res

    def to_json(self, include_attrs=(), exclude_attrs=()):
        jo = self.to_json_obj_raw(
            include_attrs=include_attrs,
            exclude_attrs=exclude_attrs
        )
        return to_json_str(jo)

    def to_json_obj(self, include_attrs=(), exclude_attrs=()):
        return json.loads(
            self.to_json(include_attrs=include_attrs, exclude_attrs=exclude_attrs)
        )


def replace_nan_with_none(d):
    if isinstance(d, dict):
        for key, value in d.items():
            if isinstance(value, dict) or isinstance(value, list):
                d[key] = replace_nan_with_none(value)  # 递归调用
            elif isinstance(value, float) and np.isnan(value):
                d[key] = None
    elif isinstance(d, list):
        for i, item in enumerate(d):
            if isinstance(item, dict) or isinstance(item, list):
                d[i] = replace_nan_with_none(item)  # 递归调用
            elif isinstance(item, float) and np.isnan(item):
                d[i] = None
    return d


def get_nodes(relation_info):
    res = list(set(sum(relation_info, start=[])))
    res = sorted([item for item in res if item])
    return res


def get_graph(relation_info):
    graph = dict()
    for node, parent in relation_info:
        if node not in graph:
            graph[node] = set()
        if parent is not None:
            graph[node].add(parent)
    for k in graph.keys():
        graph[k] = sorted(list(graph[k]))
    return graph


def get_related_nodes(graph, node, visited=None):
    if visited is None:
        visited = set()

    parents = set(graph[node])
    children = set()
    for node_in_graph, ps in graph.items():
        if node in ps and node_in_graph not in visited:
            children.add(node_in_graph)
            visited.add(node_in_graph)
            children |= set(get_related_nodes(graph, node_in_graph, visited))

    return sorted(list({node} | parents | children))


def depth_first_search(
        node: str,
        graph: dict,
        visited: Set[str],
        stack: List[str]
):
    visited.add(node)
    if node not in graph:
        stack.append(node)
    else:
        try:
            for parent in graph[node]:
                if parent not in visited:
                    depth_first_search(parent, graph, visited, stack)
        except KeyError:
            print(f'{node} is not in graph {graph}')
    stack.append(node)


def topological_sort(relation_info: list, reverse=False) -> List[str]:
    nodes = get_nodes(relation_info=relation_info)
    graph = get_graph(relation_info=relation_info)
    visited = set()
    stack = []
    for node in nodes:
        if node not in visited:
            depth_first_search(node, graph, visited, stack)

    if reverse:
        return [node for node in reversed(stack)]
    else:
        return [node for node in stack]


def construct_nested_dict(path_list):
    res = {'order': [], 'nodes': {}}
    sub_path_list = {}
    for path in path_list:
        nodes = path.split('/')
        if nodes[0] not in res['order']:
            res['order'].append(nodes[0])
            sub_path_list[nodes[0]] = []
        if len(nodes) > 1:
            sub_path_list[nodes[0]].append('/'.join(nodes[1:]))

    for k, v in sub_path_list.items():
        res['nodes'][k] = construct_nested_dict(path_list=v)

    return res


def get_table_prefix(index):
    if isinstance(index, list):
        res = [get_table_prefix(s) for s in index]
    elif isinstance(index, str):
        try:
            res = index.split('.')[0]
        except ValueError:
            res = ['']
    elif isinstance(index, tuple):
        res = get_table_prefix(index[0])
    elif index is None:
        res = ['']
    else:
        print('invalid Type for index: ')
        print(index)
        raise TypeError

    return res


def set_table_prefix(index, original, target):
    if isinstance(index, str):
        parts = index.split('.')
        if parts[0] == original:
            if target is None:
                parts = parts[1:]
            else:
                parts[0] = target
        res = '.'.join(parts)
    elif isinstance(index, list):
        res = [set_table_prefix(item, original, target) for item in index]
    elif isinstance(index, tuple):
        res = tuple([set_table_prefix(index[0], original, target), *index[1:]])
    else:
        print('invalid Type for index: ')
        print(index)
        raise TypeError
    return res


def pivot_table(data, index=None, values=None, aggfunc: AggFuncType = None):

    if isinstance(index, str):
        index_local = [index]
    elif isinstance(index, list):
        index_local = deepcopy(index)
    elif index is None:
        index_local = None
    else:
        print(f'invalid index type: {type(index)}')
        print(index)
        raise TypeError

    if values is None:
        values = [item for item in data.columns.tolist() if item not in index_local]
    elif isinstance(values, str):
        values = [values]

    if aggfunc is None:
        aggfunc = "sum"

    data_prefixes = set(get_table_prefix(values))

    if index_local is None:
        res = pd.DataFrame([data[values].sum(axis=0)])
        res_prefix = None  # 取最后一个汇总字段的后缀
    else:
        res_prefix = get_table_prefix(index_local[-1])  # 取最后一个汇总字段的后缀
        try:
            res = data.pivot_table(
                index=index_local,
                values=values,
                aggfunc=aggfunc
            ).reset_index()
        except KeyError as e:
            print('columns in data:')
            for item in data.columns.tolist():
                print(item)
            print('*' * 100)
            print(f'KeyError: {e}')
            print(f'index_local: {index_local}')
            print(f'values: {values}')
            raise KeyError

        if len(res) == 0:
            res = pd.DataFrame(columns=list(index_local) + list(values))

    cols = res.columns.tolist()
    for data_prefix in data_prefixes:
        cols = set_table_prefix(cols, data_prefix, res_prefix)
    res.columns = cols

    return res


def fill_na(obj, replace=''):
    fill_list = []
    for key, item in obj.items():
        try:
            if pd.isna(item) and not item == pd.NaT:
                fill_list.append(key)
        except ValueError:
            print(obj)
            raise ValueError

    for item in fill_list:
        obj[item] = replace

    return obj


def fill_nat(obj, replace=''):
    fill_list = []
    for key, item in obj.items():
        if item == pd.NaT:
            fill_list.append(key)

    for item in fill_list:
        obj[item] = replace

    return obj


def replace_datetime(obj):
    fill_list = []
    for key, item in obj.items():
        if isinstance(item, dt):
            fill_list.append(key)

    for item in fill_list:
        obj[item] = obj[item].strftime('%F %T')

    return obj


def df_to_ant_table_options(df: pd.DataFrame, titles=None, data_types=None):
    if titles is None:
        titles = df.columns.tolist()
    if data_types is None:
        data_types = ['String' for _ in titles]
    columns = [
        {'title': titles[i], 'dataIndex': col, 'key': col, 'dataType': data_types[i]}
        for i, col in enumerate(df.columns)
    ]
    res = {
        'columns': columns,
        'dataSource': [
            {'key': i, **row} for i, row in enumerate(df.to_dict(orient='records'))
        ]
    }
    return res


def get_tuple_cols(col_name: str, seq: Sequence | pd.DataFrame | pd.Series | pd.Index):
    if not isinstance(seq, Sequence | pd.DataFrame | pd.Series | pd.Index):
        return tuple([col_name])

    if len(seq) == 0:
        return tuple([col_name])

    if isinstance(seq[0], Sequence | pd.DataFrame | pd.Series | pd.Index):
        return [(col_name, *item) for item in seq]
    else:
        return [(col_name, item) for item in seq]


def test_construct_nested_dict():
    path_list = [
        'a1/a1-b1/a1-b1-c1',
        'a1/a1-b1/a1-b1-c2',
        'a1/a1-b2/a1-b2-c1',
        'a2/a2-b1/c1',
        'a2/a2-b2/c2',
    ]

    res = construct_nested_dict(path_list=path_list)
    print(to_json_str(res))


def test_pivot_table():
    data = pd.DataFrame(
        columns=['a[p]', 'b[c]', 'c'],
        data=[
            ['1', 11, 111],
            ['2', 12, 112],
            ['3', 13, 113],
            ['4', 14, 114],
        ]
    )
    print(data)
    print(pivot_table(
        data=data,
        index=None,
        values=['c']
    ))
    print(pivot_table(
        data=data,
        index='a[p]',
        values=['c']
    ))


def test_iterable_class():
    i = pd.date_range(dt(2024, 1, 1), dt(2024, 2, 1))
    print(*i)




if __name__ == '__main__':
    test_iterable_class()
