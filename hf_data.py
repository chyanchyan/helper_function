import json
from typing import Set, List
from datetime import datetime as dt
import pandas as pd
import numpy as np



if 'helper_function' in __name__.split('.'):
    from .hf_string import to_json_str
else:
    from hf_string import to_json_str


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
        if parent is not None:
            try:
                graph[node].add(parent)
            except KeyError:
                graph[node] = {parent}
        else:
            graph[node] = set()
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
    for parent in graph[node]:
        if parent not in visited:
            depth_first_search(parent, graph, visited, stack)
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
        obj[item] = obj[item].strftime('%Y-%m-%d %H:%M:%S')

    return obj


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


if __name__ == '__main__':
    test_construct_nested_dict()
