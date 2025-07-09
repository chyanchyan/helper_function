"""
数据处理工具模块

本模块提供了各种数据处理相关的工具函数和类，包括：
- 无限值处理类
- JSON对象序列化
- 图论算法（拓扑排序、深度优先搜索等）
- 数据表前缀处理
- 数据透视表操作
- 数据清洗和格式化
- 表格数据转换
"""

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

# 添加父目录到系统路径，以便导入其他模块
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from helper_function.hf_string import to_json_str, to_json_obj


class Infinite:
    """
    无限值类
    
    用于表示正无穷或负无穷的数值，支持基本的数学运算和比较操作。
    主要用于算法中的边界值处理。
    """
    
    def __init__(self, positive=True):
        """
        初始化无限值对象
        
        Args:
            positive (bool): 是否为正无穷，默认为True
        """
        self.positive = positive

    def __neg__(self):
        """返回相反符号的无限值"""
        return Infinite(positive=not self.positive)

    def __add__(self, other):
        """加法运算，返回相同符号的无限值"""
        return Infinite(positive=self.positive)

    def __sub__(self, other):
        """减法运算，返回相同符号的无限值"""
        return Infinite(positive=self.positive)

    def __iadd__(self, other):
        """原地加法运算"""
        return Infinite(positive=self.positive)

    def __isub__(self, other):
        """原地减法运算"""
        return Infinite(positive=self.positive)

    def __mul__(self, other):
        """乘法运算，根据other的符号决定结果符号"""
        return Infinite(positive=self.positive * (other > 0))

    def __gt__(self, other):
        """大于比较，正无穷大于任何有限值"""
        return self.positive

    def __lt__(self, other):
        """小于比较，负无穷小于任何有限值"""
        return not self.positive

    def __ge__(self, other):
        """大于等于比较"""
        return self.positive

    def __le__(self, other):
        """小于等于比较"""
        return not self.positive

    def __abs__(self):
        """绝对值，总是返回正无穷"""
        return Infinite(positive=True)

    def __repr__(self):
        """字符串表示"""
        return 'inf.'

    def __str__(self):
        """字符串表示"""
        return self.__repr__()


class JsonObj:
    """
    JSON对象基类
    
    提供将对象属性序列化为JSON格式的功能。
    """
    
    def __init__(self, *args, **kwargs):
        """初始化方法"""
        pass

    def to_json_obj_raw(
            self,
            include_attrs=(),
            exclude_attrs=()
    ):
        """
        将对象属性转换为原始JSON对象
        
        Args:
            include_attrs: 要包含的属性列表，空列表或'all'表示包含所有属性
            exclude_attrs: 要排除的属性列表
            
        Returns:
            dict: 包含对象属性的字典
        """
        res = dict()
        # 如果没有指定包含属性或指定为'all'，则包含所有属性
        if len(include_attrs) == 0 or include_attrs == 'all':
            include_attrs = list(
                self.__dir__()[:list(self.__dir__()).index('__module__')]
            )

        # 过滤并排序属性
        include_attrs = sorted(
            list(set(include_attrs) - set(exclude_attrs)),
            key=lambda x: include_attrs.index(x)
        )

        for attr in include_attrs:
            value = eval('self.%s' % attr)
            # 处理复杂数据类型
            if isinstance(value, (list, tuple, set, dict, pd.DataFrame, pd.Series)):
                js = to_json_str(value)
                res[attr] = to_json_obj(js)
            elif pd.isna(value):
                res[attr] = None
            else:
                try:
                    # 尝试递归序列化
                    res[attr] = value.to_json_obj_raw(include_attrs=include_attrs)
                    print()
                except AttributeError:
                    res[attr] = value

        return res

    def to_json(self, include_attrs=(), exclude_attrs=()):
        """
        将对象转换为JSON字符串
        
        Args:
            include_attrs: 要包含的属性列表
            exclude_attrs: 要排除的属性列表
            
        Returns:
            str: JSON字符串
        """
        jo = self.to_json_obj_raw(
            include_attrs=include_attrs,
            exclude_attrs=exclude_attrs
        )
        return to_json_str(jo)

    def to_json_obj(self, include_attrs=(), exclude_attrs=()):
        """
        将对象转换为JSON对象
        
        Args:
            include_attrs: 要包含的属性列表
            exclude_attrs: 要排除的属性列表
            
        Returns:
            dict: JSON对象
        """
        return json.loads(
            self.to_json(include_attrs=include_attrs, exclude_attrs=exclude_attrs)
        )


def replace_nan_with_none(d):
    """
    递归地将字典或列表中的NaN值替换为None
    
    Args:
        d: 要处理的字典或列表
        
    Returns:
        处理后的字典或列表
    """
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
    """
    从关系信息中提取所有节点
    
    Args:
        relation_info: 关系信息列表，每个元素为[node, parent]格式
        
    Returns:
        list: 排序后的节点列表
    """
    res = list(set(sum(relation_info, start=[])))
    res = sorted([item for item in res if item])
    return res


def get_graph(relation_info):
    """
    从关系信息构建图结构
    
    Args:
        relation_info: 关系信息列表
        
    Returns:
        dict: 图的邻接表表示，key为节点，value为父节点集合
    """
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
    """
    获取与指定节点相关的所有节点（包括父节点和子节点）
    
    Args:
        graph: 图结构
        node: 目标节点
        visited: 已访问节点集合
        
    Returns:
        list: 相关节点列表
    """
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
    """
    深度优先搜索算法
    
    Args:
        node: 当前节点
        graph: 图结构
        visited: 已访问节点集合
        stack: 结果栈
    """
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
    """
    拓扑排序算法
    
    Args:
        relation_info: 关系信息列表
        reverse: 是否反转排序结果
        
    Returns:
        list: 拓扑排序结果
    """
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
    """
    根据路径列表构建嵌套字典结构
    
    Args:
        path_list: 路径列表，每个路径用'/'分隔
        
    Returns:
        dict: 嵌套字典结构，包含'order'和'nodes'两个键
    """
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
    """
    获取表前缀
    
    Args:
        index: 索引，可以是字符串、列表、元组或None
        
    Returns:
        表前缀
    """
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
    """
    设置表前缀
    
    Args:
        index: 索引
        original: 原始前缀
        target: 目标前缀，None表示移除前缀
        
    Returns:
        处理后的索引
    """
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
    """
    创建数据透视表
    
    Args:
        data: 输入数据DataFrame
        index: 行索引
        values: 值列
        aggfunc: 聚合函数
        
    Returns:
        pd.DataFrame: 透视表结果
    """
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

    # 如果没有指定values，则使用除index外的所有列
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

    # 处理列名前缀
    cols = res.columns.tolist()
    for data_prefix in data_prefixes:
        cols = set_table_prefix(cols, data_prefix, res_prefix)
    res.columns = cols

    return res


def fill_na(obj, replace=''):
    """
    将对象中的NaN值替换为指定值
    
    Args:
        obj: 要处理的对象
        replace: 替换值，默认为空字符串
        
    Returns:
        处理后的对象
    """
    fill_list = []
    for key, item in obj.items():
        try:
            if pd.isna(item) and not item == pd.NaT:
                fill_list.append(key)
        except ValueError:
            print(item)
            print(obj)
            raise ValueError

    for item in fill_list:
        obj[item] = replace

    return obj


def fill_nat(obj, replace=''):
    """
    将对象中的NaT值替换为指定值
    
    Args:
        obj: 要处理的对象
        replace: 替换值，默认为空字符串
        
    Returns:
        处理后的对象
    """
    fill_list = []
    for key, item in obj.items():
        if item == pd.NaT:
            fill_list.append(key)

    for item in fill_list:
        obj[item] = replace

    return obj


def replace_datetime(obj):
    """
    将对象中的datetime对象转换为字符串格式
    
    Args:
        obj: 要处理的对象
        
    Returns:
        处理后的对象
    """
    fill_list = []
    for key, item in obj.items():
        if isinstance(item, dt):
            fill_list.append(key)

    for item in fill_list:
        obj[item] = obj[item].strftime('%F %T')

    return obj


def df_to_ant_table_options(df: pd.DataFrame, titles=None, data_types=None):
    """
    将DataFrame转换为Ant Design表格配置选项
    
    Args:
        df: 输入DataFrame
        titles: 列标题列表
        data_types: 数据类型列表
        
    Returns:
        dict: Ant Design表格配置
    """
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


def df_to_mui_enhanced_table_options(df: pd.DataFrame, header_labels=None, data_types=None):
    """
    将DataFrame转换为MUI增强表格配置选项
    
    Args:
        df: 输入DataFrame
        header_labels: 表头标签列表
        data_types: 数据类型列表
        
    Returns:
        dict: MUI增强表格配置
    """
    if header_labels is None:
        header_labels = df.columns.tolist()
    if data_types is None:
        data_types = ['String' for _ in header_labels]
    headers = [
        {
            'key': col,
            'label': header_labels[i],
            'dataType': data_types[i],
            'hasFilter': True,
            'filterSelection': sorted(set(df[col].dropna().tolist()))
        }
        for i, col in enumerate(df.columns)
    ]
    res = {
        'headers': headers,
        'rows': df.to_dict(orient='records')
    }
    return res


def get_tuple_cols(col_name: str, seq: Sequence | pd.DataFrame | pd.Series | pd.Index):
    """
    生成元组列名
    
    Args:
        col_name: 列名
        seq: 序列对象
        
    Returns:
        list: 元组列名列表
    """
    if not isinstance(seq, Sequence | pd.DataFrame | pd.Series | pd.Index):
        return tuple([col_name])

    if len(seq) == 0:
        return tuple([col_name])

    if isinstance(seq[0], Sequence | pd.DataFrame | pd.Series | pd.Index):
        return [(col_name, *item) for item in seq]
    else:
        return [(col_name, item) for item in seq]


def is_equal(v1, v2):
    """
    深度比较两个值是否相等
    
    支持比较的数据类型包括：
    - 字典
    - 列表和元组
    - 特殊标记值（'[delete]', '[add]'）
    - datetime对象
    - 基本数据类型（int, float, str）
    - pandas Timestamp
    - None和NaN值
    
    Args:
        v1: 第一个值
        v2: 第二个值
        
    Returns:
        bool: 是否相等
    """
    if isinstance(v1, dict) and isinstance(v2, dict):
        if len(v1) != len(v2):
            return False

        for key in v1:
            if key not in v2:
                return False
            if is_equal(v1[key], v2[key]):
                return False
    elif isinstance(v1, (list, tuple)) and isinstance(v2, (list, tuple)):
        if len(v1) != len(v2):
            return False
        for i in range(len(v1)):
            if is_equal(v1[i], v2[i]):
                return False
    elif v1 == '[delete]' or v1 == '[add]':
        return False
    elif v2 == '[delete]' or v2 == '[add]':
        return False
    elif isinstance(v1, dt):
        if isinstance(v2, dt):
            return v1.strftime('%F%T') == v2.strftime('%F%T')
        elif isinstance(v2, str):
            try:
                _v2 = dt.strptime(v2, '%Y-%m-%d')
                return v1.strftime('%F%T') == _v2.strftime('%F%T')
            except ValueError:
                try:
                    _v2 = dt.strptime(v2, '%Y-%m-%d %H:%M:%S')
                    return v1.strftime('%F%T') == _v2.strftime('%F%T')
                except ValueError:
                    return False
        else:
            return False

    elif isinstance(v1, (int, float, str)) and isinstance(v2, (int, float, str)):
        return v1 == v2
    elif isinstance(v1, pd.Timestamp):
        return pd.Timestamp(v1) == pd.Timestamp(v2)
    elif v1 is None and v2 is None:
        return True
    elif pd.isna(v1) and pd.isna(v2):
        return True
    else:
        return v1 == v2
