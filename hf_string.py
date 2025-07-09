"""
字符串处理辅助函数模块

该模块提供了字符串处理、JSON序列化、格式化转换等辅助函数。
"""

import json
import pandas as pd
import numpy as np
from typing import Generator
from datetime import datetime as dt
from sqlalchemy import Integer, BINARY, CHAR, Column, Date, DateTime, Float, String, Table, Text
from copy import copy


class UDJsonEncoder(json.JSONEncoder):
    """
    自定义JSON编码器
    
    扩展了标准JSON编码器，支持pandas对象、numpy对象、SQLAlchemy对象等的序列化。
    """
    
    def default(self, obj):
        """
        处理特殊对象的JSON序列化
        
        Args:
            obj: 要序列化的对象
            
        Returns:
            序列化后的对象
        """
        if isinstance(obj, dt):
            # 处理datetime对象
            if pd.isna(obj):
                return None
            return obj.strftime("%F %T")
        elif isinstance(obj, pd.Series):
            # 处理pandas Series
            res = obj.replace(np.nan, None).tolist()
            return res
        elif isinstance(obj, pd.DataFrame):
            # 处理pandas DataFrame
            res = obj.replace(np.nan, None).to_dict(orient='records')
            return res
        elif obj is np.nan:
            return None
        elif obj is pd.NaT:
            return None
        elif pd.isna(obj):
            return None
        elif isinstance(obj, bytes):
            return int(obj)
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            res = obj.tolist()
            return res
        elif isinstance(obj, np.datetime64):
            return str(obj)[:10]
        elif isinstance(obj, np.int64):
            return int(obj)
        elif isinstance(obj, Generator):
            return [item for item in obj]
        elif isinstance(obj, (Integer, BINARY, CHAR, Column, Date, DateTime, Float, String, Table, Text)):
            return str(obj)
        elif isinstance(obj, bytearray):
            return obj.decode("utf-8", errors="replace")
        elif isinstance(obj, float):
            # 处理特殊浮点数值
            if obj == float('inf'):
                return "Infinity"
            elif obj == float('-inf'):
                return "-Infinity"
            elif np.isnan(obj):
                return None
            else:
                return obj
        else:
            try:
                return json.JSONEncoder.default(self, obj)
            except TypeError as e:
                return str(e)


def to_json_str(json_obj, indent=4):
    """
    将对象转换为JSON字符串
    
    Args:
        json_obj: 要转换的对象
        indent: 缩进空格数，默认为4
        
    Returns:
        str: JSON字符串
    """
    return json.dumps(
        json_obj,
        indent=indent,
        ensure_ascii=False,
        cls=UDJsonEncoder,
    )


def to_json_obj(json_str):
    """
    将JSON字符串转换为Python对象
    
    Args:
        json_str: JSON字符串
        
    Returns:
        object: Python对象
    """
    return json.loads(s=json_str)


def udf_format(value, f):
    """
    用户自定义格式化函数
    
    根据指定的格式对值进行格式化处理。
    
    Args:
        value: 要格式化的值
        f: 格式化字符串或格式类型
        
    Returns:
        格式化后的值
    """
    if value is None or pd.isna(value):
        return ''
    if f is None:
        return value
    elif f == 'json':
        res = json.loads(value)
        return res
    elif f == 'str':
        res = str(value)
        return res
    elif f == 'bytes_to_string':
        res = bytes.decode(value)
        return res
    elif f == '是否':
        res = ['否', '是'][int(value)]
        return res
    try:
        res = eval('value.%s' % f)
    except SyntaxError:
        if f[0] == '{' and f[-1] == '}':
            try:
                res = f.format(value)
            except ValueError:
                print(value, f)
                raise ValueError
        else:
            print(value, f)
            print('unrecognizable value or format')
            raise ValueError
    return res


def dash_name_to_camel(s, include_first_letter=True):
    """
    将下划线命名转换为驼峰命名
    
    Args:
        s: 下划线分隔的字符串
        include_first_letter: 是否包含首字母大写，默认为True
        
    Returns:
        str: 驼峰命名字符串
    """
    eles = s.split('_')
    res = ''.join([str.upper(ele[0]) + ele[1:] for ele in eles])
    if not include_first_letter:
        res = str.lower(res[0]) + res[1:]
    return res


def list_to_attr_code(
        code_template,
        attr_list,
        df_var_name,
        st_mark,
        ed_mark,
        intent_blocks
):
    """
    根据属性列表生成代码模板
    
    Args:
        code_template: 代码模板字符串
        attr_list: 属性名列表
        df_var_name: DataFrame变量名
        st_mark: 开始标记
        ed_mark: 结束标记
        intent_blocks: 缩进块数
        
    Returns:
        str: 生成的代码字符串
    """
    attr_code_str_base = " " * 4 * intent_blocks + f"self.%s = {df_var_name}['%s']\n"
    st_index = code_template.index(st_mark)
    ed_index = code_template.index(ed_mark)
    insert_code = [attr_code_str_base % (attr, attr) for attr in attr_list]
    res_code = code_template[: st_index + 1] + insert_code + code_template[ed_index:]
    return res_code


def strip_block_intends(s):
    """
    去除代码块的缩进
    
    Args:
        s: 代码字符串
        
    Returns:
        str: 去除缩进后的代码字符串
    """
    lines = s.split('\n')
    first_line = lines[0]
    first_line_striped = first_line.lstrip(' ')
    intends = len(first_line) - len(first_line_striped)
    res = [line[intends:] for line in lines]
    res = '\n'.join(res)
    return res


def get_ext(index):
    """
    从索引中提取扩展部分
    
    Args:
        index: 索引，可以是字符串、列表、元组等
        
    Returns:
        list: 扩展部分列表
    """
    if isinstance(index, list):
        res = [get_ext(s)[-1] for s in index]
    elif isinstance(index, str):
        try:
            res = [index[index.index('['): index.index(']') + 1]]
        except ValueError:
            res = ['']
    elif isinstance(index, tuple):
        res = get_ext(index[0])
    elif index is None:
        res = ['']
    else:
        print('invalid Type for index: ')
        print(index)
        raise TypeError

    return res


def set_ext(index, original, target):
    """
    设置索引的扩展部分
    
    Args:
        index: 索引
        original: 原始扩展部分
        target: 目标扩展部分
        
    Returns:
        更新后的索引
    """
    if isinstance(index, str):
        if original == '':
            res = copy(index)
        else:
            res = index.replace(original, target)
    elif isinstance(index, list):
        res = [set_ext(item, original, target) for item in index]
    elif isinstance(index, tuple):
        res = tuple([set_ext(index[0], original, target), *index[1:]])
    else:
        print('invalid Type for index: ')
        print(index)
        raise TypeError
    return res


def get_first_letter_of_dash_name(s):
    """
    获取下划线分隔字符串中每个部分的第一个字母
    
    Args:
        s: 下划线分隔的字符串
        
    Returns:
        str: 首字母组合字符串
    """
    res = ''.join([item[0] for item in s.split('_')])
    return res


def to_chi_yes_no(raw_input):
    """
    将输入转换为中文的是/否
    
    Args:
        raw_input: 原始输入，可以是字符串或数字
        
    Returns:
        str: '是' 或 '否'
    """
    if isinstance(raw_input, str):
        if '是' in raw_input and '不是' not in raw_input:
            return '是'
        else:
            return '否'
    elif isinstance(raw_input, (float, int)):
        try:
            return ['否', '是'][int(raw_input)]
        except IndexError:
            return '否'


def nan_to_empty_string(value):
    """
    将NaN值转换为空字符串
    
    Args:
        value: 输入值
        
    Returns:
        str: 转换后的字符串
    """
    if pd.isna(value):
        return ''
    else:
        return value


def get_col_sql_str(cols, quote_mark='`'):
    return ', '.join([f'{quote_mark}{col}{quote_mark}' for col in cols])


if __name__ == '__main__':
    js = to_json_str(float('inf'))
    print(js)