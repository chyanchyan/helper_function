import json
import pandas as pd
import numpy as np
from typing import Generator
from datetime import datetime as dt
from sqlalchemy import Integer, BINARY, CHAR, Column, Date, DateTime, Float, String, Table, Text
from copy import copy


class UDJsonEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, dt):
            if pd.isna(obj):
                return None
            return obj.strftime("%F")
        elif isinstance(obj, pd.Series):
            obj = obj.replace(np.nan, None)
            return obj.tolist()
        elif isinstance(obj, pd.DataFrame):
            obj = obj.replace(np.nan, None)
            return obj.to_dict()
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
        else:
            try:
                return json.JSONEncoder.default(self, obj)
            except TypeError as e:
                return str(e)


def to_json_str(json_obj):
    return json.dumps(json_obj, indent=4, ensure_ascii=False, cls=UDJsonEncoder)


def to_json_obj(json_str):
    return json.loads(s=json_str)


def udf_format(value, f):

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
    attr_code_str_base = " " * 4 * intent_blocks + f"self.%s = {df_var_name}['%s']\n"
    st_index = code_template.index(st_mark)
    ed_index = code_template.index(ed_mark)
    insert_code = [attr_code_str_base % (attr, attr) for attr in attr_list]
    res_code = code_template[: st_index + 1] + insert_code + code_template[ed_index:]
    return res_code


def strip_block_intends(s):
    lines = s.split('\n')
    first_line = lines[0]
    first_line_striped = first_line.lstrip(' ')
    intends = len(first_line) - len(first_line_striped)
    res = [line[intends:] for line in lines]
    res = '\n'.join(res)
    return res


def get_ext(index):
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


def to_chi_yes_no(raw_input):
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
    if pd.isna(value):
        return ''
    else:
        return value


def test_dash_name_to_camel():
    s = 'table_name_example'
    print(dash_name_to_camel(s))


if __name__ == '__main__':
    test_dash_name_to_camel()


