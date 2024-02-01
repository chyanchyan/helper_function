import json
import pandas as pd
import numpy as np
from typing import Generator
from datetime import datetime as dt
from sqlalchemy import Integer, BINARY, CHAR, Column, Date, DateTime, Float, String, Table, Text


class UDJsonEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, dt):
            if pd.isna(obj):
                return None
            return obj.strftime("%Y-%m-%d")
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
        elif isinstance(obj, Generator):
            return [item for item in obj]
        elif isinstance(obj, (Integer, BINARY, CHAR, Column, Date, DateTime, Float, String, Table, Text)):
            return str(obj)
        elif isinstance(obj, bytearray):
            return obj.decode("utf-8", errors="replace")
        else:
            return json.JSONEncoder.default(self, obj)


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
