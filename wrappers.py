"""
装饰器工具模块

本模块提供了一系列实用的装饰器函数，用于增强函数的功能，
包括错误重试、API状态包装、计时、参数处理等功能。
"""

import time
import traceback
import pandas as pd
import numpy as np
from collections.abc import Sequence

import warnings
from sqlalchemy.exc import PendingRollbackError, InterfaceError, OperationalError, InternalError

from copy import deepcopy
from mint.helper_function.hf_string import to_json_obj, to_json_str
from inspect import signature
from copy import copy
warnings.filterwarnings("ignore")


def broadcast_param(param):
    """
    参数广播函数
    
    将输入参数转换为numpy数组格式，支持多种数据类型：
    - 序列类型（list, tuple等）
    - pandas对象（DataFrame, Series, Index）
    - numpy数组
    - 标量值
    
    Args:
        param: 输入参数，可以是任意类型
        
    Returns:
        numpy.ndarray: 转换后的numpy数组
    """
    if isinstance(param, (Sequence, pd.DataFrame, pd.Series, pd.Index)):
        res = np.array(param)
    elif isinstance(param, np.ndarray):
        res = copy(param)
    else:
        res = np.array([param])
    return res


def error_retry(e, trail=0, sleep_time=1):
    """
    错误重试处理函数
    
    打印错误信息并返回递增的重试次数
    
    Args:
        e: 异常对象
        trail: 当前重试次数
        sleep_time: 重试前等待时间（秒）
        
    Returns:
        int: 递增后的重试次数
    """
    print(traceback.format_exc())
    print(repr(e))
    print(f'doing it again... (trail: {trail} / 10)')
    trail += 1
    time.sleep(sleep_time)
    return trail


def sql_retry_wrapper(con, sleep_time=0):
    """
    SQL操作重试装饰器
    
    为数据库操作函数提供自动重试机制，处理各种SQL异常：
    - PendingRollbackError: 回滚挂起错误
    - OperationalError: 操作错误
    - InterfaceError: 接口错误
    - InternalError: 内部错误
    
    Args:
        con: 数据库连接对象
        sleep_time: 重试间隔时间（秒）
        
    Returns:
        function: 装饰器函数
    """
    def _(func):
        def wrapper(*args, **kwargs):
            trail = 0
            while trail < 10:  # 最多重试10次
                try:
                    res = func(*args, **kwargs)
                    print(f'{func.__name__} done')
                    break
                except PendingRollbackError as e:
                    print(f'waiting rolling back {str(con)}')
                    con.rollback()  # 执行回滚操作
                    time.sleep(sleep_time)
                    trail = error_retry(e=e, trail=trail, sleep_time=sleep_time)
                except OperationalError as e:
                    print('operational error raised')
                    trail = error_retry(e=e, trail=trail, sleep_time=sleep_time)
                except InterfaceError as e:
                    print('InterfaceError error raised')
                    trail = error_retry(e=e, trail=trail, sleep_time=sleep_time)
                except InternalError as e:
                    print('InternalError error raised')
                    trail = error_retry(e=e, trail=trail, sleep_time=sleep_time)
            else:
                print('waiting rolling back time limit exceeds')
                raise PendingRollbackError

            return res

        wrapper.__name__ = func.__name__

        return wrapper

    return _


def api_status_wrapper(func):
    """
    API状态包装装饰器
    
    将函数包装成标准API响应格式，包含状态码、状态信息和数据字段。
    自动处理异常并返回错误信息。
    
    Args:
        func: 要包装的函数
        
    Returns:
        function: 包装后的函数，返回标准API响应格式
    """
    def wrapper(*args, **kwargs):
        status = 0  # 默认成功状态
        status_info = 'success'
        data = {}
        try:
            data = func(*args, **kwargs)
        except Exception as e:
            print(traceback.format_exc())
            print(repr(e))
            status = 1  # 错误状态
            status_info = traceback.format_exc().split('\n') + [repr(e)]

        # 构建标准API响应格式
        api = dict()
        api['data'] = data
        api['status'] = status
        api['statusInfo'] = status_info
        res = to_json_str(api)
        res = to_json_obj(res)
        res['Content-Type'] = 'application/json; charset=utf-8'
        return res

    wrapper.__name__ = func.__name__

    return wrapper


def sub_wrapper(sys_mode='test'):
    """
    子过程包装装饰器
    
    为子过程函数提供统一的执行和错误处理机制。
    在测试模式下会重新抛出异常，在生产模式下只记录错误。
    
    Args:
        sys_mode: 系统模式，'test'为测试模式，其他为生产模式
        
    Returns:
        function: 装饰器函数
    """
    def _(sub):
        def wrapper(*args, **kwargs):
            status = 0
            try:
                print('running %s' % sub.__name__)
                sub(*args, **kwargs)
                print('%s done' % sub.__name__)
            except Exception as e:
                print(traceback.format_exc())
                print(repr(e))
                status = 1
                if str.lower(sys_mode) == 'test':
                    raise e  # 测试模式下重新抛出异常

            return status

        wrapper.__name__ = sub.__name__

        return wrapper
    return _


def confirm_wrapper(sub):
    """
    确认执行装饰器
    
    在执行函数前要求用户输入确认，只有输入"yes"才会执行函数。
    
    Args:
        sub: 要执行的函数
        
    Returns:
        function: 包装后的函数
    """
    def wrapper(*args, **kwargs):
        ipt = input('typing "yes" to run %s' % sub.__name__)
        if ipt == 'yes':
            sub(*args, **kwargs)
        else:
            print('abort.')

    wrapper.__name__ = sub.__name__

    return wrapper


def timer_wrapper(sub):
    """
    计时装饰器
    
    记录函数执行时间并打印耗时信息。
    
    Args:
        sub: 要计时的函数
        
    Returns:
        function: 包装后的函数，返回原函数的执行结果
    """
    def wrapper(*args, **kwargs):
        tic = time.time()  # 开始时间
        res = sub(*args, **kwargs)
        toc = time.time()  # 结束时间

        time_cost_str = '{:.4f}s'.format((toc - tic))
        print('time cost: [%s] %s' % (time_cost_str, sub.__name__))
        return res

    wrapper.__name__ = sub.__name__

    return wrapper


def byval_param_wrapper(func):
    """
    按值传递参数装饰器
    
    通过深拷贝确保函数参数不被修改，避免副作用。
    
    Args:
        func: 要包装的函数
        
    Returns:
        function: 包装后的函数
    """
    def wrapper(*args, **kwargs):
        args_copy = deepcopy(args)  # 深拷贝位置参数
        kwargs_copy = deepcopy(kwargs)  # 深拷贝关键字参数
        res = func(*args_copy, **kwargs_copy)
        return res

    wrapper.__name__ = func.__name__

    return wrapper


def nd_param_wrapper(names=None):
    """
    N维参数包装装饰器
    
    将指定的参数转换为numpy数组格式，支持参数广播。
    
    Args:
        names: 参数名称或名称列表，指定需要广播的参数
        
    Returns:
        function: 装饰器函数
    """
    def _(func):
        def wrapper(*args, **kwargs):
            args_ = list(args)
            kwargs_ = dict(kwargs)
            sig = signature(func)
            params = list(sig.parameters.keys())
            if names is not None:
                if isinstance(names, str):
                    names_ = [names]
                else:
                    names_ = names
                for name in names_:
                    if name in params:
                        # 处理关键字参数
                        for k, v in kwargs_.items():
                            if k == name:
                                kwargs_[k] = broadcast_param(v)
                                break
                        else:
                            # 处理位置参数
                            for i, param_name in enumerate(params):
                                if param_name == name:
                                    args_[i] = broadcast_param(args_[i])
                                    break

            res = func(*args_, **kwargs_)

            return res

        wrapper.__name__ = func.__name__

        return wrapper

    return _


def index_wrapper(func):
    """
    索引包装装饰器
    
    为函数结果添加默认索引，当index参数为None时设置为['total']。
    
    Args:
        func: 要包装的函数
        
    Returns:
        function: 包装后的函数
    """
    def wrapper(*args, **kwargs):
        res = func(*args, **kwargs)
        if 'index' in kwargs:
            if kwargs['index'] is None:
                res.index = ['total']  # 设置默认索引
        return res

    wrapper.__name__ = func.__name__

    return wrapper


@byval_param_wrapper
def test_func(df):
    """
    测试函数
    
    用于测试byval_param_wrapper装饰器的效果。
    
    Args:
        df: pandas DataFrame对象
        
    Returns:
        pandas.DataFrame: 修改后的DataFrame
    """
    df['a'] = [1]
    return df


def test_byval():
    """
    测试byval_param_wrapper装饰器
    
    验证装饰器是否能正确保护原始参数不被修改。
    """
    df = pd.DataFrame(columns=['b'])
    df['b'] = [2]
    test_func(df)  # 调用被装饰的函数
    print(df)  # 检查原始DataFrame是否被修改


if __name__ == '__main__':
    test_byval()