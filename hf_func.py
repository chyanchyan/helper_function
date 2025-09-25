"""
函数处理辅助函数模块

该模块提供了函数装饰器、参数处理、性能分析等辅助函数，用于增强函数的功能和调试。
"""

import time
from tqdm import tqdm
import line_profiler
import traceback
from functools import wraps
import inspect


def timecost(runningtime=1):
    """
    函数执行时间统计装饰器
    
    统计函数执行时间，可以指定运行次数来计算平均执行时间。
    
    Args:
        runningtime: 运行次数，默认为1
        
    Returns:
        function: 装饰器函数
    """
    def wrapper(func):
        def _(*args, **kwargs):
            total_time_cost = 0
            # 使用进度条显示运行进度
            for i in tqdm(range(runningtime)):
                tic = time.time()
                res = func(*args, **kwargs)
                toc = time.time()
                total_time_cost += toc - tic

            print(f'\nrunning "{func.__name__}" {runningtime} times, avg time cost: {total_time_cost / runningtime}')
            return res
        return _
    return wrapper


def print_output(func):
    """
    打印函数输出装饰器
    
    自动打印函数的返回值。
    
    Args:
        func: 要装饰的函数
        
    Returns:
        function: 装饰器函数
    """
    def _(*args, **kwargs):
        res = func(*args, **kwargs)
        print(res)
    return _


def get_func_params(func, args, kwargs):
    """
    获取函数的完整参数信息
    
    将位置参数、关键字参数和默认值合并为完整的参数字典。
    
    Args:
        func: 函数对象
        args: 位置参数列表
        kwargs: 关键字参数字典
        
    Returns:
        dict: 完整的参数字典
    """
    # 获取函数参数名列表
    params = list(func.__code__.co_varnames[:func.__code__.co_argcount])
    # 获取默认值
    defaults = func.__defaults__ or ()
    default_parameters = params[-len(defaults):]
    default_values = dict(zip(default_parameters, defaults))
    # 合并位置参数
    params = dict(zip(params, args))
    # 更新默认值
    params.update(default_values)
    # 更新关键字参数
    params.update(kwargs)
    return params


def check_param_valid_range(params_to_check=(), valid_ranges=(())):
    """
    参数有效性检查装饰器
    
    检查指定参数是否在有效范围内，如果不在范围内则打印错误信息并返回空字符串。
    
    Args:
        params_to_check: 要检查的参数名列表
        valid_ranges: 每个参数的有效值范围列表
        
    Returns:
        function: 装饰器函数
    """
    def wrapper(func):
        def _(*args, **kwargs):
            params = get_func_params(func, args, kwargs)
            for i, param_name in enumerate(params_to_check):
                try:
                    assert params[param_name] in valid_ranges[i]
                except AssertionError:
                    # 打印详细的参数信息
                    value_str = '\n'.join([item[0] + " =\t" + item[1].__repr__() for item in params.items()])
                    print(f'unsupported method: \'{params[param_name]}\' besides {valid_ranges[i]}')
                    print(f'when doing: {func.__name__} \n'
                          f'with args: \n'
                          f'{value_str}')
                    return ''
            return func(*args, **kwargs)

        _.__name__ = func.__name__

        return _
    return wrapper


class PropertyIndexer:
    """
    属性索引器类
    
    允许通过索引方式访问对象的属性方法。
    """
    
    def __init__(self, instance, property_name):
        """
        初始化属性索引器
        
        Args:
            instance: 对象实例
            property_name: 属性方法名
        """
        self.instance = instance
        self.property_name = property_name

    def __getitem__(self, args):
        """
        通过索引访问属性方法
        
        Args:
            args: 参数，可以是列表、元组或字符串
            
        Returns:
            属性方法的返回值
        """
        if isinstance(args, (list, tuple)):
            return eval(f'self.instance.{self.property_name}(*args)')
        elif isinstance(args, str):
            return eval(f'self.instance.{self.property_name}(args)')


def profile_line_by_line(func):
    """
    逐行性能分析装饰器，兼容同步/异步函数

    使用 line_profiler 对函数进行逐行性能分析。

    Args:
        func: 要分析的函数

    Returns:
        function: 装饰器函数
    """

    if inspect.iscoroutinefunction(func):
        # 处理异步函数
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            if not hasattr(async_wrapper, 'profiler'):
                async_wrapper.profiler = line_profiler.LineProfiler()
                async_wrapper.profiler.add_function(func)
                async_wrapper.recursion_level = 0

            async_wrapper.recursion_level += 1

            if async_wrapper.recursion_level == 1:
                async_wrapper.profiler.enable_by_count()

            result = await func(*args, **kwargs)

            async_wrapper.recursion_level -= 1

            if async_wrapper.recursion_level == 0:
                async_wrapper.profiler.disable_by_count()
                async_wrapper.profiler.print_stats()

            return result

        return async_wrapper
    else:
        # 处理同步函数
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            if not hasattr(sync_wrapper, 'profiler'):
                sync_wrapper.profiler = line_profiler.LineProfiler()
                sync_wrapper.profiler.add_function(func)
                sync_wrapper.recursion_level = 0

            sync_wrapper.recursion_level += 1

            if sync_wrapper.recursion_level == 1:
                sync_wrapper.profiler.enable_by_count()

            result = func(*args, **kwargs)

            sync_wrapper.recursion_level -= 1

            if sync_wrapper.recursion_level == 0:
                sync_wrapper.profiler.disable_by_count()
                sync_wrapper.profiler.print_stats()

            return result

        return sync_wrapper


def error_handler(response='', data='', exc=''):
    """
    错误处理函数
    
    打印错误相关的信息，包括数据、响应和异常信息。
    
    Args:
        response: 响应信息
        data: 数据信息
        exc: 异常信息
    """
    print(data)
    print(response)
    if len(exc) > 0:
        traceback.print_exc()
        print(str(exc))


def get_param_names(func):
    """
    获取函数的参数名列表
    
    Args:
        func: 函数对象
        
    Returns:
        list: 参数名列表
    """
    return [param.name for param in inspect.signature(func).parameters.values()]
