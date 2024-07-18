import time
from tqdm import tqdm
import line_profiler
import traceback
from functools import wraps


def timecost(runningtime=1):
    def wrapper(func):
        def _(*args, **kwargs):
            total_time_cost = 0
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
    def _(*args, **kwargs):
        res = func(*args, **kwargs)
        print(res)
    return _


def get_func_params(func, args, kwargs):
    params = list(func.__code__.co_varnames[:func.__code__.co_argcount])
    defaults = func.__defaults__ or ()
    default_parameters = params[-len(defaults):]
    default_values = dict(zip(default_parameters, defaults))
    params = dict(zip(params, args))
    params.update(default_values)
    params.update(kwargs)
    return params


def check_param_valid_range(params_to_check=(), valid_ranges=(())):
    def wrapper(func):
        def _(*args, **kwargs):
            params = get_func_params(func, args, kwargs)
            for i, param_name in enumerate(params_to_check):
                try:
                    assert params[param_name] in valid_ranges[i]
                except AssertionError:
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
    def __init__(self, instance, property_name):
        self.instance = instance
        self.property_name = property_name

    def __getitem__(self, args):
        if isinstance(args, (list, tuple)):
            return eval(f'self.instance.{self.property_name}(*args)')
        elif isinstance(args, str):
            return eval(f'self.instance.{self.property_name}(args)')


def profile_line_by_line(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not hasattr(wrapper, 'profiler'):
            wrapper.profiler = line_profiler.LineProfiler()
            wrapper.profiler.add_function(func)
            wrapper.recursion_level = 0

        wrapper.recursion_level += 1

        if wrapper.recursion_level == 1:
            wrapper.profiler.enable_by_count()

        result = func(*args, **kwargs)

        wrapper.recursion_level -= 1

        if wrapper.recursion_level == 0:
            wrapper.profiler.disable_by_count()
            wrapper.profiler.print_stats()

        return result

    return wrapper


def error_handler(response='', data='', exc=''):
    print(data)
    print(response)
    if len(exc) > 0:
        traceback.print_exc()
        print(str(exc))