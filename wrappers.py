import time
import traceback
import pandas
import pymysql.err
import warnings
from sqlalchemy.exc import PendingRollbackError, InterfaceError, OperationalError, InternalError
from flask import jsonify
from copy import deepcopy
if 'helper_function' in __name__.split('.'):
    from .hf_string import to_json_str
else:
    from hf_string import to_json_str

warnings.filterwarnings("ignore")


def error_retry(e, trail=0, sleep_time=1):
    print(traceback.format_exc())
    print(repr(e))
    print(f'doing it again... (trail: {trail} / 10)')
    trail += 1
    time.sleep(sleep_time)
    return trail


def sql_retry_wrapper(con, sleep_time=0):
    def _(func):
        def wrapper(*args, **kwargs):
            trail = 0
            while trail < 10:
                try:
                    res = func(*args, **kwargs)
                    print(f'{func.__name__} done')
                    break
                except PendingRollbackError as e:
                    print(f'waiting rolling back {str(con.info)}')

                    # con.begin()
                    # con.execute("ROLLBACK")
                    con.rollback()
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
    def wrapper(*args, **kwargs):
        status = 0
        status_info = 'success'
        data = {}
        try:
            data = func(*args, **kwargs)
        except Exception as e:
            print(traceback.format_exc())
            print(repr(e))
            status = 1
            status_info = traceback.format_exc().split('\n') + [repr(e)]

        api = dict()
        api['data'] = data
        api['status'] = status
        api['statusInfo'] = status_info
        res = jsonify(api)
        res.headers['Content-Type'] = 'application/json; charset=utf-8'
        return res

    wrapper.__name__ = func.__name__

    return wrapper


def sub_wrapper(sys_mode='test'):
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
                    raise e

            return status

        wrapper.__name__ = sub.__name__

        return wrapper
    return _


def confirm_wrapper(sub):
    def wrapper(*args, **kwargs):
        ipt = input('typing "yes" to run %s' % sub.__name__)
        if ipt == 'yes':
            sub(*args, **kwargs)
        else:
            print('aborting.')

    wrapper.__name__ = sub.__name__

    return wrapper


def timer_wrapper(sub):
    def wrapper(*args, **kwargs):
        tic = time.time()
        res = sub(*args, **kwargs)
        toc = time.time()

        time_cost_str = '{:.4f}s'.format((toc - tic))
        print('time cost: [%s] %s' % (time_cost_str, sub.__name__))
        return res

    wrapper.__name__ = sub.__name__

    return wrapper


def byval_param_wrapper(func):
    def wrapper(*args, **kwargs):
        args_copy = deepcopy(args)
        kwargs_copy = deepcopy(kwargs)
        res = func(*args_copy, **kwargs_copy)
        return res

    wrapper.__name__ = func.__name__

    return wrapper


@byval_param_wrapper
def test_func(df):
    df['a'] = [1]
    return df


def test_byval():
    df = pandas.DataFrame(columns=['b'])
    df['b'] = [2]
    test_func(df)
    print(df)


if __name__ == '__main__':
    test_byval()