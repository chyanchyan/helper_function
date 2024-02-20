import time
import traceback

import pymysql.err
import warnings
from sqlalchemy.exc import PendingRollbackError

warnings.filterwarnings("ignore")


def error_retry(e, trail=0, sleep_time=1):
    print(traceback.format_exc())
    print(repr(e))
    print('doing it again..')
    trail += 1
    time.sleep(sleep_time)


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
        return api

    wrapper.__name__ = func.__name__

    return wrapper


def sql_retry_wrapper(func):
    def wrapper(*args, **kwargs):
        trail = 0
        while trail < 30:
            try:
                res = func(*args, **kwargs)
                break
            except PendingRollbackError as e:
                print('waiting rolling back')
                error_retry(e=e, trail=trail)
            except pymysql.err.OperationalError as e:
                print('operational error raised')
                error_retry(e=e, trail=trail)
        else:
            print('waiting rolling back time limit exceeds')
            raise PendingRollbackError

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
