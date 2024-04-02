import re
import pandas as pd
from numpy import datetime64
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta

if 'helper_function' in __name__.split('.'):
    from .hf_math import crop, grid_pos
    from .hf_number import is_number
else:
    from hf_math import crop, grid_pos
    from hf_number import is_number


weekday_abbr = {
    'mon': 0,
    'tue': 1,
    'wed': 2,
    'thr': 3,
    'fri': 4,
    'sat': 5,
    'sun': 6,
}

month_abbr = {
    'jan': 0,
    'feb': 1,
    'mar': 2,
    'apr': 3,
    'may': 4,
    'jun': 5,
    'jul': 6,
    'aug': 7,
    'sep': 8,
    'oct': 9,
    'nov': 10,
    'dec': 11,
}


def to_dt(date):
    if isinstance(date, dt):
        return date
    elif isinstance(date, datetime64):
        date = datetime64(date, 's')
        return date.astype(dt)
    elif isinstance(date, str):
        try:
            year, month, day = date.split('/')
            return dt(year=int(year), month=int(month), day=int(day))
        except ValueError:
            if date == 'nan':
                return None
            print('date format not recognizable %s' % str(date))
            return None
    elif date is None:
        return date
    else:
        try:
            return date.astype(dt)
        except AttributeError:
            print('date format not recognizable %s' % str(date))
            return None


def np_date_to_dt(np_date):
    res = np_date
    if isinstance(res, datetime64):
        res = datetime64(res, 's')
        res = res.astype(dt)
    else:
        print('data %s is not np.datetime64 type' % str(re))

    return res


def crop_date(st1, ed1, st2, ed2):
    return crop(st1, ed1, st2, ed2)


def validate(date_text):
    try:
        if date_text != dt.strptime(date_text, "%Y-%m-%d").strftime('%Y-%m-%d'):
            raise ValueError
        return True
    except ValueError:
        # raise ValueError("错误是日期格式或日期,格式是年-月-日")
        return False


def read_period_string(s):
    pass


def read_state_council_vacation_info(s):
    print(s)

    date = re.findall(r'(\d{4}年\d{1,2}月\d{1,2}日)放假', s)
    year = date[:4]

    ss = s.split('。')

    holiday_ym = []
    workday_ym = []
    for line in ss:
        holiday_ym.extend(re.findall(r'(\d{1,2}月\d{1,2}日(.*)放假)', line))
        workday_ym.extend(re.findall(r'(\d{1,2}月\d{1,2}日(.*)上班)', line))
    print(holiday_ym)
    print(workday_ym)


def is_satisfy_date_string(date, date_string, vacation_calendar=None):
    ys, ms, ds = date_string.split('/')
    yss = ys.split(',')
    mss = ms.split(',')
    dss = ds.split(',')

    ress = True
    res = False

    for ys in yss:
        res |= is_satisfy_year_string(date, ys)
    ress &= res
    if not ress:
        return ress

    res = False
    for ms in mss:
        res |= is_satisfy_month_string(date, ms)
    ress &= res
    if not ress:
        return ress

    res = False
    for ds in dss:
        res |= is_satisfy_day_string(date, ds, vacation_calendar)

    ress &= res

    return ress


def is_satisfy_year_string(date, year_string):
    if year_string == 'y':
        return True
    else:
        return str(date.year) == year_string


def is_satisfy_month_string(date, month_string):
    if month_string == 'm':
        return True
    elif is_number(month_string):
        return date.month == int(month_string)
    elif month_string in month_abbr.keys():
        return date.month == month_abbr[month_string] + 1
    else:
        print('month param is not valid: %s' % month_string)
        return False


def is_satisfy_day_string(date: dt, day_string, vacation_calendar=None):
    if day_string == 'd':
        return True
    elif is_number(day_string):
        return str(date.day) == day_string
    elif day_string in weekday_abbr.keys():
        return date.weekday() == weekday_abbr[day_string]
    elif day_string[0] == 't' and is_number(day_string[1:]):
        runner = 0
        runner_date = dt(date.year, date.month, 1)
        target_t_day = int(day_string[1:]) - 1
        while runner <= target_t_day:
            if not vacation_calendar:
                if runner_date.weekday() in [5, 6]:
                    pass
                else:
                    runner += 1
            else:
                if vacation_calendar[runner_date]:
                    pass
                else:
                    runner += 1
            runner_date += relativedelta(days=1)
        return date == runner_date - relativedelta(days=1)
    else:
        print('day param is not valid: %s' % day_string)
        return False


def is_satisfy_hour_string(date: dt, hour_string):
    if hour_string == 'h':
        return True
    elif is_number(hour_string):
        return str(date.hour) == hour_string
    elif hour_string[-1] == 'h' and is_number(hour_string[:-1]):
        return date.hour % int(hour_string) == 0
    else:
        return False


def is_satisfy_minute_string(date: dt, minute_string):
    if minute_string == 'm':
        return True
    elif is_number(minute_string):
        return str(date.minute) == minute_string
    elif minute_string[-1] == 'm' and is_number(minute_string[:-1]):
        return date.minute % int(minute_string) == 0
    else:
        return False


def is_satisfy_second_string(date: dt, second_string):
    if second_string == 's':
        return True
    elif is_number(second_string):
        return str(date.second) == second_string
    elif second_string[-1] == 's' and is_number(second_string[:-1]):
        return date.second % int(second_string) == 0
    else:
        return False


def gen_date_series(date_string, start, end):
    res = pd.date_range(start=start, end=end, freq='D')
    res = [item for item in res if is_satisfy_date_string(item, date_string)]

    return res


def read_calendar(path):
    data = pd.read_excel(path)
    data = dict(zip(data['date'], data['workday'].apply(lambda x: True if x == 1 else False)))
    return data


def is_workday(date: dt, calendar=None):
    if not calendar:
        print('无日历信息')
        raise ValueError

    try:
        return calendar[date]
    except KeyError:
        print('日期在日历范围之外 %s - %s' %
              (min(calendar.keys().strftime('%Y%m%d'), max(calendar.keys().strftime('%Y%m%d')))))
        return False


def get_work_day(date, calendar, days):
    date_runner = dt(date.year, date.month, date.day)
    work_day_count = 0
    while work_day_count <= abs(days):
        if calendar[date_runner]:
            work_day_count += 1
        date_runner += relativedelta(days=((days > 0) - 0.5) * 2)

    return date_runner - relativedelta(days=((days > 0) - 0.5) * 2)


def get_date_range_str(date_range):
    s = ', '.join(["'%s'" % item.strftime('%Y-%m-%d') for item in date_range])
    return s


def test_read_state_council_vacation_info():
    f = open('e:\\2021_vacation.txt', encoding='utf-8')
    rs = f.readlines()
    rs = ''.join(rs)
    read_state_council_vacation_info(rs)


def test_gen_date_series():
    dstr = '2021/feb/sat'
    print(gen_date_series(dstr, dt(2020, 1, 1), dt(2021, 12, 31)))


def test_day_string():
    ds = 't1'
    print(is_satisfy_day_string(dt(2021, 1, 1), ds, ))


def get_calendar_pos(target_date: dt, today_date=dt.today(), grid_shape='single'):

    year_left_click_times = today_date.year - target_date.year
    if grid_shape == 'single':
        month_left_click_times = today_date.month - target_date.month
    elif grid_shape == 'double':
        month_left_click_times = (today_date.month - target_date.month) // 2

    else:
        print('grid shape "%s" not supported' % grid_shape)
        raise AttributeError

    day_x, day_y = grid_pos(target_date.day + dt(target_date.year, target_date.month, 1).weekday(), 7)

    return year_left_click_times, month_left_click_times, day_x, day_y


def test_read_calendar():
    pth = r'E:\projects\db\i_calendar_2022.xlsx'
    calendar = read_calendar(pth)
    print(calendar[dt(2022, 1, 4)])
    print(calendar[dt(2022, 1, 6)])
    print(calendar[dt(2022, 1, 8)])
    print(calendar[dt(2022, 1, 9)])


def test_get_work_day():

    pth = r'E:\projects\db\i_calendar_2022.xlsx'
    calendar = read_calendar(pth)
    dt1 = dt(2022, 1, 6)
    print(get_work_day(dt1, calendar, 1))



if __name__ == '__main__':
    # print(get_calendar_pos(dt(2021, 12, 20)))
    #test_read_state_council_vacation_info()
    #test_read_calendar()
    test_get_work_day()



