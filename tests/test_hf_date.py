from mint.helper_function.hf_date import *
from mint.settings import *


def test_get_overlap_days():
    sts = pd.date_range(dt(2024, 1, 1), dt(2025, 1, 1), freq='MS')
    exps = pd.date_range(dt(2024, 2, 1), dt(2025, 2, 1), freq='MS')

    dates = pd.date_range(dt(2024, 1, 1), dt(2025, 1, 1), freq='MS')
    ranges = np.array(list(zip(
        dates[:-1], dates[1:]
    )))

    res = get_overlap_days(
        sts=pd.to_numeric(sts.values),
        exps=pd.to_numeric(exps.values),
        ranges=ranges
    )
    print(res)


if __name__ == '__main__':
    test_get_overlap_days()
