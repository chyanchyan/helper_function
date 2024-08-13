from mint.helper_function.hf_string import *
import pandas as pd
import numpy as np


def test_json_encoding_class():
    jo = {
        "test": "test",
        'df': pd.DataFrame(
            data={
                'a': [np.nan]
            }
        )
    }
    js = to_json_str(jo)
    print(js)
    jo = to_json_obj(js)
    print(jo)


if __name__ == '__main__':
    test_json_encoding_class()