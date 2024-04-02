import os
import traceback
import pandas as pd
import numpy as np
from openpyxl.utils import get_column_letter
from sqlalchemy.exc import OperationalError


def fit_col_width(writer, df, sheet_name):
    # 自适应列宽
    column_widths = (
        df.columns.to_series().apply(lambda x: len(x.encode('utf-8'))).values
    )

    max_widths = (
        df.astype(str).applymap(lambda x: len(x.encode('utf-8'))).agg(max).values
    )

    max_widths = pd.Series(max_widths).fillna(0).to_list()
    widths = np.max([column_widths, max_widths], axis=0)

    worksheet = writer.sheets[sheet_name]
    for i, width in enumerate(widths, 1):
        if 'date' in df.columns[i - 1] or df.columns[i - 1][-1] == '日':
            worksheet.column_dimensions[get_column_letter(i)].width = 22
        else:
            worksheet.column_dimensions[get_column_letter(i)].width = max(min(width + 2, 25), 10)


def migration_pandas(engine, data_path, schema, if_exists):
    name = os.path.basename(data_path)[:-5]
    try:
        data = pd.read_excel(data_path, index_col=False)

        regular_cols = [col for col in data.columns.tolist() if col[:3] != 'dv_']
        dv_cols = [col for col in data.columns.tolist() if col[:3] == 'dv_']
        data_to_db = data[regular_cols]

        if name == 'project':
            table_name = '项目信息'
            row_index = 'name'
        elif name == 'project_level':
            table_name = '项目分级信息'
            row_index = 'name'
        elif name == 'project_change':
            table_name = '项目分级变动信息'
            row_index = 'project_level_name'
        else:
            table_name = name
            row_index = 'id'
        if len(dv_cols) > 0:
            for i, r in data.iterrows():
                for col in dv_cols:
                    if not pd.isna(r[col]):
                        print(
                            f'表：{table_name} \n'
                            f'行：{r[row_index]} \n'
                            f'列：{col.strip("dv_")} \n'
                            f'自： {data_to_db.loc[i, col.strip("dv_")]} \n'
                            f'更新为：{r[col]} \n')
                        print('*' * 50)
                        data_to_db.loc[i, col.strip('dv_')] = r[col]

        data_to_db.to_sql(
            name=name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False
        )
    except FileNotFoundError:
        print(f'table: {name} xlsx file not exist')
    except OperationalError as e:
        print(traceback.format_exc())
        raise e
