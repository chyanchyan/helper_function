import os.path
import traceback

import pandas as pd
from sqlalchemy import text
from sqlalchemy import inspect

import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from helper_function.hf_file import mkdir
from helper_function.hf_string import get_col_sql_str


def df_to_db(
        df: pd.DataFrame, 
        name: str, 
        check_cols=None, 
        if_conflict='skip',
        con=None, 
        schema=None, 
        index=False
):

    if check_cols is not None and len(check_cols) > 0:
        sql = f'select {", ".join(check_cols)} from {schema}.{name}'
        print(sql)
        data_exists = pd.read_sql(sql=sql, con=con)
        not_in_str = ' & '.join([
            f'~df["{check_field}"].isin(data_exists["{check_field}"])'
            for check_field in check_cols
        ])
        or_in_str = ' | '.join([
            f'df["{check_field}"].isin(data_exists["{check_field}"])'
            for check_field in check_cols
        ])

        try:
            df_new = eval('df[%s]' % not_in_str)
        except KeyError:
            raise KeyError
        df_conflict = eval('df[%s]' % or_in_str)
    else:
        df_new = df.copy()
        df_conflict = pd.read_sql(sql=f'select * from {schema}.{name} limit 0', con=con)

    if if_conflict == 'keep':
        df_new.to_sql(name=name, con=con, schema=schema, if_exists='append', index=index)

    elif if_conflict == 'replace':
        for check_field in check_cols:
            del_value = set(df_conflict[check_field])
            if len(del_value) > 0:
                print('*' * 100)
                print('from table: %s' % name)
                print('deleting:')
                print(del_value)
                print('*' * 100)
                if input('continue') == 'y':
                    del_value = ['"%s"' % item for item in del_value]
                    del_value = ', '.join(del_value)
                    sql = 'delete from %s where `%s` in (%s)' % (name, check_field, del_value)
                    cursor = con.connect()
                    cursor.execute(text(sql))
                    cursor.commit()
                    continue
                else:
                    return
        df.to_sql(name=name, con=con, schema=schema, if_exists='append', index=index)
    elif if_conflict == 'fill' or if_conflict == 'fill_update':
        for i, row in df.iterrows():
            c = ' and '.join(
                [f'`{check_field}` = "{row[check_field].replace("%", "%%")}"' for check_field in check_cols
                 if not pd.isna(row[check_field]) and row[check_field]]
            )
            if len(c) > 0:
                sql = f'select * from {schema}.{name} where {c}'
            else:
                sql = f'select * from {schema}.{name}'
            ori_row = pd.read_sql(sql=sql, con=con)
            if len(ori_row) == 1:
                sqls = []
                if if_conflict == 'fill':
                    for field in row.index:
                        if (pd.isna(ori_row.iloc[0, :][field]) or ori_row.iloc[0, :][field] == '待补充')\
                                and not pd.isna(row[field]):
                            value = row[field]
                            sql = f'UPDATE `{schema}`.`{name}` ' \
                                  f'SET `{field}` = "{value}" ' \
                                  f'WHERE ({c});'
                            sqls.append(sql)
                elif if_conflict == 'fill_update':
                    for field in row.index:
                        if not pd.isna(row[field]) and row[field] != ori_row[field].values[0]:
                            value = row[field]
                            sql = f'UPDATE `{schema}`.`{name}` ' \
                                  f'SET `{field}` = "{value}" ' \
                                  f'WHERE ({c});'
                            sqls.append(sql)

                if len(sqls) > 0:
                    for sql in sqls:
                        print(sql)
                        con.execute(text(sql))
                    con.commit()
            else:
                row = pd.DataFrame([row], index=None)
                row.to_sql(name=name, con=con, schema=schema, if_exists='append', index=False)

    else:
        print('skipping dup data:')
        print(df_conflict)
        df_new = pd.DataFrame(df)
        df_new.to_sql(name=name, con=con, schema=schema, if_exists='append', index=index)

    return df_conflict


def dfs_to_db(con, d_dfs, tree, schema):
    for node_root in tree.booking_sequence:
        try:
            d_dfs[node_root]
        except KeyError:
            continue

        table = tree.tables[node_root]
        df = d_dfs[node_root]
        df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        print(node_root)
        print(df)
        print('*' * 100)
        if len(df) == 0:
            continue
        df_to_db(
            df=df,
            name=node_root,
            check_cols=[
                col.col_name for col in table.cols.values()
                if col.check_pk == 1],
            if_conflict='fill_update',
            con=con,
            schema=schema
        )


def export_xl(output_folder, con, schema, table_names=None):

    if not os.path.exists(output_folder):
        mkdir(output_folder)

    sql_template = 'select * from %s.%s'

    if table_names is None:
        insp = inspect(con)
        table_names = insp.get_table_names(schema=schema)

    for table_name in table_names:
        sql = sql_template % (schema, table_name)
        file_path = os.path.join(output_folder, table_name + '.xlsx')
        try:

            df = pd.read_sql(sql=sql, con=con)
        except Exception as e:
            print(traceback.format_exc())
            raise e

        df.to_excel(file_path, index=False)

        print(f'table {table_name} exported. path: {file_path}')


def get_data_df(con, table_name, cols=None):
    if cols is None:
        col_sql_str = '*'
    else:
        col_sql_str = get_col_sql_str(cols=cols)

    sql = f'select {col_sql_str} from {table_name}'

    data = pd.read_sql(
        sql=sql,
        con=con
    )
    return data