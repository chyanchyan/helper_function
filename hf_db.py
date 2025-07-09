"""
数据库操作辅助函数模块

该模块提供了DataFrame与数据库的交互、批量导入导出、表结构检查等功能。
主要功能包括：
1. DataFrame数据写入数据库（支持冲突处理）
2. 批量DataFrame写入数据库
3. 数据库表导出为Excel文件
4. 从数据库读取数据到DataFrame
"""

import os.path
import traceback

import pandas as pd
from sqlalchemy import text
from sqlalchemy import inspect

import sys
import os

# 获取当前文件所在目录的绝对路径
current_dir = os.path.dirname(os.path.abspath(__file__))
# 获取父目录路径
parent_dir = os.path.dirname(current_dir)

# 将父目录添加到Python路径中，以便导入mint模块
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
    """
    将DataFrame数据写入数据库表
    
    支持主键冲突处理、数据去重、数据更新等多种模式。
    
    Args:
        df: 要写入的DataFrame
        name: 数据库表名
        check_cols: 用于检查冲突的列名列表，通常为主键字段
        if_conflict: 冲突处理方式
            - 'keep': 保留现有数据，只插入新数据
            - 'replace': 删除冲突数据后重新插入
            - 'fill': 只填充空值或'待补充'的字段
            - 'fill_update': 更新所有不同的字段
            - 'skip': 跳过冲突数据，只插入新数据
        con: 数据库连接对象
        schema: 数据库schema名称
        index: 是否写入DataFrame的索引列
    
    Returns:
        DataFrame: 冲突的数据记录（如有）
    
    Raises:
        KeyError: 当check_cols中的字段在DataFrame中不存在时
    """
    # 如果指定了检查列，则进行冲突检测
    if check_cols is not None and len(check_cols) > 0:
        # 查询数据库中已存在的数据
        sql = f'select {", ".join(check_cols)} from {schema}.{name}'
        print(sql)
        data_exists = pd.read_sql(sql=sql, con=con)
        
        # 构建过滤条件：不在已存在数据中的记录
        not_in_str = ' & '.join([
            f'~df["{check_field}"].isin(data_exists["{check_field}"])'
            for check_field in check_cols
        ])
        # 构建过滤条件：在已存在数据中的记录（冲突数据）
        or_in_str = ' | '.join([
            f'df["{check_field}"].isin(data_exists["{check_field}"])'
            for check_field in check_cols
        ])

        try:
            # 获取新数据（不冲突的数据）
            df_new = eval('df[%s]' % not_in_str)
        except KeyError:
            raise KeyError("检查列中的字段在DataFrame中不存在")
        # 获取冲突数据
        df_conflict = eval('df[%s]' % or_in_str)
    else:
        # 如果没有指定检查列，则所有数据都视为新数据
        df_new = df.copy()
        # 创建一个空的DataFrame作为冲突数据
        df_conflict = pd.read_sql(sql=f'select * from {schema}.{name} limit 0', con=con)

    # 根据冲突处理策略执行相应操作
    if if_conflict == 'keep':
        # 保留现有数据，只插入新数据
        df_new.to_sql(name=name, con=con, schema=schema, if_exists='append', index=index)

    elif if_conflict == 'replace':
        # 删除冲突数据后重新插入
        for check_field in check_cols:
            del_value = set(df_conflict[check_field])
            if len(del_value) > 0:
                print('*' * 100)
                print('from table: %s' % name)
                print('deleting:')
                print(del_value)
                print('*' * 100)
                # 用户确认是否继续删除
                if input('continue') == 'y':
                    # 构建删除SQL语句
                    del_value = ['"%s"' % item for item in del_value]
                    del_value = ', '.join(del_value)
                    sql = 'delete from %s where `%s` in (%s)' % (name, check_field, del_value)
                    cursor = con.connect()
                    cursor.execute(text(sql))
                    cursor.commit()
                    continue
                else:
                    return
        # 插入所有数据
        df.to_sql(name=name, con=con, schema=schema, if_exists='append', index=index)
        
    elif if_conflict == 'fill' or if_conflict == 'fill_update':
        # 逐行处理数据，进行填充或更新
        for i, row in df.iterrows():
            # 构建WHERE条件，用于查找现有记录
            c = ' and '.join(
                [f'`{check_field}` = "{row[check_field].replace("%", "%%")}"' for check_field in check_cols
                 if not pd.isna(row[check_field]) and row[check_field]]
            )
            if len(c) > 0:
                sql = f'select * from {schema}.{name} where {c}'
            else:
                sql = f'select * from {schema}.{name}'
            
            # 查询现有记录
            ori_row = pd.read_sql(sql=sql, con=con)
            if len(ori_row) == 1:
                # 找到唯一匹配的记录，进行更新
                sqls = []
                if if_conflict == 'fill':
                    # 只填充空值或'待补充'的字段
                    for field in row.index:
                        if (pd.isna(ori_row.iloc[0, :][field]) or ori_row.iloc[0, :][field] == '待补充')\
                                and not pd.isna(row[field]):
                            value = row[field]
                            sql = f'UPDATE `{schema}`.`{name}` ' \
                                  f'SET `{field}` = "{value}" ' \
                                  f'WHERE ({c});'
                            sqls.append(sql)
                elif if_conflict == 'fill_update':
                    # 更新所有不同的字段
                    for field in row.index:
                        if not pd.isna(row[field]) and row[field] != ori_row[field].values[0]:
                            value = row[field]
                            sql = f'UPDATE `{schema}`.`{name}` ' \
                                  f'SET `{field}` = "{value}" ' \
                                  f'WHERE ({c});'
                            sqls.append(sql)

                # 执行更新SQL语句
                if len(sqls) > 0:
                    for sql in sqls:
                        print(sql)
                        con.execute(text(sql))
                    con.commit()
            else:
                # 没有找到匹配记录，插入新记录
                row = pd.DataFrame([row], index=None)
                row.to_sql(name=name, con=con, schema=schema, if_exists='append', index=False)

    else:
        # 默认策略：跳过冲突数据，只插入新数据
        print('skipping dup data:')
        print(df_conflict)
        df_new = pd.DataFrame(df)
        df_new.to_sql(name=name, con=con, schema=schema, if_exists='append', index=index)

    return df_conflict


def dfs_to_db(con, d_dfs, tree, schema):
    """
    批量将多个DataFrame写入数据库
    
    按照tree.booking_sequence的顺序处理DataFrame，支持数据清理和冲突处理。
    
    Args:
        con: 数据库连接对象
        d_dfs: 包含DataFrame的字典，键为表名
        tree: 包含表结构和依赖关系的树形对象
        schema: 数据库schema名称
    """
    # 按照预定义的顺序处理每个表
    for node_root in tree.booking_sequence:
        try:
            d_dfs[node_root]
        except KeyError:
            # 如果字典中没有该表的数据，跳过
            continue

        # 获取表结构和数据
        table = tree.tables[node_root]
        df = d_dfs[node_root]
        
        # 清理字符串数据：去除首尾空格
        df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        print(node_root)
        print(df)
        print('*' * 100)
        
        # 如果数据为空，跳过
        if len(df) == 0:
            continue
            
        # 将数据写入数据库
        df_to_db(
            df=df,
            name=node_root,
            check_cols=[
                col.col_name for col in table.cols.values()
                if col.check_pk == 1],  # 使用主键字段作为检查列
            if_conflict='fill_update',  # 使用填充更新策略
            con=con,
            schema=schema
        )


def export_xl(output_folder, con, schema, table_names=None):
    """
    将数据库表导出为Excel文件
    
    Args:
        output_folder: 输出文件夹路径
        con: 数据库连接对象
        schema: 数据库schema名称
        table_names: 要导出的表名列表，如果为None则导出所有表
    
    Raises:
        Exception: 当数据库查询失败时
    """
    # 确保输出文件夹存在
    if not os.path.exists(output_folder):
        mkdir(output_folder)

    sql_template = 'select * from %s.%s'

    # 如果没有指定表名，则获取schema中的所有表
    if table_names is None:
        insp = inspect(con)
        table_names = insp.get_table_names(schema=schema)

    # 逐个导出表
    for table_name in table_names:
        sql = sql_template % (schema, table_name)
        file_path = os.path.join(output_folder, table_name + '.xlsx')
        try:
            # 从数据库读取数据
            df = pd.read_sql(sql=sql, con=con)
        except Exception as e:
            print(traceback.format_exc())
            raise e

        # 导出为Excel文件
        df.to_excel(file_path, index=False)

        print(f'table {table_name} exported. path: {file_path}')


def get_data_df(con, table_name, cols=None):
    """
    从数据库表读取数据到DataFrame
    
    Args:
        con: 数据库连接对象
        table_name: 表名
        cols: 要读取的列名列表，如果为None则读取所有列
    
    Returns:
        DataFrame: 包含查询结果的DataFrame
    """
    # 构建列名SQL字符串
    if cols is None:
        col_sql_str = '*'
    else:
        col_sql_str = get_col_sql_str(cols=cols)

    # 构建查询SQL
    sql = f'select {col_sql_str} from {table_name}'

    # 执行查询并返回结果
    data = pd.read_sql(
        sql=sql,
        con=con
    )
    return data