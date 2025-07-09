"""
元数据处理辅助函数模块

该模块提供了从代码文件中提取元数据信息的辅助函数，包括从注释和函数中提取列名等功能。
"""

import re


def get_column_list_from_comments_in_file(filepath, mark):
    """
    从文件注释中提取列名列表
    
    查找文件中以特定标记开头的注释行，提取列名信息
    
    Args:
        filepath: 文件路径
        mark: 标记字符串，用于识别特定的注释行
        
    Returns:
        list: 提取到的列名列表
    """
    # 读取文件内容
    rs = open(filepath, 'r', encoding='utf-8').readlines()
    # 去除每行的空白字符
    rs = [line.strip() for line in rs]
    # 提取以特定标记开头的注释行，并去除标记部分
    rs = [line[len(mark) + 3:] for line in rs if line[:len(mark) + 3] == f'# {mark} ']

    # 处理特殊标记[delta]，将其替换为前一个列名+差异
    for i, r in enumerate(rs):
        if r == '[delta]':
            rs[i] = rs[i - 1] + '差异'

    return rs


def get_column_list_from_strings_in_function(filepath, function_name, pattern):
    """
    从指定函数中提取匹配模式的字符串列表
    
    在文件中查找指定函数，然后在该函数体内查找匹配正则表达式的字符串
    
    Args:
        filepath: 文件路径
        function_name: 函数名
        pattern: 正则表达式模式，用于匹配字符串
        
    Returns:
        list: 匹配模式的字符串列表（去重后）
    """
    # 读取文件内容
    rs = open(filepath, 'r', encoding='utf-8').readlines()
    # 去除每行的空白字符
    rs = [line.strip() for line in rs]
    
    i = 0
    j = 0
    if len(rs) == 0:
        return []
    
    # 查找指定函数的开始和结束位置
    for i, r in enumerate(rs):
        if r[:4] == 'def ':
            if r[4:].split('(')[0] == function_name:
                # 找到函数开始位置，继续查找函数结束位置
                for j, rr in enumerate(rs[i + 1:], start=i + 1):
                    if rr[:4] == 'def ':
                        break
                break

    res = []
    # 提取函数体内容
    rs = rs[i + 1: j]
    
    # 在函数体中查找匹配模式的字符串
    for r in rs:
        for i in range(len(r)):
            if r[i] == "'":
                # 找到单引号，查找匹配的结束单引号
                for j in range(len(r[i + 1:])):
                    if r[j] == "'":
                        s = r[i + 1: j]
                        # 检查字符串是否匹配模式
                        if re.match(pattern, s) is not None:
                            res.append(s)
                            i = j + 1
    
    # 去重并返回结果
    res = list(set(res))
    return res
