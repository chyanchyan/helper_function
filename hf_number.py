"""
数字处理辅助函数模块

该模块提供了数字类型判断、数值范围处理等辅助函数。
"""

def is_int(s):
    """
    判断字符串是否可以转换为整数
    
    Args:
        s: 要检查的字符串
        
    Returns:
        bool: 如果可以转换为整数返回True，否则返回False
    """
    try:
        int(s)
        return True
    except ValueError:
        return False


def is_number(s):
    """
    判断字符串是否可以转换为数字（整数或浮点数）
    
    Args:
        s: 要检查的字符串
        
    Returns:
        bool: 如果可以转换为数字返回True，否则返回False
    """
    try:
        float(s)
        return True
    except ValueError:
        return False
    except TypeError:
        return False


def crop_length(st, ed, c_st=None, c_ed=None):
    """
    计算两个区间的重叠长度
    
    Args:
        st: 第一个区间的起始位置
        ed: 第一个区间的结束位置
        c_st: 第二个区间的起始位置，如果为None则使用第一个区间的起始位置
        c_ed: 第二个区间的结束位置，如果为None则使用第一个区间的结束位置
        
    Returns:
        float: 两个区间的重叠长度，如果没有重叠则返回0
    """
    if not c_st:
        c_st_ = st
    else:
        c_st_ = c_st

    if not c_ed:
        c_ed_ = ed
    else:
        c_ed_ = c_ed

    # 检查是否有重叠
    if c_st_ > c_ed_:
        return 0
    if c_st_ > ed:
        return 0
    if c_ed_ < st:
        return 0

    # 计算重叠长度
    return min(ed, c_ed_) - max(st, c_st_)


def crop(st, ed, c_st, c_ed):
    """
    计算两个区间的交集区间
    
    Args:
        st: 第一个区间的起始位置
        ed: 第一个区间的结束位置
        c_st: 第二个区间的起始位置
        c_ed: 第二个区间的结束位置
        
    Returns:
        tuple: (交集起始位置, 交集结束位置)，如果没有交集则返回边界点
    """
    if not c_st:
        c_st_ = st
    else:
        c_st_ = c_st

    if not c_ed:
        c_ed_ = ed
    else:
        c_ed_ = c_ed

    # 处理无交集的情况
    if c_st_ < st and c_ed_ < st:
        return st, st

    if c_st_ > ed and c_ed_ > ed:
        return ed, ed

    # 计算交集区间
    return min(max(st, c_st_), ed), max(min(ed, c_ed_), st)







