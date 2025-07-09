"""
加密相关辅助函数模块

该模块提供了常用的加密和哈希函数，包括HMAC-SHA256签名、SHA1哈希和UUID生成等功能。
"""

from datetime import datetime as dt
import os
import hashlib
import hmac


def sign_by_sha256(data, key):
    """
    使用HMAC-SHA256算法对数据进行签名
    
    Args:
        data: 要签名的数据字符串
        key: 签名密钥字符串
        
    Returns:
        str: 十六进制格式的签名结果
    """
    signature_computed = hmac.new(
        key=key.encode('utf-8'),
        msg=data.encode('utf-8'),
        digestmod=hashlib.sha256
    ).hexdigest()
    return signature_computed


def hash_string_by_sha1(s, encoding='utf-8'):
    """
    使用SHA1算法对字符串进行哈希
    
    Args:
        s: 要哈希的字符串
        encoding: 字符串编码格式，默认为utf-8
        
    Returns:
        str: 十六进制格式的SHA1哈希值
    """
    return hashlib.sha1(str(s).encode(encoding=encoding)).hexdigest()


def gen_uuid():
    """
    生成基于时间戳和随机数的UUID
    
    使用当前时间戳和随机字节生成唯一的标识符
    
    Returns:
        str: 基于SHA1哈希的UUID字符串
    """
    return hash_string_by_sha1(str(os.urandom(16)) + dt.now().strftime(
        '%Y%m%d%H%M%S%f'))
