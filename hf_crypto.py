import hashlib
import hmac


def sign_by_sha256(data, key):
    signature_computed = hmac.new(
        key=key.encode('utf-8'),
        msg=data.encode('utf-8'),
        digestmod=hashlib.sha256
    ).hexdigest()
    return signature_computed


def hash_string_by_sha1(s, encoding='utf-8'):
    return hashlib.sha1(str(s).encode(encoding=encoding)).hexdigest()