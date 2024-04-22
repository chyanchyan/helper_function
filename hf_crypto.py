import hashlib
import hmac


def sign_by_sha256(data, key):
    signature_computed = hmac.new(
        key=key.encode('utf-8'),
        msg=data.encode('utf-8'),
        digestmod=hashlib.sha256
    ).hexdigest()
    return signature_computed

