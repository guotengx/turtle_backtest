from hashlib import sha256
import base64
import hmac


def get_sign(data, key):
    key = key.encode('utf-8')
    message = data.encode('utf-8')
    sign = base64.b64encode(hmac.new(key, message, digestmod=sha256).digest())
    sign = str(sign, 'utf-8')
    print(sign)
    return sign


def _get_sign(self, data):
    m = hmac.new(self.secret.encode("utf-8"), data.encode("utf-8"), sha256)
    return m.hexdigest()


def sign(message, secretKey):
    mac = hmac.new(bytes(secretKey, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
    d = mac.digest()
    return base64.b64encode(d)


def out_sign(exc, key, data):
    if exc == 'OKEX' or exc == 'okex' or exc == 'OKEx' or exc == 'OKex' or exc == 'OK':
        print(key, data)
        sign_ = hmac.new(bytes(key, encoding='utf8'), bytes(data, encoding='utf-8'), digestmod='sha256')
        sign_ = sign_.digest()
        sign_ = base64.b64encode(sign_)
        print(sign_)
        # base64
    else:
        sign_ = hmac.new(key.encode("utf-8"), data.encode("utf-8"), sha256)
        sign_ = sign_.hexdigest()
        # sha256
    return sign_
