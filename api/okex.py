import time

from exchange import Exchange
import requests
import json
import base64
import hmac


class Okex(Exchange):

    url = "https://www.okex.com"

    def url_conv(self, path, params):
        req_url = self.url + path
        param_url = ''
        if params is not None:
            for k, v in params.items():
                if v is None:
                    continue
                param_url += k + '=' + v + '&'
        if param_url == '':
            return req_url
        return req_url+'?'+param_url[:-1]

    def _get(self, path, params=None):
        return self._request('GET', path, params)

    def _post(self, path, params=None):
        return self._request('POST', path, params)

    def _sign(self, data):
        sign_ = hmac.new(bytes(self.secret_key, encoding='utf8'), bytes(data, encoding='utf-8'), digestmod='sha256')
        sign_ = sign_.digest()
        sign_ = base64.b64encode(sign_)
        return sign_

    def _request(self, method, path, params):
        timestamp = Exchange.get_timestamp_utc()
        if params is None:
            data = timestamp + method + path
        else:
            data = timestamp + method + self.url_conv(path, params)[len(self.url):]

        header = {
            'Content-Type': 'application/json',
            'OK-ACCESS-KEY': self.api_key,
            'OK-ACCESS-SIGN': self._sign(data),
            'OK-ACCESS-TIMESTAMP': timestamp,
            'OK-ACCESS-PASSPHRASE': self.passphrase,
            'x-simulated-trading': '0'
        }
        if method == 'GET':
            response = requests.get(self.url_conv(path, params), headers=header)
        else:
            response = requests.post(self.url_conv(path, params), data=json.dumps(params), headers=header)

        return self._process_response(response)


class Swap(Okex):

    def get_kline(self, instId, bar, after=None, before=None, limit=None):
        path = "/api/v5/market/candles"
        params = {'instId': instId, 'bar': bar, 'after': after, 'before': before, 'limit': limit}
        return self._get(path, params)

    def get_ticker(self, instId):
        path = "/api/v5/market/ticker"
        params = {'instId': instId}
        return self._get(path, params)

    def get_asset_valuation(self, ccy=None):
        path = "/api/v5/asset/asset-valuation"
        params = {'ccy': ccy}
        return self._get(path, params)

    def get_kline_his(self, instId, bar, after=None, before=None, limit=None):
        path = "/api/v5/market/history-candles"
        params = {'instId': instId, 'bar': bar, 'after': after, 'before': before, 'limit': limit}
        return self._get(path, params)

    def get_instruments(self, instType, uly=None, instId=None):
        path = "/api/v5/public/instruments"
        params = {'instType': instType, 'uly': uly, 'instId': instId}
        return self._get(path, params)

    def get_account_balance(self, ccy=None):
        path = "/api/v5/account/balance"
        params = {'ccy': ccy}
        return self._get(path, params)
