from exchange import Exchange
import requests
import json

furl = "https://fapi.binance.com"


class Binance(Exchange):

    def _get(self, url, path, params=None, auth=None):
        return self.__request(url, 'GET', path, params, auth)

    def _post(self, url, path, params=None, auth=None):
        return self.__request(url, 'POST', path, params, auth)

    def __sign(self, data):
        import hmac
        m = hmac.new(self.secret_key.encode("utf-8"), data.encode("utf-8"), digestmod='sha256')
        return m.hexdigest()

    def __request(self, url, method, path, params, auth):

        if auth is None:
            response = requests.get(self.path_to_url(url, path) + '?' + self.params_to_url_str(params))
            return self._process_response(response)

        timestamp = self.get_timestamp_ms()
        data = {
            'recvWindow': '5000', 'timestamp': str(timestamp)
        }
        print(params)

        if params is None:
            data['signature'] = self.__sign(self.params_to_url_str(data))
        else:
            data['signature'] = self.__sign(self.params_to_url_str(params)+'&timestamp='+str(self.get_timestamp_ms()))
            # data = self.params_to_url_str(params) + '&signature=' + self.__sign(self.params_to_url_str(params))

        print(data)

        header = {
            'Content-Type': 'application/json',
            'X-MBX-APIKEY': 'api-key'
        }

        _d = self.params_to_url_str(data) + '&signature='

        if method == 'GET':
            response = requests.get(self.path_to_url(url, path)+'?'+self.params_to_url_str(params), headers=header)
        else:
            response = requests.post(self.path_to_url(url, path), data=json.dumps(params), headers=header)

        return self._process_response(response)


class Swap(Binance):

    furl = "https://fapi.binance.com"

    def get_kline(self, pair, contractType, interval, startTime=None, endTime=None, limit=None):
        path = "/fapi/v1/continuousKlines"
        params = {
            'pair': pair, 'contractType': contractType, 'interval': interval, 'startTime': startTime,
            'endTime': endTime, 'limit': limit
        }
        return self._get(furl, path, params, auth=None)

