

class Exchange(object):

    def __init__(self, api_key, secret_key, passphrase=''):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase

    @staticmethod
    def get_timestamp_utc():
        import datetime
        now = datetime.datetime.utcnow()
        t = now.isoformat("T", "milliseconds")
        return t + "Z"

    @staticmethod
    def get_timestamp_ms():
        import time
        return int(round(time.time() * 1000))

    @staticmethod
    def _process_response(response):
        try:
            data = response.json()
        except ValueError:
            import time
            print(response.status_code, 'http code')
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            response.raise_for_status()
            raise
        else:
            return data

    @staticmethod
    def params_to_url_str(params):
        param_url = ''
        if params is not None:
            for k, v in params.items():
                if v is None:
                    continue
                param_url += k + '=' + v + '&'
        return param_url[:-1]

    @staticmethod
    def path_to_url(url, path):
        return url + path
