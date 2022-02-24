import config as conf
from api import okex
from api import binance
from api import ftx
import random


class API(object):
    def __init__(self, exchange):
        self.exchange = str(exchange).upper()
        if str(exchange).upper() == 'OKEX':
            self.api = okex.Swap(conf.ok_key['apikey'], conf.ok_key['secretkey'], conf.ok_key['passphrase'])
        elif str(exchange).upper() == 'BINANCE':
            self.api = binance.Swap(conf.bn_key['apikey'], conf.bn_key['secretkey'])

    def get_kline(self, _id, _bar, _len, _after=None, _before=None):
        _arr = list()
        if self.exchange == 'OKEX':
            _api_ = okex.Swap(conf.ok_key['apikey'], conf.ok_key['secretkey'], conf.ok_key['passphrase'])
            if _len > 100:
                i = 0
                while True:
                    i += 1
                    try:
                        _result = _api_.get_kline_his(_id, _bar, _after, _before, limit=str(100))
                    except:
                        continue
                    if _result['code'] == '0':
                        if len(_result['data']) == 0:
                            break
                        _after = _result['data'][-1][0]
                        for j in range(len(_result['data'])):
                            _arr.append(_result['data'][j])
                    else:
                        print('kline err', _result)
                        continue
                    if len(_arr) > _len:
                        break
                return _arr
            else:
                _result = _api_.get_kline_his(_id, _bar, _after, _before, limit=str(_len))
                if _result['code'] == '0':
                    return _result['data']
                else:
                    print('kline err', _result)
                    return False
        elif self.exchange == 'BINANCE':
            pass
        elif self.exchange == 'FTX':
            if str(_bar)[-1] == 'm':
                _bar = int(str(_bar)[:-1]) * 60
            if str(_bar)[-1] == 'h':
                _bar = int(str(_bar)[:-1]) * 60 * 60
            if str(_bar)[-1] == 'd':
                _bar = int(str(_bar)[:-1]) * 60 * 60 * 24
            if str(_bar)[-1] == 'w':
                _bar = int(str(_bar)[:-1]) * 60 * 60 * 24 * 7
            if str(_bar)[-1] == 'M':
                _bar = int(str(_bar)[:-1]) * 60 * 60 * 24 * 7 * 30

    def get_account_totalEq(self, ccy=None):
        _api_ = okex.Swap(conf.ok_key['apikey'], conf.ok_key['secretkey'], conf.ok_key['passphrase'])
        result = _api_.get_account_balance(ccy)
        if result['code'] == '0':
            totalEq = float(result['data'][0]['totalEq'])
        else:
            return False
        return totalEq
