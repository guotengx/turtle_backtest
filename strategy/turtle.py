import pandas as pd
import quotalib.quota as quota


class Turtle(object):
    def __init__(self, params, database):
        self.database = database
        self.params = params
        self.ma = quota.ma(database['records'][::-1], params['maLength'])
        self.atr = quota.atr(database['records'][::-1], self.params['atrLength'])
        self.atr_s = quota.atr(database['records'][::-1], int(self.params['atrLength']/4.28))
        self.last_record = database['records'][0]
        '''
        {
            'risk': 0.01,
            'atrLength': 21,
            'switch': {
                'stop': True, 'add': True, 'long': True, 'short': True
            },
            'limit': 4,
            'add': 0.5,
            'stop': 2,
            'length': {
                'enter': {'short': 720, 'long': 720},
                'exit': {'short': 360, 'long': 360}
            }
        }
        '''

    def unit(self):
        amount = self.database['net'] * self.params['risk']
        volatility = self.atr['atr'] / self.ma
        return amount/volatility*self.params['adjust']

    def channel(self, mode):
        records = self.database['records'][::-1]
        if self.database['sys'] == 'short':
            if mode == 'enter':
                return quota.dc(records, self.params['length']['enter']['short'])
            else:
                return quota.dc(records, self.params['length']['exit']['short'])
        else:
            if mode == 'enter':
                return quota.dc(records, self.params['length']['enter']['long'])
            else:
                return quota.dc(records, self.params['length']['exit']['long'])

    def signal(self):
        #if self.database['position'] == 0 and self.atr_s['atr'] > self.atr['atr']:
        if self.database['position'] == 0 and self.atr_s['atr'] > self.atr['atr']:
            if self.last_record[2] > self.channel('enter')['max'] and self.params['switch']['long']:
                return {
                    'side': 'long', 'type': 'open', 'price': self.channel('enter')['max'],
                    'add_price': self.channel('enter')['max'] + self.atr['atr'] * self.params['add'],
                    'stop_price': self.channel('enter')['max'] - self.atr['atr'] * self.params['stop']
                }
            elif self.last_record[3] < self.channel('enter')['min'] and self.params['switch']['short']:
                return {
                    'side': 'short', 'type': 'open', 'price': self.channel('enter')['min'],
                    'add_price': self.channel('enter')['min'] - self.atr['atr'] * self.params['add'],
                    'stop_price': self.channel('enter')['min'] + self.atr['atr'] * self.params['stop']
                }
        if self.database['position'] > 0:
            if self.last_record[2] > self.database['add_price'] and self.database['position'] < self.params['limit']:
                return {
                    'side': 'long', 'type': 'add', 'price': self.database['add_price'],
                    'add_price': self.database['add_price'] + self.atr['atr'] * self.params['add'],
                    'stop_price': self.database['add_price'] - self.atr['atr'] * self.params['stop']
                }
            #elif self.last_record[3] < self.channel('exit')['min'] and self.atr_s['atr'] < self.atr['atr']:
            elif self.last_record[3] < self.channel('exit')['min'] and self.atr_s['atr'] < self.atr['atr']:
                return {
                    'side': 'long', 'type': 'close', 'price': self.channel('exit')['min']
                }
            elif self.last_record[3] < self.database['stop_price'] and self.params['switch']['stop']:
                return {'side': 'long', 'type': 'stop', 'price': self.database['stop_price']}
        elif self.database['position'] < 0:
            if self.last_record[3] < self.database['add_price'] and self.database['position']*-1 < self.params['limit']:
                return {
                    'side': 'short', 'type': 'add', 'price': self.database['add_price'],
                    'add_price': self.database['add_price'] - self.atr['atr'] * self.params['add'],
                    'stop_price': self.database['add_price'] + self.atr['atr'] * self.params['stop']
                }
            #elif self.last_record[2] > self.channel('exit')['max'] and self.atr_s['atr'] < self.atr['atr']:
            elif self.last_record[2] > self.channel('exit')['max'] and self.atr_s['atr'] < self.atr['atr']:
                return {'side': 'short', 'type': 'close', 'price': self.channel('exit')['max']}
            elif self.last_record[2] > self.database['stop_price'] and self.params['switch']['stop']:
                return {'side': 'short', 'type': 'stop', 'price': self.database['stop_price']}
        return None





'''

        atr = quota.atr(df.iloc[:-self.atrLength-1], self.atrLength)['atr']
        print(atr, df.iloc[:-self.atrLength-1])
        print('atr first', -self.atrLength-1)
        for i in range(self.atrLength):
            tr = quota.atr(df.iloc[:-self.atrLength+i], self.atrLength)['tr']
            atr = (atr * (self.atrLength-1) + tr) / self.atrLength
        return atr'''



