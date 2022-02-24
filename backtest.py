import time

import pypika
import sqlite3
import pandas as pd
import strategy.turtle as turtle
import quotalib.quota as quota


# params
params = {
    'risk': 0.0025,
    'atrLength': 180,
    'maLength': 180,
    'switch': {
        'stop': True, 'add': True, 'long': True, 'short': True
    },
    'limit': 4,
    'add': 0.5,
    'stop': 2,
    'length': {
        'enter': {'short': 180, 'long': 720},
        'exit': {'short': 90, 'long': 360}
    },
    'adjust': 0.9,
    'slip': 0.1/100,
    'fee': 0.1/100
}


def create_data(_id):
    path = './data/%s_database.db' % _id
    import os
    if os.path.exists(path):
        con = sqlite3.connect(path)
        cur = con.cursor()
        cur.execute("DROP TABLE pos_data;")
    else:
        con = sqlite3.connect(path)
        cur = con.cursor()
    cur.execute("CREATE TABLE pos_data (id TEXT, cost_price REAL, open_price REAL, stop_price REAL, add_price REAL, net REAL, unit REAL, position INTEGER, sys TEXT)")
    cur.execute("INSERT INTO pos_data VALUES (?, 0, 0, 0, 0, 10000, 0, 0, 'short')", (_id,))
    con.commit()
    con.close()


def read_database(id='BTC', period='1H'):
    def dict_factory(cursor, row):
        d = {}
        for index, col in enumerate(cursor.description):
            d[col[0]] = row[index]
        return d
    path = './data/%s_database.db' % id
    con = sqlite3.connect(path)
    con.row_factory = dict_factory
    cur = con.cursor()
    cur.execute("SELECT * FROM pos_data where id=?", (id, ))
    rows = cur.fetchall()
    con.commit()
    con.close()
    result, result['records'] = rows[0], [pd.read_csv, 'data/record/%s-USDT-SWAP_%s.csv' % (id, period)]
    return result


def updata_database(id='BTC', key=None, value=None):
    path = './data/%s_database.db' % id
    con = sqlite3.connect(path)
    cur = con.cursor()
    if key == 'sys':
        exec = str("UPDATE pos_data SET %s = '%s' WHERE id = '%s';" % (key, value, id))
    else:
        exec = str("UPDATE pos_data SET %s = %s WHERE id = '%s';" % (key, value, id))
    cur.execute(exec)
    con.commit()
    con.close()


def data_view(inst_id='BTC', period='1H'):
    data = read_database(id=inst_id, period=period)
    return data


def data_to_df(data):
    df = data['records'][0](data['records'][1], index_col=0)
    print(data.keys())
    df[list(data.keys())] = 0
    del df['id'], df['records']
    return df


def df_to_signal(df):
    df = df.sort_values(by='ts')
    df.reset_index(inplace=True)
    del df['index']
    df['ma'] = df['close'].rolling(21).mean()
    df['net'] = 10000
    df['h-l'] = df['high'] - df['low']
    df['h-c'] = df['high'] - df['close'].shift()
    df['c-l'] = df['close'].shift() - df['low']
    df['tr'] = df[['h-l', 'h-c', 'c-l']].max(axis=1)
    df['atr'] = df['tr'].rolling(21).mean()
    df_chan = df[['high', 'low']].shift(1)
    df_chan['max_enter'] = df_chan['high'].rolling(720).max()
    df_chan['min_enter'] = df_chan['low'].rolling(720).min()
    df_chan['max_exit'] = df_chan['high'].rolling(360).max()
    df_chan['min_exit'] = df_chan['low'].rolling(360).min()
    df_chan['close'] = df['close']
    df['signal_long_open'] = df['high'] > df_chan['max_enter']
    df['signal_short_open'] = df['low'] < df_chan['min_enter']
    df['signal_long_close'] = df['high'] > df_chan['max_enter']
    df['signal_short_close'] = df['low'] < df_chan['min_enter']
    def signals(x, y, z):
        if x is True:
            print(y, z)
            return z + (y*0.5)
    df['add_price_long'] = df_chan['max_enter']
    print(df[['signal_long_open', 'add_price']][df['signal_long_open']==True])
    del df['h-l'], df['h-c'], df['c-l'], df['tr']
    return df


def test_main(params_=params, id_='BTC', period='1H'):

    def df_to_list(df_):
        arr = list()
        for i in range(len(df_)):
            _record = list(df_.values[i])
            _temp = [int(_record[0]), _record[1], _record[2], _record[3], _record[4], int(_record[5])]
            arr.append(_temp)
        return arr

    create_data(id_)
    database = data_view(inst_id=id_, period=period)
    df_records = database['records'][0](database['records'][1], index_col=0).iloc[::-1].reset_index(drop=True)
    print(df_records)
    print(len(df_records.iloc[:2241599222]), len(df_records))
    # data.iloc[::-1]
    df_pnl = pd.DataFrame()
    print(id_, len(df_records))

    if len(df_records) < 800:
        return None

    for i in range(len(df_records)):
        database = data_view(inst_id=id_, period=period)
        if 800 <= len(df_records.iloc[i:i+800]):
            records = df_to_list(df_records.iloc[i:i + 800])[::-1]
            database['records'] = records
            stg = turtle.Turtle(params_, database)
            signal = stg.signal()
            if isinstance(signal, dict):
                print(id_, signal, stg.unit(), database['unit'])
                if signal['type'] == 'open':
                    updata_database(id=id_, key='add_price', value=signal['add_price'])
                    updata_database(id=id_, key='stop_price', value=signal['stop_price'])
                    updata_database(id=id_, key='open_price', value=signal['price'])
                    net_sub = database['net'] - stg.unit() * params_['fee']
                    if signal['side'] == 'long':
                        database['unit'] += stg.unit()
                        signal_price = signal['price'] * (1 + params_['slip'])
                        updata_database(id=id_, key='cost_price', value=signal_price)
                        updata_database(id=id_, key='position', value=1)
                        updata_database(id=id_, key='unit', value=round(database['unit'], 1))
                        updata_database(id=id_, key='net', value=net_sub)
                    elif signal['side'] == 'short':
                        database['unit'] -= stg.unit()
                        signal_price = signal['price'] * (1 - params_['slip'])
                        updata_database(id=id_, key='cost_price', value=signal_price)
                        updata_database(id=id_, key='position', value=-1)
                        updata_database(id=id_, key='unit', value=round(database['unit'], 1))
                        updata_database(id=id_, key='net', value=net_sub)
                elif signal['type'] == 'add':
                    updata_database(id=id_, key='add_price', value=signal['add_price'])
                    updata_database(id=id_, key='stop_price', value=signal['stop_price'])
                    updata_database(id=id_, key='open_price', value=signal['price'])
                    if signal['side'] == 'long':
                        database['position'] += 1
                        database['unit'] += stg.unit()
                        unit_rate = stg.unit() / database['unit']
                        signal_price = signal['price'] * (1 + params_['slip'])
                        cost_price = unit_rate * signal_price + (1-unit_rate)*database['cost_price']
                    else:
                        database['position'] -= 1
                        database['unit'] -= stg.unit()
                        unit_rate = stg.unit()*-1 / database['unit']
                        signal_price = signal['price'] * (1 - params_['slip'])
                        cost_price = unit_rate * signal_price + (1-unit_rate)*database['cost_price']
                    updata_database(id=id_, key='unit', value=round(database['unit'], 1))
                    updata_database(id=id_, key='cost_price', value=cost_price)
                    updata_database(id=id_, key='position', value=database['position'])
                    net_sub = database['net'] - stg.unit() * params_['fee']
                    updata_database(id=id_, key='net', value=net_sub)
                elif signal['type'] == 'close' or signal['type'] == 'stop':
                    if database['cost_price'] < signal['price']:
                        updata_database(id=id_, key='sys', value='long')
                    else:
                        updata_database(id=id_, key='sys', value='short')
                    if signal['side'] == 'long':
                        signal_price = signal['price'] * (1 - params_['slip'])
                    else:
                        signal_price = signal['price'] * (1 + params_['slip'])
                    pnl = signal_price / database['cost_price'] - 1
                    database['net'] += (pnl-params_['fee']) * database['unit']
                    updata_database(id=id_, key='net', value=database['net'])
                    updata_database(id=id_, key='unit', value=0)
                    updata_database(id=id_, key='position', value=0)
                    updata_database(id=id_, key='add_price', value=0)
                    updata_database(id=id_, key='stop_price', value=0)
                    updata_database(id=id_, key='open_price', value=0)
                    updata_database(id=id_, key='cost_price', value=0)
            else:
                if database['position'] != 0:
                    pnl = database['records'][0][4]/database['cost_price']-1
                    if database['position'] > 0:
                        database['net'] += pnl * database['unit']
                        database['unit'] += pnl * database['unit']
                    else:
                        database['net'] += pnl * database['unit']
                        database['unit'] -= pnl * database['unit']
                    data_temp = {
                        'net': round(database['net'], 1), 'unit': round(database['unit'], 1),
                        'ts': str(database['records'][0][0]), 'position': int(database['position']),
                        'rate': database['unit'] / database['net'], 'price': database['records'][0][4]
                    }
                else:
                    data_temp = {
                        'net': round(database['net'], 1), 'unit': 0,
                        'ts': str(database['records'][0][0]), 'position': 0,
                        'rate': 0, 'price': database['records'][0][4]
                    }
                df_pnl = df_pnl.append(data_temp, ignore_index=True)
                print(df_pnl.tail(3))
        else:
            period = params_['length']['enter']['short']
            df_pnl.to_csv('./data/result/%s_%s.csv' % (id_, period))
            return df_pnl
    period = params_['length']['enter']['short']
    df_pnl.to_csv('./data/result/%s_%s.csv' % (id_, period))
    return df_pnl


def create_thread():
    from time import sleep, ctime
    import threading
    from multiprocessing import Process
    print('starting at:', ctime())
    process = dict()
    inst_list = pd.read_csv('./data/inst_list.csv')
    inst_list = ['BTC', 'ETH', 'DOT', 'LTC']
    for i in range(len(inst_list)):
        inst_id = inst_list[i]
        p = Process(target=test_main, args=(params, inst_id, '5m'))
        process[inst_id] = p

    print(process)
    print(ctime())

    for i in range(len(inst_list)):
        #inst_id = inst_list['ctValCcy'].values[i]
        inst_id = inst_list[i]
        process[inst_id].start()

    print('thread', 'start...Done', ctime())

    for i in range(len(inst_list)):
        #inst_id = inst_list['ctValCcy'].values[i]
        inst_id = inst_list[i]
        process[inst_id].join()
        print('thread', inst_id, 'join...')

    print('all Done at:', ctime())

#create_thread()
if __name__ == '__main__':
    # create_thread()
    test_main(id_='BTC', period='1H')


'''

data = database()
print(data)
print(data['records'][0](data['records'][1], index_col=0))

'''

# 'data/record/%s-USDT-SWAP_%s.csv' % ('BTC', '1H'), index_col=0)
