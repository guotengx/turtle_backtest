import time

import pandas as pd
from api import okex as ok
from api_agg import API
from quotalib import tools
import config as conf

api = API('OKEX')

api_ok = ok.Swap(conf.ok_key['apikey'], conf.ok_key['secretkey'], conf.ok_key['passphrase'])


def generate_inst_list(inst_type='SWAP', uly=None, inst_id=None, quote_type='USDT'):
    arr = list()
    inst_list = api_ok.get_instruments(inst_type, uly, inst_id)
    if inst_list['code'] == '0':
        for j in range(len(inst_list['data'])):
            if str(inst_list['data'][j]['uly']).split('-')[1] == quote_type:
                arr.append(inst_list['data'][j])
        df = pd.DataFrame(arr, index=list(range(len(arr))), columns=[
            "instType", "instId", "uly", "category", "baseCcy", "quoteCcy", "settleCcy", "ctVal",
            "ctMult", "ctValCcy", "optType", "stk", "listTime", "expTime", "lever", "tickSz",
            "lotSz", "minSz", "ctType", "alias", "state"
        ])
        df.to_csv('./data/inst_list.csv')
        print('generate success')
        return df
    else:
        print('generate failed')
        return False


def save_kline_data(inst_id, period='5m'):
    df = pd.DataFrame()
    _after = None
    inst_id = inst_id+'-USDT-SWAP'
    while True:
        result = api.get_kline(inst_id, period, 1000, _after=_after)
        if len(result) == 0:
            break
        if result is not False:
            _after = result[-1][0]
            Tools = tools.Tools()
            df_new = Tools.records_to_df(result)
            df = df.append(df_new, ignore_index=True)
            print(df_new)
            print(_after)
    df.to_csv('./data/record/%s_%s.csv' % (inst_id, period))
    print('%s %s success' % (inst_id, period))
    return True


def create_thread(inst_id, period):
    from time import sleep, ctime
    import threading, random
    from multiprocessing import Process
    print('starting at:', ctime())
    threads = dict()
    inst_list = pd.read_csv('./data/inst_list.csv')
    for i in range(len(period)):
        p = threading.Thread(target=save_kline_data, args=(inst_id, period[i]))
        threads[i] = p

    print(threads)
    print(ctime())

    for i in range(len(period)):
        time.sleep(1)
        threads[i].start()

    print('thread', 'start...Done', ctime())

    for i in range(len(period)):
        threads[i].join()

    print('all Done at:', ctime())

# print(generate_inst_list())

if __name__ == '__main__':
    #create_thread()
    '''
        inst_list = pd.read_csv('./data/inst_list.csv')
    for i in range(len(inst_list)):
        inst_id = str(inst_list['instId'].values[i]).split('-')[0]
        pass_arr = ['BTC', 'ETH', 'DOT', 'LTC', 'DOGE', 'FTM', 'ATOM', 'NEAR', 'LUNA', 'ADA', '1INCH', 'AAVE', 'AGLD']
        if i >= len(pass_arr):
            print(inst_id)
            print(save_kline_data(inst_id, '5m'))
    '''

    inst_list = pd.read_csv('./data/inst_list.csv')
    for i in range(len(inst_list)):
        inst_id = str(inst_list['instId'].values[i]).split('-')[0]
        save_kline_data(inst_id, '1m')






