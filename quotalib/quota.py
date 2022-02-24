import pandas as pd
from quotalib.tools import Tools


def atr(records, length):
    if isinstance(records, list):
        df = Tools().records_to_df(records)
    else:
        df = records
    df['h-l'] = df['high'] - df['low']
    df['h-c'] = df['high'] - df['close'].shift()
    df['c-l'] = df['close'].shift() - df['low']
    df['tr'] = df[['h-l', 'h-c', 'c-l']].max(axis=1)
    df['atr'] = df['tr'].rolling(length).mean()
    return {'tr': float(df['tr'].values[-1]), 'atr': float(df['atr'].values[-1])}


def ma(records, length):
    if isinstance(records, list):
        df = Tools().records_to_df(records)
    else:
        df = records
    df['ma'] = df['close'].rolling(length).mean()
    return float(df['ma'].values[-1])


def dc(records, length):
    if isinstance(records, list):
        df = Tools().records_to_df(records)
    else:
        df = records
    df = df.iloc[-1-length:-1]
    return {'max': float(df['high'].max()), 'min': float(df['low'].min())}
