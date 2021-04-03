import os
import glob

import pandas as pd


def find_increased_days(df):
    agg_data = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'symbol': 'first'}

    df_1D = df.resample('1D').agg(agg_data)
    df_1D['pct'] = (df_1D['close'] - df_1D['open']) * 100 / df_1D['open']

    criterion1 = df_1D['pct'] > 20
    criterion2 = df_1D['pct'] < -20
    results = df_1D[criterion1 | criterion2]

    return results


def get_ticker(filename):
    ticker = os.path.basename(filename)
    ticker = ticker.upper().replace(".CSV", "")
    return ticker


def get_df(filename):
    df = pd.read_csv(filename, index_col=0)
    df['symbol'] = [get_ticker(filename) for x in range(len(df))]
    df.index.name = 'date'
    df.index = pd.to_datetime(df.index)

    return df


def main():
    filenames = glob.glob("ohlc1m/*.csv")

    results = []

    for filename in filenames:
        print(filename)
        df = get_df(filename)
        days = find_increased_days(df)
        results.append(days)

    megadf = pd.concat(results)
    megadf.to_csv('megadf.csv')
    print('CSV Successfully Generated.')



main()
