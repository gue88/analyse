import glob
import os

import pandas as pd
import numpy as np


def create_pivot_table(df, ticker):
    agg_data = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'pct_type': 'first',
    }

    df_30T = df.resample('30T').agg(agg_data)
    df_30T['timeonly'] = df_30T.index.time
    df_30T['symbol'] = ticker
    table = pd.pivot_table(df_30T, values='volume', index=['pct_type', 'symbol'], columns='timeonly',
                           aggfunc=np.sum).dropna()
    table = table.div(table.iloc[:, 0], axis=0)

    # table_sum = table.sum(level='pct_type', axis=0, numeric_only=True)

    return table
    # table.to_csv('table.csv')
    # print('CSV successfully created.')


def get_ticker(filename):
    ticker = os.path.basename(filename)
    ticker = ticker.replace(".csv", "")
    return ticker


def process_file(filename):
    df = pd.read_csv(filename, index_col=0)
    df.index.name = 'date'
    df.index = pd.to_datetime(df.index)

    agg_data = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'}

    df_1D = df.resample('1D').agg(agg_data)
    df_1D['pct'] = (df_1D['close'] - df_1D['open']) * 100 / df_1D['open']

    criterion1 = df_1D['pct'] > 0
    criterion2 = df_1D['pct'] < 5
    criterion3 = df_1D['pct'] > 5
    criterion4 = df_1D['pct'] < 20
    criterion5 = df_1D['pct'] > 20
    criterion6 = df_1D['pct'] < 100
    criterion7 = df_1D['pct'] > 100
    criterion8 = df_1D['pct'] < 0
    criterion9 = df_1D['pct'] > -5
    criterion10 = df_1D['pct'] < -5
    criterion11 = df_1D['pct'] > -20
    criterion12 = df_1D['pct'] < -20
    criterion13 = df_1D['pct'] > -100
    criterion14 = df_1D['pct'] < -100

    large_green = df_1D[criterion7]
    moderate_green = df_1D[criterion5 & criterion6]
    green = df_1D[criterion3 & criterion4]
    boring_green = df_1D[criterion1 & criterion2]
    boring_red = df_1D[criterion8 & criterion9]
    red = df_1D[criterion10 & criterion11]
    moderate_red = df_1D[criterion12 & criterion13]
    large_red = df_1D[criterion14]

    for i in boring_green.index:
        # df.loc[df['b'] == 4, 'a'] = 99
        date_filter = (df.index.date == i)
        df.loc[date_filter, 'pct_type'] = ">0 _ <5"

    for i in boring_red.index:
        date_filter = (df.index.date == i)
        df.loc[date_filter, 'pct_type'] = ">-5 _ <0"

    for i in green.index:
        date_filter = (df.index.date == i)
        df.loc[date_filter, 'pct_type'] = ">5 _ <20"

    for i in red.index:
        date_filter = (df.index.date == i)
        df.loc[date_filter, 'pct_type'] = ">-20 _ <-5"

    for i in moderate_green.index:
        date_filter = (df.index.date == i)
        df.loc[date_filter, 'pct_type'] = ">20 _ <100"

    for i in moderate_red.index:
        date_filter = (df.index.date == i)
        df.loc[date_filter, 'pct_type'] = ">-100 _ <-20"

    for i in large_green.index:
        date_filter = (df.index.date == i)
        df.loc[date_filter, 'pct_type'] = ">100"

    for i in large_red.index:
        date_filter = (df.index.date == i)
        df.loc[date_filter, 'pct_type'] = "'<-100'"

    ticker = get_ticker(filename)
    pivot_table = create_pivot_table(df, ticker)
    return df, pivot_table


if __name__ == '__main__':
    filenames = glob.glob("ohlc1m/*.csv")
    pivot_tables = []
    blacklist = ["CLIS", "CYIO"]
    for filename in filenames:
        ticker = get_ticker(filename)
        if ticker in blacklist:
            # we want to ignore this ticker
            pass
        else:
            df, pivot_table = process_file(filename)
            print(filename)
            pivot_tables.append(pivot_table)
    table = pd.concat(pivot_tables).groupby('pct_type').sum()
    table_n = pd.concat(pivot_tables)

    with pd.ExcelWriter('tables.xlsx') as writer:
        table.to_excel(writer, sheet_name='sum')
        table_n.to_excel(writer, sheet_name='table')

