from datetime import datetime
import pandas as pd
import requests


def get_latest_candlestick_coinbase(interval=60, product='BTC-USD'):
    url = f'https://api.pro.coinbase.com/products/{product}/candles?granularity={interval}'
    response = requests.get(url)
    if response.status_code == 200:
        df = pd.DataFrame(response.json(), columns=[
                          'time', 'low', 'high', 'open', 'close', 'volume'])
        df.set_index('time', inplace=True)
        df.sort_values('time', inplace=True, ascending=False)

        print(df.head())
        return df
    else:
        return []


def get_latest_trades_coinbase(product='BTC-USD', startAt=0):
    url = f'https://api.pro.coinbase.com/products/{product}/trades'
    if startAt != 0:
        url = f'https://api.pro.coinbase.com/products/{product}/trades?after={startAt}&limit=1000'

    
    response = requests.get(url)
    if response.status_code == 200:
        # df = pd.DataFrame(response.json(), columns=['time', 'trade_id', 'price', 'size', 'side'])
        # df.set_index('time', inplace=True)
        # df.sort_values('time', inplace=True, ascending=False)
        object = response.json()
        # print(object)
        df = pd.DataFrame(
            object, columns=['time', 'trade_id', 'price', 'size', 'side'])
        df.sort_values('time', inplace=True, ascending=False)

        return df

    else:
        return []
