import time
import requests
import pandas as pd
import sys 
from thoughts.mytools import *

class SimpleDataFetch:
    def __init__(self, interval=900 ): # in seconds
        self.interval = interval 
        self.df = pd.DataFrame(columns=['time', 'low', 'high', 'open', 'close', 'volume'])
        self.df.set_index('time', inplace=True)
        self.df.index = pd.to_datetime(self.df.index, unit='s')
        self.events = []

        self.get_latest_data_from_coinbase()
        self.keep_alive()

    

    def timeToHumanReadable(self, unreadableTime):
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(unreadableTime))

    def get_latest_data_from_coinbase(self, interval=900, product_id='BTC-USD'):
        # request data from coinbase
        # return data as a list of dicts
        base_url = 'https://api.pro.coinbase.com'
        url = f'{base_url}/products/{product_id}/candles'
        params = {
            'granularity': interval,
        }

        response = requests.get(url, params=params)

        candles = response.json()
        for candle in candles:
            [t, low, high, open, close, volume] = candle
            #check if candle already exists in dataframe
            if t not in self.df.index:
                self.df.loc[t] = [low, high, open, close, volume]
        
        # sort the dataframe by time
        self.df.sort_index(inplace=True, ascending=False)
        
        # update useful variables
        self.oldestCandle = self.df.iloc[-1]
        self.newestCandle = self.df.iloc[0]
        self.totalCandles = len(self.df)
        self.lengthInMinutes = (self.newestCandle.name - self.oldestCandle.name) / 60
        nextCandleTime = self.newestCandle.name + 900

        self.events.append({'title': 'Fetch Latest Data', 'startAt': nextCandleTime, 'action': self.get_latest_data_from_coinbase})

    def keep_alive(self):
        # run a server that listens for new data
        # when new data is received, update the dataframe
        while True:
            for event in self.events:
                if event['startAt'] < time.time():
                    event['action']()
                    self.events.remove(event)
                    print(f'Running => {event["title"]}')
                else:
                    startat = self.timeToHumanReadable(event['startAt'])
                    print(f'Will run => {event["title"]} at {startat}')

            time.sleep(10)

    def panel_data(self, windowsize=10, offsetfrompresent=0):
        panel = self.df.iloc[offsetfrompresent:offsetfrompresent+windowsize]
        return panel
    
    def get_metadata(self):
        return {
            'oldestCandle': self.oldestCandle,
            'newestCandle': self.newestCandle,
            'totalCandles': self.totalCandles,
            'lengthInMinutes': self.lengthInMinutes
        }
    


def main():
    paperTrader = CoinbasePaperTrader()


    
    


if __name__ == '__main__':
    main()
