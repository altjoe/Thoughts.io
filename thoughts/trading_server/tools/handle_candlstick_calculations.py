from datetime import timezone
import datetime
import json
import time
import traceback
import pandas as pd


class CandleStickCalculations:
    def __init__(self, initialinteval=60, data_points_to_store=1000):
        self.data_points_to_store = data_points_to_store
        self.store = pd.DataFrame(
            columns=['time', 'open', 'high', 'low', 'close', 'volume', 'date'])
        self.store.set_index('time', inplace=True)

    def get_next_candle_time(self, interval):
        utctime = datetime.datetime.utcnow()
        utctimeinseconds = utctime.timestamp()

        current_time_in_interval = utctimeinseconds // interval
        next_candle_time = (current_time_in_interval + 1) * (interval)
        return next_candle_time

    def typesafe_trade(self, trade):

        trade['time'] = datetime.datetime.strptime(
            trade['time'], '%Y-%m-%dT%H:%M:%S.%fZ').timestamp()
        trade['size'] = float(trade['size'])
        trade['price'] = float(trade['price'])
        return trade

    def get_interval_index_from_time(self, interval, time):
        return int(time // interval) * interval

    def add_trade(self, interval, trade):
        # params
        # type trade_id, sequence, maker_order_id, taker_order_id, time, product_id, size, price, side
        try:
            trade = self.typesafe_trade(trade)
            index = self.get_interval_index_from_time(interval, trade['time'])
            if index not in self.store.index:
                self.store.loc[index] = {'time': index, 'open': trade['price'], 'high': -
                                         1, 'low': -1, 'close': -1, 'volume': trade['size'], 'date': index}

                self.store.sort_index(inplace=True, ascending=False)
                if len(self.store.index) > self.data_points_to_store:
                    self.store = self.store.iloc[:self.data_points_to_store]

                # self.update_client()
            else:
                record = self.store.loc[index].to_dict()

                if record['high'] == -1 or trade['price'] > record['high']:
                    record['high'] = trade['price']
                if record['low'] == -1 or trade['price'] < record['low']:
                    record['low'] = trade['price']
                record['close'] = trade['price']
                record['volume'] += trade['size']

                # print(record)
                self.store.loc[index] = record
        except Exception as e:
            traceback.print_exc()

        self.previous_trade_price = trade['price']

    # def update_client(self):
    #     self.store.sort_index(inplace=True)
    #     print(self.store.to_dict('records'))

    #     candlestickevent = {'type': 'live-candlestick', 'data': self.store[:-1].to_dict('records')}
        # self.server.send_message(json.dumps(candlestickevent))
# def main():
#     csc = CandleStickCalculations()

# if __name__ == '__main__':
#     main()
