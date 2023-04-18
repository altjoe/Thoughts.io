import json
import websocket
from thoughts.trading_server.tools.handle_candlstick_calculations import CandleStickCalculations
from thoughts.trading_server.coinbase.get_latest_data import get_latest_trades_coinbase
import time
from thoughts.mytools.db.db import Database
# this is a client that will establish a websocket connection to a single coinbase product,
# and will handle the data that comes in from the websocket and store that information
# it will also provide a method to get the latest data


class CoinbaseLiveData:
    def __init__(self, product_id='BTC-USD', interval=60, data_points_to_store=1000):
        print('Starting Coinbase Client: ', product_id, interval)
        self.data_points_to_store = data_points_to_store
        self.product_id = product_id
        self.interval = interval
        self.candlesticks = CandleStickCalculations(interval)
        # self.fetch_all_data_points()
        self.fetch_existing_data()
        self.main_loop()

    # function to pull already existing data from database
    def fetch_existing_data(self):
        df = self.db_connection.select_df(
            f'SELECT * FROM all_pairs')
        print('allpairs', df)
        pass

    # function to insert new trade data into database
    def insert_new_data(self):
        pass

    def fetch_all_data_points(self):
        nextstartAt = 0
        while len(self.candlesticks.store) < self.data_points_to_store:
            trades = get_latest_trades_coinbase(self.product_id, nextstartAt)
            for index, trade in trades.iterrows():
                trade_dict = trade.to_dict()
                self.candlesticks.add_trade(self.interval, trade_dict)

            print(f'Fetched {self.product_id}: ', len(self.candlesticks.store),
                  ' data points out of ', self.data_points_to_store)

            nextstartAt = trades.iloc[-1]['trade_id']
            time.sleep(2)

    def main_loop(self):
        self.ws = websocket.WebSocketApp("wss://ws-feed.pro.coinbase.com",
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)
        self.ws.on_open = self.on_open
        self.ws.run_forever()

    def on_open(self, ws):
        print("opened connection")
        subscribe_message = {
            "type": "subscribe",
            "product_ids": [
                self.product_id
            ],
            "channels": [
                "full"
            ]
        }
        ws.send(json.dumps(subscribe_message))

    def on_message(self, ws, message):
        self.delegate_message_type(message)

    def string_to_dict(self, message):
        object_with_types = json.loads(
            message, parse_float=float, parse_int=int)
        return object_with_types

    def delegate_message_type(self, message):
        message = self.string_to_dict(message)

        message_type = message['type']
        if message_type == 'match':  # this is for every trade
            self.candlesticks.add_trade(self.interval, message)

        # for live data we will have a type here that will trigger a message to be sent to a client

        # elif message_type == 'snapshot':
        #     self.handle_snapshot(message)
        # elif message_type == 'l2update':
        #     self.handle_l2update(message)
        # elif message_type == 'received':
        #     self.handle_received(message)
        # elif message_type == 'open':
        #     self.handle_open(message)
        # elif message_type == 'done':
        #     self.handle_done(message)
        # elif message_type == 'match':
        #     self.handle_match(message)
        # elif message_type == 'change':
        #     self.handle_change(message)
        # elif message_type == 'activate':
        #     self.handle_activate(message)
        # elif message_type == 'subscriptions':
        #     self.handle_subscriptions(message)
        else:
            # print('Unknown message type: {}'.format(message_type))
            pass

    def on_error(self, ws, error):
        print(f'Coinbase Websocket Client Error: {error}')

    def on_close(self, ws):
        print("closed connection")


# if __name__ == "__main__":
#     client = CoinbaseClient()
