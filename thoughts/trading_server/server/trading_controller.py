# this will be a websocket server that will listen for new data from the trading server
# it will also listen for new data from the sentiment server
# it will handle sending data to the front end


import asyncio
import json
import threading
import time
import websockets
import websocket
from thoughts.mytools.db.db import Database
from thoughts.mytools.db.db_tunnel import tunnel

from thoughts.trading_server.coinbase.coinbase_live_data import CoinbaseLiveData

# only needs to be one of these running
# refactor to delagate data fetch to historical data servers for each coin
# will limit to coinbase only for now with no options to add more


class TradingController:

    def __init__(self):
        self.coinbase_connections = {}
        self.coins_to_track = ['BTC-USD']  # , 'XLM-USD', 'ETH-USD']
        self.streaming_data = {}
        self.db_connection = tunnel(Database())
        

        # this only starts data collection, there is no live data yet
        for coin in self.coins_to_track:
            self.start_data_collection(coin)

        validationthread = threading.Thread(target=self.validate_panel_data)
        validationthread.start()

        asyncio.run(self.serve())

    def start_data_collection(self, product):
        product_data_collection_thread = threading.Thread(
            target=CoinbaseLiveData, args=(product, ))
        product_data_collection_thread.start()
        self.coinbase_connections[product] = product_data_collection_thread

    def handle_data_collection_messages(self, message):
        print(message)

    async def serve(self):
        async with websockets.serve(self.handler, "localhost", 8000):
            await asyncio.Future()

    async def handler(self, websocket, path):
        print("connected")
        async for message in websocket:
            message = json.loads(message)
            print(message['type'])


if __name__ == "__main__":
    # constroller = tunnel(TradingController())
    # print('ran')

    

class ConnectToSocket:
    def __init__(self):
        self.url = 'ws://localhost:8000'
        self.main_loop()

    def main_loop(self):
        self.ws = websocket.WebSocketApp(self.url,
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)
        self.ws.on_open = self.on_open

        thread = threading.Thread(target=self.ws.run_forever, args=())
        thread.start()

    def on_open(self, ws):
        print("opened connection")

    def on_message(self, ws, message):
        print(message)

    def on_error(self, ws, error):
        print(error)

    def on_close(self, ws):
        print("### closed ###")

    # send message to server
    def send_message(self, message):
        self.ws.send(message)
