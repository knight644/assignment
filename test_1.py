import time
import requests
import json
import threading
import hmac
import hashlib
from urllib.parse import urlencode
import websocket
from influxdb import InfluxDBClient


bitmex_test_api_key = 'Bv3dVJg3W3D5Budhp_YtWmfV'  # API-key
bitmex_test_api_secret = 'It4gDls24Mv3T9ain3b_sz7W--275sSXn3_a2AYn32yiiwAK'  # API-secret

# Global constants for creating the request path
SYMBOL_STRING = 'symbol'
QTY_STRING = 'orderQty'
ORDTYPE_STRING = 'ordType'
REQUEST_BASE_PATH = 'https://testnet.bitmex.com'
ORDER_API_PATH = '/api/v1/order'

# Timer for collecting and storing 1 hour data
TIMER = time.time()

# Flag to check for arrival of 'partial' from bitmex
PARTIAL_RECEIVED_FLAG = 0

# Global DB constants
DB_CLIENT = InfluxDBClient(host='localhost', port=8086)  # Establish connection to influxDb
DB_CLIENT.create_database('OrderBook')  # Create a database
DB_CLIENT.switch_database('OrderBook')  # Switch to the said database


# Function to calculate signature required for authorization
def calculateSignature(secret_key, verb, path, expire):
    req_string = verb + path + expire
    req_string = bytes(req_string, 'utf-8')
    signature = hmac.new(bytes(secret_key, 'utf-8'), req_string, hashlib.sha256).hexdigest()
    return signature


# Function to place a single order
def placeSingleOrder(symbol, qty, ord_type):
    params = {SYMBOL_STRING: symbol, QTY_STRING: qty, ORDTYPE_STRING: ord_type}
    expire = str(int(time.time() + 10))

    encoded_params = urlencode(params)  # parameters url-encoded
    request_string = ORDER_API_PATH + '?' + encoded_params
    signature = calculateSignature(bitmex_test_api_secret, 'POST', request_string, expire)
    request_string = REQUEST_BASE_PATH + request_string

    # headers required for authorization
    headers = {'api-expires': expire, 'api-key': bitmex_test_api_key, 'api-signature': signature}

    # sending request
    response = requests.post(request_string, headers=headers)
    data = response.json()
    print(data)


# Function to place orders over multiple assets every 15 minutes
def placeMultipleOrders():
    symbol_list = ['XBTUSD', 'ETHUSD', 'XBTZ20', 'XBTH21', 'ADAZ20']  # Assets on which orders will be placed
    qty_list = [10, 20, 30, -20, -10]  # quantity of assets
    ord_type = 'Market'

    for count in range(5):
        placeSingleOrder(symbol_list[count], qty_list[count], ord_type)  # placing orders individually

    threading.Timer(900, placeMultipleOrders).start()  # start a new thread doing the same thing after 15 minutes


# Function to process message received from websocket
def on_message(ws, message):
    global PARTIAL_RECEIVED_FLAG, DB_CLIENT
    message = json.loads(message)  # convert string to json object

    # check if partial has been received
    if PARTIAL_RECEIVED_FLAG == 0:
        if message['action'] == 'partial':
            PARTIAL_RECEIVED_FLAG = 1
    else:
        success = DB_CLIENT.write_points([message], database='OrderBook')  # write point to database
        if success:
            print('Datapoint added to db')

    # check if it has been 1 hour
    if time.time() > (TIMER+3600):
        on_close(ws)


# function to process error received from websocket
def on_error(ws, error):
    print(error)


# Function to elegantly close the sockets and db connections while shutting down
def on_close(ws):
    ws.close()
    DB_CLIENT.close()
    print("### closed ###")


# Function to subscribe to bitmex orderbook for continuous update
def getOrderBook():
    global TIMER
    TIMER = time.time()  # set time

    # connect to bitmex and subscribe to orderbook
    ws = websocket.WebSocketApp('wss://testnet.bitmex.com/realtime?subscribe=orderBookL2_25:XBTUSD',
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    ws.run_forever()  # run socket till on_close() is called


if __name__ == '__main__':
    '''
    Un-Comment the following lines to place single orders
    '''
    # placeSingleOrder('XBTUSD', -10, 'Market')
    # placeSingleOrder('ETHUSD', -10, 'Market')
    # placeSingleOrder('XBTZ20', -10, 'Market')
    # placeSingleOrder('XBTH21', -10, 'Market')
    # placeSingleOrder('ADAZ20', -10, 'Market')


    '''
    Un-Comment the following lines to place order every 15 minutes
    '''
    # placeMultipleOrders()


    '''
    Un-Comment the following lines to get orderBook updates and save them to db for 1 hour
    '''
    getOrderBook()
