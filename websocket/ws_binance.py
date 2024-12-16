#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 08 23:48:20 2021

@author_ dhaneor
"""

from pyclbr import Function
import time
import sys
import configparser
import threading
import asyncio

from binance.client import Client
from binance import ThreadedWebsocketManager
# from binance.depthcache import DepthCacheManager
from binance.enums import *

from threading import Thread, Lock
from multiprocessing import Queue
from pprint import pprint

from models.orderbook import Orderbook
from models.symbol import Symbol

from util.timeops import unix_to_utc, utc_timestamp


# TODO error checks?
# TODO check for websocket alive / restart if necessary
# rebuild the whole thing in a way that is more flexible and allows for more
# three different symbols (multiplex stream?)

# =============================================================================
class Timer(Thread):
    def __init__(self, callback:Function, threshhold:float):
        Thread.__init__(self, daemon=True)
        self.callback = callback
        self.threshhold = threshhold
        self.last_updated = time.time()
        self.running = True

    def run(self):
        while self.running:
            if time.time() - self.last_updated > self.threshhold:
                print('timer triggered')
                self.callback()
            time.sleep(1)

    def reset(self):
        print('timer reset')
        self.last_updated = time.time()

    def stop(self):
        self.running = False


# ==============================================================================
class Stream(Thread):

    def __init__(self):
        Thread.__init__(self, daemon=False)
        self.name = 'VIGILANTE'
        self._connected = False

        self.stream = None
        self.twm = None # for klines
        self.dcm = None # for orderbook updates

        self.api_key = None
        self.api_secret = None

        self.running = True
        self.reconnect_counter = 0

    @property
    def connected(self):
        counter = 0
        while not self._connected:
            self._connected = self._check_for_connection()

            if counter == 5:
                self.send_queue.put(f'[{self.name}] no connection to host')
                break

            counter += 1
            self.reconnect_counter += 1
            time.sleep(1)

        return self._connected

    def start_websocket_manager(self, private:bool=False):
        self.twm = self._get_websocket_manager(private=private)
        if self.twm is not None:
            self.twm.start()
            print('websocket manager started')

    def stop_websocket_manager(self):
        try:
            self.twm.stop()
        except Exception as e:
            print(e)
        finally:
            self.twm = None
            print('websocket manager stopped')

    # -------------------------------------------------------------------------
    def _check_for_connection(self):
        try:
            client = Client()
        except:
            print('failed to establish connection!')
            return False

        del client
        return True

    def _get_websocket_manager(self, private:bool):
        if not self.connected:
            return None

        if private:
            self._import_api_key()
            return ThreadedWebsocketManager(api_key=self.api_key,
                                            api_secret=self.api_secret)
        else:
            return ThreadedWebsocketManager()

    def _import_api_key(self):

        c = configparser.ConfigParser()
        c.read("config.ini")

        self.api_key = c['API KEY']['api_key']
        self.api_secret = c['API KEY']['api_secret']

# =============================================================================
class KlineStreamBinance(Stream):
    '''This class is for running a klines and orderbook stream from
    Binance for a single symbol.

    It is running in its own thread and requires a queue to put the results in.
    Most of the times you will want to also start a user stream to be informed
    about order updates, executed trades and general account updates. This is
    implemented in a separate class
    '''
    def __init__(self, symbol:str, interval:str, send_queue:Queue):
        Stream.__init__(self)
        self.symbol = symbol
        self.interval = interval
        self.send_queue = send_queue

        self.price_counter = 0
        self.timer = Timer(callback=self.restart, threshhold=10)

        self.run()

    # -------------------------------------------------------------------------
    # the main function that starts automatically when the thread is started
    # it instantiates the BinanceSocketManager and creates the streams for
    # the prices and the orderbook
    def run(self):
        if self.running and self.twm is None:
            self.start_websocket_manager()

        if self.running and self.twm is None:
            self.stop()
            return

        if self.twm is not None and self.stream is None:
            self.start_socket_for_one_symbol(self.symbol, self.interval)

        self.timer.start()

    def stop(self):

        print(f"""Closing stream for {self.symbol} ({self.reconnect_counter} \
        reconnects)""")

        self.timer.running = False
        self.running = False
        self.stop_socket()
        self.stop_websocket_manager()

        if self.dcm is not None:
            try:
                self.dcm.join()
            except:
                pass

        return self

    # -------------------------------------------------------------------------
    def start_socket_for_one_symbol(self, symbol:str, interval:str):
        if self.twm is not None:
            self.stream = self.twm.start_kline_socket(
                callback=self.process_klines, symbol=symbol, interval=interval)
            print(f'socket for {symbol} ({interval} started)')
        else:
            print(f'sovket for {self.symbol} could not be started')

    def stop_socket(self):
        try:
            self.twm.stop_socket(self.stream)
            self.stream = None
            # time.sleep(3)
            print(f'socket for {self.symbol} stopped ...')
        except:
            pass

    def restart(self):
        self.stop()
        time.sleep(5)
        # self.send_queue.put('no connection')
        # sys.exit()
        self._connected = False
        self.start_websocket_manager()
        self.start_socket_for_one_symbol(self.symbol, self.interval)
        self.timer.reset()

    # -------------------------------------------------------------------------
    # process the orderbook coming from the stream and create an instance of
    # the Orderbook class for further processing
    def process_orderbook(self, depth_cache):

        orderbook = Orderbook(symbol = depth_cache.symbol,
                                    bids = depth_cache.get_bids(),
                                    asks = depth_cache.get_asks())

        # print(orderbook)
        return

    # this is the callback function that processes the messages from the price
    # (klines) stream and sends them to a queue for further processing
    def process_klines(self, msg):
        self.timer.reset()
        msg['at'] = time.time()

        # close the socket manager if we got an error as result (possibly after
        # being disconnected) and call the run( method which (hopefully)
        # restarts the socket manager
        if msg['e'] == 'error':
            print(f'ERROR in stream (klines) for {self.symbol} ... trying to restart!')
            self.reconnect_counter += 1
            self.twm.close()
            self.twm = None
            self.run()
            return

        result = {}
        dict_ = msg.get('k')

        result['Open Time'] = int(dict_.get('t'))
        result['Human Open Time'] = unix_to_utc(dict_.get('t'))
        result['Open'] = float(dict_.get('o'))
        result['High'] =  float(dict_.get('h'))
        result['Low'] =  float(dict_.get('l'))
        result['Close'] =  float(dict_.get('c'))
        result['Volume'] =  float(dict_.get('v'))
        result['Close Time'] =  dict_.get('T')
        result['Quote Asset Volume'] =  float(dict_.get('q'))
        result['Number of Trades'] =  int(dict_.get('n'))

        # calculate how much time lies between the event and the time it
        # arrived here
        result['Delivery Time'] = round(msg.get('at') * 1000 - msg.get('E'), 2)

        if dict_.get('x'): result['Closing Candle'] = True
        else: result['Closing Candle'] = False

        with Lock():
            self.send_queue.put(result)

# =============================================================================
class UserStreamBinance(Stream):
    ''' This class is for running a user event stream.

    User events are events that direct the specific user account, like
    order updates (created, filled ...) and balance updates (deposits,
    withdrawals, changes in balance during trading).
    '''

    def __init__(self, send_queue:Queue, symbol:str=None, market:str='SPOT'):
        """Initilization for user stream class.

        :param send_queue: queue for sending the messages/results
        :type send_queue: Queue
        :param symbol: Filter for symbol, use <None> for all symbols
        :type symbol: str, optional
        :param market: 'SPOT' or 'CROSS MARGIN', defaults to 'SPOT'
        :type market: str, optional
        """
        Stream.__init__(self)

        self.symbol = symbol if isinstance(symbol, Symbol) else None
        self.symbol_name = symbol.name if self.symbol is not None else 'all'
        self.market = market
        self.send_queue = send_queue

        self.filter_messages = True if symbol is not None else False

        self.twm = None
        self.stream = None

        self._import_api_key()

        self.run()

    # -------------------------------------------------------------------------
    # the main function that starts automatically when the thread is started
    # it instantiates the 'Threaded Websocket Manager' and creates the user stream
    def run(self):
        if self.twm is None:
            self.start_websocket_manager(private=True)

        if self.twm is None:
            self.stop()
            return

        if self.twm is not None and self.stream is None:
            self._start_socket()

    def stop(self):

        print(f"""Closing user stream for {self.symbol_name} \
        ({self.reconnect_counter} reconnects)""")

        # self._stop_socket()
        self.stop_websocket_manager()


    # --------------------------------------------------------------------------
    def on_message(self, message):
        if message['e'] == 'error':
            self.restart_socket()
        else:
            if self.filter_messages:
                if self._do_we_even_care_about(message):
                    self.send_queue.put(message)
            else:
                self.send_queue.put(message)

    def restart_socket(self):
        self._stop_socket()
        self.start_websocket_manager(private=True)
        self._start_socket()

    # --------------------------------------------------------------------------
    # helper functions
    def _start_socket(self):
        try:
            self.stream = self.twm.start_user_socket(callback=self.on_message)
            print(f'user socket for {self.symbol_name} started ...')
        except Exception as e:
            print(e)

    def _stop_socket(self):
        try:
            self.twm.stop_socket(self.stream)
        except Exception as e:
            print(e)
        finally:
            self.stream = None
            print(f'user socket for {self.symbol_name} stopped ...')

    def _do_we_even_care_about(self, event):

        if event.get('e') == 'outboundAccountPosition':

            balance = event.get('B')
            base_asset, quote_asset = self.symbol.baseAsset, self.symbol.quoteAsset
            base_in_balance, quote_in_balance = False, False

            for item in balance:
                if item.get('a') == base_asset: base_in_balance = True
                if item.get('a') == quote_asset: quote_in_balance = True

            if base_in_balance and quote_in_balance:
                return True

        elif event.get('e') == 'executionReport':

            if event.get('s') == self.symbol_name: return True

        return False


# ============================================================================ #
#                               test functions                                 #
# ============================================================================ #
def test_kline_stream(symbol, interval):
    q = Queue()
    kst = KlineStreamBinance(symbol, interval, q)
    counter, running = 0, True

    while running:
        try:
            if not q.empty():
                counter += 1
                res = q.get()
                pprint(res)
                if 'no connection' in res:
                    break
                print('----------------------------------------------------\n')
            else:
                print('queue is empty')
            time.sleep(1.5)

        except KeyboardInterrupt:
            kst.stop()
            running = False
            print('Shutting down ...')

def test_user_stream(symbol:Symbol):
    _st = time.time()
    q = Queue()
    ust = UserStreamBinance(symbol=symbol, market='SPOT', send_queue=q)

    running = True
    while running:
        try:
            if not q.empty():
                res = q.get()
                if 'no connection' in res:
                    break
                pprint(res)
                print('\n')

            time.sleep(0.1)

            if (time.time() - _st) > 15:
                ust.restart_socket()
                _st = time.time()
                print('-'*80)
                print('\n\n')


        except KeyboardInterrupt:
            try:
                ust.stop()
            except:
                pass
            running = False
            print('Shutting down ...')


# ============================================================================ #
#                                   MAIN                                       #
# ============================================================================ #
if __name__ == "__main__":

    symbol_name = 'ADAUSDT'
    interval = '1m'
    test = 'kline'

    if test == 'kline':
        test_kline_stream(symbol_name, interval)
    else:
        symbol = Symbol(symbol_name)
        test_user_stream(symbol=symbol)






