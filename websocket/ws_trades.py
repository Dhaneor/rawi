#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides a webocket streamer components.

Created on Tue Oct 10  09:10:23 2023

@author_ dhaneor
"""
import asyncio
import ccxt.pro as ccxt
import time

from ccxt.base.errors import (
    BadSymbol, NetworkError, ExchangeNotAvailable, AuthenticationError,
    ExchangeError
)
from typing import TypeVar

ExchangeT = TypeVar("ExchangeT", bound=ccxt.Exchange)


async def trades(exchange: ExchangeT, symbol: str, limit: int = 1000):
    since = None

    while True:
        try:
            data = await exchange.watch_trades(symbol=symbol, since=since, limit=limit)
        except NetworkError as e:
            print(e)
        except ExchangeNotAvailable as e:
            print(e)
        except BadSymbol as e:
            print(e)
        except AuthenticationError as e:
            print(e)
        except ExchangeError as e:
            print(e)
        except asyncio.CancelledError:
            print('Cancelled...')
            break
        except Exception as e:
            print(e)
        else:
            if data:
                since = data[-1]["timestamp"] + 1
                for trade in data:
                    print(trade)


if __name__ == '__main__':
    exchange = ccxt.binance()

    try:
        asyncio.run(trades(exchange, "BTC/USDT"))
    except KeyboardInterrupt:
        print('Interrupted...')
        exchange.close()
        time.sleep(1)
        print("shutdown completed: OK")
