#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 16 13:58:23 2023

@author_ dhaneor
"""
import asyncio
# import ccxt.async_support as ccxt
import ccxt.pro as ccxt
from ccxt.base.errors import BadSymbol
import logging
from pprint import pprint
from time import perf_counter

logger = logging.getLogger("main")
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()

formatter = logging.Formatter(
    "%(asctime)s - %(name)s.%(funcName)s.%(lineno)d  - [%(levelname)s]: %(message)s"
)
ch.setFormatter(formatter)

logger.addHandler(ch)

exchange = ccxt.kucoin()


async def main(exchange):

    symbols = ["BTC/USDT", "ETH/USDT"]
    start = perf_counter()
    counter = 0

    if exchange.has['watchTicker']:

        while True:

            try:

                # await asyncio.sleep(0.01)
                # if counter == 3:
                #     new_symbol = "ETHA/USDT"
                #     symbols.append(new_symbol)

                # if counter == 5:
                #     symbols.append("ETH/USDT")

                tickers = await exchange.watch_ohlcv(symbols[1])

                # if tickers:
                #     for ticker in tickers.values():
                #         if isinstance(ticker, dict) and "info" in ticker.keys():
                #             del ticker["info"]

                logger.info(
                    "---------------------------------------------------"
                )
                logger.info("[%s] %s", counter, tickers)
                logger.info("[%s] no of tickers: %s", counter, len(tickers))

            except BadSymbol as e:
                logger.error("[%s] %s", counter, e)
                # symbols.remove(new_symbol)

            # except ccxt.NetworkError as e:
            #     logger.error("[%s] CCXT network error: %s", counter, e)

            except asyncio.CancelledError:
                logger.info('Cancelled...')
                break

            except Exception as e:
                logger.exception(e)
                break

            finally:
                counter += 1

    else:
        logger.info("watchTickers not supported")
        supported = {k: v for k, v in exchange.has.items() if v}
        pprint(supported)

    await exchange.close()
    logger.info("exchange closed: OK")

    if counter > 0:
        duration = perf_counter() - start
        msg_per_sec = counter / duration
        sec_per_msg = duration / counter

        logger.info(
            "processed %s messages in %s seconds (%s msgs/s)",
            counter, duration, msg_per_sec
        )
        logger.info("one message every %s milliseconds", sec_per_msg / 1000)

if __name__ == '__main__':
    try:
        asyncio.run(main(exchange))
    except KeyboardInterrupt:
        print('Interrupted...')
