#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides a specialized OHLCV repository for the OHLCV containers.

You cannot set start/end, or the nu,mber of returned values. So, this
module is not intended for general purpose use, but only to provide
the initial data for OHLCV containers

The configuration can be changed in the zmq_config.py file which must
be in the same directory as this file.

Created on Sat Sep 16 13:58:23 2023

@author_ dhaneor
"""
import asyncio

# import ccxt.async_support as ccxt
import ccxt.pro as ccxt
import json
import logging
import time
import zmq
import zmq.asyncio

from ccxt.base.errors import BadSymbol, ExchangeNotAvailable
from typing import Optional

logger = logging.getLogger("main.ohlcv_repository")
logger.setLevel(logging.DEBUG)

DEFAULT_ADDRESS = "inproc://ohlcv_repository"
KLINES_LIMIT = 1000


def exchange_factory_fn():
    exchange_instance = None

    async def get_exchange(exchange_name: Optional[str] = None) -> object | None:
        """Get a working exchange instance

        Call it with None (or without a parameter) to close the
        current exchange instance, which is required by CCXT
        before quitting.

        Parameters
        ----------
        exchange_name : str, optional
            Name of the exchange to get an instance of, by default None

        Returns
        -------
        object | None
            An exchange instance, or None if the exchange does not exist
        """
        nonlocal exchange_instance

        exchange_name = exchange_name.lower() if exchange_name else None

        # close exchange if it exists
        if not exchange_name and exchange_instance:
            logger.info("closing exchange: %s", exchange_instance.id)
            await exchange_instance.close()
            exchange_instance = None
            return None

        # exchange close request, but no active exchange instance
        if not exchange_name and not exchange_instance:
            logger.error("no exchange requested")
            return None

        # return cached exchange if the same exchange is requested again
        if exchange_instance is not None and exchange_name == exchange_instance.id:
            logger.debug("returning cached exchange for: %s", exchange_instance.id)
            return exchange_instance

        # close existing exchange instance if a different exchange is requested
        if exchange_instance is not None and exchange_name != exchange_instance.id:
            logger.debug("closing exchange: %s", exchange_instance.id)
            await exchange_instance.close()
            exchange_instance = None

        # create a new exchange instance
        logger.info("instantiating  exchange: %s", exchange_name)
        try:
            exchange = getattr(ccxt, exchange_name)({"enableRateLimit": False})
        except AttributeError as e:
            logger.error("exchange %s does not exist (%s)", exchange_name.upper(), e)
            exchange = None

        finally:
            exchange_instance = exchange
            return exchange

    return get_exchange


exchange_factory = exchange_factory_fn()


async def get_ohlcv(exchange_name: str, symbol: str, interval: str) -> list:
    """Get OHLCV data for a given exchange, symbol and interval.

    Parameters
    ----------
    exchange_name : str
        Name of the exchange

    symbol : str
        Symbol to fetch OHLCV data for

    interval : str
        Interval to fetch OHLCV data for

    Returns
    -------
    list
        List of OHLCV data | empty list if unsuccessful
    """
    res = []
    start = time.time()

    if not (exchange := await exchange_factory(exchange_name)):
        logger.error("unable to get exchange for: %s", exchange_name)
        return res

    if not hasattr(exchange, "fetch_ohlcv"):
        logger.error("exchange %s does not support fetch_ohlcv", exchange.id)
        return res

    logger.debug("-------------------------------------------------------------------")
    logger.debug("... fetching OHLCV data for %s %s", symbol, interval)

    try:
        res = await exchange.fetch_ohlcv(
            symbol=symbol, timeframe=interval, limit=KLINES_LIMIT
        )
    except ExchangeNotAvailable as e:
        logger.error(
            "[ExchangeNotAvailable] unable to fetch raw datafor %s %s -> %s",
            symbol,
            interval,
            e,
        )
    except BadSymbol as e:
        logger.error(
            "[BadSymbol] unable to fetch raw data for %s %s -> %s",
            symbol,
            interval,
            e,
        )
    except asyncio.CancelledError:
        logger.error(
            "[CancelledError] unable to fetch raw data for %s %s", symbol, interval
        )
    except Exception as e:
        logger.error(
            "[Exception] unable to fetch raw data for %s %s -> %s",
            symbol,
            interval,
            e,
            exc_info=True,
        )
    finally:
        if res:

            exc_time = round((time.time() - start) * 1000)

            logger.debug(
                "... fetched %s elements for %s %s in %s ms: OK",
                len(res), symbol, interval, exc_time
            )
        return res


async def send_ohlcv(data: list, socket: zmq.asyncio.Socket, id_: bytes) -> None:
    await socket.send_multipart([id_, (json.dumps(data)).encode()])


async def process_request(req: dict, socket: zmq.asyncio.Socket, id_: bytes) -> None:
    """Process a client request for OHLCV data

    Parameters
    ----------
    req: dict
        dictionary with these keys: "exchange", "symbol", "interval"

    socket: zmq.asyncio.Socket
        a working/initialized ZeroMQ socket

    id_: bytes
        caller identity of the request
    """
    # fetch the raw data
    raw = await get_ohlcv(req.get("exchange"), req.get("symbol"), req.get("interval"))

    # transpose the raw data, so the sublists represent 'open' ...'volume'
    data = list(zip(*raw)) if raw else []

    # send it back to the client
    await send_ohlcv(data, socket, id_)


async def ohlcv_repository(
    ctx: Optional[zmq.asyncio.Context] = None,
    addr: Optional[str] = None
):
    """Start the OHLCV repository.

    Clients can request OHLCV data by sending a multipart message.

    example:

    ..code-block:: python
        [
            b'client identity',
            {"exchange": "binance", "symbol": "BTC/USDT", "interval": "1m"}
        ]

    Parameters
    ----------
    ctx : zmq.asyncio.Context, optional
        ZMQ context to use. If None, a new context is created. This
        allows for flexibility wrt how this is used.
        You can run this function in a separate process, or in a
        separate thread and provide no context, or you can combine the
        repository with other components and use the provided/shared
        context, by default None

    addr : str, optional
        Address to bind to, only necessary if you want to overwrite
        the DEFAULT_ADDRESS (defined in the 'zmq_config' file in
        this directory), by default None
    """
    context = ctx or zmq.asyncio.Context()
    addr = addr or DEFAULT_ADDRESS

    requests = context.socket(zmq.ROUTER)
    requests.bind(addr)

    poller = zmq.asyncio.Poller()
    poller.register(requests, zmq.POLLIN)

    while True:
        try:
            events = dict(await poller.poll(100))

            if requests in events:
                msg = await requests.recv_multipart()
                identity, request = msg[0], msg[1]

                logger.info("received request: %s from %s", request, identity)

                request = json.loads(request.decode())

                # stop operation if we got a 'close' command
                if request.get("action") == "close":
                    logger.info("received close request -> exiting ...")
                    await requests.send_json([])
                    raise asyncio.CancelledError()

                # process request in the background
                asyncio.create_task(process_request(request, requests, identity))

        except asyncio.CancelledError:
            logger.info("task cancelled -> closing exchange ...")
            break
        except Exception as e:
            try:
                e = e.split("\n")[0]
            except Exception:
                pass

            logger.exception(e)
            await requests.send_json([])

    # cleanup
    await exchange_factory(None)
    requests.close(1)


if __name__ == "__main__":
    ctx = zmq.asyncio.Context()

    try:
        asyncio.run(ohlcv_repository(ctx))
    except KeyboardInterrupt:
        ctx.term()
