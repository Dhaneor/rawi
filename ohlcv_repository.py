#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides a specialized OHLCV repository for the OHLCV containers.

classes
    None

functions
    ohlcv_repository

    This is the only function that needs to be used/called to start the
    repository which can be accessed by clients via its ZeroMQ socket.
    All other functions in this module support this main function.

    Parameters
        ctx Optional[zmq.asyncio.Context]
            a ZeroMQ Context object, only necessary if multiple ZerOMQ
            connections/functions are running in the same thread;
            otherwise THE Context will be initialized and managed here
        addr: Optional[str]
            the 0MQ address to connect to

You cannot set start/end, or the number of returned values. So, this
module is not intended for general purpose use, it will always return
the n most recent candlestick values for the SPOT market on given
exchange (n depends on the exchange but will usually equal 1000).

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

from dataclasses import dataclass
from ccxt.base.errors import BadSymbol
from typing import Optional

logger = logging.getLogger("main.ohlcv_repository")
logger.setLevel(logging.INFO)


DEFAULT_ADDRESS = "inproc://ohlcv_repository"
KLINES_LIMIT = 10

# ====================================================================================


@dataclass
class Response:
    exchange: str = None
    symbol: str = None
    interval: str = None
    socket: object = None
    id: str = None
    success: bool = True
    data: list = None
    _exchange_error: bool = False
    _fetch_ohlcv_not_available: bool = False
    _symbol_error: bool = False
    _interval_error: bool = False
    _network_error: bool = False

    @property
    def exchange_error(self):
        return self._exchange_error

    @exchange_error.setter
    def exchange_error(self, value):
        self._exchange_error = value
        if value:
            self.success = False

    @property
    def fetch_ohlcv_not_available(self):
        return self._fetch_ohlcv_not_available

    @fetch_ohlcv_not_available.setter
    def fetch_ohlcv_not_available(self, value):
        self._fetch_ohlcv_not_available = value
        if value:
            self.success = False

    @property
    def symbol_error(self):
        return self._symbol_error

    @symbol_error.setter
    def symbol_error(self, value):
        self._symbol_error = value
        if value:
            self.success = False

    @property
    def interval_error(self):
        return self._interval_error

    @interval_error.setter
    def interval_error(self, value):
        self._interval_error = value
        if value:
            self.success = False

    @property
    def network_error(self):
        return self._network_error

    @network_error.setter
    def network_error(self, value):
        self._network_error = value
        if value:
            self.success = False

    def __post_init__(self):
        # Basic checks on instantiation
        if not isinstance(self.exchange, str):
            self.exchange_error = True
            self.success = False
        if not isinstance(self.symbol, str):
            self.symbol_error = True
            self.success = False
        if not isinstance(self.interval, str):
            self.interval_error = True
            self.success = False

    async def send(self):
        # Create the response payload based on the current state of the object
        response = {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "interval": self.interval,
            "success": self.success,
            "data": self.data,
            "errors": {
                "exchange_error": self.exchange_error,
                "fetch_ohlcv_not_available": self.fetch_ohlcv_not_available,
                "symbol_error": self.symbol_error,
                "interval_error": self.interval_error,
                "network_error": self.network_error
            }
        }
        # Send the response back through the socket
        logger.debug("sending response: %s", response)
        await self.socket.send_multipart(
            [self.id, b'', json.dumps(response).encode('utf-8')]
        )
        logger.debug("Response sent successfully.")


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


async def get_ohlcv(response: Response) -> None:
    """Get OHLCV data for a given exchange, symbol and interval.

    NOTE: This will always return the data for the SPOT market (not FUTURES)!

    Parameters
    ----------
    response

    Returns
    -------
    None
    """
    res, start = [], time.time()

    # get an exchange instance, return empty result if unsuccessful
    if not (exchange := await exchange_factory(response.exchange)):
        logger.error("[ExchangeNotAvailable] %s", response.exchange)
        response.exchange_error = True
        return response

    # check if the exchange supports fetch_ohlcv method, return empty result if not
    if not hasattr(exchange, "fetch_ohlcv"):
        logger.error("exchange %s does not support fetch_ohlcv", exchange.id)
        response.fetch_ohlcv_not_available = True
        return response

    logger.debug("-------------------------------------------------------------------")
    logger.debug("... fetching OHLCV for %s %s", response.symbol, response.interval)

    # Fetch OHLCV data from the exchange
    try:
        res = await exchange.fetch_ohlcv(
            symbol=response.symbol, timeframe=response.interval, limit=KLINES_LIMIT
        )
    except BadSymbol:
        logger.error("[BadSymbol] %s ", response.symbol)
        response.symbol_error = True
    except asyncio.CancelledError:
        logger.error(
            "[CancelledError] unable to fetch raw data for %s %s",
            response.symbol, response.interval
        )
    except Exception as e:
        logger.error(
            "[Exception] unable to fetch raw data for %s %s -> %s",
            response.symbol, response.interval, e, exc_info=True,
        )
        response.network_error = True
    else:
        logger.info(
            "... fetched %s elements for %s %s in %s ms: OK",
            len(res), response.symbol, response.interval,
            round((time.time() - start) * 1000)
        )
        # convert the result, so that the sublists correspond to OHLCV
        response.data = res
        # response.data = list(zip(*res))
    finally:
        return response


# async def send_ohlcv(data: list, socket: zmq.asyncio.Socket, id_: bytes) -> None:
#     logger.debug("sending data with %s elements", len(data))
#     await socket.send_multipart([id_, b'', (json.dumps(data)).encode()])
#     logger.debug("done")


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
    # Create a Response object with the request details
    response = Response(
        exchange=req.get("exchange"),
        symbol=req.get("symbol"),
        interval=req.get("interval"),
        socket=socket,
        id=id_
    )

    logger.info(response)

    if response.success:
        response = await get_ohlcv(response)

    await response.send()


async def ohlcv_repository(
    ctx: Optional[zmq.asyncio.Context] = None,
    addr: Optional[str] = None
):
    """Start the OHLCV repository.

    Clients can request OHLCV data by sending a (JSON encoded) dictionary.

    example:

    ..code-block:: python
        {"exchange": "binance", "symbol": "BTC/USDT", "interval": "1m"}

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
                logger.debug("received message: %s", msg)
                identity, request = msg[0], msg[2].decode()

                logger.info("received request: %s from %s", request, identity)

                request = json.loads(request)

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

            logger.exception(e, exc_info=False)
            logger.info("task cancelled -> closing exchange ...")

            await requests.send_json([])
            break

    # cleanup
    await exchange_factory(None)
    requests.close(1)


if __name__ == "__main__":
    ctx = zmq.asyncio.Context()

    try:
        asyncio.run(ohlcv_repository(ctx))
    except KeyboardInterrupt:
        ctx.term()
