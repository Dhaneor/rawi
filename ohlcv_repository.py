#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides a specialized OHLCV repository for the OHLCV containers.

classes
    Response

    This class standardizes the response, does type checks for the
    values from the request, and includes a method for sending the
    response after fetching the OHLCV data (including some error
    flags for the client).

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
import functools
import json
import logging
import numpy as np
import time
import zmq
import zmq.asyncio

from dataclasses import dataclass
from ccxt.base.errors import (
    BadSymbol, BadRequest, AuthenticationError,
    InsufficientFunds, NetworkError,
    ExchangeNotAvailable, RequestTimeout,
    ExchangeError
    )
from typing import Optional, Dict, Tuple

from .util.binance_async import Binance

logger = logging.getLogger("main.ohlcv_repository")

DEFAULT_ADDRESS = "inproc://ohlcv_repository"
KLINES_LIMIT = 1000
RATE_LIMIT = True
CACHE_TTL_SECONDS = 30
MAX_RETRIES = 3
RETRY_DELAY = 5.0
RUNNING_CLEANUP = False


# ====================================================================================
@dataclass
class Response:
    """Response class.

    This class standardizes the response, does type checks for the values from
    the request, and includes a method for sending the response after fetching
    the OHLCV data (including some error flags for the client).
    """
    exchange: str = None
    symbol: str = None
    interval: str = None
    socket: object = None
    id: str = None
    data: list | None = None
    _execution_time: float | None = None
    _bad_request_error: bool = False
    _authentication_error: bool = False
    _exchange_error: bool = False
    _fetch_ohlcv_not_available: bool = False
    _symbol_error: bool = False
    _interval_error: bool = False
    _network_error: bool = False

    def __repr__(self):
        if self.data is not None and len(self.data) > 0:
            data_str = f", data[{len(self.data)}]: {self.data[1]}...{self.data[-1]}"
        else:
            data_str = f", errors: {self.errors}"

        return f"Response(exchange={self.exchange}, symbol={self.symbol}, " \
               f"interval={self.interval}, success={self.success}, "\
               f"bad_request={self.bad_request}{data_str})"

    @property
    def execution_time(self):
        return self._execution_time

    @execution_time.setter
    def execution_time(self, value: float):
        self._execution_time = value

        logger.info(
            "Fetched %s elements for %s %s in %s ms: %s",
            len(self.data) if self.data else 0,
            self.symbol,
            self.interval,
            int(value * 1000),
            "OK" if self.data else "FAIL"
        )

    @property
    def errors(self):
        return {
            attr: getattr(self, attr) for attr in dir(self)
            if attr.startswith("_") and "error" in attr and getattr(self, attr)
        }

    @property
    def authentication_error(self):
        return self._authentication_error

    @authentication_error.setter
    def authentication_error(self, value: bool):
        self._authentication_error = value

    @property
    def exchange_error(self):
        return self._exchange_error

    @exchange_error.setter
    def exchange_error(self, value: bool):
        self._exchange_error = value

    @property
    def fetch_ohlcv_not_available(self):
        return self._fetch_ohlcv_not_available

    @fetch_ohlcv_not_available.setter
    def fetch_ohlcv_not_available(self, value: bool):
        self._fetch_ohlcv_not_available = value

    @property
    def symbol_error(self):
        return self._symbol_error

    @symbol_error.setter
    def symbol_error(self, value: bool):
        self._symbol_error = value

    @property
    def interval_error(self):
        return self._interval_error

    @interval_error.setter
    def interval_error(self, value: bool):
        self._interval_error = value

    @property
    def network_error(self):
        return self._network_error

    @network_error.setter
    def network_error(self, value: bool) -> None:
        self._network_error = True

    @property
    def bad_request(self) -> bool:
        return True if any(
            (
                self._bad_request_error,
                self._authentication_error,
                self._exchange_error,
                self._symbol_error,
                self._interval_error,
            )
        ) else False

    @bad_request.setter
    def bad_request(self, value: bool) -> None:
        self._bad_request_error = value

    @property
    def success(self) -> bool:
        return False if self.bad_request else True

    def __post_init__(self):
        # Basic checks on instantiation
        self.exchange_error = True if not isinstance(self.exchange, str) else False
        self.symbol_error = True if not isinstance(self.symbol, str) else False
        self.interval_error = True if not isinstance(self.interval, str) else False

    # ------ Functions for sending the response and reconstructing it from JSON ------
    @classmethod
    def from_json(cls, json_string: str) -> 'Response':
        """
        Reconstruct a Response object from a JSON string.

        Parameters:
        -----------
        json_string : str
            A JSON string containing the serialized Response data.

        Returns:
        --------
        Response
            A new Response object reconstructed from the JSON data.
        """
        json_data = json.loads(json_string)

        response = cls(
            exchange=json_data.get('exchange'),
            symbol=json_data.get('symbol'),
            interval=json_data.get('interval'),
            socket=None,  # Socket can't be serialized, so we set it to None
            id=None
        )

        # Restore other attributes
        response.data = json_data.get('data', [])

        # Reconstruct errors
        errors = json_data.get('errors', {})
        errors = errors if isinstance(errors, dict) else {}
        for error_type, error_message in errors.items():
            if error_message:  # Only set non-False error messages
                setattr(response, error_type, error_message)

        return response

    def to_json(self):
        response = {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "interval": self.interval,
            "success": self.success,
            "data": self.data,
            "bad_request": self.bad_request,
            "errors": self.errors or None,
        }

        # prevent crashes due to unserializable values
        for k, v in response.items():
            try:
                json.dumps(v)
            except Exception as e:
                logger.warning(f"Could not serialize {k} value: {v} ({e})")
                response[k] = None
                response["errors"][k] = str(e)
                response["success"] = False
        return response

    async def send(self):
        if not self.socket:
            logger.error("Cannot send response: socket is not set")
            return

        # Send the response back through the socket
        await self.socket.send_multipart(
            [self.id, b'', json.dumps(self.to_json()).encode('utf-8')]
        )

    # -------- Functions for converting the OHLCV data to the desired format ---------
    def to_dict(self) -> Optional[Dict[str, np.ndarray]]:
        """Convert OHLCV data to a dictionary.

        Returns
        -------
        Optional[Dict[str, Any]]
            A dictionary containing the OHLCV data, or None if we have no data
        """
        if not self.data:
            return None

        data_array = np.array(self.data).T

        return {
            "open time": data_array[0],
            "open": data_array[1],
            "high": data_array[2],
            "low": data_array[3],
            "close": data_array[4],
            "volume": data_array[5],
        }


# ====================================================================================
def exchange_factory_fn():
    exchange_instances = {}

    async def get_exchange(exchange_name: Optional[str] = None) -> object | None:
        """Get a working exchange instance

        Call it with None (or without a parameter) to close
        all current exchange instances, which is required by CCXT
        before quitting.

        Parameters
        ----------
        exchange_name : str, Optional
            Name of the exchange to get an instance of, by default None

        Returns
        -------
        object | None
            An exchange instance, or None if the exchange does not exist
        """
        nonlocal exchange_instances
        global RUNNING_CLEANUP

        if exchange_name:
            exchange_name = exchange_name.lower()

        # Close all exchanges request
        if exchange_name is None:
            for name, instance in exchange_instances.items():
                await instance.close()
                logger.info(f"Exchange closed: {name}")
            exchange_instances.clear()
            return None

        # Return cached exchange if it exists
        if exchange_name in exchange_instances:
            logger.debug(f"Returning cached exchange for: {exchange_name}")
            return exchange_instances[exchange_name]

        # Create a new exchange instance
        logger.info(f"Instantiating exchange: {exchange_name}")
        if exchange_name.lower() == "binance":
            exchange = Binance()
            await exchange.initialize()
            exchange_instances[exchange_name] = exchange
            return exchange

        try:
            exchange = getattr(ccxt, exchange_name)({"enableRateLimit": RATE_LIMIT})
            await exchange.load_markets()
            exchange_instances[exchange_name] = exchange
            return exchange
        except AttributeError as e:
            logger.error(f"Exchange {exchange_name.upper()} does not exist ({e})")
            return None

    return get_exchange


# instantiate the exchange factory function
exchange_factory = exchange_factory_fn()


# ====================================================================================
def cache_ohlcv(ttl_seconds: int = CACHE_TTL_SECONDS):
    """
    Decorator function to cache OHLCV (Open, High, Low, Close, Volume) data.

    This decorator implements a caching mechanism for OHLCV data. It stores the
    results of the decorated function in a dictionary, using a tuple of
    (exchange, symbol, interval) as the key. If the same request is made again,
    it returns the cached data instead of calling the original function.

    Parameters:
    func (callable): The function to be decorated. It should be an asynchronous
                     function that takes a Response object as an argument and
                     returns a Response object.

    Returns:
    callable: A wrapper function that implements the caching logic.

    The wrapper function:
    - Takes a Response object as an argument.
    - Returns a Response object, either from the cache or by calling the
      original function.
    """
    def decorator(func):
        ohlcv_cache: Dict[Tuple[str, str, str], Tuple[Dict, float]] = {}

        @functools.wraps(func)
        async def wrapper(response, exchange) -> Response:
            start_time = time.time()
            current_time = start_time

            # Clean expired cache entries
            expired_keys = [
                key for key, (_, timestamp) in ohlcv_cache.items()
                if current_time - timestamp > ttl_seconds
            ]
            for key in expired_keys:
                del ohlcv_cache[key]

            cache_key = (response.exchange, response.symbol, response.interval)

            if cache_key in ohlcv_cache:
                cached_data, timestamp = ohlcv_cache[cache_key]
                if current_time - timestamp <= ttl_seconds:
                    logger.debug(f"Returning cached OHLCV data for {cache_key}")
                    response.data = cached_data
                    response.cached = True
            else:
                for attempt in range(MAX_RETRIES):
                    try:
                        response = await func(response, exchange)
                        if response.data:
                            ohlcv_cache[cache_key] = (response.data, current_time)
                            logger.debug(f"Cached OHLCV data for {cache_key}")
                        break
                    except (NetworkError, ExchangeNotAvailable, RequestTimeout) as e:
                        if attempt == MAX_RETRIES - 1:
                            logger.error(
                                "Max retries reached for %s: %s",
                                cache_key,
                                str(e)
                                )
                            response.network_error = str(e)
                        else:
                            logger.warning(
                                f"Retry {attempt + 1} for {cache_key} due to: {str(e)}"
                                )
                            await asyncio.sleep(RETRY_DELAY * (attempt + 1))

            response.execution_time = (time.time() - start_time)
            return response

        return wrapper
    return decorator


async def get_ohlcv_for_no_of_days(
    response: Response,
    exchange: ccxt.Exchange,
    n_days: int = 1296
) -> None:
    # Calculate the starting timestamp
    end_time = exchange.parse8601(
        f'{time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}'
        )
    start_time = end_time - n_days * 24 * 60 * 60 * 1000  # Convert days to milliseconds

    # Store all data
    ohlcv_data = []
    current_time = start_time

    while current_time < end_time:
        # Fetch data with limit (usually 1000)
        batch = await exchange.fetch_ohlcv(
            symbol=response.symbol,
            timeframe=response.interval,
            since=current_time,
            limit=None
        )
        if not batch:
            break  # Exit if no more data is returned

        # Append batch data
        ohlcv_data.extend(batch)

        # Move to the next time interval
        current_time = batch[-1][0] + 1  # +1 to avoid overlap

    response.data = ohlcv_data
    return response


@cache_ohlcv()
async def get_ohlcv(response: Response, exchange: ccxt.Exchange) -> Response:
    """Get OHLCV data for a given exchange, symbol and interval.

    Parameters
    ----------
    response

    Returns
    -------
    Response
    """
    interval_errors = (
        "period", "interval", "timeframe", "binSize", "candlestick", "step",
        )

    if not hasattr(exchange, "fetch_ohlcv"):
        response.fetch_ohlcv_not_available = True
        return response

    if response.interval not in exchange.timeframes:
        response.interval_error = True
        return response

    try:
        if response.symbol not in exchange.symbols:
            response.symbol_error = True
            return response
    except:  # noqa: E722
        pass

    # ................................................................................
    # with Binance we can use parallel calls to make this step faster
    if exchange.name == "binance":
        result = await exchange.fetch_ohlcv(
            symbol="".join(response.symbol.split("/")),
            interval=response.interval,
            limit=1296
        )

        response.data = [
            [float(row[i]) if i != 0 else int(row[i]) for i in range(len(row))]
            for row in result
        ]  # Convert to ccxt format (Binance returns strings)

        return response

    # serial calls to fetch OHLCV data for all other exchanges
    try:
        response = await get_ohlcv_for_no_of_days(response, exchange)
        logger.debug(response.data)
    except AuthenticationError as e:
        logger.error(f"[AuthenticationError] {str(e)}")
        response.authentication_error = str(e)
    except BadSymbol as e:
        logger.error(f"[BadSymbol] {str(e)}")
        response.symbol_error = True

    # ccxt is inconsistent when encountering an error
    # that is caused by an invalid interval. some special
    # handling is required here
    except InsufficientFunds as e:
        logger.error(f"[InsufficientFunds] {str(e)}")
        response.interval_error = True
    except BadRequest as e:
        logger.error(f"[BadRequest] {str(e)}")
        if any(s in str(e) for s in interval_errors):
            response.interval_error = True
        else:
            response._bad_request_error = str(e)
    except ExchangeError as e:
        logger.error(f"[ExchangeError] {str(e)}")
        if "poloniex" in str(e):
            response.interval_error = True
        elif any(s in str(e) for s in interval_errors):
            response.interval_error = True
        else:
            response.exchange_error = str(e)
    except ExchangeNotAvailable as e:
        logger.error(f"[ExchangeNotAvailable] {str(e)}")
        if any(s in str(e) for s in interval_errors):
            response.interval_error = True
        else:
            response.exchange_error = str(e)
    except Exception as e:
        logger.error(f"[Unexpected Error] {str(e)}", exc_info=True)
        response.unexpected_error = str(e)

    return response


async def process_request(
    req: dict,
    socket: zmq.asyncio.Socket | None = None,
    id_: bytes | None = None
) -> None:
    """Process a client request for OHLCV data.

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

    logger.debug(response)

    # proceed only if a valid Response object has been created
    if response.success:
        # try to get a working exchange instance
        if not (exchange := await exchange_factory(response.exchange)):
            response.exchange_error = f"Exchange {response.exchange} not available"
            return response

        # # log server time in human-readable format
        # logger.info(
        #     "Server time: %s",
        #     exchange.iso8601(await exchange.fetch_time()).split('T')[1][:-5]
        #     )

        # log server status
        status = await exchange.fetch_status()
        status = 'OK' if status['status'] == 'ok' else status
        logger.info("Server status: %s" % status)

        # download OHLCV data
        response = await get_ohlcv(response=response, exchange=exchange)

    if response.socket:
        await response.send()

    return response


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
            events = dict(await poller.poll())

            if requests in events:
                msg = await requests.recv_multipart()
                logger.debug("received message: %s", msg)
                identity, request = msg[0], msg[2].decode()

                logger.debug("received request: %s from %s", request, identity)

                request = json.loads(request)

                if request.get("action") == "close":
                    logger.info("shutdown request received ...")
                    await exchange_factory(None)
                    requests.send_multipart([identity, b"", b"OK"])
                    break

                # process request in the background
                asyncio.create_task(process_request(request, requests, identity))

        except asyncio.CancelledError:
            logger.info("task cancelled -> closing exchange ...")
            break
        except Exception as e:
            logger.exception(e, exc_info=True)
            logger.info("task cancelled -> closing exchange ...")

            await requests.send_json([])
            asyncio.sleep(0.001)
            break

    # cleanup
    await exchange_factory(None)
    await asyncio.sleep(3)
    requests.close(1)

    logger.info("ohlcv repository shutdown complete: OK")


if __name__ == "__main__":
    ctx = zmq.asyncio.Context()

    try:
        asyncio.run(ohlcv_repository(ctx))
    except KeyboardInterrupt:
        ctx.term()
