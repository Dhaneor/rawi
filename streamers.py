#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides webocket streamer components.

Created on Tue Oct 10  09:10:23 2023

@author_ dhaneor
"""
import asyncio
import ccxt.pro as ccxt
import json
import logging
import time
import zmq
import zmq.asyncio

from ccxt.base.errors import BadSymbol, NetworkError, ExchangeNotAvailable
from functools import partial
from typing import Coroutine, TypeVar, Optional

from util.sequence import sequence
from util.enums import SubscriptionType, MarketType
from util.subscription_request import SubscriptionRequest
from zmqbricks.gond import Gond
from zmq_config import Streamer, BaseConfig  # noqa: F401
from zmqbricks.util.sockets import get_random_server_socket  # noqa: F401, E402

logger = logging.getLogger("main.streamers")

# types
ContextT = TypeVar("ContextT", bound=zmq.asyncio.Context)
ConfigT = TypeVar("ConfigT", bound=BaseConfig)
SocketT = TypeVar("SocketT", bound=zmq.Socket)
ExchangeT = TypeVar("ExchangeT", bound=ccxt.Exchange)

SLEEP_ON_ERROR = 10  # seconds
FREQ_UPDATES = 100  # milliseconds
LIMIT_UPDATES = 1000  # max number of updates for one request

VALID_EXCHANGES = {
    MarketType.SPOT: {
        "binance": ccxt.binance,
        "bitfinex": ccxt.bitfinex,
        "bitmex": ccxt.bitmex,
        "bittrex": ccxt.bittrex,
        "bybit": ccxt.bybit,
        "kraken": ccxt.kraken,
        "kucoin": ccxt.kucoin,
    },
    MarketType.FUTURES: {
        "binance": ccxt.binanceusdm,
        "kucoin": ccxt.kucoinfutures,
    },
}

shutdown = False
config = Streamer()


# ======================================================================================
async def get_exchange_instance(exc_name: str, market: MarketType) -> ExchangeT:
    # public API makes no  difference between spot and margin
    market = MarketType.SPOT if market == MarketType.MARGIN else market

    if exc_name in vars(ccxt) and exc_name in VALID_EXCHANGES[market].keys():
        return getattr(ccxt, exc_name)({"newUpdates": True})
    else:
        raise ExchangeNotAvailable(exc_name)


async def close_exchange(name: str, exchanges: dict) -> None:
    try:
        await exchanges[name].close()
    except KeyError as e:
        logger.warning(
            "unable to close exchange instance, not found: %s --> %s", name, e
        )
    except Exception as e:
        logger.error("unable to close exchange instance, unexpected erorr: %s", e)
    else:
        del exchanges[name]
        logger.info("--> EXCHANGE instance CLOSED: %s", name)


async def shut_down(workers: dict, exchanges: dict) -> None:
    # cancel worker tasks
    logger.info("shutdown requested ...")
    logger.info("-" * 120)
    tasks, count = [], 1
    for exc_name, exchange in workers.items():
        for sub_str, sub_type in exchange.items():
            for name, worker_task in sub_type.items():
                logger.info("-" * 120)
                logger.info(
                    "cancelling worker task %s: %s %s -> %s",
                    count, exc_name.upper(), sub_str, name
                )
                worker_task.cancel()
                tasks.append(worker_task)
                count += 1

    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        logger.error("error gathering tasks: %s" % e)

    # close exchange instance(s)
    for exc_name in list(exchanges.keys()):
        await close_exchange(exc_name, exchanges)


# --------------------------------------------------------------------------------------
@sequence(logger=logger, identifier="seq")
async def send(msg: dict, socket: zmq.Socket, topic: str):
    logger.debug(f"sending message: {msg}")
    await socket.send_multipart(
        [
            topic.encode("utf-8"),
            json.dumps(msg).encode("utf-8")
        ]
    )


@sequence(logger=logger, identifier="seq")
async def log_message(msg: dict):
    pass
    # logger.info("---------------------------------------------------------------------")
    # logger.info("[%s] %s", msg.get("sequence", None), msg)
    # logger.info(msg)


async def process_ohlcv(msg: list) -> dict:
    logger.info(f"Processing OHLCV: {msg}")
    return {
        "timestamp": msg[0],
        "open": msg[1],
        "high": msg[2],
        "low": msg[3],
        "close": msg[4],
        "volume": msg[5],
    }


# --------------------------------------------------------------------------------------
async def worker(
    name: str,
    rcv_coro: Coroutine,
    snd_coro: Coroutine,
    use_since: bool,
    add_to_result: Optional[dict] = None,
    process: Optional[Coroutine] = None,
    limit: Optional[int] = LIMIT_UPDATES,
    freq: Optional[int] = FREQ_UPDATES,
) -> None:
    logger.info(add_to_result)

    async def process_update(update: dict) -> dict:
        if "info" in update:
            del update["info"]
        update = await process(update) if process is not None else update
        return add_to_result | update if add_to_result is not None else update

    global shutdown
    counter, since = 0, time.time()

    while True:
        try:
            if use_since:
                data = await rcv_coro(since=since, limit=limit)
            else:
                data = await rcv_coro()
        except asyncio.CancelledError:
            logger.debug("worker cancelled ... exiting")
            shutdown = True
            break
        except ExchangeNotAvailable as e:
            logger.error("[%s] %s", counter, e)
            await asyncio.sleep(SLEEP_ON_ERROR)
        except BadSymbol as e:
            logger.error("[%s] %s", counter, e)
            break
        except NetworkError as e:
            if not shutdown:
                logger.error("[%s] CCXT network error: %s", counter, e, exc_info=0)
                await asyncio.sleep(SLEEP_ON_ERROR)
        except Exception as e:
            logger.error(e)
            break
        else:
            data = [data] if not isinstance(data, list) else data

            if data:
                if use_since is not None:
                    since = data[-1].get("timestamp") + 1
                    if freq:
                        await asyncio.sleep(freq / 1000)

                for update in data:
                    await snd_coro(await process_update(update))

                counter += 1

    logger.info("worker %s shutdown complete: OK" % name)


async def create_worker(
    req: SubscriptionRequest,
    workers: dict,
    exchanges: dict,
    snd_coro: Coroutine,
) -> None:
    # abort if the topic is already registered
    if workers.get(req.exchange, {}).get(req.market, {}).get(req.topic, None):
        logger.warning(
            "duplicate subscription requested: %s %s %s",
            req.exchange, req.market, req.topic,
        )
        return

    # create exchange instance, if it doesn't exist yet
    if not exchanges.get(req.exchange, None):
        exchanges[req.exchange] = exchange = await get_exchange_instance(
            req.exchange, req.market
        )
    else:
        exchange = exchanges[req.exchange]

    # create necessary keys in workers registry
    if req.exchange not in workers:
        workers[req.exchange] = {}

    # create sub-dictionary for the market, if necessary
    if not workers.get(req.exchange).get(req.market):
        workers[req.exchange][req.market] = {}

    # prepare parameters for worker coroutine
    match req.sub_type:

        case SubscriptionType.OHLCV:
            rcv_coro = partial(
                exchange.watch_ohlcv,
                symbol=req.symbol,
                timeframe=req.interval,
            )
            use_since = True

        case SubscriptionType.BOOK:
            rcv_coro = partial(exchange.watch_order_book, symbol=req.symbol)
            use_since = False

        case SubscriptionType.TRADES:
            rcv_coro = partial(exchange.watch_trades, symbol=req.symbol)
            use_since = True

        case SubscriptionType.TICKER:
            rcv_coro = partial(exchange.watch_ticker, symbol=req.symbol)
            use_since = False

        case _:
            raise ValueError(f"invalid subscription type: {req.sub_type}")

    process = process_ohlcv if req.sub_type == SubscriptionType.OHLCV else None
    add_to_result = {"exchange": req.exchange, "market": req.market.value}
    snd_coro = partial(snd_coro, topic=req.to_json())

    # create a worker task for the topic
    workers[req.exchange][req.market][req.topic] = asyncio.create_task(
        worker(req.topic, rcv_coro, snd_coro, use_since, add_to_result, process),
        name=req.topic,
    )


async def remove_worker(
    req: SubscriptionRequest,
    workers: dict,
    exchanges: dict
) -> None:
    try:
        workers[req.exchange][req.market][req.topic].cancel()
    except KeyError as e:
        logger.warning("unable to remove worker, not found for: %s --> %s", req, e)
        return
    else:
        await asyncio.sleep(1)
        del workers[req.exchange][req.market][req.topic]

    if not workers[req.exchange][req.market]:
        del workers[req.exchange][req.market]

    if not workers[req.exchange]:
        logger.debug("no topics left -> removing exchange instance: %s", req.exchange)
        await close_exchange(req.exchange, exchanges)
        await asyncio.sleep(2)
        del workers[req.exchange]


def get_stream_manager(snd_coro: Coroutine):
    snd_coro = snd_coro
    exchanges: dict = {}
    workers: dict = {}
    topics: dict[str, int] = {}

    async def stream_manager(action: bytes, req: SubscriptionRequest | None) -> None:
        """Manages creation/removal of stream workers.

        Parameters
        ----------
        action : bytes
            subscribe (1) or unsubscribe (0)
        req : SubscriptionRequest
            An (un)subscribe request. For clean shutdown: If this is None,
            then we will cancel all tasks/subscriptions, and close the
            exchange instances.
        """
        # log request
        action_str = "subscribe" if action else "unsubscribe"
        logger.debug("=" * 80)
        logger.info("received %s request --> %s", action_str.upper(), req)

        # shutdown if we got None as subscription request
        if req is None:
            await shut_down(workers, exchanges)
            return

        # ..............................................................................
        sub_req_json = req.to_json()

        # subscribe to topic, create worker & exchange if needed
        if action_str == "subscribe":
            try:
                await create_worker(req, workers, exchanges, snd_coro)
            except Exception as e:
                logger.error("unable to create worker: %s", e)
            else:
                topics[sub_req_json] = topics[sub_req_json] + 1 \
                    if sub_req_json in topics else 1

        # unsubscribe from topic, remove worker & exchange if needed
        elif action_str == "unsubscribe":

            if sub_req_json not in topics:
                logger.warning(
                    "got unsubscribe for non-existent topic: %s", sub_req_json
                )
                return

            topics[sub_req_json] -= 1

            if topics[sub_req_json] == 0:
                logger.info("removing topic: %s", sub_req_json)
                del topics[sub_req_json]

                try:
                    await remove_worker(req, workers, exchanges)
                except Exception as e:
                    logger.error("unable to remove worker: %s", e)

        # this should never happen, but just in case ...
        else:
            raise ValueError(f"invalid action: {action}")

    return stream_manager


# --------------------------------------------------------------------------------------
async def streamer(config: ConfigT):
    publisher = await get_random_server_socket("publisher", zmq.XPUB, config)
    manager = get_stream_manager(snd_coro=partial(send, socket=publisher))

    async with Gond(config):
        while True:
            try:
                msg = await publisher.recv()

                logger.info("received message: %s" % msg)

                await manager(
                    action=msg[0], req=SubscriptionRequest.from_json(msg[1:].decode())
                )

            except asyncio.CancelledError:
                logger.info("Cancelled...")
                break

            except Exception as e:
                logger.exception(e)

        # tell the manager to pack it up ...
        await manager(b"", None)

    # if counter > 0:
    #     duration = perf_counter() - start
    #     msg_per_sec = counter / duration
    #     sec_per_msg = duration / counter

    #     logger.info(
    #         "processed %s messages in %s seconds (%s msgs/s)",
    #         counter, duration, msg_per_sec
    #     )
    #     logger.info("one message every %s milliseconds", sec_per_msg / 1000)


# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    logger = logging.getLogger("main")
    logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s.%(funcName)s.%(lineno)d  - [%(levelname)s]: %(message)s"
    )
    ch.setFormatter(formatter)

    logger.addHandler(ch)

    try:
        asyncio.run(streamer(config=config))
    except KeyboardInterrupt:
        print("Interrupted...")
