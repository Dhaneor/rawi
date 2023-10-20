#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import json
import logging
import sys
import time
import zmq
import zmq.asyncio

from typing import Optional, Callable, Sequence, NamedTuple

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


# ======================================================================================
class TickerEvent(NamedTuple):
    """Named tuple for ticker events."""

    exchange: str
    symbol: str
    sequence: int
    timestamp: int
    timestamp_recv: int
    price: float
    size: float
    best_bid: float
    best_ask: float
    best_bid_size: float
    best_ask_size: float


class SnapshotEvent(NamedTuple):
    """Named tuple for snapshot events."""

    symbol: str
    sequence: int
    timestamp: int
    timestamp_recv: int
    average_price: float
    base_currency: str
    buy: float
    change_price: float
    change_rate: float
    close: float
    high: float
    last_traded_price: float
    low: float
    margin_trade: bool
    market: str
    markets: Sequence[str]
    open: float
    quote_currency: str
    sell: float
    taker_fee_rate: float
    trading: bool
    volume: float
    quote_volume: float


class CandleEvent(NamedTuple):
    """Named tuple for candles events."""

    exchange: str
    symbol: str
    timestamp: int
    timestamp_recv: int
    latency: float
    type: str
    interval: str
    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float


def make_ticker_event(msg: dict) -> TickerEvent:
    """_summary_

    Parameters
    ----------
    msg
        ticker message
    Returns
    -------
    TickerEvent
    """
    return TickerEvent(
        exchange=msg['exchange'],
        symbol=msg["topic"],
        sequence=msg["data"]["sequence"],
        timestamp=msg["data"]["time"],
        timestamp_recv=msg["received_at"],
        price=msg["data"]["price"],
        size=msg["data"]["size"],
        best_bid=msg["data"]["bestBid"],
        best_ask=msg["data"]["bestAsk"],
        best_bid_size=msg["data"]["bestBidSize"],
        best_ask_size=msg["data"]["bestAskSize"]
    )


def make_snapshot_event(msg: dict) -> SnapshotEvent:
    return SnapshotEvent(
        symbol=msg["topic"],
        sequence=msg["data"]["sequence"],
        timestamp=msg["data"]["data"]["datetime"],
        timestamp_recv=msg["received_at"],
        average_price=msg["data"]["data"]["averagePrice"],
        base_currency=msg["data"]["data"]["baseCurrency"],
        buy=msg["data"]["data"]["buy"],
        change_price=msg["data"]["data"]["changePrice"],
        change_rate=msg["data"]["data"]["changeRate"],
        close=msg["data"]["data"]["close"],
        high=msg["data"]["data"]["high"],
        last_traded_price=msg["data"]["data"]["lastTradedPrice"],
        low=msg["data"]["data"]["low"],
        margin_trade=msg["data"]["data"]["marginTrade"],
        market=msg["data"]["data"]["market"],
        markets=msg["data"]["data"]["markets"],
        open=msg["data"]["data"]["open"],
        quote_currency=msg["data"]["data"]["quoteCurrency"],
        sell=msg["data"]["data"]["sell"],
        taker_fee_rate=msg["data"]["data"]["takerFeeRate"],
        trading=msg["data"]["data"]["trading"],
        volume=msg["data"]["data"]["vol"],
        quote_volume=msg["data"]["data"]["volValue"]
    )


def make_candle_event(msg: dict):
    """Convert a message to a namedtuple.

    Parameters
    ----------
    msg : dict
        a message to convert

    Returns
    -------
    namedtuple
        a namedtuple representation of the message
    """
    now = time.time()

    return CandleEvent(
        exchange=msg['exchange'],
        symbol=msg["symbol"],
        timestamp=msg["time"],
        timestamp_recv=now,
        latency=round((now - msg["time"]) * 1000, 2),
        type=msg["type"],
        interval=msg["interval"],
        open_time=msg["data"]["open time"],
        open=msg["data"]["open"],
        high=msg["data"]["high"],
        low=msg["data"]["low"],
        close=msg["data"]["close"],
        volume=msg["data"]["volume"],
        quote_volume=msg["data"]["quote volume"]
    )


async def sub_listener(
    ctx: zmq.asyncio.Context,
    address: str,
    topic: str,
    callback: Optional[Callable[[dict], None]] = None,
):
    """A simple subscriber that listens to a given topic.

    The messages that we receive are passed to the callback function.
    Their inner structure depends on the publisher, but they are always
    dictionaries.

    Parameters
    ----------
    ctx : zmq.asyncio.Context
        a working zmq context
    address : str
        the IP address:port of the publisher
    topic : str
        a valid topic to subscribe to
    callback : Optional[Callable[[dict], None]], optional
        a callback function/method that can handle the messages that
        we receive here, by default None
    """

    subscriber = ctx.socket(zmq.SUB)
    subscriber.connect(address)
    subscriber.setsockopt(zmq.SUBSCRIBE, topic.encode())

    logger.info(f'subscriber started for topic {topic}')

    count = 0
    start = time.time()
    try:
        while True:
            msg = None

            try:
                msg = await subscriber.recv_multipart()
            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM:  # Interrupted
                    break
                else:
                    raise

            msg = json.loads(msg[1].decode())

            if msg["subject"] == "candles":
                msg = make_candle_event(msg)

                if msg.type == 'add':
                    logger.info('------------------------------------------')

            elif msg["subject"] == "ticker":
                msg = make_ticker_event(msg)

            elif msg["subject"] == "snapshot":
                msg = make_snapshot_event(msg)

            else:
                logger.error(f"Unknown message subject: {msg['subject']}")

            logger.info(msg)
            count += 1

    finally:
        end = time.time()
        duration = round(end - start)
        avg_time_between_messages = round(duration / count * 1000)
        logger.info(
            f"Subscriber received {count} messages in {duration} seconds "
            f"(={round(count / duration, 3)} msg/s)"
            f" -> one message every {avg_time_between_messages} ms"
        )


async def main(address, topic):
    ctx = zmq.asyncio.Context()
    logger.debug("starting zmq subscriber %s %s", address, topic)

    try:
        await sub_listener(ctx=ctx, address=address, topic=topic)
    except KeyboardInterrupt:
        ctx.term()


if __name__ == '__main__':
    try:
        address = sys.argv[1]
        if 'tcp://' not in address or len(address.split(':')) != 3:
            raise ValueError(f"Invalid address format: {address}")

        topic = sys.argv[2] if len(sys.argv) == 3 else ''
        if not isinstance(topic, str):
            raise ValueError(f"Invalid topic type: {type(topic)}")

        logger.info(f'connecting to {address} and subscribing for topic: {topic}')
        asyncio.run(main(address, topic))

    except IndexError:
        logger.error('usage: python zmq_subscriber.py "tcp://<host ip>:<port>" <topic>')
        logger.error('\\nWhen no topic is given, we subscribe to all topics.')
    except ValueError as ve:
        logger.error(ve)
    except KeyboardInterrupt:
        pass
