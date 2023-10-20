#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides a formalized stream events for candles, tickers, snapshots.

Created on Tue Sep 12 19:41:23 2023

@author_ dhaneor
"""
import time
from typing import NamedTuple, Sequence


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
