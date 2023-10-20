#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides acces to all public WS endpoints for one exchange.

Created on Thu Oct 19 13:24:23 2023

@author_ dhaneor
"""
import asyncio

from typing import Coroutine

from .ws_manager import ws_manager


class WsExchange:

    trades: Coroutine
    tickers: Coroutine
    book: Coroutine
    candles: Coroutine

    def __init__(self, exchange: str):
        self.exchange = exchange
        self.ws_manager = ws_manager


async def ws_exchange_factory(exc: str, sub_q: asyncio.Queue, unsub_q: asyncio.Queue):

    ...