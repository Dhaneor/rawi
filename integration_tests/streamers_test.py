#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 16:39:23 2023

@author_ dhaneor
"""
import asyncio
import logging
import sys

from os.path import dirname as dir
from random import choice

sys.path.append(dir(dir(__file__)))

from streamers import streamer, get_stream_manager, VALID_EXCHANGES  # noqa: E402, F401
from util.enums import SubscriptionType, MarketType  # noqa: E402, F401
from util.subscription_request import SubscriptionRequest  # noqa: E402, F401
from zmq_config import Streamer  # noqa: E402

config = Streamer()

logger = logging.getLogger('main')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
logger.addHandler(handler)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s.%(funcName)s.%(lineno)d  - [%(levelname)s]: %(message)s"
)
handler.setFormatter(formatter)


# --------------------------------------------------------------------------------------
async def test_sub_unsub():
    sm = get_stream_manager()
    exchange_name = "binance"

    await sm(
        True,
        SubscriptionRequest(
            exchange_name,
            MarketType.SPOT,
            SubscriptionType.TRADES,
            "BTC/USDT"
        )
    )

    await asyncio.sleep(5)

    await sm(
        False,
        SubscriptionRequest(
            exchange_name,
            MarketType.SPOT,
            SubscriptionType.TRADES,
            "BTC/USDT"
        )
    )


async def test_shutdown():
    sm = get_stream_manager()
    exchange_names = list(VALID_EXCHANGES[MarketType.SPOT].keys())[:2]

    for _ in range(8):
        await sm(
            True,
            SubscriptionRequest(
                choice(exchange_names),
                MarketType.SPOT,
                SubscriptionType.TRADES,
                choice(["BTC/USDT", "ETH/USDT", "XRP/USDT", "ADA/USDT", "ETH/BTC"])
            )
        )

        await asyncio.sleep(1)

    await asyncio.sleep(3)
    await sm(b"", None)

    while tasks := [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]:
        logger.debug(tasks)
        await asyncio.sleep(1)


async def test_stream_manager():
    sm = get_stream_manager()
    exchange_names = list(VALID_EXCHANGES[MarketType.SPOT].keys())

    for _ in range(100):

        action = choice([True, True, True, False])
        symbol = choice(["BTC/USDT", "ETH/USDT", "XRP/USDT", "ADA/USDT", "ETH/BTC"])

        await sm(
            action,
            SubscriptionRequest(
                choice(exchange_names),
                MarketType.SPOT,
                SubscriptionType.TRADES,
                symbol
            )
        )

        await asyncio.sleep(0.5)

    await sm(b"", None)

    while tasks := [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]:
        logger.debug(tasks)

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(1)


async def test_streamer():
    try:
        await streamer(config)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass

    await asyncio.sleep(1)

# --------------------------------------------------------------------------------------
if __name__ == '__main__':
    asyncio.run(test_streamer())
