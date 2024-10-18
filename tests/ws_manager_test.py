#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 18 00:10:23 2023

@author_ dhaneor
"""
import asyncio
import logging
import sys

from os.path import dirname as dir
from random import choice

sys.path.insert(0, dir(dir(dir(__file__))))

from rawi.websocket.ws_manager import ws_manager  # noqa: E402
from rawi.util.subscription_request import SubscriptionRequest  # noqa: E402
from rawi.util.enums import SubscriptionType, MarketType  # noqa: E402

logger = logging.getLogger('main')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
logger.addHandler(handler)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s.%(funcName)s.%(lineno)d  - [%(levelname)s]: %(message)s"  # noqa: E501
)
handler.setFormatter(formatter)


sub_q, unsub_q = asyncio.Queue(), asyncio.Queue()
symbols = [
    "BTC-USDT", "ETH-USDT", "LTC-USDT", "XRP-USDT", "BCH-USDT", "EOS-USDT"
]


async def mock_client():
    subbed = []

    while True:
        try:
            q = choice([sub_q, unsub_q]) if subbed else sub_q
            symbol = (
                choice(symbols) if q == sub_q else choice(subbed) if subbed else ""  # noqa: E501
            )

            req = SubscriptionRequest(
                exchange='kucoin',
                market=MarketType.SPOT,
                sub_type=SubscriptionType.TRADES,
                symbol=symbol
            )

            await q.put(req)

            if q == sub_q:
                subbed.append(symbol)
            else:
                if symbol and symbol in subbed:
                    subbed.remove(symbol)

            await asyncio.sleep(5)

        except Exception as e:
            logger.error(e)
        except asyncio.CancelledError:
            break


async def test_ws_manager():
    try:
        tasks = [
            asyncio.create_task(ws_manager(sub_q=sub_q, unsub_q=unsub_q)),
            asyncio.create_task(mock_client()),
        ]

        await asyncio.gather(*tasks, return_exceptions=True)
    except (KeyboardInterrupt, asyncio.CancelledError):
        [task.cancel() for task in tasks]
        res, exc = await asyncio.gather(*tasks, return_exceptions=True)

        if res:
            logger.info(res)
        if exc:
            logger.error(exc)


async def main():
    await test_ws_manager()


if __name__ == "__main__":
    asyncio.run(main())
