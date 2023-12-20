#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 24 11:55:23 2023

@author_ dhaneor
"""
import asyncio
import logging
import sys
import zmq

from functools import partial
from os.path import dirname as dir
from pprint import pprint
from random import choice
from time import time

sys.path.append(dir(dir(__file__)))
sys.path.append(dir(dir(dir(__file__))))

from collector import collector  # noqa: E402, F401
from zmqbricks.gond import Gond  # noqa: E402, F401
from zmq_config import Collector, OhlcvRegistry  # noqa: E402, F401
from util.subscription_request import (  # noqa: E402, F401
    SubscriptionRequest, MarketType, SubscriptionType
)

# --------------------------------------------------------------------------------------

logger = logging.getLogger('main')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
logger.addHandler(handler)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s.%(funcName)s.%(lineno)d  - [%(levelname)s]: %(message)s"
)
handler.setFormatter(formatter)

symbols = ["BTC/USDT", "ETH/USDT", "XRP/USDT", "ADA/USDT"]


# --------------------------------------------------------------------------------------
async def get_sub_req():
    return SubscriptionRequest(
        exchange="binance",
        market=MarketType.SPOT,
        sub_type=SubscriptionType.TRADES,
        symbol=choice(symbols)
    )


async def connect_to_producer(producer, socket):
    if producer.service_type == "collector":
        logger.debug("connecting to %s at %s", producer, producer.endpoints.get("publisher"))
        socket.curve_serverkey = producer.public_key.encode("ascii")
        socket.connect(producer.endpoints.get("publisher"))
    else:
        logger.debug("not connecting to %s", producer)


async def test_client():
    ctx = zmq.asyncio.Context()
    poller = zmq.asyncio.Poller()
    config = OhlcvRegistry(exchange="kucoin", markets=["spot"])

    pprint(config.as_dict())

    subscriber = ctx.socket(zmq.SUB)
    subscriber.curve_secretkey = config.private_key.encode("ascii")
    subscriber.curve_publickey = config.public_key.encode("ascii")
    poller.register(subscriber, zmq.POLLIN)

    on_rgstr_success = partial(connect_to_producer, socket=subscriber)
    next_sub_event = 0

    async with Gond(config=config, on_rgstr_success=[on_rgstr_success]):
        while True:
            try:
                events = dict(await poller.poll(1000))

                if subscriber in events:
                    logger.info("something @ subscriber ...")
                    msg = await subscriber.recv_multipart()
                    logger.info(msg)

                if time() > next_sub_event:
                    sr = await get_sub_req()
                    action = choice([zmq.SUBSCRIBE, zmq.UNSUBSCRIBE])
                    logger.debug("%s - %s", action, sr.to_json().encode())
                    subscriber.setsockopt(action, sr.to_json().encode())

                    next_sub_event = time() + 15
            except asyncio.CancelledError:
                await asyncio.sleep(0.5)
                break
            except Exception as e:
                logger.error(e, exc_info=1)
                break

    logger.info("shuting down ...")
    subscriber.close(0)


async def run_collector():
    ctx = zmq.asyncio.Context.instance()
    # config = Collector("kucoin", ["spot"], "collector")

    try:
        await test_client()
    except asyncio.CancelledError:
        logger.info("run_collector cancelled ...")

    ctx.term()


async def main():
    try:
        await test_client()
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("shutdown complete: OK")


if __name__ == "__main__":
    asyncio.run(main())
