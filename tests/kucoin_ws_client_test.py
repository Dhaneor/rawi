#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 16 17:26:23 2023

@author_ dhaneor
"""
import asyncio
import ccxt.pro as ccxt
import logging
import sys
import time

from kucoin.client import WsToken
from os.path import dirname as dir
from random import random, choice  # noqa: E402

sys.path.append(dir(dir(dir(__file__))))

from data_sources.kucoin.kucoin import ws_client as wsc  # noqa: E402

logger = logging.getLogger('main')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
logger.addHandler(handler)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s.%(funcName)s.%(lineno)d  - [%(levelname)s]: %(message)s"
)
handler.setFormatter(formatter)


# symbols downloader
async def get_symbols(count):
    #  create a CCXT instance for symbol names download
    exchange = ccxt.kucoin({
        "enableRateLimit": True,
        "options": {
            "defaultType": "future",
            "fetchMarkets": True,
            "fetchOHLCV": True,
            "fetchOrderBook": True,
            "fetchTicker": True,
            "fetchTickers": True,
            "fetchTrades": True,
        },
        "verbose": False,
    })

    symbols = [elem["symbol"] for elem in (await exchange.fetch_markets())]
    symbols = [s.replace("/", "-") for s in symbols]

    await exchange.close()

    return [choice(symbols) for _ in range(count)]


# random topic generator
def random_topic():
    return f"topic_{int(time.time() / 1000 * random() )}"


# mock publish coroutine
async def callback(msg):
    logger.info(f"received message: {msg}")
    return

# --------------------------------------------------------------------------------------
#                     test methods of TOPICS class
async def test_add_remove_topics(runs=20):
    # create a Topics object
    t = wsc.Topics()
    topics = ["topic1", "topic2", "topic3", "topic4", "topic5", "topic6", "topic7"]
    subjects = ["ticker", "depth", "trades", "klines"]

    for _ in range(runs):
        topic = f"{choice(subjects)}:{choice(topics)}"
        subs_for_topic = t._topics.count(topic)

        if random() < 0.5:
            await t.add_subscriber(topic)
            assert subs_for_topic + 1 == t._topics.count(topic)

        else:
            if subs_for_topic:
                await t.remove_subscriber(topic)
                if subs_for_topic >= 1:
                    assert subs_for_topic - 1 == t._topics.count(topic)
                else:
                    assert t._topics.count(topic) == 0, \
                        f"{t._topics.count(topic)} != 0"

    logger.info("test passed: OK")
    logger.info("~-*-~" * 30)


async def test_batch_topics():
    wsc.MAX_BATCH_SUBSCRIPTIONS = 10
    no_of_topics = int(random() * 100)
    should_be_batches = no_of_topics // wsc.MAX_BATCH_SUBSCRIPTIONS + 1

    logger.info(f"creating {no_of_topics} topics")
    logger.info("should be batches: %s", should_be_batches)
    topics = [f"subject:{random_topic()}" for _ in range(no_of_topics)]

    batched_topics = wsc.batch_topics(topics)
    assert len(batched_topics) == should_be_batches
    for row in batched_topics:
        logger.info(row)

    batched_topics_str = wsc.batch_topics_str(topics)
    assert len(batched_topics_str) == should_be_batches
    for row in batched_topics_str:
        logger.info(row)

    logger.info("test passed: OK")
    logger.info("~-*-~" * 30)


async def test_process_subscribe():
    wsc.MAX_TOPICS_PER_CLIENT = 5
    wsc.MAX_BATCH_SUBSCRIPTIONS = 5
    t = wsc.Topics()
    topic = f"/market/ticker:{','.join(random_topic() for _ in range(10))}"
    logger.info(topic)
    subscribe, too_many = await t.process_subscribe(topic)

    logger.info("topics to subscribe to: %s" % subscribe)
    logger.info("topics exceeding the limit: %s" % too_many)

    assert len(subscribe[0].split(",")) == 5
    assert len(too_many) == 5

    t._topics = subscribe

    logger.info("=" * 100)
    logger.info("topics: %s", t)
    for topic in t:
        logger.info(topic)

    logger.info("test passed: OK")
    logger.info("~-*-~" * 30)


async def test_process_unsubscribe():
    wsc.MAX_BATCH_SUBSCRIPTIONS = 5
    t, subject, topics = wsc.Topics(), "subject", [random_topic() for _ in range(12)]
    prepped = [f"{subject}:{t}" for t in topics]
    t._topics = prepped

    topic = f"{subject}:{','.join(topics)}"
    logger.info(topic)

    to_unsubscribe = await t.process_unsubscribe(topic)
    logger.info("ready for unsubscribe: %s" % to_unsubscribe)

    assert t._topics == [], f"topics: {t._topics} should be empty"

    logger.info("=" * 100)

    t._topics = prepped * 2

    to_unsubscribe = await t.process_unsubscribe(topic)

    assert t._topics == prepped, f"topics: {t._topics} should be: {prepped}"

    logger.info("test passed: OK")
    logger.info("~-*-~" * 30)


async def test_batched_topics_str():
    wsc.MAX_TOPICS_PER_CLIENT = 10
    wsc.MAX_BATCH_SUBSCRIPTIONS = 4
    t = wsc.Topics()

    subject = "subject"
    topics = ["topic1", "topic2", "topic3", "topic4", "topic5", "topic6", "topic7"]
    t._topics = [f"{subject}:{t}" for t in topics]

    logger.info(t._topics)

    batched = t.batched_topics_str
    logger.info(batched)

    should_be = ['subject:topic1,topic2,topic3,topic4', 'subject:topic5,topic6,topic7']
    assert batched == should_be, f"{batched} != {should_be}"

    logger.info("test passed: OK")
    logger.info("~-*-~" * 30)


async def test_topics_class():
    await test_add_remove_topics()
    await test_batch_topics()
    await test_process_subscribe()
    await test_process_unsubscribe()
    await test_batched_topics_str()


# --------------------------------------------------------------------------------------
#                     test methods of KucoinWsClient class
async def test_multiple_unsubscribe():
    client = await wsc.KucoinWsClient.create(
        loop=asyncio.get_event_loop(),
        client=WsToken(),
        callback=callback
    )

    logger.info("using callback: %s", client._callback)

    topic = "/market/ticker:BTC-USDT"

    await client.subscribe(topic)
    await asyncio.sleep(1)
    await client.subscribe(topic)
    await asyncio.sleep(5)

    logger.info("topics after 2x subscribe: %s", client._conn.topics)

    should_be = ["/market/ticker:BTC-USDT", "/market/ticker:BTC-USDT"]
    assert client._topics._topics == should_be, \
        f"client: {client._topics} != {should_be}"
    assert client._conn._topics._topics == should_be, \
        f"ws: {client._topics} != {should_be}"

    for _ in range(3):
        await client.subscribe(topic)

    await asyncio.sleep(2)

    for _ in range(3):
        await client.unsubscribe(topic)

    await client.unsubscribe(topic)
    logger.info("topics after 1x unsubscribe: %s", client._conn.topics)
    assert client._topics._topics == ["/market/ticker:BTC-USDT"]
    assert client._conn._topics._topics == ["/market/ticker:BTC-USDT"]

    await client.unsubscribe(topic)
    logger.info("topics after 2x unsubscribe: %s", client._conn.topics)

    await asyncio.sleep(0.5)
    assert client._topics._topics == []
    assert client._conn._topics._topics == []

    await asyncio.sleep(10)

    logger.info("test passed: OK")


async def test_rate_limiter():
    symbols = await get_symbols(100)
    topics = [f"/market/ticker:{s}" for s in symbols]
    topics.append("/market/sticker:DEFINITELYNOTEXIST-USDT")

    client = await wsc.KucoinWsClient.create(
        loop=asyncio.get_event_loop(),
        client=WsToken(),
        callback=callback
    )

    # await asyncio.sleep(5)
    logger.info("------> let's go! <------")

    for topic in topics:
        await client.subscribe(topic)

    await asyncio.sleep(12)


async def test_batch_subscribe():
    wsc.MAX_TOPICS_PER_CLIENT = 300
    wsc.MAX_BATCH_SUBSCRIPTIONS = 50
    symbols = await get_symbols(200)
    topics = [f"/market/ticker:{s}" for s in symbols]
    # topics.append("/market/sticker:DEFINITELYNOTEXIST-USDT")

    client = await wsc.KucoinWsClient.create(
        loop=asyncio.get_event_loop(),
        client=WsToken(),
        callback=callback
    )

    # await asyncio.sleep(5)
    logger.info("------> let's go! <------")

    logger.debug(topics)
    topic = client._topics.batch_topics_str(topics)

    for t in topic:
        logger.debug("sending topic: %s", t)
        await client.subscribe(t)

    await client.subscribe("/market/ticker:NOTEXIST2:USDT,ETH-USDT")

    await asyncio.sleep(30)


async def test_close():
    c = await wsc.KucoinWsClient.create(
        loop=asyncio.get_event_loop(),
        client=WsToken(),
        callback=callback
    )

    await asyncio.sleep(3)
    logger.debug(c)

    await c.close()
    await asyncio.sleep(3)
    logger.info("test passed: OK")


async def main():
    try:
        # await test_batched_topics_str()
        # await test_process_unsubscribe()
        # await test_topics_class()
        # await test_multiple_unsubscribe()
        # await test_rate_limiter()
        # await test_batch_subscribe()
        await test_close()
    except Exception as e:
        logger.error(e, exc_info=1)


if __name__ == "__main__":
    asyncio.run(main())
