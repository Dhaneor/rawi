#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 11 23:44:23 2023

@author_ dhaneor
"""
import asyncio
import ccxt.pro as ccxt
import logging
import sys
import time

from os.path import dirname as dir
from random import choice, random

sys.path.append(dir(dir(dir(__file__))))

logger = logging.getLogger('main')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
logger.addHandler(handler)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s.%(funcName)s.%(lineno)d  - [%(levelname)s]: %(message)s"
)
handler.setFormatter(formatter)

import data_sources.websockets.ws_kucoin as ws  # noqa: E402


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

    return symbols[:count]


# random topic generator
def random_topic():
    return f"topic_{int(time.time() / 1000 * random() )}"


# mock publish coroutine
async def callback(msg):
    logger.info(f"received message: {msg}")


# --------------------------------------------------------------------------------------
#                     test methods of SUBSCRIBERS class
async def test_add_remove_topics(runs=20):
    # create a Topics object
    t = ws.Subscribers()
    topics = ["topic1", "topic2", "topic3", "topic4", "topic5", "topic6", "topic7"]

    for _ in range(runs):
        topic = choice(topics)

        if random() < 0.5:
            subs_for_topic = t._topics.get(topic, 0)
            await t.add_topic(topic)
            assert subs_for_topic + 1 == t._topics.get(topic, 0)
        else:
            subs_for_topic = t._topics.get(topic, 0)
            await t.remove_topic(topic)
            if subs_for_topic > 1:
                assert subs_for_topic - 1 == t._topics.get(topic, 0)
            else:
                assert t._topics.get(topic, None) is None


async def test_get_item():
    # create a Topics object
    t = ws.Subscribers()

    topic = random_topic()
    await t.add_subscriber(topic)
    assert topic in t._topics
    assert t._topics[topic] == 1

    await t.add_subscriber(topic)
    assert t._topics[topic] == 2

    logger.debug(t[topic])

    await t.remove_subscriber("not_there")

    await t.remove_subscriber(topic)
    assert t._topics[topic] == 1

    await t.remove_subscriber(topic)
    assert topic not in t._topics


# ......................................................................................
#                    test methods of CONNECTION class
async def test_connection_prep_topic():
    # create a Connection object
    endpoint = "/mock/websockets"
    c = ws.Connection(None, endpoint, debug=True)

    c._topics = {i: 1 for i in range(280)}

    max_topics_per_conn = ws.MAX_TOPICS_PER_CONNECTION - 1
    no_of_topics = 50
    topics = ["topic"] * no_of_topics
    max_left = c.max_topics_left

    should_be_too_much = no_of_topics - max_left

    topics, too_much = await c._prep_topic_str(topics)
    topics = [sub_t for t in topics for sub_t in t.split(",")]
    c._topics = c._topics | {i + 500: t for i, t in enumerate(topics)}

    assert len(c._topics) == max_topics_per_conn, \
        f"{len(c._topics)} <-> {max_topics_per_conn}"
    assert len(too_much) == should_be_too_much

    # ........................................
    topics, too_much = await c._prep_topic_str("extra topic")

    assert topics == []
    assert too_much == ["extra topic"]

    logger.debug("test passed: OK")


async def test_prep_unsub_str():
    # create a Connection object
    endpoint = "/mock/websockets"
    c = ws.Connection(None, endpoint, debug=True)

    topics = [random_topic() for _ in range(50)]
    unsub_str = await c._prep_unsub_str(topics)

    logger.debug(unsub_str)


async def test_remove_multiple_subs():
    topics = ["topic1", "topic2", "topic3", "topic4", "topic5", "topic6", "topic7"]

    # prepare two test Connection instances
    c1 = ws.Connection(None, "", True)
    await c1.watch(topics[:5])
    c2 = ws.Connection(None, "", True)
    await c2.watch(topics[2:])
    await asyncio.sleep(1)
    await c2.watch(topics[2:])
    await asyncio.sleep(1)

    logger.debug("connection 1 topics: %s" % (c1._topics))
    logger.debug("connection 2 topics: %s" % (c2._topics))

    logger.debug("." * 120)

    # add them to a WebsocketBase instance
    wsb = ws.WebsocketBase(callback=callback, debug=True)
    wsb._connections = {c._id: c for c in (c1, c2)}

    # see if our coroutine works as expected ...
    await wsb.remove_multiple_subs()
    await asyncio.sleep(2)

    assert c1._topics["topic3"] == 3
    assert c1._topics["topic4"] == 3
    assert c1._topics["topic5"] == 3

    assert "topic3" not in c2._topics
    assert "topic4" not in c2._topics
    assert "topic5" not in c2._topics

    logger.debug("topics conn 1/2: %s <---> %s" % (c1._topics, c2._topics))
    logger.debug("test passed: OK")


async def test_conn_watch_unwatch():
    # create a Connection object
    endpoint = "/mock/websockets"
    c = ws.Connection(None, endpoint, debug=True)
    no_of_topics = 3

    topics = [random_topic() for _ in range(no_of_topics)]

    await c.watch(topics)
    await asyncio.sleep(1)
    assert len(c._topics) == no_of_topics, f"{len(c._topics)} != 50"

    await c.unwatch(topics)
    await asyncio.sleep(1)
    assert len(c._topics) == 0, f"{len(c._topics)}!= 0 --> {c._topics}"


# ......................................................................................
#                  test methods of WEBSOCKETBASE class
async def test_filter_topics():
    wsb = ws.WebsocketBase(callback=callback, debug=True)
    topics = [random_topic() for _ in range(10)]
    logger.debug(topics)

    topics = await wsb.handle_existing_topics(topics, "subscribe")
    logger.debug(topics)

    assert len(topics) == 10, f"{len(topics)}!= 10"

    await wsb.watch(topics)
    await asyncio.sleep(2)
    await wsb.close()
    await asyncio.sleep(2)


async def test_move_topics():
    topics = [
        "topic1", "topic2", "topic3", "topic4", "topic5", "topic6", "topic7",
        "topic8", "topic9", "topic10", "topic11", "topic12", "topic13", "topic14",
    ]

    # prepare two test Connection instances
    c1 = ws.Connection(None, "", True)
    await c1.watch(topics[:7])
    c2 = ws.Connection(None, "", True)
    await c2.watch(topics[7:])
    # await asyncio.sleep(1)
    # await c2.watch(topics[2:])
    await asyncio.sleep(1)

    # add them to a WebsocketBase instance
    wsb = ws.WebsocketBase(callback=callback, debug=True)
    wsb._connections = [c1, c2]
    topics_before = {k: v for k, v in wsb.topics.items()}

    # see if our coroutine works as expected...
    await wsb.move_topics(c1, c2)

    await asyncio.sleep(2)

    c2_topics_after = {
        k: c2._topics[k] for k in sorted(c2._topics._topics.keys())
    }
    logger.debug("topics conn 1/2: %s <---> %s" % (c1._topics, c2_topics_after))

    assert not c1._topics, f"{c1._topics} != " + "{}"
    assert c2.topics.sort() == topics.sort(), f"{c2.topics} != {topics}"
    assert topics_before == wsb.topics, f"{topics_before}!= {wsb.topics}"

    logger.debug("test passed: OK")


async def test_switch_connection(debug=True):
    if debug:
        logger.setLevel(logging.DEBUG)
        interval = 15
    else:
        logger.setLevel(logging.INFO)
        interval = 30

    symbols = await get_symbols(3)
    wsb = ws.WsTickers(callback=callback, cycle_interval=interval, debug=debug)
    await wsb.watch(symbols)
    await asyncio.sleep(1)
    await wsb.watch(symbols)

    while True:
        try:
            await asyncio.sleep(interval)
            logger.debug("=" * 120)
            await wsb.switch_connections()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(e, exc_info=1)
            break


async def test_wsb_watch_unwatch_batch():
    wsb = ws.WebsocketBase(callback=callback, debug=True)
    topics = [random_topic() for _ in range(50)]

    await wsb.watch(topics)
    await asyncio.sleep(10)

    for conn in wsb._connections.values():
        logger.debug("%s has topics: %s", conn.name, len(conn._topics))

    # assert len(wsb._topics) == 50, f"{len(c._topics)} != 50"

    logger.debug("-~•~-" * 50)

    await wsb.unwatch(topics)
    await asyncio.sleep(20)
    # assert len(wsb._topics) == 0, f"{len(c._topics)}!= 0"

    logger.debug("test passed: OK")


async def test_wsb_watch_unwatch_random():
    # create a WebsocketBase object
    wsb = ws.WebsocketBase(callback=callback, debug=True)
    symbols = await get_symbols(50)
    sleep_time = 0.1
    run = 0

    while True:
        try:
            topic = choice(symbols)
            runs = int(0.5 + random() * 2)
            threshhold = random() + (run / 1000) - 0.5
            # logger.debug("----------> %s ~ %s <----------", run, threshhold)

            if random() > threshhold:
                for _ in range(runs):
                    await wsb.watch(topic)
                    await asyncio.sleep(sleep_time / runs)

            else:
                for _ in range(runs):
                    if (subbed_topics := list(wsb.topics.keys())):
                        await wsb.unwatch(choice(subbed_topics))
                    else:
                        logger.debug("no topics to unwatch")
                        if run > 20:
                            raise asyncio.CancelledError()
                    await asyncio.sleep(sleep_time / runs)

            run += 1
            # await asyncio.sleep(random() * 1)

        except TypeError as e:
            logger.error(e, exc_info=1)
            logger.debug(type(wsb.topics))
        except asyncio.CancelledError:
            logger.info("test cancelled ...")
            break
        except Exception as e:
            logger.error(e, exc_info=1)
            break
    await wsb.close()
    logger.info("exchange closed: OK")


async def test_it_for_real():
    """Test with the real websocket client.

    Subscriptions are changed after sleep_time, simulating multiple
    pretty active/dynamic consumers to check if this is handled well.

    """
    wsb = ws.WsTrades(callback=callback, cycle_interval=3600)
    symbols = await get_symbols(10)
    sleep_time = 5
    run = 0

    bg_task = asyncio.create_task(wsb.run())

    while True:
        try:
            topic = choice(symbols)
            runs = int(0.5 + random() * 2)
            threshhold = random() + (run / 50) - 0.5
            # logger.debug("----------> %s ~ %s <----------", run, threshhold)

            if random() > threshhold:
                for _ in range(runs):
                    await wsb.watch(topic)
                    await asyncio.sleep(sleep_time / runs)

            else:
                for _ in range(runs):
                    if (subbed_topics := list(wsb.topics.keys())):
                        await wsb.unwatch(choice(subbed_topics))
                        await asyncio.sleep(sleep_time / runs)
                    else:
                        logger.debug("no topics to unwatch")
                        if run > 20:
                            raise asyncio.CancelledError()

            run += 1
            await asyncio.sleep(random() * 1)

        except TypeError as e:
            logger.error(e, exc_info=1)
            logger.debug(type(wsb.topics))
        except asyncio.CancelledError:
            logger.info("test cancelled ...")
            break
        except Exception as e:
            logger.error(e, exc_info=0)
            break

    logger.info("terminating background task...")

    try:
        bg_task.cancel()
    except Exception as e:
        logger.error("exception while cancelling background task: %s", e)

    logger.info("closing WebsocketBase instance ...")

    try:
        await wsb.close()
    except Exception as e:
        logger.error("exception during shutdown of WebsocketBase: %s", e)

    await asyncio.sleep(3)

    logger.info("exchange closed: OK")


async def test_trades_stream():
    ts = ws.WsSnapshots(callback=callback)
    await ts.watch(["BTC-USDT"])  # , "ETH-USDT", "XRP-USDT"])

    while True:
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            logger.info("test cancelled ...")
            break
        except Exception as e:
            logger.error(e, exc_info=0)
            break

    logger.info("closing WsTrades instance ...")

    await ts.close()
    await asyncio.sleep(3)

    logger.info("exchange closed: OK")


async def main():
    # await test_add_remove_topics()
    # await test_get_item()

    # await test_connection_prep_topic()
    # await test_prep_unsub_str()
    # await test_remove_multiple_subs()
    # await test_conn_watch_unwatch()

    # await test_filter_topics()
    # await test_move_topics()
    # await test_switch_connection(False)
    # await test_wsb_watch_unwatch_batch()
    # await test_wsb_watch_unwatch_random()
    # await test_it_for_real()
    await test_trades_stream()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("shutdown complete: OK")
