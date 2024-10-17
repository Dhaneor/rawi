#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 17 22:51:23 2023

@author_ dhaneor
"""
import asyncio
import logging

from typing import Coroutine, Optional

from ..kucoin.kucoin.ws_client import KucoinWsClient
from ..kucoin.kucoin.ws_token.token import GetToken
from ..util.subscription_request import SubscriptionRequest
from ..util.enums import SubscriptionType


logger = logging.getLogger("main.ws_manager")


async def publish(msg):
    # logger.info(msg)
    return


async def subject_from_request(req: SubscriptionRequest) -> str:
    subjects = {
        SubscriptionType.TICKER: "/market/ticker",
        SubscriptionType.OHLCV: "/market/candles",
        SubscriptionType.BOOK: "/market/level2",
        SubscriptionType.TRADES: "/market/match",
    }
    return subjects[req.sub_type]


async def wrapper(req: SubscriptionRequest, coro: Coroutine):
    await coro(f"{await subject_from_request(req)}:{req.topic}")


async def get_connection(existing: list) -> KucoinWsClient:
    for conn in existing:
        if conn.topics_left:
            return conn

    existing.append(
        await KucoinWsClient.create(
            loop=asyncio.get_event_loop(),
            client=GetToken(),
            callback=publish,
        )
    )
    return existing[-1]


async def close_unused_connections(connections: list):
    for conn in connections:
        if not conn.topics:
            logger.debug("closing connection: %s", conn)
            connections.remove(conn)
            # wait until all topics are unsubscribed
            await asyncio.sleep(0.5)
            # ... then close the connection
            await conn.close()
            del conn
            logger.debug("%s connections remaining" % len(connections))


async def filter_request(
    req: SubscriptionRequest,
    valid_type: Optional[SubscriptionType] = None
) -> SubscriptionRequest | None:
    logger.debug("checking request: %s", req)
    if valid_type:
        return req if req.topic and req.sub_type == valid_type else None
    else:
        return req if req.topic else None


async def watch_subscribe(
    connections: list,
    queue: asyncio.Queue,
    wrapper: Coroutine
):
    while True:
        try:
            if req := await filter_request(await queue.get()):
                conn = await get_connection(existing=connections)
                await wrapper(req, conn.subscribe)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(e, exc_info=1)


async def watch_unsubscribe(
    connections: list,
    queue: asyncio.Queue,
    wrapper: Coroutine
):
    while True:
        try:
            if req := await filter_request(await queue.get()):
                topic = f"{await subject_from_request(req)}:{req.topic}"
                done = False

                for conn in connections:
                    if topic in conn.topics:
                        await conn.unsubscribe(topic)
                        done = True

                if done:
                    await close_unused_connections(connections)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(e, exc_info=1)


async def ws_manager(sub_q: asyncio.Queue, unsub_q: asyncio.Queue):
    connections = []

    tasks = [
        asyncio.create_task(watch_subscribe(connections, sub_q, wrapper)),
        asyncio.create_task(watch_unsubscribe(connections, unsub_q, wrapper)),
    ]

    while True:
        try:
            await asyncio.sleep(30)

        except asyncio.CancelledError:
            for task in tasks:
                task.cancel()

            await asyncio.gather(*tasks, return_exceptions=True)
            break
