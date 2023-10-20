#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 16 13:58:23 2023

@author_ dhaneor
"""
import asyncio
import logging
import os
import sys
import zmq
import zmq.asyncio


# --------------------------------------------------------------------------------------
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
# --------------------------------------------------------------------------------------

from data_sources.craeft_pond import ContainerRegistry, OHLCVContainer  # noqa: E402

logger = logging.getLogger("main.stream_manager")

logger.setLevel(logging.DEBUG)

# ======================================================================================
container_registry = ContainerRegistry()

context = None
socks_addr = {
    "publisher": "tcp://localhost:5595",
    "subscriber": "tcp://localhost:5596",
    "requests": "tcp://*:5597",
}

publisher = None
subscriber = None
requests = None


# ======================================================================================
def publish(ohlcv_container: OHLCVContainer):
    publisher.send_pyobj(ohlcv_container)


async def subscription_handler():
    poller = zmq.asyncio.Poller()
    poller.register(subscriber, zmq.POLLIN)
    poller.register(requests, zmq.POLLIN)

    logger.info("subscription handler started: OK")

    counter = 0

    while True:
        events = dict(await poller.poll(100))

        if requests in events:
            request = await requests.recv_json()
            logger.debug(request)

        if counter % 100 == 0:
            logger.debug("subscription_handler -> waiting...")

        counter += 1


async def mesh_reactor():
    poller = zmq.asyncio.Poller()
    poller.register(publisher, zmq.POLLIN)

    logger.info("mesh reactor started: OK")

    counter = 0

    while True:
        events = dict(await poller.poll(10))

        if subscriber in events:
            event = subscriber.recv_pyobj()
            logger.debug(event)
            container_registry.update(event)

        if counter % 1_000 == 0:
            logger.debug("mesh_reactor --> waiting ...")

        counter += 1


async def stream_manager(ctx: zmq.asyncio.Context):
    logger.debug("starting stream manager...")

    global context
    context = zmq.asyncio.Context()

    global publisher
    publisher = context.socket(zmq.PUSH)
    publisher.connect(socks_addr["publisher"])
    logger.debug("publisher started at %s: OK", socks_addr["publisher"])

    global subscriber
    subscriber = context.socket(zmq.SUB)
    subscriber.connect(socks_addr["subscriber"])
    logger.debug("subscriber started at %s: OK", socks_addr["subscriber"])

    global requests
    requests = context.socket(zmq.PULL)
    requests.bind(socks_addr["requests"])
    logger.debug("requests started at %s: OK", socks_addr["requests"])

    tasks = [
        subscription_handler(),
        mesh_reactor(),
    ]

    logger.info("stream manager started: OK")

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("stopping tasks ...")

    logger.info("stream manager stopped: OK")


if __name__ == "__main__":
    handler = logging.StreamHandler()
    logger.addHandler(handler)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s.%(funcName)s.%(lineno)d  - [%(levelname)s]: %(message)s"
    )

    handler.setFormatter(formatter)

    logger.debug("starting script ...")

    try:
        asyncio.run(stream_manager(zmq.asyncio.Context()))
    except KeyboardInterrupt:
        logger.info("shutdown complete: OK")
