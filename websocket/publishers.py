#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 28 13:32:20 2022

@author dhaneor
"""
import asyncio
import json
import logging
from abc import abstractmethod
from typing import Any

import zmq
import zmq.asyncio

logger = logging.getLogger("main.publisher")
logger.setLevel(logging.ERROR)


# =============================================================================
class IPublisher:
    @abstractmethod
    async def publish(self, event: dict):
        pass


class PrintPublisher(IPublisher):
    async def publish(self, event):
        print(event)


class LogPublisher(IPublisher):
    def __init__(self, logger_name: str):
        self.logger = logging.getLogger(logger_name)

    async def publish(self, event):
        self.logger.debug(event)


class CallbackPublisher(IPublisher):
    def __init__(self, callback):
        self.callback = callback

    async def publish(self, event: Any):
        await self.callback(event)


class QueuePublisher(IPublisher):
    def __init__(self, queue: asyncio.Queue):
        self.queue: asyncio.Queue = queue

    async def publish(self, event):
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.error("publish queue is full - event will not be delivered!")


class ZeroMqPublisher(IPublisher):
    def __init__(self, ctx: zmq.asyncio.Context, address: str):
        self.socket = ctx.socket(zmq.PUSH)
        rc = self.socket.bind(address)
        logger.debug(rc)

    async def publish(self, event):
        try:
            logger.debug("sending event to socket ...")
            msg = json.dumps(event).encode("utf-8")
            await self.socket.send(msg)
        except zmq.ZMQError as e:
            logger.error(f"ZMQ error: {e}")
