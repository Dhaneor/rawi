#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides a central confiuration service/registry for ZMQ components.

Functions:
    amanya
        The main function for this module. This is the entry point/
        main interface for the service registry. All other components
        register here when they start. if a component need information
        about other available components, it can get this information
        by sending a request to the requests socket.

Created on Tue Sep 12 19:41:23 2023

@author_ dhaneor
"""
import asyncio
import json
import logging
# import os
# import sys
import zmq
import zmq.asyncio

from typing import Optional, TypeVar  # noqa: F401

# # --------------------------------------------------------------------------------------
# current = os.path.dirname(os.path.realpath(__file__))
# parent = os.path.dirname(current)
# sys.path.append(parent)
# # --------------------------------------------------------------------------------------

from zmqbricks import gond  # noqa: F401, E402
from zmqbricks import heartbeat as hb  # noqa: F401, E402
from zmq_config import BaseConfig, Amanya  # noqa: F401, E402

if __name__ == "__main__":
    logger = logging.getLogger("main")
    ch = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s.%(funcName)s.%(lineno)d  - [%(levelname)s]: %(message)s"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.setLevel(logging.DEBUG)


ConfigT = TypeVar("ConfigT", bound=BaseConfig)
ContextT = TypeVar("ContextT", bound=zmq.asyncio.Context)


# ======================================================================================
async def amanya(config: ConfigT, context: Optional[ContextT] = None):
    """The Central Configuration Service (service registry)

    Amanya -> according to pi.ai:

    me: Today, I'm looking for a word that describes an institution
        or person where everyone can go and ask for some information!

        Please give me some choices, and try to find words that come
        from different languages - including Tolkien's elven language,
        but also Spanish, French, Japanese, German, Old English and
        Chinese!

        We're aiming for a word that conveys the meaning but also some
        sense of of wonder or mystery.

    Pi: Starting with Quenya, the elven language created by Tolkien,
        you might consider "Amanya" which means "fount of wisdom"

    Parameters
    ----------
    config : ConfigT
        the configuration for the amanaya component
    ctx : ContextT, optional
        A ZeroMQ Context object, default None
    """
    ctx = context or zmq.asyncio.Context()

    async with gond.Gond(config, context=ctx) as g:  # noqa: F841

        poller = zmq.asyncio.Poller()
        registry = g.kinsfolk

        logger.info("configuring requests socket at %s", config.req_addr)
        requests = ctx.socket(zmq.ROUTER)
        requests.curve_secretkey = config.private_key.encode("ascii")
        requests.curve_publickey = config.public_key.encode("ascii")
        requests.curve_server = True
        requests.bind(config.endpoints.get("requests"))

        poller.register(requests, zmq.POLLIN)

        while True:
            try:
                logger.info("running ...")
                events = dict(await poller.poll(1000))

                if requests in events:
                    msg = await requests.recv_multipart()
                    key, req = msg[0], msg[1]

                    service_type = req.get("service_type")

                    services = registry.get_all(service_type)
                    reply = json.dumps(services) if services else b""

                    requests.send_multipart([key, reply])

            except zmq.ZMQError as e:
                logger.error(e)
            except asyncio.CancelledError:
                break

        requests.close()

    if context is not None:
        ctx.term()


# ======================================================================================
async def main():
    ctx = zmq.asyncio.Context()
    config = Amanya()

    try:
        await amanya(config, ctx)
    except asyncio.CancelledError:
        logger.info("cancelled ...")

    logger.info("shutdown complete: OK")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.debug("shutdown complete: KeyboardInterrupt")
