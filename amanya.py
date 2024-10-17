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

from functools import partial
from typing import TypeVar  # noqa: F401

import zmq
import zmq.asyncio

from zmqbricks import gond  # noqa: F401, E402
from zmqbricks.registration import ScrollT  # noqa: F401, E402
from zmq_config import BaseConfig, Amanya, Collector  # noqa: F401, E402

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
SocktT = TypeVar("SocktT", bound=zmq.asyncio.Socket)

test_msg = Collector().as_dict()
test_msg["endpoints"]["registration"] = "tcp://127.0.0.1:5570"


# ======================================================================================
async def publish(scroll: ScrollT, socket: SocktT) -> None:
    """Publishes a scroll to the given socket

    Parameters
    ----------
    scroll : ScrollT
        The scroll to publish
    socket : SocktT
        The socket to publish to
    """
    logger.debug("–~•~–" * 30)
    logger.debug("===> publishing scroll for NEW KINSMAN: %s", scroll)
    await socket.send_multipart(
        [
            scroll.service_type.encode(),
            b"ADD",
            json.dumps(scroll.as_dict()).encode()
        ]
    )


# ======================================================================================
async def amanya(config: ConfigT):
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
    ctx = zmq.asyncio.Context.instance()

    # requests = get_random_server_socket("requests", zmq.ROUTER, config)
    requests = ctx.socket(zmq.ROUTER)
    requests.curve_secretkey = config.private_key.encode("ascii")
    requests.curve_publickey = config.public_key.encode("ascii")
    requests.curve_server = True
    requests.bind(config.endpoints.get("requests"))
    logger.info("configured requests socket at %s", config.endpoints.get("requests"))

    publisher = ctx.socket(zmq.PUB)
    publisher.curve_secretkey = config.private_key.encode("ascii")
    publisher.curve_publickey = config.public_key.encode("ascii")
    publisher.curve_server = True
    publisher.bind(config.endpoints.get("publisher"))
    logger.info("configured publisher socket at %s", config.endpoints.get("publisher"))

    poller = zmq.asyncio.Poller()
    poller.register(requests, zmq.POLLIN)

    on_rgstr_success = [partial(publish, socket=publisher)]

    async with gond.Gond(config=config, on_rgstr_success=on_rgstr_success) as g:

        while True:
            try:
                events = dict(await poller.poll(1000))

                if requests in events:
                    msg = await requests.recv_multipart()
                    key, service_type = msg[0], msg[1].decode()

                    if kinsmen := await g.kinsfolk.get_all(service_type):
                        reply = [
                            json.dumps(k.to_scroll().as_dict()).encode()
                            for k in kinsmen
                        ]
                    else:
                        reply = [b""]

                    reply.insert(0, key)
                    reply.insert(1, b"ADD")  # command
                    reply.insert(1, msg[1])  # topic

                    requests.send_multipart(reply)

            except zmq.ZMQError as e:
                logger.error(e)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("unexpected error: %s", e, exc_info=1)
                break

        requests.close(0)
        publisher.close(0)

    # if context is not None:
    #     await ctx.term()


# ======================================================================================
async def main():
    try:
        await amanya(config=Amanya())
    except asyncio.CancelledError:
        await asyncio.sleep(2)
        logger.info("shutdown complete: OK")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.debug("shutdown complete: KeyboardInterrupt")
