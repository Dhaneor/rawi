#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 08 19:01:23 2023

@author_ dhaneor
"""
import asyncio
import logging
import sys

from os.path import dirname as dir

sys.path.append(dir(dir(__file__)))


from zmqbricks.gond import Gond  # noqa: E402, F401
from zmq_config import BaseConfig  # noqa: E402, F401

# --------------------------------------------------------------------------------------

logger = logging.getLogger('main')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
logger.addHandler(handler)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s.%(funcName)s.%(lineno)d  - [%(levelname)s]: %(message)s"
)
handler.setFormatter(formatter)


# --------------------------------------------------------------------------------------
async def main():
    """Tests the Gond context manager.

    For testing it registers with itself, and sends itself heartbeats.
    For practical applications this would make no sense, but that's how
    we can test both directions on one go.
    """

    cnf = BaseConfig("kucoin", ["spot"], [])
    cnf._endpoints = {
        "registration": "tcp://127.0.0.1:5600",
        "publisher": "tcp://127.0.0.1:5601",
        "heartbeat": "tcp://127.0.0.1:5602",
    }

    cnf.service_registry = {
        "endpoint": "tcp://127.0.0.1:5600",
        "public_key": cnf.public_key,
    }

    async with Gond(cnf) as g:  # noqa: F841
        # logger.debug(g)

        # await g.heart.listen_to(cnf.endpoints["heartbeat"])

        await asyncio.sleep(10)
        logger.debug("waking up from sleep ...")

        # await g.heart.stop_listening_to(cnf.endpoints["heartbeat"])


if __name__ == '__main__':
    asyncio.run(main())
