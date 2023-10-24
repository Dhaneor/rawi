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

from os.path import dirname as dir

sys.path.append(dir(dir(__file__)))
sys.path.append(dir(dir(dir(__file__))))

from collector import collector  # noqa: E402, F401
from zmqbricks.gond import Gond  # noqa: E402, F401
from zmq_config import Collector  # noqa: E402, F401

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
async def run_collector():
    ctx = zmq.asyncio.Context()
    config = Collector()

    try:
        await collector(config=config)
    except asyncio.CancelledError:
        logger.info("run_collector cancelled ...")

    ctx.term()


async def main():
    try:
        await run_collector()
    except KeyboardInterrupt:
        logger.info("shutdown complete: OK")


if __name__ == "__main__":
    asyncio.run(main())
