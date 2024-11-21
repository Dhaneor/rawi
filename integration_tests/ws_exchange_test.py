#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 19 13:31:23 2023

@author_ dhaneor
"""
import asyncio
import logging
import random
import sys

from os.path import dirname as dir

sys.path.append(dir(dir(dir(__file__))))

from websocket import ws_exchange as wse  # noqa: E402

logger = logging.getLogger('main')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
logger.addHandler(handler)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s.%(funcName)s.%(lineno)d  - [%(levelname)s]: %(message)s"
)
handler.setFormatter(formatter)


async def test_ws_exchange_factory():



async def main():
    await test_ws_exchange_factory()


if __name__ == "__main__":
    asyncio.run(main())