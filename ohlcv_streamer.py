#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 07 12:44:23 2022

@author_ dhaneor
"""
import asyncio
import logging

import streamer as st

# listener = QueueListener(que, handler)
logger = logging.getLogger('main')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
logger.addHandler(handler)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s.%(funcName)s.%(lineno)d  - [%(levelname)s]: %(message)s"
)
handler.setFormatter(formatter)


async def main():

    try:
        await st.streamer(market='kucoin.spot', mode='candles')
        # # Start the streamer task
        # streamer_task = asyncio.create_task(
        #     st.streamer(market='kucoin.spot', mode='candles')
        # )

        # await asyncio.gather(streamer_task, return_exceptions=True)

    except asyncio.CancelledError:
        logger.debug("cancelling tasks after keyboard interrupt")
        # streamer_task.cancel()
        # await asyncio.gather(streamer_task, return_exceptions=True)
    except Exception as e:
        logger.exception(e)
    finally:
        logger.info("streamer shutdown complete: OK")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("keyboard interrupt caught with asyncio.run()")
    finally:
        logger.info("shut down complete: OK")
