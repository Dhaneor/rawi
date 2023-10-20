#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides a function that removes duplicates (from data streams).

Created on Sun Sep 24 20:16:23 2023

@author_ dhaneor
"""
import hashlib
import logging
import xxhash

from collections import deque
from typing import Any, Optional, Callable

# from . import stream_events as se

logger = logging.getLogger('main.deduplicate')


MAX_CACHE_SIZE = 5_000  # maximum entries in cache


def hash_func(msg: dict) -> str:
    return hashlib.sha256(str(sorted(msg.items())).encode()).hexdigest()


def fast_hash_xxhash(msg: dict) -> str:
    return xxhash.xxh64(str(sorted(msg.items()))).hexdigest()


def fast_hash_md5(msg: dict) -> str:
    return hashlib.md5(str(sorted(msg.items())).encode()).hexdigest()


# --------------------------------------------------------------------------------------
def create_deduplicate_function(
    max_cache_size: int,
    hash_func: Optional[Callable[[dict], str]] = None
) -> Any:

    if hash_func is None:
        hash_func = fast_hash_xxhash

    msg_cache = deque(maxlen=max_cache_size)

    async def deduplicate(msg: dict[str, Any]) -> Any:
        """Removes duplicate messages from a data stream."""

        # Generate a hash for the message
        msg_hash = hash_func(msg)

        if msg_hash in msg_cache:
            logger.debug("Duplicate message -> discarding ...")
            return None
        else:
            msg_cache.append(msg_hash)
            return msg

    return deduplicate


# def create_deduplicate_function(max_cache_size: int) -> Any:
#     msg_cache = set()
#     max_cache_size = max_cache_size

#     async def deduplicate(msg: dict[str, Any]) -> se.CandleEvent | None:
#         """Removes duplicate messages from a data stream.

#         Parameters
#         ----------
#         msg : dict
#             A message from a data stream

#         Returns
#         -------
#         se.CandleEvent| None
#             The message if it is not a duplicate

#         Raises
#         ------
#         ValueError
#             If the message cannot be converted to a candle event
#         """
#         try:
#             msg = se.CandleEvent(
#                 exchange=msg['exchange'],
#                 symbol=msg["symbol"],
#                 timestamp=msg["time"],
#                 timestamp_recv=0,
#                 latency=0,
#                 type=msg["type"],
#                 interval=msg["interval"],
#                 open_time=msg["data"]["open time"],
#                 open=msg["data"]["open"],
#                 high=msg["data"]["high"],
#                 low=msg["data"]["low"],
#                 close=msg["data"]["close"],
#                 volume=msg["data"]["volume"],
#                 quote_volume=msg["data"]["quote volume"]
#             )
#         except ValueError:
#             logger.error("Invalid message: %s", msg)
#             return None

#         # Generate a hash for the update
#         msg_hash = hashlib.sha256(str(msg).encode()).hexdigest()

#         if msg_hash in msg_cache:
#             logger.debug("Duplicate message -> discarding ...")
#             return None
#         else:
#             msg_cache.add(msg_hash)
#             if len(msg_cache) > max_cache_size:
#                 msg_cache.pop()

#             return msg

#     return deduplicate
