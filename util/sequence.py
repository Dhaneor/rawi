#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides decorators to produce and monitor sequence numbers in a data stream.

Created on Wed Dec 07 12:44:23 2022

@author_ dhaneor
"""
import asyncio
import logging
from functools import wraps
from typing import Coroutine

logger = None

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    # logging.basicConfig(level=logging.DEBUG)
    ch = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s.%(funcName)s.%(lineno)d  - [%(levelname)s]: %(message)s"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)
else:
    logger = logging.getLogger('main.sequence')


# --------------------------------------------------------------------------------------
#                           sequence number monitor decorator                          #
# --------------------------------------------------------------------------------------
def monitor_sequence(
    callback: Coroutine = None,
    reset_threshold: int = 1e6,
    fuzziness: int = 10,
    identifier: str = 'sequence'
) -> Coroutine:
    """A decorator for monitoring sequence numbers in a data stream.

    Parameters
    ----------
    callback : Coroutine, optional
        A coroutine to call when a missing sequence number was
        detected, by default None
        If None, it will be tried to use the curent logger object
        to log a warning.
    reset_threshold : int, optional
        where to expect the seq number to be reset, by default 1e6
    fuzziness : int, optional
        Fuzzy factor for detecting reset of sequence number by
        the sender, by default 10
    identifier : str, optional
        The key in the message dictionary that represents the sequence number

    Returns
    -------
    Coroutine
    """
    # Local dictionary to hold the last sequence number for each producer
    last_seq_for_producer = {}
    fuzziness = fuzziness

    def decorator(cor):
        @wraps(cor)
        async def wrapper(msg, *args, **kwargs):
            producer_id = msg['uid']
            seq_num = int(msg[identifier])
            last_seq = last_seq_for_producer.get(producer_id, None)

            # Check for missing sequence numbers
            if last_seq is not None:
                if seq_num != last_seq + 1:
                    # Check for sequence number reset with fuzziness
                    if seq_num <= fuzziness and last_seq >= reset_threshold:
                        ...
                    elif callback:
                        await callback(producer_id, last_seq, seq_num)
                    elif not callback:
                        try:
                            logger.warning(
                                "Missing or wrong sequence number for producer %s. "
                                "Last: %d, Current: %d",
                                producer_id, last_seq, seq_num
                            )
                        except Exception:
                            pass

            # Update last sequence number for this producer
            last_seq_for_producer[producer_id] = seq_num

            return await cor(msg, *args, **kwargs)
        return wrapper
    return decorator


# ......................................................................................
# test for monitor_sequence decorator
async def report_missing(producer_id, last_seq, current_seq):
    logger.warning(
        f"Missing sequence number(s) from producer {producer_id}. "
        f"Last: {last_seq}, Current: {current_seq}"
    )


@monitor_sequence(callback=report_missing, reset_threshold=100)
async def process_msg(msg):
    logger.info(f"Processing message {msg}")


async def test_monitor(runs: int = 100):
    for i in range(runs):
        if not i % 10 == 0:
            await process_msg({'uid': 'my_uid', 'seq_no': i})
        else:
            logger.info(f"simulating lost message with sequence number: {i}")


# --------------------------------------------------------------------------------------
#                       sequence number generator decorator                            #
# --------------------------------------------------------------------------------------
def sequence(logger=None, identifier: str = "sequence", reset_threshhold: int = 1e6):
    "Decorator to generate and remember sequence numbers."

    def seq_no():
        seq = 1
        while True:
            reset = seq % reset_threshhold == 0
            seq = 1 if reset else seq + 1
            yield seq

    sequence = seq_no()
    logger = logger or logging.getLogger(__name__)

    # @wraps(func)
    def wrapper(func):

        def inner(msg: dict, *args, **kwargs):

            try:
                msg[identifier] = next(sequence)
            except TypeError as e:
                if logger:
                    logger.error(f"Expected dictionary, got: {msg}")
                else:
                    raise e

            return func(msg=msg, *args, **kwargs)

        return inner

    return wrapper


# ......................................................................................
# test for sequence decorator
@sequence(logger=logger, reset_threshhold=100)
async def test_generator(msg: dict, identifier: str = "sequence"):
    seq_no = msg[identifier]
    # logger.debug(f"... processing message {msg}")

    if not seq_no % 10 == 0:
        await process_msg(msg)
    else:
        logger.info(f"... simulating lost message with sequence number: {seq_no}")


async def main(which: int, runs: int = 100):
    if which == 0:
        logger.info("testing generator")
        for i in range(runs):
            await test_generator(msg={'uid': 'my_uid'})
    elif which == 1:
        logger.info("testing monitor")
        await test_monitor(runs)

if __name__ == '__main__':
    asyncio.run(main(0, 110))
