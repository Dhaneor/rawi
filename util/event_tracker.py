#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Asynchronous decorator to monitor the number of times a coroutine is called.

It sends statistics for averages of multiple intervals via the callback.

Created on Mon Oct 02 :15:20 2023

@author dhaneor
"""
import asyncio
import logging
import random
from collections import deque
from functools import wraps
from time import time, sleep
from typing import Any, Callable, Coroutine

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)


# ======================================================================================
class EventCache:

    def __init__(self, bucket_interval=1, num_buckets=86400):
        self.bucket_interval = bucket_interval  # e.g., 60 seconds
        self.num_buckets = num_buckets  # e.g., 1440 buckets for 24 hours
        self.buckets = deque(maxlen=self.num_buckets)
        self.current_bucket = 0
        self.last_update = time()

    def add_event(self):
        current_time = time()
        elapsed_time = current_time - self.last_update

        # Update buckets based on elapsed time
        while elapsed_time >= self.bucket_interval:
            self.buckets.append(self.current_bucket)
            self.current_bucket = 0
            elapsed_time -= self.bucket_interval
            self.last_update += self.bucket_interval

        self.current_bucket += 1

    def avg_calls_per_second(self, period):
        num_buckets = int(period / self.bucket_interval)
        if num_buckets > len(self.buckets):
            return 0
        if num_buckets > self.num_buckets:
            num_buckets = self.num_buckets
        relevant_buckets = list(self.buckets)[-num_buckets:]
        return round(sum(relevant_buckets) / period)


def event_tracker(
    source: str,
    notify: Callable | Coroutine,
    notify_interval: int = 2
) -> None:
    """Create statistics for the number of times a function is called.

    The callback will receive a dictionary with the following format:

    >>> {
    >>>     "source": "my event source",
    >>>     "count": 54872,
    >>>     "1s_avg": 5,
    >>>     "1min_avg": 10,
    >>>     "5min_avg": 9,
    >>>     "15min_avg": 12,
    >>>     "1h_avg": 11,
    >>>     "24h_avg": 11,
    >>> }

    Parameters
    ----------
    source: str
        The name of the source for the statistics that are collected.

    notify : Callable | Coroutine
        A function or coroutine to send statistics to.

    notify_interval : int, optional
        Call the callback every x seconds, by default 2
    """
    source = source.lower()
    cb = notify
    cb_interval = notify_interval

    def c_gen():
        """Generate never-ending series of integers."""
        c = 0
        while True:
            yield c
            c += 1

    counter = c_gen()

    # ..................................................................................
    def decorator(func):
        number_of_calls = 0  # number of calls
        event_cache = EventCache()
        next_notification = 0  # timestamp of next notification

        def stats():
            """Generate statistics for average calls per second."""
            return {
                "source": source,
                "count": number_of_calls,
                "1s_avg": event_cache.avg_calls_per_second(1),
                "1min_avg": event_cache.avg_calls_per_second(60),
                "5min_avg": event_cache.avg_calls_per_second(300),
                "15min_avg": event_cache.avg_calls_per_second(900),
                "1h_avg": event_cache.avg_calls_per_second(3600),
                "24h_avg": event_cache.avg_calls_per_second(86400),
            }

        # ..............................................................................
        @wraps(func)
        async def wrapper(*args, **kwargs):
            nonlocal number_of_calls, next_notification
            event_cache.add_event()
            number_of_calls = next(counter)

            # is it time to send a notification?
            if time() >= next_notification:
                if asyncio.iscoroutinefunction(cb):
                    await cb(stats())
                else:
                    cb(stats())

                next_notification = time() + cb_interval

            return await func(*args, **kwargs)

        return wrapper

    return decorator


def add_event_tracker(coro: Coroutine, callback: Coroutine | Callable, evt_src: str):
    """Add event tracking to a function.

    Parameters
    ----------
    func : Coroutine
        The function to decorate.

    callback : Coroutine | Callable
        The callback to send statistics to.

    evt_src : str
        The name if the event source to include in the statistics.

    Returns
    -------
    Coroutine
        The decorated function.
    """

    @event_tracker(source=evt_src, notify=callback)
    @wraps(coro)
    async def wrapper(*args, **kwargs):
        return await coro(*args, **kwargs)

    return wrapper


# --------------------------------------------------------------------------------------
# helper functions for testing the decorator
def sync_cb(stats: Any):
    logger.info("sync callback here --> stats: {}".format(stats))


async def async_cb(stats: Any):
    logger.info("async callback here --> stats: {}".format(stats))


# @event_tracker(source="test", notify=async_cb)
async def async_test_fn():
    pass


async def async_main():
    logger.info("Running module test...")
    decorated_test_fn = add_event_tracker(async_test_fn, async_cb, "test")
    while True:
        try:
            await decorated_test_fn()
            sleep(random.random() ** 10)
        except asyncio.CancelledError:
            break


if __name__ == "__main__":
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        pass
