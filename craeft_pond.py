#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 28 13:12:20 2022

Note: For now this module will always assume that we are using the
SPOT or MARGIN markets. But this can easily be changed later on ...

@author dhaneor
"""
import asyncio
import json
import logging
import numpy as np
import os
import sys
import zmq
import zmq.asyncio

from functools import partial
from typing import Callable, Optional, NamedTuple, Coroutine

# --------------------------------------------------------------------------------------
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
# --------------------------------------------------------------------------------------

try:
    from src.data_sources.listener import CandleEvent
except ImportError:
    from data_sources.listener import CandleEvent  # noqa: E402, F401

from .ohlcv_repository import ohlcv_repository  # noqa: E402
from .util.subscriber_tracker import subscriber_tracker  # noqa: E402, F401
from . import zmq_config  # noqa: E402

logger = logging.getLogger("main.craeft_pond")
logger.setLevel(logging.DEBUG)

# get the configuration (a NamedTuple)
config = zmq_config.OhlcvRegistry("kucoin", ["spot"], [])

VERSION = "0.1.0"


# ======================================================================================
class OneTimeSocket:
    """Context manager for a one-time/throw-away socket"""

    def __init__(self, ctx: zmq.asyncio.Context) -> None:
        self.ctx = ctx

    async def __aenter__(self) -> "OneTimeSocket":
        self.socket = self.ctx.socket(zmq.DEALER)
        return self.socket

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.socket.close()


class Container:
    """Container that holds data

    The data can be of any type that can be stored as numpy arrays
    in a dictionary.
    """

    size_limit: int = 1000

    def __init__(
        self,
        topic: str,
        data: dict[str, np.ndarray],
        callback: Optional[Callable[[dict], None]] = None,
    ) -> None:
        """
        A container for storing OHLCV data for a certain lookback period.

        Parameters
        ----------
        topic
            Topic for which this container holds data, for instance:
            "ohlcv_kucoin_spot_btcusd_1m"

        data
            The data to be stored in the container.

        size_limit
            Maximum number of periods to keep in the container.

        callback
            Optional callback function/method to be called after each update of data.
        """
        self.topic: str = topic
        self.data: dict[str, np.ndarray] = data
        self.callback: Coroutine = callback  # called after every update

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        try:
            data_str = f"with {self.__len__()} periods"
        except Exception:
            data_str = "with no data"

        return f"<OHLCVContainer ({self.topic}) {data_str}"

    def __len__(self):
        return len(self.data["open"]) if self.data else 0

    @property
    def type(self) -> str:
        return self.topic.split("_")[0]

    @property
    def exchange(self) -> str:
        return self.topic.split("_")[1]

    @property
    def market(self) -> str:
        return self.topic.split("_")[2]

    @property
    def symbol(self) -> str:
        return self.topic.split("_")[3]

    @property
    def interval(self) -> str:
        return self.topic.split("_")[4]

    # .................................................................................
    async def update(self, update: NamedTuple) -> None:
        """
        Update the OHLCV data based on the incoming update.

        Parameters
        ----------
        update
            The incoming update containing OHLCV data.

        Raises
        ------
        ValueError
            if symbol/interval mismatch is detected.
        ValueError
            if the update type is not recognized.
        """
        # is this even meant to be here?
        if not update["symbol"] == self.symbol:
            raise ValueError(
                "Symbol mismatch: %s != %s" % (update["symbol"], self.symbol)
            )

        if not update["interval"] == self.interval:
            raise ValueError(
                "Interval mismatch: %s != %s" % (update["interval"], self.interval)
            )

        # update data based on update type, updates can be updates
        # for the currently active candle/interval, or signal the
        # start of a new candle/interval.
        if update["type"] == "update":
            # this is an update for the current candle/interval
            for key in self.data:
                self.data[key][-1] = float(update["data"][key])
        elif update["type"] == "add":
            # this is a new candle/interval
            for key in self.data:
                self.data[key] = await self._update_array(
                    self.data[key], update["data"][key]
                )
        else:
            # what is this?
            raise ValueError("Unknown update type: %s", update["type"])

        # call the callback, if it exists
        if self.callback:
            await self.callback(self.data)

    async def get_initial_data(self) -> None:
        """
        Get the initial OHLCV data for this container.
        """
        socket = self.ctx.socket(zmq.DEALER)
        socket.connect(config.OHLCV_REPO_ADDR)

        logger.debug(
            "--> retrieving OHLCV for %s %s (%s)",
            self.exchange,
            self.symbol,
            self.interval,
        )

        params = {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "interval": self.interval,
        }

        await socket.send(json.dumps(params).encode())

        response = await socket.recv_json()

        try:
            prnt = response[0][:2]
        except KeyError:
            logger.error("Unable to split response!")
            prnt = response
            logger.warning(response)

        logger.debug(
            "received response for %s-%s: %s ...",
            self.symbol,
            self.interval,
            prnt,
        )

        socket.close()

        # self.data = data

    async def _update_array(self, arr: np.ndarray, value: float) -> np.ndarray:
        if len(arr) == self.size_limit:
            out = np.empty_like(arr)
            out[:-1] = arr[:-1]
            out[-1] = value
            return out

        else:
            return np.append(arr, value)


# --------------------------------------------------------------------------------------
async def publish(topic: str, data: dict, socket: zmq.asyncio.Socket):
    """
    Publish a message to a ZMQ socket.

    Every Container instance gets this function as callback function
    that is called after every update to the Container data.

    Parameters
    ----------
    topic
        The topic to publish to.

    data
        The data to publish.

    socket
        The socket to publish to (must be aPUB or XPUB socket).
    """
    await socket.send_multipart([topic, json.dumps(data).encode()])
    logger.debug("[%s] sent %s ...", topic, data["close"][:5])


async def topic_to_tuple(topic: str) -> tuple[str, str, str, str, str]:
    """
    Convert a topic to a tuple.

    Parameters
    ----------
    topic
        The topic to convert to a tuple.

    Returns
    -------
    A tuple of the form (type, exchange, market, symbol, interval).
    """
    if not isinstance(topic, str):
        err_msg = "topic must be a string, not %s" % type(topic)
        logger.error(err_msg)
        raise TypeError(err_msg)

    try:
        type_, exchange, market, symbol, interval = topic.split("_")
    except ValueError as e:
        logger.error("invalid topic format: %s -> %ss", topic, e)
        raise ValueError(f"Invalid topic: {topic}")

    return type_, exchange, market, symbol, interval


async def trim_topic(topic: str):
    """
    Trim a topic to the form (symbol, interval).

    Parameters
    ----------
    topic
        The topic to trim.

    Returns
    -------
    str
        example: "BTC/USDT-1m" or "BTC-USDT_1min" of exchange is Kucoin
    """
    _, exchange, _, symbol, interval = await topic_to_tuple(topic)

    if exchange == "kucoin":
        interval = "".join(interval, "in") if interval.endswith("m") else interval
        interval = "".join(interval, "our") if interval.endswith("h") else interval

    return f"{symbol.replace('-', '/')}_{interval}"


async def get_initial_data(topic, ctx, repo_addr) -> None:
    """Get the initial OHLCV data for this container."""
    _, exchange, _, symbol, interval = await topic_to_tuple(topic)

    async with OneTimeSocket(ctx) as socket:
        socket.connect(repo_addr)

        logger.debug("--> retrieving OHLCV for %s %s (%s)", exchange, symbol, interval)

        await socket.send(
            json.dumps(
                {"exchange": exchange, "symbol": symbol, "interval": interval}
            ).encode()
        )

        resp = await socket.recv_json()

        try:
            logger.debug(
                "got response for %s-%s: %s ...", symbol, interval, resp[0][:5]
            )
        except IndexError:
            logger.warning("invalid response: %s -> %s", resp)

    return {
        "open": resp[0],
        "high": resp[1],
        "low": resp[2],
        "close": resp[3],
        "volume": resp[4],
    }


async def build_container(topic: str, get_data_fn: Coroutine) -> Container:
    return Container(topic, await get_data_fn(topic))


# --------------------------------------------------------------------------------------
async def craeft_pond(ctx: Optional[zmq.asyncio.Context] = None, byor: bool = False):
    """Updates Container instances with incoming data.

    This is the main function - Run only this!

    Parameters
    ----------
    ctx
        The ZMQ (asnycio) context, optional but necessary if you
        want run this together with other components in one thread
        or process.

    byor
        Bring Your Own Repo - Create a new one in the same process
        (as opposed to, it is runnning somewhere else already).
    """
    context = ctx or zmq.asyncio.Context()

    # SUB subcriber socket (connection to collector)
    subscriber = context.socket(zmq.SUB)
    subscriber.connect(config.COLLECTOR_PUB_ADDR)
    subscriber.subscribe(b"")

    # XPUB socket (connection to clients/strategies)
    publisher = context.socket(zmq.XPUB)
    publisher.bind(config.PUBLISHER_ADDR)

    poller = zmq.asyncio.Poller()
    poller.register(subscriber, zmq.POLLIN)
    poller.register(publisher, zmq.POLLIN)

    # start the ohlcv repository (task) if required
    if byor:
        repo_task = asyncio.create_task(
            ohlcv_repository(ctx, config.REPO_ADDR)
        )

    # initialize variables for the main loop
    get_initial_data_fn = partial(
        get_initial_data, ctx=context, repo_addr=config.REPO_ADDR
    )

    subscribers, containers, counter = {}, {}, 0

    logger.info("starting crǽft pond ...")

    while True:
        try:
            events = dict(await poller.poll(1000))

            # got new data from collector
            if subscriber in events:
                logger.debug("update @ subscriber ...")
                update = await subscriber.recv_multipart()

                topic, data = update[0].decode(), json.loads(update[1].decode())

                # poison pill?
                if topic == "stop_it":
                    raise asyncio.CancelledError("gotta go! \o/")

                # update & publish, if we have a container for this topic
                try:
                    await containers[topic].update(data)
                except KeyError as e:
                    logger.error("unknown topic: %s --> %s", topic, e)
                except ValueError as e:
                    logger.error(e)
                else:
                    await publish(topic.encode(), containers[topic].data, publisher)
                finally:
                    counter += 1

            # check for subscribe/unsubscribe events
            if publisher in events:
                msg = await publisher.recv()
                logger.debug(f"[{counter:,.0f}] recv msg @ publisher port: {msg}")
                subscribe, topic = msg[0], msg[1:].decode("utf-8")

                if subscribe:
                    logger.debug("subscribe @ publisher: %s for %s", subscribe, topic)

                    if topic in containers:
                        subscribers[topic] += 1
                    else:
                        containers[topic] = await build_container(
                            topic, get_initial_data_fn
                        )
                        subscribers[topic] = 1

                        logger.debug("created container for topic: %s", topic)

                        # topic = await trim_topic(topic)
                        # logger.debug("trimmed topic: %s", topic)
                        subscriber.subscribe(topic)
                else:
                    logger.debug("unsubscribe @ publisher: %s for %s", subscribe, topic)
                    if topic in containers and subscribers[topic] > 1:
                        subscribers[topic] -= 1
                    else:
                        del containers[topic]
                        del subscribers[topic]

        except asyncio.CancelledError as e:
            logger.info("crǽft pond got cancelled: %s", e if e else "it's over")
            await asyncio.sleep(1)
            break
        except Exception as e:
            logger.exception(e)
            break

    if byor:
        repo_task.cancel()
        while not (repo_task.cancelled() or repo_task.done()):
            await asyncio.sleep(0.1)  # wait a bit to let the repo_task finish

    publisher.close(1)
    subscriber.close(1)

    logger.info("crǽft pond stopped: OK")


"""
Some thoughts about the distribution mechanism for newly arriving data. This
is from a conversation with ChatGPT and may become interesting later. For now
we create a new asyncio task for each update to prevent blocking the main loop
in the ohlcv_containers function. This is not a problem for now, but may become
important in the future. The alternative would be to implement a queue for sending
the updates to the containers, but this would obviously be more complicsated.

ChatGPT:

You're right in identifying a potential concern. While using a queue decouples data
ingestion from processing and provides a buffer, overwhelming the asyncio event loop
with too many tasks can still be a concern. Here are some challenges and potential
solutions:

1. **Too Many Concurrent Tasks:** If you're creating a new task for every piece of
    data, and the processing of that data is not keeping pace with its arrival, you'll
    accumulate a large number of concurrent tasks. This can lead to increased memory
    usage and decreased performance.

   **Solution:** Introduce a worker pattern. Instead of creating a new task for every
   item, have a fixed number of worker tasks that continuously process items from the
   queue. This limits the number of concurrently executing tasks.

2. **Starvation:** If one type of task is much slower and you're processing all tasks
    as they come in, faster tasks might get starved and have to wait.

   **Solution:** Use priority queues or multiple queues. For example, you can
    categorize tasks as high, medium, or low priority and process them accordingly.

3. **Unbounded Queue Growth:** If data is coming in faster than it's being processed,
    the queue can grow indefinitely, leading to increased memory usage.

   **Solution:** Implement a back-pressure mechanism. If the queue reaches a certain
   size, you can slow down or pause data ingestion. Alternatively, you can discard old
   data if it's not critical to process every item.

4. **Task Cancellation & Exception Handling:** If a handler encounters an error, it
    shouldn't crash the entire system.

   **Solution:** Within each worker or handler task, include proper exception handling.
   Additionally, consider implementing graceful shutdown procedures to handle tasks
   during shutdowns or restarts.

5. **Monitoring & Alerting:** You should be aware of how the system is performing and
if it's getting overwhelmed.

   **Solution:** Implement monitoring to keep track of metrics like queue size, task
   completion times, and error rates. Set up alerts for unusual patterns or thresholds.

To decide which approach and mitigations to adopt, consider the following:

- **Rate of Data Arrival vs. Processing:** If the processing of data is significantly
slower than its arrival, consider implementing more robust measures.
- **Criticality of Data:** If it's essential to process every piece of data, ensure
you have mechanisms to prevent data loss.
- **System Resources:** If you're running on constrained resources, be mindful of
the number of concurrent tasks and the size of the queue.

Remember, the key is to balance between system complexity and the requirements of
your trading bot. It's beneficial to start simple and then introduce additional
measures as you observe how the system behaves under different conditions.

To decide which approach and mitigations to adopt, consider the following:

Rate of Data Arrival vs. Processing:
    If the processing of data is significantly
    slower than its arrival, consider implementing more robust measures.

Criticality of Data:
    If it's essential to process every piece of data, ensure you have mechanisms
    to prevent data loss.

System Resources:
    If you're running on constrained resources, be mindful of the number of
    concurrent tasks and the size of the queue.

Remember, the key is to balance between system complexity and the requirements
of your trading bot. It's beneficial to start simple and then introduce additional
measures as you observe how the system behaves under different conditions.
"""
