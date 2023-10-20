#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides a wrapper for the websocket client for the Kucoin API.

There are two implemtations for the public API endpoints. The old
one is one class for all kinds of subjects (tickers, candles, etc.).

The new implementation splits these up into separate classes. This
allows to use them as components in in an Exchange class that I
introduced after switching to CCXT. CCXT however seems to use more
resources for WS streaming. The Exchange class makes it posssible to
mix the general WS client from CCXT with specialized implementations,
like the ones below.

The WebsocketBase class is used to derive specialized implementations
for all stream type, like Tickers, Candles, etc.
It use a rate limiter to respect the Kucoin API limits, just in case
that clients are sending a lot of requests in short order.

This module uses the websocket client implementation from the
kucoin-python library/SDK.

These are the Kucoin limits for websocket connections:

Number of connections per user ID: ≤ 50
Connection Limit: 30 per minute
Message limit sent to the server: 100 per 10 seconds
Maximum number of batch subscriptions at a time: 100 topics
Subscription limit for each connection: 300 topics

NOTE:   KuCoin API server is located in:
        AWS Tokyo zone A (subnet ap-northeast-1a apne1-az4).  az4

Created on Sun Nov 28 13:12:20 2022

@author dhaneor
"""
import time
import asyncio
import logging
from typing import Callable, Optional, Coroutine
from uuid import uuid4

from kucoin.client import WsToken
from ..kucoin.kucoin.ws_client import KucoinWsClient

from data_sources.websocket.i_websockets import (  # noqa: E402, F401
    IWebsocketPublic,
    IWebsocketPrivate,
    ITrades,
    IOhlcv,
    IOrderBook,
    ITicker,
    IAllTickers,
    ISnapshots,
    IAllSnapshots,
)
from .publishers import (  # noqa: E402, F401
    IPublisher,
    LogPublisher,
    ZeroMqPublisher,
    PrintPublisher,
)
from data_sources.util.random_names import random_celtic_name
from data_sources.util.rate_limiter import async_rate_limiter as rate_limiter
from data_sources.util.subscription_request import SubscriptionRequest  # noqa: F401

logger = logging.getLogger("main.websocket")

TICKERS_ENDPOINT = "/market/ticker"
CANDLES_ENDPOINT = "/market/candles"
SNAPSHOTS_ENDPOINT = "/market/snapshot"
TRADES_ENDPOINT = "/market/match"
BOOK_ENDPOINT = "/market/level2"

MAX_CONNECTIONS_PER_USER = 50
CONNECTION_LIMIT = 30  # per minute
MSG_LIMIT = 100  # 100 per 10 seconds
MSG_LIMIT_LOOKBACK = 10  # seconds
MAX_BATCH_SUBSCRIPTIONS = 5  # 100 topics
MAX_TOPICS_PER_CONNECTION = 300  # 300 topics


# ======================================================================================
#                            WS CLIENTS PUBLIC API (NEW)                               #
# ======================================================================================
class Subscribers:
    """Helper class to keep track of the number of subscribers for each topic."""

    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger

        self._topics: dict[str, int] = {}
        self.warnings: int = 0

    def __repr__(self) -> str:
        return self._topics.__repr__()

    def __len__(self) -> int:
        return len(self._topics)

    def __contains__(self, topic) -> bool:
        return topic in self._topics

    def __getitem__(self, topic: str) -> int:
        return self._topics.get(topic, None)

    def __delitem__(self, topic: str) -> None:
        del self._topics[topic]

    @property
    def topics(self) -> dict[str, int]:
        return self._topics

    @property
    def topics_as_list(self) -> list[str]:
        return list(self._topics.keys())

    # ..................................................................................
    def add_subscriber(self, topic: str) -> None:
        if topic not in self._topics:
            self._topics[topic] = 1
            self.logger.info(
                "... new topic: %s (now: %s subscribers)",
                topic,
                self._topics.get(topic, 0),
            )
            return topic

        else:
            self._topics[topic] += 1
            self.logger.info(
                "... adding subscriber to topic: %s (now: %s subscribers)",
                topic,
                self._topics.get(topic, 0),
            )
            return None

    def remove_subscriber(self, topic: str) -> int:
        if topic in self._topics:  # and (subs := self._topics.get(topic, 0)) > 1:
            self._topics[topic] -= 1
            self.logger.info(
                "... decreased subscribers for: %s (now: %s subscribers)",
                topic,
                self._topics[topic],
            )
            return self._topics.get(topic, 0)

        return 0

    def get_subscriber_count(self, topic: str) -> int:
        return self._topics.get(topic, 0)

    async def clear_subscribers(self) -> None:
        self._topics = {k: v for k, v in self._topics.items() if v > 0}


class Connection:
    """Helper class that manages one WS connection (= one WS client)."""

    def __init__(self, publish: Coroutine, endpoint: str, debug: bool = False):
        """Initializes a new Connection instance.

        If the debug mode is on, no real connections will be made.

        Parameters
        ----------
        publish : Coroutine
            publisher coroutine = where to send the messages to
        endpoint : str
            which websocket endpoint to use for the connection
        debug : bool, optional
            debug mode yes/no, by default False
        """
        self.publish: Coroutine = publish
        self.endpoint: str = endpoint
        self.debug: bool = debug

        self.name: str = random_celtic_name()  # name for humans
        self._id: str = str(uuid4())  # unique id

        self.logger = logging.getLogger("main.websocket." + self.name)

        self._topics: Subscribers = Subscribers(self.logger)  # topic registry
        self._pending: set = set()  # pending topics

        self.warnings: int = 0  # number of warning messages
        self.errors: int = 0  # number of errors

        self.client: Optional[KucoinWsClient] = None

        self.logger.info("   connection %s created: %s", self.name, self._id)

        if self.debug:
            self.logger.setLevel(logging.DEBUG)

            self.logger.warning(
                "   <<<--- RUNNING IN DEBUG MODE. "
                "NO REAL CONNECTIONS WILL BE MADE! --->>>"
            )

        else:
            self.logger.setLevel(logging.INFO)

    async def topic_exists(self, topic: str) -> bool:
        return topic in self._topics

    @property
    def topics(self) -> list[str]:
        return self._topics.topics_as_list

    @property
    def no_of_topics(self) -> int:
        return len(self._topics) + len(self._pending)

    @property
    def pending_topics(self) -> int:
        return self._pending

    @property
    def topic_limit_reached(self) -> bool:
        return self.no_of_topics >= MAX_TOPICS_PER_CONNECTION

    @property
    def max_topics_left(self) -> int:
        return max(-1, (MAX_TOPICS_PER_CONNECTION - self.no_of_topics))

    # ..................................................................................
    async def watch(self, topics: list[str]) -> list | None:
        """Watch one or more topics from the websocket strean.

        Parameters
        ----------
        topics : list[str]
            a list of topics to watch

        Returns
        -------
        list | None
            a list of topics that could not be accepted, because the
            maximum number of topics for one websocket connection
            would have been exceeded. Otherwise returns None.
        """
        # return immediately if we have no capacity left
        if self.topic_limit_reached:
            return topics

        # turn the list of topics into a list of topic strings,
        # as we can send batch requests up to the allowed limit.
        topic_strings, too_much = self._prep_topic_str(topics)

        # add topics to 'pending' to prevent new assignments.
        # This is meant to prevent new assigments by the parent class
        # while pending requests are being processed
        self._pending = set(topics) - set(too_much)

        self.logger.debug(
            "going to add topics: %s --- too much: %s (increases topics to: %s)",
            topic_strings,
            too_much,
            self.no_of_topics,
        )

        # increase subscriber number right away (also to prevent new assignments)
        for topic in self._pending:
            self._topics.add_subscriber(topic)

        # start client if necessary
        if not self.client and not self.debug:
            self.logger.info("launching websocket client ...")
            self.client = await self._start_client()

        # subscribe with endpoint
        for topics in topic_strings:
            await self.subscribe(topics)

        return too_much or None

    async def unwatch(self, topics: list[str]) -> list | None:
        # turn the list of topics into a list of topic strings,
        # as we can send batch requests up to the allowed limit.
        topic_strings = await self._prep_unsub_str(topics)

        self.logger.debug("%s ... removing topics: %s", self.name, topic_strings)

        # unsubscribe for each topic string
        for ts in topic_strings:
            await self.unsubscribe(ts)

    # ..................................................................................
    @rate_limiter(
        max_rate=MSG_LIMIT, time_window=MSG_LIMIT_LOOKBACK, send_immediately=True
    )
    async def subscribe(self, topic: str) -> None:
        """Subscribes to a topic.

        Parameters
        ----------
        topic : str
            one or more topic(s) as string, comma separated if multiple
        """
        try:
            # only do a dry-run if debug flag is set
            if not self.debug:
                await self.client.subscribe(f"{self.endpoint}:{topic}")

        except Exception as e:
            self.logger.error(
                "unexpected error while subscribing to topic: %s", e, exc_info=1
            )
            for topic in self._pending:
                del self._topics[topic]
        else:
            self.logger.info("%s: subscribed to topic: %s", self.name, topic)

            for topic in topic.split(",") if "," in topic else [topic]:
                # self._topics.add_subscriber(topic)
                self._pending.remove(topic)

    @rate_limiter(
        max_rate=MSG_LIMIT, time_window=MSG_LIMIT_LOOKBACK, send_immediately=True
    )
    async def unsubscribe(self, topic: str) -> None:
        """Unsubscribes from a topic.

        Parameters
        ----------
        topic : str
            one or more topic(s) as string, comma separated if multiple
        """
        try:
            if not self.debug:
                await self.client.unsubscribe(f"{self.endpoint}:{topic}")
        except ValueError:
            pass
        except Exception as e:
            self.logger.error(
                "%s: unexpected error while unsubscribing from topic: %s",
                self.name,
                e,
                exc_info=1,
            )
        else:
            self.logger.info("%s: unsubscribed from topic: %s", self.name, topic)

            for topic in topic.split(",") if "," in topic else [topic]:
                if topic in self._topics:
                    del self._topics[topic]

    async def add_subscribers_to_existing(self, topics: list[str]) -> list[str]:
        """Add subscribers to existing topics."""
        for topic in tuple(topics):
            if await self.topic_exists(topic):
                self.logger.debug(
                    "... increasing subscriber count for topic: %s", topic
                )
                self._topics.add_subscriber(topic)
                topics.remove(topic)

        return topics

    async def remove_subscribers_from_existing(self, topics: list[str]) -> list[str]:
        """Remove subscribers from existing topics."""
        if not len(self._topics):
            return topics

        for topic in tuple(topics):
            if topic in self._topics:
                self.logger.debug(self._topics)
                self.logger.debug(
                    "   %s --> decreasing subscriber count for topic: %s (was: %s)",
                    self.name.upper(),
                    topic,
                    self._topics.get_subscriber_count(topic),
                )

                # decrease subscriber count for the topic & remove it from
                # the unwatch list, if we still have subscribers left for
                # the topic
                if self._topics.remove_subscriber(topic):
                    topics.remove(topic)

        return topics

    async def remove_dead_topics(self) -> None:
        await self._topics.clear_subscribers()

    async def get_subscriber_count(self, topic: str) -> int:
        return self._topics.get_subscriber_count(topic)

    async def close(self) -> None:
        """Closes the connection"""
        # unwatch all topics & stop client
        await self.unwatch(self._topics.topics_as_list)
        await self._stop_client()
        self.logger.info(
            "%s: connection closed (%s warnings)",
            self.name,
            self.warnings + self._topics.warnings,
        )

    # ..................................................................................
    async def _start_client(self) -> KucoinWsClient:
        """Start a websocket client for Kucoin."""
        try:
            client = await KucoinWsClient.create(
                loop=asyncio.get_running_loop(),
                client=WsToken(),
                callback=self.publish,
                private=False,
            )
        except Exception as e:
            logger.error("unexpected error while creating client: %s", e, exc_info=1)
        else:
            logger.info(
                "KUCOIN PUBLIC websocket client started ...",
            )
            return client

    async def _stop_client(self) -> None:
        if self.client:
            del self.client

    def _prep_topic_str(self, topics: list[str] | str) -> tuple[list[str], list[str]]:
        """Prepares the topic string for use in sub/unsub message

        We must consider/respect the limits given by Kucoin and
        divide huge batch requests into chunks.

        Parameters
        ----------
        topics : list[str] | str
            a list of topics or a single topic

        Returns
        -------
        tuple[list[str], list[str]]
            the following two lists are returned:
            -> A list of strings (concatenated if necessary). Each
            string will contain a comma separated list of topics,
            up to the maximum number of topics allowed for batch
            subscriptions.
            -> A list of strings for topics that cannot subscribed
            to, because one of the API limits would be exceeded.

        Raises
        ------
        TypeError
            if topics is not a list or a string
        """
        # for tickers and snapshots, we can subscribe to all topics,
        # this will be assumed if topics are not specified
        if not topics or topics == "all":
            return [], ["all"], []

        if isinstance(topics, str):
            topics = [topics]

        if isinstance(topics, list):
            sub_strings, max_topics_left = [], self.max_topics_left

            while max_topics_left > 0 and topics:
                max_topics = min(max_topics_left, MAX_BATCH_SUBSCRIPTIONS)
                # self.logger.debug("...%s has %s topics left", self.name, max_topics)
                sub_strings.append(",".join(topics[:max_topics]))
                topics = topics[max_topics:]
                max_topics_left -= max_topics

            return sub_strings, topics

        raise TypeError(
            f"<topics> parameter must be str or list[str]" f" but was {type(topics)}"
        )

    async def _prep_unsub_str(self, topics: list[str]) -> list[str]:
        n = MAX_BATCH_SUBSCRIPTIONS
        topics = [topics[i: i + n] for i in range(0, len(topics), n)]
        topics = [",".join(t) for t in topics]
        return topics

    # ..................................................................................
    def log_topics(self) -> None:
        logger.info("%s: topics: %s", self.name, self.topics)


# --------------------------------------------------------------------------------------
class WebsocketBase(IWebsocketPublic):
    """Base class for websocket clients for the Kucoin API"""

    publisher: IPublisher = PrintPublisher()

    def __init__(
        self,
        publisher: Optional[IPublisher] = None,
        callback: Optional[Callable] = None,
        cycle_interval: Optional[int] = 0,
        debug: Optional[bool] = True,
    ):
        """Initialize a KucoinWebsocketPublic.

        Note: If both publisher and callback are not specified, the received
        messages will just be printed to the console.

        Parameters
        ----------
        publisher : Optional[IPublisher]
            A class that publishes the updates/messages, by default None

        callback : Optional[Callable]
            Alternative/additional function for publisher, by default None

        cycle_interval : int
            How often should the oldest connection be replaced, by default None.
            This should not be done too frequently, so maybe values between
            3600 and 86400 are sensible.

        debug : Optional[bool]
            If set to True, the actual WS client will not be started, by default False.
        """
        # set the callable to be used for publishing the results
        # callback parameter takes precedence
        self.publisher = publisher or KucoinWebsocketPublic.publisher
        self.publish = callback or self.publisher.publish
        self.endpoint = "/this/is/not/an/endpoint"  # must be replaced by subclasses

        self.cycle_interval: int = cycle_interval  # interval in seconds
        self.switch_in_progress: bool = False  # switch in progress yes/no
        self.debug = debug  # debug mode yes/no

        self.warnings: int = 0  # number of warnings

        logger_name = f"main.{__class__.__name__}"
        self.logger = logging.getLogger(logger_name)

        self.logger.info(
            f"kucoin public websocket initialized ..."
            f"with publisher {self.publisher}"
        )

        self._connections: list[Connection] = []  # active websocket connections

        if self.debug:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

    @property
    def topics(self) -> dict[str, int]:
        """Returns all topics from all active connections"""
        topics = {}

        for conn in self._connections:
            topics = {**topics, **conn._topics._topics}

        return topics

    # ..................................................................................
    async def run(self) -> None:
        """Automatically switches websocket connections, if enabled.

        Run this as a task, or use the class instance as a context manager
        to run this coroutine in the background! But you can use the class
        without this as well. To actually do something useful shere, the
        'cycle_interval' parameter must be set to a positive integer.
        """
        self.logger.info("starting background task ...")
        while True:
            try:
                await asyncio.sleep(self.cycle_interval)
                if self.cycle_interval:
                    await self.switch_connections()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(
                    "%s: unexpected error while running: %s",
                    self.name,
                    e,
                    exc_info=1,
                )
                self.warnings += 1
                if self.warnings > 100:
                    self.logger.critical(
                        "%s: too many warnings, disconnecting...",
                        self.name,
                    )
                    await self.close()

        self.logger.info("background task stoppped: OK")

    async def watch(self, topics: str | list[str]) -> None:
        """Watch one or more topics with a websocket connection.

        Parameters
        ----------
        topics : str | list[str]
            one or more topic(s) as string, a list for multiple
        """
        # wait if a connection switch is in progress ...
        while self.switch_in_progress:
            await asyncio.sleep(0.1)

        self.logger.debug("•~-*-~•" * 15)
        # self.logger.debug("WebsocketBase.watch() called ...")

        # make a list, if we got a string
        topics = [topics] if isinstance(topics, str) else topics

        # filter out topics that already exist
        if not (topics := await self.handle_existing_topics(topics, "subscribe")):
            return

        # get a connection that we can use
        connection = await self._get_connection()

        # try to subscribe to the given topics with the given connection
        # this may return a list of topics, that could not be handled by
        # the connection ... if so, call yourself again recursively
        if rest := await connection.watch(topics):
            self.logger.debug(
                "connection %s returned topics --> %s", connection.name, rest
            )
            await self.watch(rest)

    async def unwatch(self, topics: str | list[str]) -> None:
        """Unwatch one or more topics with a websocket connection.

        Parameters
        ----------
        topics : str | list[str]
            one or more topic(s) as string, a list for multiple
        """
        # wait if a connection switch is in progress ...
        while self.switch_in_progress:
            await asyncio.sleep(0.1)

        self.logger.debug("•~-*-~•" * 15)
        self.logger.debug("WebsocketBase.unwatch() called ...")
        self.logger.debug("unwatch request for topics: %s", topics)

        # make a list, if we got a string & remove duplicates
        topics = [topics] if isinstance(topics, str) else topics

        # filter out topics that we need to keep, because there
        # are other subscribers & return immediately if there are
        # other subscribers for all topics in the provided list
        if not (topics := await self.handle_existing_topics(topics, "unsubscribe")):
            self.logger.debug("... no topics left to unsubscribe")
            [await conn.remove_dead_topics() for conn in self._connections]
            return

        self.logger.debug("unwatch topics after filtering: %s", topics)

        # tell each connection that has the topic(s) to unwatch them
        # usually that should be only one of them, but you never know
        # for sure ...
        for conn in self._connections:
            if to_remove := [t for t in topics if t in conn._topics]:
                self.logger.debug("%s --> %s", conn.name, to_remove)
                await conn.unwatch(to_remove)

    async def handle_existing_topics(self, topics: list[str], action: str) -> list[str]:
        """Filter out topics that have other subscribers.

        This will just increase the subscriber count for the topics,
        instead of sending a subscription request to the WS client.

        Parameters
        ----------
        topics : list[str]
            list of topics
        action : str
            "subscribe"/"unsubscribe"

        Returns
        -------
        list[str]
            filtered list of topics, all existing removed for "subscribe",
            all that we need to keep removed for "unsubscribe" action

        Raises
        ------
        ValueError
            if action is not'subscribe' or 'unsubscribe'
        """
        for conn in self._connections:
            if action == "subscribe":
                topics = await conn.add_subscribers_to_existing(topics)
            elif action == "unsubscribe":
                topics = await conn.remove_subscribers_from_existing(topics)
            else:
                raise ValueError(f"Unexpected action: {action}")

        return topics

    async def remove_multiple_subs(self) -> None:
        self.logger.debug(self.topics)

        for t in self.topics:
            has_topic = [conn for conn in self._connections if t in conn._topics]
            keep, inc = has_topic.pop(0), 0

            if has_topic:
                for conn in has_topic:
                    inc += (dec := await conn.get_subscriber_count(t))
                    await conn.unwatch([t] * dec)

                await keep.add_subscribers_to_existing([t] * inc)

    # .................................................................................
    async def switch_connections(self) -> None:
        """Switch topics to a new connection.

        This will take all topics from the oldest connection, create a
        new one, and move all topics to the new connection. Doing this
        is meant to guard against broken WS connections, which happened
        to me when running the bot for a long time. The interval can be
        configured and the behaviour enabled or disabled.
        """
        self.logger.info("==========> CONNECTION SWITCH INITIATED <==========")
        self.switch_in_progress = True
        await asyncio.sleep(1)

        if self.debug:
            [conn.log_topics() for conn in self._connections]

        if not self._connections:
            self.logger.info("no connections to switch... returning")
            return
        elif len(self._connections) == 1:
            self.logger.info("only one connection to switch, will create a new one")
            await self._create_connection()

        src = self._connections[0]
        dst = self._connections[-1]
        src_topics = list(src.topics)
        src_name = src.name
        all_topics_before = self.topics

        await self.move_topics(src=src, dst=dst, topics=src_topics)

        success = all_topics_before == self.topics

        logger.info(
            "all topics from %s moved to new connection: %s",
            src_name,
            success
        )

        if not success:
            raise asyncio.CancelledError("connection switch failed")

        self.switch_in_progress = False

        if self.debug:
            [conn.log_topics() for conn in self._connections]

        self.logger.info("==========> CONNECTION SWITCH COMPLETED <==========")

    async def move_topics(
        self,
        src: Connection,
        dst: Optional[Connection] = None,
        topics: Optional[list[str]] = None,
    ) -> None:
        """Move topics from a source to a destination connection."""
        dst = dst or self._connections[-1]
        dst._pending = src._pending
        topics_to_move = topics or src.topics
        topics_done, subscriber_count = [], {}

        # move all topics to the destination
        while dst.max_topics_left > 0 and topics_to_move:

            next_batch = []
            max_batch = min(
                dst.max_topics_left, MAX_BATCH_SUBSCRIPTIONS, len(topics_to_move)
            )

            for _ in range(max_batch):
                topic = topics_to_move.pop(0)
                subscriber_count[topic] = await src.get_subscriber_count(topic)

                logger.info(
                    "moving %s (%s subscribers)", topic, subscriber_count[topic]
                )

                next_batch.append(topic)
                del src._topics._topics[topic]

            # move the topic(s) to the new connection
            await dst.watch(next_batch)

            topics_done += next_batch

        # remove all topics from the source
        topics_done = [
            topics_done[i: i + MAX_BATCH_SUBSCRIPTIONS]
            for i in range(0, len(topics_done), MAX_BATCH_SUBSCRIPTIONS)
        ]

        for sub_list in topics_done:
            t_str = ",".join(sub_list)

            try:
                if not self.debug:
                    await src.client.unsubscribe(f"{src.endpoint}:{t_str}")
                else:
                    self.logger.debug("simulating unsubscribe for %s", t_str)
            except Exception as e:
                logger.error(e, exc_info=1)

        # update the subscriber count for all topics in the destination
        for topic, subs in subscriber_count.items():
            dst._topics._topics[topic] = subs
            logger.info("increased subscriber count for %s, now: %s", topic, subs)

        # if there are topics left to move, create a new connection
        # and call this coroutine again
        if topics_to_move:
            logger.debug("some topics left to move: %s", topics)
            await self._create_connection()
            new_dst = self._connections[-1]
            logger.debug(
                "created new connection: %s (%s)", new_dst.name, len(new_dst.topics)
            )
            await self.move_topics(src=src, dst=new_dst, topics=topics_to_move)
        else:
            self._connections.remove(src)

    async def close(self) -> None:
        """Shutdown."""
        self.logger.info("•~-*-~•" * 15)

        [await conn.close() for conn in self._connections]

        self.logger.info(
            "WebsocketBase:  SHUTDWON COMPLETE (%s warnings)", self.warnings
        )

    # ..................................................................................
    async def _get_connection(self) -> Connection:
        if not self._connections:
            await self._create_connection()

        for connection in self._connections:
            self.logger.debug(
                "--> checking connection %s ::::: (%s topics - %s)::::: "
                "limit reached? :: %s",
                connection.name.upper(),
                connection.no_of_topics,
                len(connection.pending_topics),
                connection.topic_limit_reached,
            )

            if not connection.topic_limit_reached:  # and (not connection.busy):
                self.logger.debug("will use connection %s", connection.name)
                return connection

        await self._create_connection()
        return await self._get_connection()

    async def _create_connection(self) -> Connection:
        self.logger.debug("Creating new connection ...")

        # create new connection
        connection = Connection(self.publish, self.endpoint, self.debug)

        # start WS client, if we´re not in debug mode
        if not self.debug:
            connection.client = await connection._start_client()
            await asyncio.sleep(2)  # wait for the connection to be ready

        # append to list of active connections
        self._connections.append(connection)


class WsTickers(WebsocketBase):
    def __init__(
        self,
        publisher: Optional[IPublisher] = None,
        callback: Optional[Callable] = None,
        cycle_interval: Optional[int] = 0,
        debug: Optional[bool] = False,
    ):
        super().__init__(
            publisher=publisher,
            callback=callback,
            cycle_interval=cycle_interval,
            debug=debug,
        )

        self.endpoint = TICKERS_ENDPOINT


class WsTrades(WebsocketBase):
    def __init__(
        self,
        publisher: Optional[IPublisher] = None,
        callback: Optional[Callable] = None,
        cycle_interval: Optional[int] = 0,
        debug: Optional[bool] = False,
    ):
        super().__init__(
            publisher=publisher,
            callback=callback,
            cycle_interval=cycle_interval,
            debug=debug,
        )

        self.endpoint = TRADES_ENDPOINT


class WsCandles(WebsocketBase):
    def __init__(
        self,
        publisher: Optional[IPublisher] = None,
        callback: Optional[Callable] = None,
        cycle_interval: Optional[int] = 0,
        debug: Optional[bool] = False,
    ) -> None:
        super().__init__(
            publisher=publisher,
            callback=callback,
            cycle_interval=cycle_interval,
            debug=debug,
        )

        self.endpoint = CANDLES_ENDPOINT


class WsSnapshots(WebsocketBase):

    def __init__(
        self,
        publisher: Optional[IPublisher] = None,
        callback: Optional[Callable] = None,
        cycle_interval: Optional[int] = 0,
        debug: Optional[bool] = False,
    ) -> None:
        super().__init__(
            publisher=publisher,
            callback=callback,
            cycle_interval=cycle_interval,
            debug=debug,
        )

        self.endpoint = SNAPSHOTS_ENDPOINT


class WsBook(WebsocketBase):

    def __init__(
        self,
        publisher: Optional[IPublisher] = None,
        callback: Optional[Callable] = None,
        cycle_interval: Optional[int] = 0,
        debug: Optional[bool] = False,
    ) -> None:

        super().__init__(
            publisher=publisher,
            callback=callback,
            cycle_interval=cycle_interval,
            debug=debug,
        )

        self.endpoint = BOOK_ENDPOINT


# ======================================================================================
#                               WS CLIENT PUBLIC API (OLD)                             #
# ======================================================================================
class KucoinWebsocketPublic:
    """Provides a websocket connection for the Kucoin API.

    This is the depraceted version which is replaced by classes below
    that are now split into separate classes for tickers, OHLCV, etc.
    This makes it possible to have more topics and still stay within
    the limits of the Kucoin API (see module docstring).
    """

    publisher: IPublisher = PrintPublisher()

    def __init__(
        self,
        publisher: Optional[IPublisher] = None,
        callback: Optional[Callable] = None,
        id: Optional[str] = None,
    ):
        """Initialize a KucoinWebsocketPublic.

        Parameters
        ----------
        publisher : Optional[IPublisher], optional
            A class tht publishes the updates/messages, by default None
        callback : Optional[Callable], optional
            Alternative/additional function for publisher, by default None
        id : Optional[str], optional
            identifier for this instance, by default None
        """
        # set the callable to be used for publishing the results
        self.publisher = publisher or KucoinWebsocketPublic.publisher
        self.publish = callback or self.publisher.publish

        logger_name = f"main.{__class__.__name__}"
        self.logger = logging.getLogger(logger_name)

        self.logger.info(
            f"kucoin public websocket initialized ..."
            f"with publisher {self.publisher}"
        )

        self.id = id

    async def run(self):
        try:
            await self.start_client()
        except asyncio.CancelledError:
            pass

    async def start_client(self):
        # is public
        # client = WsToken()
        # is private
        # client = WsToken(key='', secret='', passphrase='', is_sandbox=False, url='')
        # is sandbox
        # client = WsToken(is_sandbox=True)
        client = WsToken()

        try:
            loop = asyncio.get_running_loop()
        except Exception:
            self.logger.error("unable to get running loop")
            loop = None

        try:
            self.ws_client = await KucoinWsClient.create(
                loop, client, self._handle_message, private=False
            )
            self.logger.info("kucoin public websocket client started ...")
        except asyncio.CancelledError:
            self.logger.info("websocket client stopped")

    async def watch_ticker(self, symbols: str | list[str] | None = None):
        if not self.ws_client:
            await self.start_client()

        symbols, symbols_str = await self._transform_symbols_parameter(symbols)

        if self.ws_client:
            try:
                await self.ws_client.subscribe(f"{TICKERS_ENDPOINT}:{symbols_str}")
                if symbols:
                    [
                        self.logger.info(f"subscribed to ticker stream for: {s} ")
                        for s in symbols
                    ]
            except Exception as e:
                self.logger.exception(e)
        else:
            raise Exception("watch ticker failed: missing client")

    async def unwatch_ticker(self, symbols: str | list[str] | None = None):
        symbols, symbols_str = await self._transform_symbols_parameter(symbols)

        if self.ws_client:
            try:
                await self.ws_client.unsubscribe(f"{TICKERS_ENDPOINT}:{symbols_str}")
                if symbols:
                    [
                        self.logger.info(f"unsubscribed from ticker stream for: {s}")
                        for s in symbols
                    ]
            except ValueError:
                self.logger.error(
                    "someone tried to unsubscribe from a topic where we "
                    "had no subscription anyway"
                )
            except Exception as e:
                self.logger.exception(e)

    async def watch_candles(self, symbols: str | list[str], interval: str):
        if not self.ws_client:
            await self.start_client()

        symbols, _ = await self._transform_symbols_parameter(symbols)

        try:
            [
                await self.ws_client.subscribe(
                    f"{CANDLES_ENDPOINT}:{symbol}_{interval}"
                )
                for symbol in symbols
                if self.ws_client
            ]
            [
                self.logger.info(f"subscribed to kline stream for: {s}_{interval} ")
                for s in symbols
            ]
        except Exception as e:
            self.logger.exception(e)

    async def unwatch_candles(self, symbols: str | list[str], interval: str):
        if not self.ws_client:
            self.logger.error(
                "unwatch candles not possible as we have no subscriptions"
            )
            return

        symbols, _ = await self._transform_symbols_parameter(symbols)

        try:
            [
                await self.ws_client.unsubscribe(
                    f"{CANDLES_ENDPOINT}:{symbol}_{interval}"
                )
                for symbol in symbols
                if self.ws_client
            ]
            [
                self.logger.info(f"unsubscribed from kline stream for: {s} ")
                for s in symbols
            ]
        except ValueError:
            self.logger.error(
                "someone tried to unsubscribe from a topic where"
                " we had no subscription anyway"
            )
        except Exception as e:
            self.logger.exception(e)

    async def watch_snapshot(self, symbols: str | list[str] | None = None):
        if not self.ws_client:
            await self.start_client()

        symbols, _ = await self._transform_symbols_parameter(symbols)

        # subscribe based on market(s)
        if not symbols:
            self.logger.debug("subscribing to market snapshots ...")
            try:
                markets = ["USDS", "BTC", "ALTS", "KCS"]
                [
                    await self.ws_client.subscribe(f"{SNAPSHOTS_ENDPOINT}:{m}")
                    for m in markets
                    if self.ws_client
                ]
                [
                    self.logger.info(f"subscribed to snapshot stream for market: {m}")
                    for m in markets
                ]
            except Exception as e:
                self.logger.exception(e)

        # subscribe based on specific symbols(s)
        else:
            self.logger.debug(f"subscribing to symbol snapshots ... {symbols}")
            try:
                [
                    await self.ws_client.subscribe(f"{SNAPSHOTS_ENDPOINT}:{s}")
                    for s in symbols
                    if self.ws_client
                ]
                [
                    self.logger.info(f"subscribed to snapshot stream for: {s} ")
                    for s in symbols
                ]
            except Exception as e:
                self.logger.exception(e)

    async def unwatch_snapshot(self, symbols: str | list[str]):
        if not self.ws_client:
            self.logger.error(
                "unwatch candles not possible as we have no subscriptions"
            )
            return

        symbols, _ = await self._transform_symbols_parameter(symbols)

        try:
            [
                await self.ws_client.unsubscribe(f"{SNAPSHOTS_ENDPOINT}:{symbol}")
                for symbol in symbols
                if self.ws_client
            ]
            [
                self.logger.info(f"unsubscribed from snapshot stream for: {s} ")
                for s in symbols
            ]
        except ValueError as e:
            self.logger.error(
                "someone tried to unsubscribe from a topic (%s) where"
                " we had no subscription anyway -> %s",
                symbols,
                e,
            )
        except Exception as e:
            self.logger.exception(e)

    async def _handle_message(self, msg: dict) -> None:
        """Handles all messages the we get from the websocket client.

        A fairly long method. but this helps with performance. I
        measured the time for letting a specific method for every 'subject'
        handle the response and it was significantly slower, so I put
        everything in here. In the most demanding case (subscribing to all
        tickers or snapshots) we may have thousands of messages per second,
        that's why we wanna be fast here.

        Parameters
        ----------
        msg
            the original websocket message
        """
        if msg["type"] == "message":
            subject = msg.get("subject")
            received_at = time.time()

            # ohlcv candle updates
            if subject and "candles" in subject:
                """
                .. code:: python
                # the original message format

                {
                    'data': {
                        'candles': [
                            '1669652580',
                            '16063.1',
                            '16058.2',
                            '16063.2',
                            '16056.7',
                            '2.90872302',
                            '46711.615964094'
                        ],
                        'symbol': 'BTC-USDT',
                        'time': 1669652610226195133
                    },
                    'subject': 'trade.candles.update',
                    'topic': '/market/candles:BTC-USDT_1min',
                    'type': 'message'
                }
                """
                await self.publish(
                    {
                        "id": self.id,
                        "exchange": "kucoin",
                        "subject": "candles",
                        "topic": msg["topic"].split(":")[-1],
                        "type": msg["subject"].split(".")[-1],
                        "symbol": msg["data"]["symbol"],
                        "interval": msg["topic"].split(":")[1].split("_")[1][:-2],
                        "data": {
                            "open time": msg["data"]["candles"][0],
                            "open": msg["data"]["candles"][1],
                            "high": msg["data"]["candles"][3],
                            "low": msg["data"]["candles"][4],
                            "close": msg["data"]["candles"][2],
                            "volume": msg["data"]["candles"][5],
                            "quote volume": msg["data"]["candles"][6],
                        },
                        "time": msg["data"]["time"] / 1_000_000_000,
                        "received_at": received_at,
                    }
                )
                return

            # ticker updates
            elif subject and "ticker" in subject:
                """
                .. code:: python
                # the original message format

                {
                    "type":"message",
                    "topic":"/market/ticker:BTC-USDT",
                    "subject":"trade.ticker",
                    "data":{
                        "sequence":"1545896668986",
                        "price":"0.08",
                        "size":"0.011",
                        "bestAsk":"0.08",
                        "bestAskSize":"0.18",
                        "bestBid":"0.049",
                        "bestBidSize":"0.036"
                    }
                }
                """
                try:
                    await self.publish(
                        {
                            "exchange": "kucoin",
                            "subject": "ticker",
                            "topic": msg["topic"].split(":")[-1],
                            "data": msg["data"],
                            "received_at": received_at,
                            "type": "message",
                        }
                    )
                    return
                except Exception:
                    return

            # market snapshot updates
            elif subject and "snapshot" in subject:
                """
                .. code:: python
                # the original message format

                {
                    'type': 'message',
                    'topic': '/market/snapshot:UNI-USDT',
                    'subject': 'trade.snapshot',
                    'data': {
                        'sequence': '169723469',
                        'data': {
                            'averagePrice': 6.04811259,
                            'baseCurrency': 'UNI',
                            'board': 1,
                            'buy': 5.9512,
                            'changePrice': 0.05,
                            'changeRate': 0.0084,
                            'close': 5.9536,
                            'datetime': 1670487368010,
                            'high': 6.0521,
                            'lastTradedPrice': 5.9536,
                            'low': 5.8732,
                            'makerCoefficient': 1.0,
                            'makerFeeRate': 0.001,
                            'marginTrade': True,
                            'mark': 0,
                            'market': 'DeFi',
                            'markets': ['DeFi', 'USDS'],
                            'open': 5.9036,
                            'quoteCurrency': 'USDT',
                            'sell': 5.9536,
                            'sort': 100,
                            'symbol': 'UNI-USDT',
                            'symbolCode': 'UNI-USDT',
                            'takerCoefficient': 1.0,
                            'takerFeeRate': 0.001,
                            'trading': True,
                            'vol': 41390.5055,
                            'volValue': 247542.9903228
                            }
                        }
                    }
                """
                try:
                    msg["topic"] = msg["topic"].split(":")[1]
                except Exception:
                    pass

                try:
                    msg["exchange"] = "kucoin"
                    msg["subject"] = "snapshot"
                    msg["received_at"] = received_at
                    await self.publish(msg)
                except Exception as e:
                    self.logger.error(f"unable to handle message: {msg} {e}")
                return

            # probably an update from watching all tickers where subject
            # refers to the symbol name
            elif subject:
                try:
                    await self.publish(
                        {
                            "exchange": "kucoin",
                            "subject": "ticker",
                            "topic": msg["topic"].split(":")[-1],
                            "symbol": msg["subject"],
                            "data": msg["data"],
                            "type": "ticker",
                            "received_at": received_at,
                        }
                    )
                except Exception as e:
                    self.logger.error(f"unable to handle message: {msg} {e}")
                return

        # .....................................................................
        # something went wrong and we got an error message
        elif msg["type"] == "error":
            """
            when trying to subscribe to a non-existent topic, we get
            an error message like this one:
            {
                'id': '1670421943842',
                'type': 'error',
                'code': 404,
                'data': 'topic /market/candles:abc-USDT_1min is not found'
            }
            """
            try:
                wrong_topic = msg["data"].split(":")[1].split(" ")[0]
            except Exception:
                wrong_topic = False

            if wrong_topic:
                error_msg = {
                    "type": "error",
                    "error": "subscription failed",
                    "code": msg["code"],
                    "topic": wrong_topic,
                }
            else:
                error_msg = {
                    "type": "error",
                    "error": "unknown",
                    "code": msg["code"],
                    "topic": msg["topic"],
                }

            self.logger.error(error_msg)
            await self.publish(error_msg)
            return

        # .....................................................................
        # all other messages that are not covered by the cases above,
        # we should never get here ...
        else:
            self.logger.error(f"unable to handle message: {msg}")

    async def _transform_symbols_parameter(
        self,
        symbols: list[str] | str,
    ) -> tuple[list[str], str]:
        if not symbols:
            return [], "all"

        if isinstance(symbols, list):
            return symbols, ",".join(symbols)
        elif isinstance(symbols, str):
            return [symbols], symbols
        else:
            raise ValueError(
                f"<symbols> parameter must be str or list[str]"
                f" but was {type(symbols)}"
            )


# ======================================================================================
#                               WS CLIENT PRIVATE API                                  #
# ======================================================================================
class KucoinWebsocketPrivate(IWebsocketPrivate):
    ws_client = None
    credentials = {}
    logger_name = f"main.{__name__}"
    publisher = LogPublisher(logger_name)

    def __init__(
        self,
        credentials: dict,
        callback: Callable,
        publisher: Optional[IPublisher] = None,
    ):
        # set the callable to be used for publishing the results
        self.publisher = publisher or KucoinWebsocketPublic.publisher
        self.publish = callback or self.publisher.publish

        self.credentials = credentials
        self.logger = logging.getLogger(self.logger_name)

    async def run(self):
        await self.start_client()
        await self.watch_account()

    async def start_client(self):
        # is public
        # client = WsToken()
        # is private
        client = WsToken(
            key=self.credentials["api_key"],
            secret=self.credentials["api_secret"],
            passphrase=self.credentials["api_passphrase"],
            is_sandbox=False,
            url="",
        )
        # is sandbox
        # client = WsToken(is_sandbox=True)
        # client = WsToken()

        self.ws_client = await KucoinWsClient.create(
            None, client, self._handle_message, private=True
        )

    async def watch_account(self):
        await self.watch_balance()
        await self.watch_orders()
        await self.watch_debt_ratio()

    async def unwatch_account(self):
        await self.unwatch_balance()
        await self.unwatch_orders()

    async def watch_balance(self):
        if not self.ws_client:
            await self.start_client()

        if self.ws_client:
            try:
                await self.ws_client.subscribe("/account/balance")
                self.logger.debug("subscribed to balance stream")
            except Exception as e:
                self.logger.exception(e)

    async def unwatch_balance(self):
        if self.ws_client:
            try:
                await self.ws_client.unsubscribe("/account/balance")
                self.logger.debug("unsubscribed from balance stream")
            except Exception as e:
                self.logger.exception(e)

    async def watch_orders(self):
        if not self.ws_client:
            await self.start_client()

        if self.ws_client:
            try:
                await self.ws_client.subscribe("/spotMarket/tradeOrders")
                self.logger.info("subscribed to orders stream")
                await self.ws_client.subscribe("/spotMarket/advancedOrders")
                self.logger.info("subscribed to advanced orders stream")
            except Exception as e:
                self.logger.exception(e)

    async def unwatch_orders(self):
        if self.ws_client:
            try:
                await self.ws_client.unsubscribe("/spotMarket/tradeOrders")
                self.logger.info("unsubscribed from order stream")
                await self.ws_client.subscribe("/spotMarket/advancedOrders")
                self.logger.info("unsubscribed from advanced orders stream")
            except Exception as e:
                self.logger.exception(e)

    async def watch_debt_ratio(self):
        if not self.ws_client:
            await self.start_client()

        if self.ws_client:
            try:
                await self.ws_client.subscribe("/margin/position")
                self.logger.debug("subscribed to debt ratio stream")
            except Exception as e:
                self.logger.exception(e)

    async def unwatch_debt_ratio(self):
        if self.ws_client:
            try:
                await self.ws_client.unsubscribe("/margin/position")
                self.logger.debug("unsubscribed from debt ratio stream")
            except Exception as e:
                self.logger.exception(e)

    async def _handle_message(self, msg):
        if msg["type"] == "message":
            subject = msg.get("subject")

            # account balance updates
            if subject and subject == "account.balance":
                await self.publish(msg)
                return

            # order updates
            elif subject and subject == "orderChange":
                await self.publish(msg)
                return

            # advanced (stop) order updates
            elif subject and subject == "stopOrder":
                await self.publish(msg)
                return

            # debt ratio updates
            elif subject and subject == "debt.ratio":
                await self.publish(msg)
                return
        else:
            self.logger.error(f"unable to handle message: {msg}")
