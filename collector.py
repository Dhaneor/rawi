#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides a function that receives candle updates and removes duplicates.

This is intended to be used as a filter in a data stream and will be
the collection point that is given to the <streamer> instances.
Streamers will then connect to the REP port to announce their existence.
After that, this function will subscribe to all relevant topics.

This module helps to increase resilience of the OHLCV data stream,
because now we can have multiple streamer instances. The streamers will
also send a keepalive signal (if no data is received within 5 seconds).
The keepalive helps to detect streamers that may have died for whatever
reason.

Functions:
    collector
        removes duplicate messages from a data stream

Created on Tue Sep 12 19:41:23 2023

@author_ dhaneor
"""
import asyncio
import logging
from collections import deque
from functools import partial
from random import choice
from statistics import mean
from time import time
from typing import Mapping, Optional, Sequence, TypeVar

import uvloop
import zmq
import zmq.asyncio

from zmq_config import Collector  # noqa: F401, E402
from zmqbricks import heartbeat as hb  # noqa: F401, E402
from zmqbricks.gond import Gond  # noqa: F401, E402
from zmqbricks.kinsfolk import Kinsfolk, KinsfolkT, Kinsman  # noqa: F401, E402
from zmqbricks.registration import Scroll, monitor_registration  # noqa: F401, E402
from zmqbricks.util.sockets import get_random_server_socket  # noqa: F401, E402

from util.sequence import monitor_sequence  # noqa: F401, E402
from util.subscription_request import SubscriptionRequest  # noqa: F401, E402

logger = logging.getLogger("main.collector")

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

ConfigT = TypeVar("ConfigT", bound=Collector)
TopicsT: TypeVar = Sequence[str]
SockT: TypeVar = zmq.Socket
StatsT: TypeVar = Mapping[str, Sequence[float]]

test_topics = set(
    [
        "BTC-USDT_1min",
        "BTC-USDT_5min",
        "BTC-USDT_15min",
        "BTC-USDT_30min",
        "ETH-USDT_1min",
        "ETH-USDT_5min",
        "ETH-USDT_15min",
        "ETH-USDT_30min",
        "XRP-USDT_1min",
        "XRP-USDT_5min",
        "XRP-USDT_15min",
        "XRP-USDT_30min",
        "ADA-USDT_1min",
        "ADA-USDT_5min",
        "ADA-USDT_15min",
        "ADA-USDT_30min",
        "LTC-USDT_1min",
        "LTC-USDT_5min",
        "ETH-BTC_1min",
        "ETH-BTC_5min",
        "ETH-BTC_15min",
        "ETH-BTC_30min",
        "XRP-BTC_1min",
        "XRP-BTC_5min",
        "XRP-BTC_15min",
        "XRP-BTC_30min",
        "ADA-BTC_1min",
        "ADA-BTC_5min",
        "ADA-BTC_15min",
    ]
)


# ======================================================================================
async def handle_missing_seq_no(
    producer_id: str, last_seq: int, current_seq: int
) -> None:
    logger.warning(
        f"Missing sequence number(s) from producer {producer_id}. "
        f"Last: {last_seq}, Current: {current_seq}"
    )


async def on_inactive_kinsman(kinsman: Kinsman) -> None:
    logger.warning("kinsman %s set to status 'inactive'", kinsman.name)


async def get_streamers(kinsfolk: Kinsfolk) -> Sequence[Kinsman]:
    return [k for k in kinsfolk.values() if k.service_type == "streamer"]


async def connect_to_streamer(socket: SockT, config: ConfigT, req: Scroll) -> None:
    if not (endpoint := req.endpoints.get("publisher", None)):
        logger.error("unable to connect to publisher socket: %s", endpoint)
        return

    logger.debug("... connect: %s", endpoint)

    socket.curve_serverkey = req.public_key.encode("ascii")
    socket.connect(endpoint)


async def subscribe_to_all(socket: zmq.Socket, topics: TopicsT):
    for topic in topics:
        logger.debug("... subscribing to topic %s", topic)
        socket.setsockopt(zmq.SUBSCRIBE, topic.encode("utf-8"))


async def subscribe_topic(socket: zmq.Socket, topic):
    socket.setsockopt(zmq.SUBSCRIBE, topic.encode("utf-8"))
    logger.debug("subscribed to topic: %s", topic)


async def unsubscribe_topic(socket: zmq.Socket, topic):
    socket.setsockopt(zmq.UNSUBSCRIBE, topic.encode("utf-8"))
    logger.debug("unsubscribed from topic: %s", topic)


async def unsubscribe_from_all(socket: SockT, topics: TopicsT):
    logger.debug("unsubscribing from all topics ...")

    for topic in topics:
        logger.debug("removing topic %s", topic)
        socket.setsockopt(zmq.UNSUBSCRIBE, topic.encode("utf-8"))

    logger.debug("unsubscribed from all topics")
    topics = set()


async def handle_xpub_msg(
    msg: bytes,
    peers: KinsfolkT,
    topics: TopicsT,
    socket: SockT,
) -> tuple[KinsfolkT, TopicsT]:
    def has_permission(token):
        for peer in peers.values():
            logger.debug("%s <-> %s", token, peer.token)
            if peer is not None and token == peer.token:
                return True
        return False

    # extract (un)subscribe bit and subscription request
    try:
        subscribe, sub_req = msg[0], msg[1:].decode("utf-8")
    except KeyError:
        logger.error("subcription message is too short: %s", msg)
        return peers, topics

    # extract topic and token from subscription request
    try:
        topic, token = sub_req.split(":", 1)
    except ValueError:
        logger.error(
            "unable to handle subscription message: %s "
            "--> forgot to add ':<token>'?",
            msg,
        )
        return peers, topics

    # check permission
    if not has_permission(token):
        logger.warning("wrong token for topic %s: permission denied!", topic)
        return peers, topics

    # subscribe/unsubscribe with producer(s)
    if subscribe:
        socket.setsockopt(zmq.SUBSCRIBE, topic.encode("utf-8"))
        logger.info("subscribed to topic: %s", topic)
        topics.add(topic)
        logger.debug(topics)
    elif not subscribe:
        socket.setsockopt(zmq.UNSUBSCRIBE, topic.encode("utf-8"))
        logger.info("unsubscribed from topic: %s", topic)
        if topic in topics:
            topics.remove(topic)
        else:
            logger.warning("topic %s should have been in topics, but was not", topic)
        logger.debug(topics)
    else:
        logger.error("unable to handle subscription message: %s", msg)

    return peers, topics


async def process_registration(
    req: Scroll,
    config: ConfigT,
    kinsfolk: KinsfolkT,
    sub_sock: SockT,
    topics: TopicsT,
):
    if not await kinsfolk.accept(req):
        return

    reply = Scroll.from_dict(config.as_dict())

    await reply.send(socket=req._socket, routing_key=req._routing_key)

    if req.service_type == "streamer" and "publisher" in req.endpoints:
        await connect_to_streamer(sub_sock, config, req)

        logger.debug("... subscibe: 'heartbeat' ->  %s", req.endpoints["publisher"])

        await subscribe_topic(sub_sock, "heartbeat")

        for topic in topics:
            sub_sock.setsockopt(zmq.SUBSCRIBE, topic.encode("utf-8"))

    else:
        logger.error("service type or endpoints['publisher'] missing, got: %s", req)


async def request_to_register(uid: str, socket: zmq.Socket) -> None:
    """Request (re-)registration for a given uid."""
    logger.info("request to register: %s (%s)", uid, socket)
    socket.setsockopt(zmq.SUBSCRIBE, uid.encode("utf-8"))


@monitor_sequence(callback=handle_missing_seq_no)
async def process_update(msg: dict):
    """I'm just here to monitor the sequence number ... for now."""


async def metrics(start_ts: float, epochs: int, msg_count: int, stats: StatsT) -> None:
    """Calculates metrics during shutdown and logs them

    Parameters
    ----------
    start_ts : float
        When was the main loop started.
    epochs : int
        How many epochs were processed.
    msg_count : int
        How many messages were processed.
    stats : StatsT
        A dictionary containing the processing times for each step in an epoch.

    """
    seconds = time() - start_ts
    milliseconds = seconds * 1000

    # average processing time for an epoch (= one loop iteration)
    logger.info(
        "epochs/seconds: %s epochs / %s seconds --> %s ms/epoch | %s epochs/s",
        epochs,
        seconds,
        round(milliseconds / epochs, 4),
        round(epochs / seconds),
    )

    # rate of messages
    logger.info(
        "messages processed: %s -> %s msg/s | one message every %s ms",
        msg_count,
        round(msg_count / seconds, 1),
        round(milliseconds / msg_count, 3) if msg_count else 0,
    )

    # processing times for steps in an epoch
    for key, value in stats.items():
        if value and not key == "epoch":
            avg_time = round(mean([t * 1_000_000 for t in value]), 3)
            updates_per_second = 1_000_000 / avg_time if avg_time else 0

            logger.info(
                "average processing time %s: %s µs --> %s updates per second",
                key,
                round(avg_time),
                round(updates_per_second),
            )
        elif key != "epoch":
            logger.info("average processing time for %s: N/A" % key)

    # average latency if latencies were collected
    if (latencies := stats.get("latencies")) and len(latencies) > 0:
        avg_latency = sum(latencies) / len(latencies)
        latencies = [elem for elem in latencies if elem < avg_latency * 3]
        avg_latency = round(sum(latencies) / len(latencies), 2)
        logger.info("avg latency: %s ms", avg_latency)


# ======================================================================================
async def collector_old(
    config: Collector,
    ctx: Optional[zmq.asyncio.Context] = None,
):
    """Collects data from multiple producers and removes duplicates.

    Parameters
    ----------
    config : cnf.Collector
        Configuration for the collector.

    ctx : zmq.asyncio.Context
        The zmq context, optional.
    """
    context = ctx or zmq.asyncio.Context()
    poller = zmq.asyncio.Poller()
    kinsfolk = Kinsfolk(config.hb_interval, config.hb_liveness, on_inactive_kinsman)

    # configure the subscriber port
    subscriber = context.socket(zmq.SUB)
    subscriber.curve_secretkey = config.private_key.encode("ascii")
    subscriber.curve_publickey = config.public_key.encode("ascii")

    # configure the publisher port
    logger.info("configuring publisher socket at %s", config.pub_addr)
    publisher = context.socket(zmq.XPUB)
    publisher.bind(config.pub_addr)

    # configure the registration port
    logger.info("configuring registration socket at %s", config.rgstr_addr)
    registration = context.socket(zmq.ROUTER)
    registration.curve_secretkey = config.private_key.encode("ascii")
    registration.curve_publickey = config.public_key.encode("ascii")
    registration.curve_server = True
    registration.bind(config.rgstr_addr)

    # configure the heartbeat port for the downstream clients
    logger.debug("configuring heartbeat socket at %s", config.hb_addr)
    heartbeat = context.socket(zmq.PUB)
    heartbeat.bind(config.hb_addr)

    # register sockets with poller
    for s in (publisher, subscriber, heartbeat):
        poller.register(s, zmq.POLLIN)

    topics = set()  # {"XDC-USDT_1min"}

    # prepare registration & heartbeat functions for use in the loop
    register_fn = partial(
        process_registration,
        config=config,
        kinsfolk=kinsfolk,
        sub_sock=subscriber,
        topics=topics,
    )

    hb_send_fn = partial(hb.send_hb, heartbeat, config.uid, config.name)

    req_rgstr_fn = partial(request_to_register, socket=subscriber)

    # start background tasks
    hb_task, _ = await hb.start_hb_send_task(hb_send_fn, config.hb_interval)
    hb_task.set_name("hb_task")
    tasks = [
        hb_task,
        asyncio.create_task(
            monitor_registration(registration, [register_fn]), name="register_task"
        ),
        asyncio.create_task(kinsfolk.watch_out(), name="kinsfolk_task"),
    ]

    # prepare necessary variables
    consumers, loop_start, msg_count, epoch = {}, time(), 0, 0
    stats = dict(epoch=[], subscriber=[], publisher=[], kinsfolk=[])
    msg_cache = deque(maxlen=config.max_cache_size)
    next_kinsfolk_check = time() + config.kinsfolk_check_interval

    next_topic = time() + 10

    # ......................... .......................................................
    # start the main loop
    while True:
        epoch += 1

        try:
            if time() > next_topic:
                next_topic = time() + 10
                await subscribe_topic(subscriber, choice(list(test_topics)))

            events = dict(await poller.poll())
            epoch_start = time()

            # did new data arrive?
            if subscriber in events:
                msg = await subscriber.recv_multipart()
                msg_count += 1
                start = time()

                # handle data update
                if not msg[0] == b"heartbeat":
                    if msg not in msg_cache:
                        await publisher.send_multipart(msg)
                        msg_cache.append(msg)

                    # logger.debug("[%s]data update: %s", msg_count + 1, msg[1])

                    # send to process function
                    await process_update(
                        {
                            "uid": msg[2].decode("utf-8"),
                            "sequence": int.from_bytes(msg[3], byteorder="little"),
                        }
                    )

                # take everything as a heartbeat message
                await kinsfolk.update(msg[2].decode("utf-8"), on_missing=req_rgstr_fn)

                # latencies.append((time() - float(data.get("ts"))) * 1000)

                stats["subscriber"].append(time() - start)

            # check for new subscriptions at publisher socket
            if publisher in events:
                start = time()
                msg = await publisher.recv()

                logger.debug(
                    f"[{msg_count:,.0f}] subscribe | unsubscribe @ publisher port: {msg}"
                )

                consumers, topics = await handle_xpub_msg(
                    msg, consumers, topics, subscriber
                )

                stats["publisher"].append(time() - start)

            # # check if we have streamers
            if epoch_start > next_kinsfolk_check:
                start = time()

                if not await kinsfolk.get_all("streamer"):
                    logger.warning("no streamers available")

                # check if we have consumers
                if consumers := await kinsfolk.get_all("consumer"):
                    logger.debug(
                        "known consumers & topics: %s --> %s",
                        [k.name for k in consumers],
                        topics if topics else None,
                    )
                else:
                    # unsubscribe from all topics if there are no consumers
                    # and the flag is set in config
                    if topics and config.no_consumer_no_subs:
                        logger.debug("unsubscribing from all topics ...")
                        await unsubscribe_from_all(subscriber, topics)

                    # logger.warning("[epoch %s] no consumers available", epoch)

                stats["kinsfolk"].append(time() - start)
                next_kinsfolk_check += config.kinsfolk_check_interval

            stats["epoch"].append(time() - epoch_start)

        except asyncio.CancelledError as e:
            logger.debug("caught CancelledError: %s", e)
            break

        except KeyboardInterrupt:
            logger.debug("caught KeyboardInterrupt")
            break

        except zmq.ZMQError as e:
            logger.error(e, exc_info=1)

        except Exception as e:
            logger.exception(e)
            break

    # ..................................................................................
    # shutdown procedure
    for task in tasks:
        logger.debug("cancelling task: %s", task.get_name())
        task.cancel()
        await asyncio.sleep(1)

    await asyncio.gather(*tasks, return_exceptions=True)

    heartbeat.close(1)
    registration.close(1)
    subscriber.close(1)
    publisher.close(1)

    # calculate metrics
    await metrics(loop_start, epoch, msg_count, stats)

    if not ctx:
        context.term()


async def connect_to_producer(producer, socket):
    if producer.service_type == "streamer":
        logger.debug(
            "connecting to %s at %s", producer, producer.endpoints.get("publisher")
        )
        socket.curve_serverkey = producer.public_key.encode("ascii")
        socket.connect(producer.endpoints.get("publisher"))
    else:
        logger.debug("not connecting to %s", producer)


async def collector(config: Collector):
    ctx = zmq.asyncio.Context()
    poller = zmq.asyncio.Poller()

    # configure publisher & subscriber socket
    publisher = await get_random_server_socket("publisher", zmq.XPUB, config)
    subscriber = ctx.socket(zmq.SUB)
    subscriber.curve_secretkey = config.private_key.encode("ascii")
    subscriber.curve_publickey = config.public_key.encode("ascii")

    poller.register(publisher, zmq.POLLIN)
    poller.register(subscriber, zmq.POLLIN)

    msg_cache, msg_count = deque(maxlen=config.max_cache_size), 0
    on_rgstr = partial(connect_to_producer, socket=subscriber)

    async with Gond(config=config, on_rgstr_success=[on_rgstr]):
        while True:
            try:
                events = dict(await poller.poll())

                if subscriber in events:
                    if (msg := await subscriber.recv_multipart()) not in msg_cache:
                        await publisher.send_multipart(msg)
                        msg_cache.append(msg)

                    msg_count += 1

                if publisher in events:
                    msg = await publisher.recv()
                    action, topic = msg[0], msg[1:].decode("utf-8")
                    logger.info(
                        'received %s message: %s',
                        "SUBSCRIBE" if action else "UNSUBSCRIBE",
                        topic
                    )
                    action = zmq.SUBSCRIBE if action else zmq.UNSUBSCRIBE
                    subscriber.setsockopt(action, topic.encode("utf-8"))

            except asyncio.CancelledError:
                logger.debug("collector cancelled ...")
                break

    publisher.close(0)
    subscriber.close(0)


async def main():
    config = Collector("kucoin", ["spot"], "collector")
    tasks = [asyncio.create_task(collector(config))]

    try:
        await asyncio.gather(*tasks)

    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("cancelling tasks after keyboard interrupt ...")

        for task in tasks:
            task.cancel()

        mystery = await asyncio.gather(*tasks, return_exceptions=True)

        if mystery:
            logger.error("task result, exception: %s", mystery)

    logger.info("main routine stopped: OK")


if __name__ == "__main__":
    logger = logging.getLogger("main")
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s.%(funcName)s.%(lineno)d  - [%(levelname)s]: %(message)s"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("shutdown complete: OK")
