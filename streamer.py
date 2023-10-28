#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides a websocket streamer that routes the data to a ZeroMQ socket
for consumption by different consumers.

Implemented streams/data:
- tickers
- 24hr market snapshots
- kline/candles/ohlcv

Functions:
    streamer
        stream function for one market /mode, handles subscriptions
        between ZMQ clients and the WebSocket streamer (dynamically
        changes the topics that the WebSocket streamer listens to,
        based on client requests)

    streaming_server
        starts a streaming server with multiple streamers for different
        markets/modes. All of these run in one asyncio thread

    tickers
        handles ticker messages from the websocket streamer, and
        publishes them on the right ZMQ socket

    snapshots
        handles snapshot messages from the websocket streamer ...

    candles
        handles candle messages from the websocket streamer ...

    error
        handles error messages from the websocket streamer ...

Note: The tickers/snapshots/candles/error functions are not called
directly by the streamer function. Instead, they are passed as
callbacks to the websocket client.

A TICKER message tells us about the current state for an asset and
looks like this:

..code:: python
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

A SNAPSHOT message tells us about the 24hr stats for an asset and
looks like this:

..code:: python
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

A CANDLES message provides an update for the current trading interval
and looks like this:

..code:: python
{
    'exchange': 'kucoin',
    'subject': 'candles',
    'topic': 'XRP-USDT_1min',
    'type': 'update',
    'symbol': 'XRP-USDT',
    'interval': '1m',
    'data': {
        'open time': '1694461740',
        'open': '0.47037',
        'high': '0.47037',
        'low': '0.47018',
        'close': '0.47018',
        'volume': '4638.8665',
        'quote volume': '2181.438717295'
    },
    'time': 1694461754.5556684,
    'received_at': 1694461754.683858
}

The 'streamer' enables to have multiple consumers for this data while
requiring a minimum number of websocket connections to the exchange.
This concentrates the whole implementation in this file and also helps
to stay well within the range of max allowed websocket connections,
even if we have hundreds of consumers for this data.

Depending on the mode that the streamer is running in, we have
different mechanisms to dynamically deal with changing topic
subscriptions by the connected consumers.

The function can be run multiple times in one asyncio event loop,
as single thread or as process. This gives us flexibility and makes
it possible to tailor the deployment to the specific resources
(servers, cores, ...) that we have available.

TODO    implement management of more than one websocket client for
        cases where we have more topics/websocket subscriptions
        than allowed by the exchange per websocket connection (for
        instance on Kucoin this is set to 100 topics per connection)!
        This limit is sufficent for most types of bots, but would need
        to be higher if for instance we want to stream klines for all
        coins on the exchange for some reason.

requirements:
    - uvloop
    - zmq
    - kucoin-python-sdk
    - a websocket client implementing IWebsocketPublic

Created on Wed Dec 07 11:05:20 2022

@author dhaneor
"""
import asyncio
import json
import logging
import os
import sys
import time
import uvloop
import zmq
import zmq.asyncio

from functools import partial
from random import randint, random
from typing import (
    Optional,
    Iterable,
    Literal,
    TypeAlias,
    Sequence,
    Callable,
    Coroutine,
)

# --------------------------------------------------------------------------------------
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
# --------------------------------------------------------------------------------------

from rawi import zmq_config as cnf  # noqa: F401, E402
from rawi.websocket.ws_kucoin import KucoinWebsocketPublic  # noqa: F401, E402
from util.sequence import sequence  # noqa: F401, E402
from zmqbricks import registration as rgstr  # noqa: F401, E402
from zmqbricks import heartbeat as hb  # noqa: F401, E402
from zmqbricks.kinsfolk import Kinsfolk, Kinsman  # noqa: F401, E402

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

logger = logging.getLogger("main.streamer")
logger.setLevel(logging.DEBUG)

config = cnf.Streamer(exchange="kucoin", markets=["spot", "margin"])
ConfigT = cnf.Streamer

# ======================================================================================
# add additional websocket client class here to enable more exchanges/markets
clients = {"kucoin.spot": KucoinWebsocketPublic, "kucoin.margin": KucoinWebsocketPublic}

# valid values for streamer() param 'mode'
valid_modes = ["tickers", "candles", "snapshots", "all_tickers", "all_snapshots"]

# for convenience
ModeT: TypeAlias = Literal[
    "tickers", "candles", "snapshots", "all_tickers", "all_snapshots"
]
MarketT: TypeAlias = Literal["spot", "margin", "futures"]
SockT: TypeAlias = zmq.Socket


sockets = {"tickers": None, "snapshots": None, "candles": None}

RGSTR_LOG_INTERVAL = 300  # seconds
SEQ_IDENTIFIER = "sequence"
VERSION = "1.1.0"


# ======================================================================================
# callback functions for the websocket clients
async def tickers(msg: dict, uid: str, reset_timer: bool) -> None:
    """Handles ticker messages

    :param msg: {
                    'exchange': 'kucoin'
                    "subject":"ticker",
                    "topic":"BTC-USDT",
                    "data":{
                        "sequence":"1545896668986",
                        "price":"0.08",
                        "size":"0.011",
                        "bestAsk":"0.08",
                        "bestAskSize":"0.18",
                        "bestBid":"0.049",
                        "bestBidSize":"0.036"
                    },
                    "received_at":1545896668986
                }
    :type msg: dict
    """
    logger.info(f"received ticker message: {msg}")

    try:
        # we received a normal message
        if msg["type"] == "message":
            subject, topic = msg.get("subject", ""), msg.get("topic")
            pub_msg = None

            if subject and "ticker" in subject:
                pub_msg = msg

            # probably an update from watching all tickers where 'subject'
            # refers to the symbol name
            elif subject and topic and "all" in topic:
                try:
                    pub_msg = {
                        "exchange": msg.get("exchange"),
                        "subject": "ticker",
                        "topic": subject,
                        "data": msg["data"],
                        "type": "ticker",
                        "received_at": msg["received_at"],
                    }
                except Exception as e:
                    logger.error(f"unable to handle ticker message: {msg} {e}")
                    return

            # if the message could be handled above, we now have a
            # pub_msg and can publish it
            if pub_msg:
                try:
                    await sockets["tickers"].send_multipart(
                        [
                            pub_msg["topic"].encode("utf-8"),
                            json.dumps(pub_msg).encode("utf-8"),
                            uid.encode("utf-8"),
                        ]
                    )
                    await reset_timer()
                    return
                except zmq.ZMQError as e:
                    logger.error(f"ZMQ error: {e} for {pub_msg}")
                    return
                except Exception as e:
                    logger.error(f"sending failed with error {e} for: {pub_msg}")
                    return
        # .....................................................................
        # something went wrong and we got an error message
        elif msg["type"] == "error":
            await error(msg)
    except Exception:
        pass

    # everything that couldn't be processed above goes here
    await confused(msg)


async def snapshots(msg: dict, uid: str, reset_timer: bool):
    """Handles snapshot (24hr stats) messages.

    :param msg: {
                    'exchange': 'kucoin',
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
    :type msg: dict
    """
    try:
        if msg["type"] == "message":
            subject = msg.get("subject")

            if subject and "snapshot" in subject:
                try:
                    await sockets["snapshots"].send_multipart(
                        [
                            msg["topic"].encode("utf-8"),
                            json.dumps(msg).encode("utf-8"),
                            uid.encode("utf-8"),
                        ]
                    )
                    await reset_timer()
                    return
                except zmq.ZMQError as e:
                    logger.error(f"ZMQ error: {e} for {msg}")
                    return
                except Exception as e:
                    logger.error(f"sending failed with error {e} for: {msg}")
                    return

        # .....................................................................
        # something went wrong and we got an error message
        elif msg["type"] == "error":
            await error(msg)
    except Exception:
        pass

    # everything that couldn't be processed above goes here
    await confused(msg)


@sequence(logger=logger, identifier=SEQ_IDENTIFIER)
async def candles(msg: dict, uid: str, reset_timer: Callable):

    try:
        if msg["type"] in ("update", "add") and msg["subject"] == "candles":
            msg["ts"] = time.time()
            msg["uid"] = uid

            # logger.debug("sending candles update from %s to ...", msg["time"])
            logger.info(msg)

            try:
                seq = msg[SEQ_IDENTIFIER]
            except KeyError:
                seq = 0

            try:
                await sockets["candles"].send_multipart(
                    [
                        msg["topic"].encode("utf-8"),
                        json.dumps(msg).encode("utf-8"),
                        str(uid).encode("utf-8"),
                        seq.to_bytes(4, byteorder='little'),
                    ]
                )
                await reset_timer()
                return
            except zmq.ZMQError as e:
                logger.error(f"ZMQ error: {e} for {msg}")
                return
            except Exception as e:
                logger.error(f"sending failed with error {e} for: {msg}")
                logger.exception(e)
                return

        # .....................................................................
        # something went wrong and we got an error message
        elif msg["type"] == "error":
            await error(msg)
    except Exception:
        confused(msg)

    # everything that couldn't be processed above goes here
    await confused(msg)


async def error(msg: dict):
    """Handles error messages from the ws stream.

    When trying to subscribe to a non-existent topic, we get
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
            "error": "subscription failed",
            "code": msg["code"],
            "topic": wrong_topic,
        }
    else:
        error_msg = {
            "error": "unknown",
            "code": msg["code"],
            "topic": msg["topic"],
            "msg": msg,
        }

    logger.error(error_msg)
    # await log_socket.send(json.dumps(error_msg).encode("utf-8"))


async def confused(msg: dict):
    logger.warning(f"I am confused and unable to handle message: {msg}")


# --------------------------------------------------------------------------------------
# basic helper functions for streamer
async def get_publisher_socket(ctx: zmq.asyncio.Context, config: ConfigT, mode: str):
    socket = ctx.socket(zmq.XPUB)
    socket.setsockopt(zmq.LINGER, 0)

    if config.public_key and config.private_key:
        socket.curve_secretkey = config.private_key.encode("ascii")
        socket.curve_publickey = config.public_key.encode("ascii")
        socket.curve_server = True

    socket.bind(config.publisher_addr)
    sockets[mode] = socket
    logger.info(f"bound {mode} to {config.publisher_addr}")
    return socket


async def get_management_socket(ctx: zmq.asyncio.Context, management_address: str):
    management_socket = zmq.asyncio.Socket(ctx, zmq.DEALER)
    management_socket.setsockopt(zmq.SNDHWM, 1)
    management_socket.connect(management_address)
    return management_socket


async def get_ws_client(uid: str, market: MarketT, mode: str, reset_timer: Coroutine):
    """Initializes a websocket client for the given market & mode.

    Parameters
    ----------
    uid : str
        The unique identifier of the caller.
    market : MarketT
        The market to subscribe (see above for valid values).
    mode : ModeT
        The mode for the WS client (see above for valid values).
    reset_timer : Coroutine
        _description_

    Returns
    -------
    _type_
        _description_
    """
    callbacks = {
        "tickers": partial(tickers, uid=uid, reset_timer=reset_timer),
        "all_tickers": partial(tickers, uid=uid, reset_timer=reset_timer),
        "snapshots": partial(snapshots, uid=uid, reset_timer=reset_timer),
        "all_snapshots": partial(snapshots, uid=uid, reset_timer=reset_timer),
        "candles": partial(candles, uid=uid, reset_timer=reset_timer),
    }

    try:
        ws_client = clients[market](id=uid, callback=callbacks[mode])
        await ws_client.start_client()
        await asyncio.sleep(2)  # giving the client time to connect
    except KeyError:
        logger.critical(f"no client for {market}")
        raise
    except Exception as e:
        logger.critical(f"starting ws client failed with: {e}")
        raise

    return ws_client


async def add_hb_sender(scroll: rgstr.Scroll, hb_sock: SockT):
    # we should have the information about the endpoint for the socket
    # where collector sends out heartbeats -> connect and start the
    # background task that listens for them
    if not (addr := scroll.endpoints.get("heartbeat", None)):
        logger.critical("no heartbeat endpoint in rgstr reply")
        return

    addr = addr.replace("*", "127.0.0.1")
    success = False

    logger.info("adding heartbeat sender %s with %s", addr, scroll.name)
    logger.info("heartbeat socket on our side: %s", hb_sock)

    try:
        hb_sock.connect(addr)
        hb_sock.subscribe(b"heartbeat")
    except zmq.ZMQError as e:
        logger.error(f"ZMQ error: {e} for {scroll}")
    except Exception as e:
        logger.exception(e)
    else:
        success = True
        logger.info("connected to heartbeat endpoint %s", addr)
    finally:
        return success


async def remove_hb_sender(hb_sock: SockT, kinsman: Kinsman):
    success = False

    endpoint = kinsman.endpoints.get("heartbeat", None)

    logger.info("removing heartbeat sender %s with %s", kinsman.name, endpoint)

    try:
        hb_sock.unsubscribe(b"heartbeat")
        hb_sock.disconnect(endpoint)
    except zmq.ZMQError as e:
        logger.error(f"ZMQ error: {e} for {kinsman}")
    except Exception as e:
        logger.error(
            "unable to remove heartbeat sender: %s -> %s", kinsman.name, e, exc_info=1
        )
    else:
        success = True
        logger.info("disconnected from heartbeat endpoint %s", endpoint)
    finally:
        return success


async def handle_hb(hb_msg: hb.HeartbeatMessage, actions: Sequence[Coroutine]) -> None:
    """Handle a heartbeat message.

    Parameters
    ----------
    hb_msg : hb.HeartbeatMessage
        The heartbeat message received from the collector.
    actions : Sequence[Coroutine]
        The actions to perform on the heartbeat message.
    """
    for action in actions:
        await action(hb_msg)


async def unsub_all(subscriber_socket, topics: Sequence[str]) -> None:
    """Unsubscribe from all topics.

    Parameters:
    subscriber_socket
        The ZeroMQ socket to send the message.
    topics
        The topics to unsubscribe from.
    """
    for topic in topics:
        logger.info("Unsubscribing from topic %s", topic)
        subscriber_socket.setsockopt_string(zmq.UNSUBSCRIBE, topic)


# --------------------------------------------------------------------------------------
# main function for streamer
async def streamer(
    market: MarketT,
    mode: ModeT,
    context: Optional[zmq.asyncio.Context] = None
) -> None:
    """Streams websocket data and manages subscriptions.

    Parameters
    ----------
    market : str
        the relevant market, for now: kucoin.spot/kucoin.margin

    mode : ModeT
        streaming mode: tickers, snapshots, candles, all_tickers,
        all_snapshots

    ctx: zmq.Context
        An initialized ZMQ Context object

    Raises
    ------
    ValueError
        if the mode is not a valid mode
    """
    if mode not in valid_modes:
        raise ValueError(f" mode {mode} is not a vald mode. Use one of {valid_modes}")

    # get the ZMQ context
    ctx = context or zmq.asyncio.Context()

    # configure sockets
    publisher = await get_publisher_socket(ctx, config, mode)
    management = await get_management_socket(ctx, config.register_at)
    heartbeat = zmq.asyncio.Socket(ctx, zmq.SUB)
    logger.debug("configured sockets: OK")

    # prepare partial functions for use in steps below
    remove_hb_sender_coro = partial(remove_hb_sender, hb_sock=heartbeat)
    add_hb_sender_coro = partial(add_hb_sender, hb_sock=heartbeat)
    rgstr_info_fn = partial(cnf.get_rgstr_info, "collector", "kucoin", "spot")
    send_hb_coro = partial(hb.send_hb, publisher, config.uid, config.name)

    # initialize peer registry
    kinsfolk = Kinsfolk(config.hb_interval, config.hb_liveness, remove_hb_sender_coro)

    # ..................................................................................
    # do this after successful registration with collector
    rgstr_actions = [kinsfolk.accept, add_hb_sender_coro]

    # register with collector so it knows where to subscribe to topics
    try:
        await rgstr.register(ctx, config, rgstr_info_fn, rgstr_actions)
    except Exception as e:
        logger.critical(e, exc_info=1)
        return

    # ..................................................................................
    # start background tasks ...
    #
    # for listening for heartbeats
    recv_hb_task = await hb.start_hb_recv_task(heartbeat, [kinsfolk.update])
    logger.debug("started heartbeat recv task: OK")

    # .. for sending heartbeats
    send_hb_task, reset_fn = await hb.start_hb_send_task(
        send_hb_coro, config.hb_interval
    )
    logger.debug("started heartbeat send task: OK")

    # ... for watching connected peers and their health
    kinsfolk_task = asyncio.create_task(
        kinsfolk.watch_out(
            actions=[
                partial(rgstr.register, ctx, config, rgstr_info_fn, rgstr_actions)
            ]
        )
    )

    # ..................................................................................
    # start the appropriate websocket client for streaming data for
    # the given market. we are using one dedicated instance per streamer
    # because this increases the number of topics we can subscribe to.
    ws_client = await get_ws_client(config.uid, market, mode, reset_fn)

    # start websocket subscriptions for one of the two catch-all modes
    # if we are running in one of those. These function slightly
    # differently than the other modes => special treatment here
    if mode == "all_tickers":
        await ws_client.watch_ticker()
        logger.info("starting stream for all tickers ...")
    elif mode == "all_snapshots":
        logger.info("starting stream for all snapshots ...")
        await ws_client.watch_snapshot()

    # initialize poller & register sockets
    poller = zmq.asyncio.Poller()
    poller.register(publisher, zmq.POLLIN)
    poller.register(management, zmq.POLLIN)

    topics = set()

    logger.info(f"streaming service for {mode} started on {config.publisher_addr}")

    # ..................................................................................
    # main loop
    while True:
        try:
            events = dict(await poller.poll(500))

            # manage new or cancelled subscriptions
            if publisher in events:
                logger.debug("received message at publisher port ...")

                msg = await publisher.recv()

                logger.debug(f"received message: {msg}")

                # check that we got a valid subscription message
                try:
                    subscribe, topic = msg[0], msg[1:].decode()  # type:ignore
                    valid_msg = True
                except KeyError:
                    logger.error(f"received invalid subscription messsage: {msg}")
                    valid_msg, subscribe, topic = False, False, None

                # abandon if we don't have a valid subscription message
                if not valid_msg and topic:
                    raise ValueError("invalid message or topic")

                # first check for subscription to "heartbeat"
                if topic == "heartbeat":
                    continue

                elif topic == config.uid:
                    await rgstr.register(ctx, config, rgstr_info_fn, rgstr_actions)

                # handle subscribe/unsubscribe for mode 'candles'
                elif mode == "candles":
                    try:
                        symbols, interval = topic.split("_")
                    except Exception as e:
                        logger.error("invalid topic: %s -> %s", topic, e)
                        raise ValueError("invalid topic")

                    if subscribe and topic:
                        try:
                            logger.info("sub req: %s %s", symbols, interval)
                            await ws_client.watch_candles(symbols, interval)
                        except Exception:
                            pass
                        else:
                            topics.add(topic)

                    elif not subscribe and msg:
                        try:
                            logger.info("unsub req: %s %s", symbols, interval)
                            await ws_client.unwatch_candles(symbols, interval)
                        except Exception:
                            pass
                        else:
                            topics.remove(topic)

                # handle subscribe/unsubscribe for mode 'tickers'
                elif mode == "tickers":
                    if subscribe and topic:
                        try:
                            logger.info("sub req: %s", topic)
                            await ws_client.watch_ticker(symbols=topic)
                        except Exception:
                            pass
                        else:
                            topics.add(topic)
                    elif not subscribe and topic:
                        try:
                            logger.info("unsub req: %s", topic)
                            await ws_client.unwatch_ticker(symbols=topic)
                        except Exception:
                            pass
                        else:
                            topics.remove(topic)

                # handle subscribe/unsubscribe for mode'snapshots'
                elif mode == "snapshots":
                    if subscribe and topic:
                        try:
                            logger.info("sub req: %s", topic)
                            await ws_client.watch_snapshot(symbols=topic)
                        except Exception:
                            pass
                        else:
                            topics.add(topic)
                    elif not subscribe and topic:
                        try:
                            logger.info("unsub req: %s", topic)
                            await ws_client.unwatch_snapshot(symbols=topic)
                        except Exception:
                            pass
                        else:
                            topics.remove(topic)

        except ValueError as e:
            logger.error(e)
        except asyncio.CancelledError:
            logger.info("streamer main got loop cancelled ...")
            break
        except ConnectionError as e:
            logger.exception(e)
            asyncio.sleep(5)
        except Exception as e:
            logger.critical("general exception in streamer main loop: 's", e)
            asyncio.sleep(1)

    # ..................................................................................
    # shutdown ...
    logger.debug(f"shutting down streamer {config.name} ...")

    # cancel the background tasks
    send_hb_task.cancel()
    recv_hb_task.cancel()
    kinsfolk_task.cancel()

    # NOTE: implement logic to unsubcribe from all topics
    # this is wrong!
    # await unsub_all(subscriber, topics)

    publisher.close(2)
    logger.debug("publisher socket closed: OK")
    management.close(2)
    logger.debug("management socket closed: OK")

    # TODO: check if this can be removed?!
    # shut down the context if we created it at the start
    # if not context:
    #     logger.debug("terminating context ...")
    #     ctx.term()
    #     logger.debug("context terminated: OK")

    logger.info(f"streaming service for {mode} stopped on {config.publisher_addr}")


# --------------------------------------------------------------------------------------
async def streaming_server(members: Iterable[ModeT]):
    ctx = zmq.asyncio.Context()

    market = "kucoin.margin"
    base_port = 5560
    tasks = []

    for idx, type_ in enumerate(members):
        if type_ in valid_modes:
            tasks.append(
                asyncio.create_task(
                    streamer(
                        market=market,
                        mode=type_,
                        port=base_port + idx,
                        worker_id=idx,
                        context=ctx,
                    )
                )
            )

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        ctx.term()


def start_streaming_server(members: list[ModeT]):
    asyncio.run(streaming_server(members))


# --------------------------------------------------------------------------------------
async def mock_streamer(ctx: zmq.Context, msg_per_sec: int = 10, repeat: int = 1):
    """A  mock producer for generating and sending mock messages.

    Parameters:
        zmq_address (str): The address of the ZeroMQ socket to connect to.
        msg_per_sec (int): The rate at which messages should be sent per second.
        repeat (int): The number of times to send the message.
    """
    ctx = ctx or zmq.asyncio.Context.instance()

    # configure sockets
    publisher = await get_publisher_socket(ctx, "candles", config.publisher_addr)
    management = await get_management_socket(ctx, config.register_at)

    rgstr_info_fn = partial(cnf.get_rgstr_info, "collector", "kucoin", "spot")
    register_fn = partial(  # noqa F841
        rgstr.register, ctx, config, rgstr_info_fn, []
    )

    logger.info("configured sockets: OK")

    # register with collector so it knows where to subscribe to topics
    try:
        await rgstr.register(ctx, config, rgstr_info_fn, None)
    except Exception as e:
        logger.exception(e)

    # Calculate sleep time between each message to control rate
    sleep_time, seq = (1.0 / msg_per_sec), 0
    candle_open_time = time.time()
    start = time.time()
    next_iteration = time.time() + sleep_time
    uid = config.uid
    msgs = 0
    topic = "XDC-USDT_1min"

    async def mock_task():
        try:
            while True:
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            logger.info("mock task cancelled...")
            raise asyncio.CancelledError()

    task = asyncio.create_task(mock_task())

    # Generate mock message
    msg = {
        "sequence": str(seq),
        "id": uid,
        "exchange": "kucoin",
        "subject": "candles",
        "topic": topic,
        "type": "update",
        "symbol": "BTC-USDT",
        "interval": "1min",
        "data": {
            "open time": candle_open_time,
            "open": str(randint(26000, 27000)),
            "high": str(randint(26000, 27000)),
            "low": str(randint(26000, 27000)),
            "close": str(randint(26000, 27000)),
            "volume": str(randint(1, 100) / 10),
            "quote volume": str(randint(2500, 267000) + random() * 99),
        },
        "time": None,
        "received_at": None,
    }

    logger.info("mock streamer started...")

    while True:
        now = time.time()

        try:
            for _ in range(repeat):
                seq += 1

                msg["sequence"] = str(seq)
                msg["time"] = now
                msg["received_at"] = now

                encoded = [
                    topic.encode("utf-8"),
                    json.dumps(msg).encode("utf-8"),
                    uid.encode("utf-8"),
                    seq.to_bytes(4, byteorder='little'),
                ]

                # Sleep for the calculated time to control message rate
                if time.time() >= next_iteration:
                    #   Send message
                    publisher.send_multipart(encoded)
                    msgs += 1

                    # determine time for next iteration
                    next_iteration = time.time() + sleep_time
                else:
                    await asyncio.sleep(0)

        except asyncio.CancelledError:
            logger.error("mock streamer main loop got cancelled...")
            break
        except Exception as e:
            logger.exception(e)
            break

    task.cancel()

    logger.info(f"streaming service in 'mock' mode stopped on {config.publisher_addr}")

    # log metrics
    logger.info(msgs)
    duration = time.time() - start
    msg_per_sec = round(msgs / duration, 2)
    logger.info(
        "sent %s messages in %s seconds -> %s msgs/s",
        msgs, round(duration, 1), msg_per_sec
    )

    publisher.close(2)
    logger.info("publisher socket closed: OK")
    management.close(2)
    logger.info("management socket closed: OK")
    ctx.term()
    logger.info("context terminated: OK")


def start_mock_streamer(msg_per_sec: int = 10):
    asyncio.run(mock_streamer(None, msg_per_sec))


if __name__ == "__main__":
    start_mock_streamer(10)
