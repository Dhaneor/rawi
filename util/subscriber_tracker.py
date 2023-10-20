#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides a tracker for subscriptions with publishers.

This tracker is meant to be used in a PUB/SUB architecture.

Sometimes it is important to know, how many subscribers there are
for a certain topic. If we keep track of this we can unsubscribe
from data producers upstream to reduce network and system load.
This module provides this functionality with a registry for topics
and their subscribers.

Created on Tue Sep 19 22:57:23 2023

@author_ dhaneor
"""
import logging

from collections import defaultdict
from functools import wraps
from typing import Callable


if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    logger.addHandler(handler)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s.%(funcName)s.%(lineno)d  - [%(levelname)s]: %(message)s"
    )
    handler.setFormatter(formatter)
    logger.info('Running module test ...')
else:
    logger = logging.getLogger("main.subscriber_tracker")


SUB_FUNC_NAMES = {'subscribe', 'watch'}
UNSUB_FUNC_NAMES = {'unsubscribe', 'unwatch'}


# ======================================================================================
def subscriber_tracker(callback: Callable = None):
    """Decorator function that can be used to track subscribers for different topics.

    NOTE: Because we need to keep track of topics and subscriber identifiers,
    this decorator expects the first two arguments of the decorated function
    to be the topic name and the subscriber identifier (in this order)

    Parameters
    ----------
    callback: Callable, optional):
        A callback function that will be called when a topic has zero subscribers.

    Returns
    -------
    function
        The decorator function.

    Example Usage:

    >>>    # with class/instance methods ...
    >>>    #
    >>>    class Test:
    >>>        @subscriber_tracker()
    >>>        def subscribe(self, topic, client_id):
    >>>            logger.debug(f"... Subscribed to topic: {topic}")
    >>>            logger.debug('-' * 80)
    >>>
    >>>        @subscriber_tracker(callback=callback)
    >>>        def unsubscribe(self, topic, client_id):
    >>>            logger.debug(f"... Unsubscribed from topic: {topic}")
    >>>            logger.debug('-' * 80)
    >>>
    >>>    # with functions ...
    >>>    #
    >>>    @subscriber_tracker()
    >>>    def subscribe(topic, client_id):
    >>>        logger.debug(f"Subscribed to topic: {topic}")
    >>>        logger.debug('-' * 80)
    >>>
    >>>    @subscriber_tracker(callback=callback)
    >>>    def unsubscribe(topic, client_id):
    >>>        logger.debug(f"Unsubscribed from topic: {topic}")
    >>>        logger.debug('-' * 80)
    >>>
    >>>    # Create an instance of the class and test subscription
    >>>    # and unsubscription
    >>>    t = Test()
    >>>    t.subscribe('test', 1)
    >>>    t.subscribe('test', 2)
    >>>    t.unsubscribe('test', 1)
    >>>    t.unsubscribe('test', 2)
    >>>
    >>>    # Test subscription and unsubscription using functions
    >>>    subscribe('test', 1)
    >>>    subscribe('test', 2)
    >>>    unsubscribe('test', 1)
    >>>    unsubscribe('test', 2)
    """

    def decorator(func):
        if not hasattr(subscriber_tracker, 'internal_registry'):
            subscriber_tracker.internal_registry = defaultdict(set)

        @wraps(func)
        def wrapper(*args, **kwargs):

            # determine if 'func' is a method or a function
            if '.' in func.__qualname__:
                _, topic, client_id, *_ = args
                instance = args[0]
                decorated_type = 'method'

                # create/use the registry for the instance
                if not hasattr(instance, "_registry"):
                    instance._registry = defaultdict(set)
                registry = instance._registry

            else:
                logger.debug("<%s> is a function! --> %s", func.__qualname__, args[0])

                topic, client_id, *_ = args
                registry = subscriber_tracker.internal_registry
                decorated_type = 'function'

            # Update the registry based on the name of the function or
            # method being called

            # if this is meant to unsubscribe, remove the subscriber
            if func.__name__ in UNSUB_FUNC_NAMES:
                logger.info("removing subscriber <%s> for topic: %s", client_id, topic)

                if topic in registry:
                    registry[topic].discard(client_id)

                    if not registry[topic]:
                        logger.debug(f"removing topic {topic} from registry")
                        del registry[topic]

                        if callback:
                            logger.debug("calling callback ...")
                            callback(topic)
                else:
                    logger.warning(f"topic {topic} not in registry")

            # if this is meant to subscribe, add the subscriber
            elif func.__name__ in SUB_FUNC_NAMES:
                logger.info("adding subscriber <%s> for topic: %s", client_id, topic)
                registry[topic] = registry.get(topic, set())
                registry[topic].add(client_id)

            # if this is not a subscribe or unsubscribe function, log an error
            else:
                logger.error(
                    "unable to track subscribers for  %s %s -> "
                    "make sure the %s name is 'subscribe', 'unsubscribe', "
                    "'watch' or 'unwatch'",
                    decorated_type,
                    func.__qualname__,
                    decorated_type
                )

            return func(*args, **kwargs)

        return wrapper

    return decorator


# ======================================================================================
#                              TEST CLASS & FUNCTIONS                                  #
# ======================================================================================
def callback(topic):
    """Dummy callback for testing the decorator"""
    logger.info(
        f"no more subscribers for topic '{topic}' --> informing upstream producers ..."
    )


# as class
class Test:

    @subscriber_tracker()
    def subscribe(self, topic, client_id):
        logger.debug(f"... Subscribed to topic: {topic}")
        logger.debug('-' * 80)

    @subscriber_tracker(callback=callback)
    def unsubscribe(self, topic, client_id):
        logger.debug(f"... Unsubscribed from topic: {topic}")
        logger.debug('-' * 80)


# as function
@subscriber_tracker()
def subscribe(topic, client_id):
    logger.debug(f"Subscribed to topic: {topic}")
    logger.debug('-' * 80)


@subscriber_tracker(callback=callback)
def unsubscribe(topic, client_id):
    logger.debug(f"Unsubscribed from topic: {topic}")
    logger.debug('-' * 80)


@subscriber_tracker()
def watch(topic, id):
    logger.debug(f"Subscribed to topic: {topic}")
    logger.debug('-' * 80)


@subscriber_tracker(callback=callback)
def leave_me_alone(topic, client_id):
    logger.debug(f"Unsubscribed from topic: {topic}")
    logger.debug('-' * 80)


if __name__ == "__main__":

    logger.debug("================================")
    logger.debug("<<< testing with a class ... >>>")

    t = Test()
    t.subscribe('test', 1)
    t.subscribe('test', 2)

    t.unsubscribe('test', 1)
    t.unsubscribe('test', 2)

    logger.debug("")
    logger.debug("==================================")
    logger.debug("<<< testing with a function... >>>")

    subscribe('test', 1)
    watch('test', 2)

    unsubscribe('test', 1)
    unsubscribe('test', 2)

    leave_me_alone('test', 1)
