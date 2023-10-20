#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides the interfaces for public and private websocket classes.


Created on Wed Dec 07 12:44:23 2022

@author_ dhaneor
"""
from abc import ABC, abstractmethod
from typing import Callable, Optional

from .publishers import IPublisher


# ======================================================================================
# for PUBLIC API endpoints
class IWebsocketPublic(ABC):
    """Interface / abstract base class for websocket handlers
    for public (not requiring API keys) endpoints.
    """

    publish: Callable
    publisher: IPublisher

    @abstractmethod
    def __init__(
        self,
        publisher: IPublisher | None = None,
        callback: Callable | None = None,
        id: str | None = None,
    ):
        """Initializes websocket handler for public endpoints.

        Parameters
        ----------

        publisher: IPublisher | None
            class/object that handles the actual publishing of events
            received from the websocket stream. This can be configured
            dynamically to suit different needs. Implementations of
            this interface should set a default publisher for the case
            that both parameters are None. See publisher.py for some
            implementations. If not set, a callback function or method
            that handles received data should be provided.

        callback: Callable
            callback that processes ws data, defaults to None
        """
        ...

    @abstractmethod
    async def watch(self, topic: str):
        ...

    @abstractmethod
    async def unwatch(self, topic: str):
        ...


class ITrades(IWebsocketPublic):
    ...


class IOhlcv(IWebsocketPublic):
    ...


class IOrderBook(IWebsocketPublic):
    ...


class ITicker(IWebsocketPublic):
    ...


class IAllTickers(IWebsocketPublic):
    ...


class ISnapshots(IWebsocketPublic):
    ...


class IAllSnapshots(IWebsocketPublic):
    ...


# --------------------------------------------------------------------------------------
# for PRIVATE API endpoints
class IWebsocketPrivate(ABC):
    """Interface / abstract base class for websocket
    handlers for private (requiring API keys) endpoints.
    """

    publisher: IPublisher

    @abstractmethod
    def __init__(self, publisher: Optional[IPublisher] = None):
        """Initializes websocket handler for private endpoints.

        :param publisher: class/object that handles the actual
        publishing of events received from the websocket stream.
        This can be configured dnymaically to suit different needs.
        Implementations of this interface should set a default
        publisher. See publisher.py for some implentations.
        :type publisher: Optional[IPublisher], optional
        """
        ...

    @abstractmethod
    def run(self):
        ...

    @abstractmethod
    def start_client(self):
        ...

    @abstractmethod
    async def watch_account(self):
        ...

    @abstractmethod
    async def unwatch_account(self):
        ...

    @abstractmethod
    async def watch_balance(self):
        ...

    @abstractmethod
    async def unwatch_balance(self):
        ...

    @abstractmethod
    async def watch_orders(self):
        ...

    @abstractmethod
    async def unwatch_orders(self):
        ...

    @abstractmethod
    async def watch_debt_ratio(self):
        ...

    @abstractmethod
    async def unwatch_debt_ratio(self):
        ...

    @abstractmethod
    async def _handle_message(self, msg: dict) -> None:
        ...
