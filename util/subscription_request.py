#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides a standardized and flexible format for subscription requests.

This is intended to be used in a PUB/SUB architecture. Instead of
sending simple encoded strings, it is possible to send a JSON
payload for more fine-grained topic management.

This could also be done by joining strings with a separator, but I
found this to be easier to work with and more explicit. I think
it helps tp prevent clients from sending invalid requests that would
be silently ignored by the producers (websocket clients on our case).

The Enum classes make it harder to send invalid requests.

Created on Mon Oct 09 19:57:23 2023

@author_ dhaneor
"""
import json

from dataclasses import dataclass, asdict
from typing import Optional

from .enums import MarketType, SubscriptionType


@dataclass
class SubscriptionRequest:
    """A subscription request."""

    exchange: str
    market: MarketType
    sub_type: SubscriptionType
    symbol: str
    interval: Optional[str] = None

    def __post_init__(self, **kwargs):
        """Type check for provided values."""

        if not isinstance(self.symbol, str):
            raise TypeError("symbol must be a string")

        if self.interval and not isinstance(self.interval, str):
            raise TypeError("interval must be a string")

        if not isinstance(self.exchange, str):
            raise TypeError("exchange must be a string")

        if not isinstance(self.market, MarketType):
            raise TypeError("market must be a string")

        if not isinstance(self.sub_type, SubscriptionType):
            raise TypeError("sub_type must be a SubscriptionType")

    @property
    def topic(self) -> str:
        return (
            f"{self.symbol}_{self.interval}"
            if self.sub_type == SubscriptionType.OHLCV
            else f"{self.symbol}"
        )

    def to_json(self) -> str:
        d = asdict(self)

        d["market"] = d["market"].value
        d['sub_type'] = d['sub_type'].value

        return json.dumps(d)

    @staticmethod
    def from_json(json_str: str):
        d = json.loads(json_str)

        d['market'] = MarketType(d['market'])
        d['sub_type'] = SubscriptionType(d['sub_type'])

        return SubscriptionRequest(**d)


if __name__ == "__main__":
    sr = SubscriptionRequest(
        exchange='kraken',
        market=MarketType.SPOT,
        sub_type=SubscriptionType.OHLCV,
        symbol='XBTUSD',
        interval='1m'
    )

    as_json = sr.to_json()
    sr2 = SubscriptionRequest.from_json(as_json)

    print(as_json)
    print(sr2)
