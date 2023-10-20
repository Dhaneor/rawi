#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides enums for the data sources components.

Created on Oct 09 20:32:23 2023

@author_ dhaneor
"""

from enum import Enum


class SubscriptionType(Enum):
    """Subscription types."""
    OHLCV = 'ohlcv'
    TRADES = 'trades'
    TICKER = 'ticker'
    BOOK = 'orderbook'


class MarketType(Enum):
    """Market types."""
    SPOT = 'spot'
    MARGIN = 'margin'
    FUTURES = 'futures'
    OPTIONS = 'options'  # uncomment, if necessary
