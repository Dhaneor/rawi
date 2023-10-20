#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 16 13:58:23 2023

@author_ dhaneor
"""
import asyncio
import logging
import os
import sys

# --------------------------------------------------------------------------------------
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
# --------------------------------------------------------------------------------------

from src.analysis import strategy_builder as sb  # noqa: E402
from src.analysis.strategies.definitions import get_all_strategies  # noqa: E402

logger = logging.getLogger("main.strategy_registry")


# ======================================================================================
class StrategyRegistry:

    def __init__(self):
        self.strategies = {}

    def register(self, strategy_name, strategy):
        self.strategies[strategy_name] = strategy

    def get(self, strategy_name):
        return self.strategies[strategy_name]