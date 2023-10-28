#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# flake8: noqa: F403
"""
The rawi package is a streaming framework for crypto market data.

Most important components/modules:

amanya
    The Central Service Registry - all components need to register
    with this component. The provided information is then used to
    inform all components about the availability of a new service.

streamer
    Establishes one or more websocket connections to one or more
    exchange(s)/markets (spot/futures), depending on its configuration.

collector
    Collects and combines the streams from one or more streamers.
    This component makes it possible to introduce redundancy. The
    intention is to have two streamers with identical configuration.
    Both connect to the collector, and if one of them crashes or
    loses its websocket connection, the other one will continue to
    provide updates to the collector. Collector removes duplicates,
    so its clients only get everything once.

craeft_pond
    This component streams OHLCV data and alwys has the complete OHLCV
    data for the configured lookback period for all topics (symbols)
    that its clients are interested in. In other words, it is a
    dynamic OHLCV registry that changes its contents depending on the
    subscriptions from clients.

ohlcv_repository
    A helper component for craeft_pond. It provides a repository of
    for the initialization of new topics in craeft_pond. For now it
    downloads the requested data from the exchange API.

zmq_config
    Contains classes that configure the other components. Here the
    (static) address for amanya can be changed, configuration for
    future additional components can be added, or restrictions with
    regard to the exchange/market that a component should handle can
    be set.

Amanya is the only component that must have a static address, so the
other components can find it when starting up. Every other component
will send its address/endpoint information during registration. That
makes the system dynamic, and new components can be added at any point.
They can run anywhere, and the other components will still be able
to find them. The same mechanism ensure that components that components
that were offline temporarily, will be re-integrated into the system.
This makes the system very resilient and self-helaing in case of
failures, server reboots or loss of connectivity.

Every component can be started as script. But it is also possible to
combine components by means of a glue script - in case of multiple
available CPU cores or when expecting low load/throughput.

Created on Thu Sep 22 13:00:23 2021

@author_ dhaneor
"""
