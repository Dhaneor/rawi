# rawi: Crypto Market Data Streaming Service

![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)

## Introduction

Welcome to `rawi`, a reliable and modular crypto market data streaming service. Developed with Python and ZeroMQ (ZMQ), rawi offers bot developers and cryptocurrency algo traders a highly flexible and dependable framework for streaming various types of market data.

## Features

- **Modular Design**: Combine various modules to create different configurations tailored to your specific needs.
- **High Reliability**: Achieve operational consistency through redundancy features.
- **Distributed System Support**: Run rawi on a single machine or as microservices to scale as per your performance requirements.

## Getting Started

### Installation

Clone the repository:

```bash
git clone https://github.com/Dhaneor/rawi.git
```

### Usage

To get started, simply combine the modules according to your requirements. The modules communicate over ZeroMQ connections, providing you with the flexibility to run the system either on a single machine or as a distributed system of microservices.

The Configuration can be changed in zmq_config.py. Most importantly, you can change the static address for the service registry (amanya) there. Otherwise your components won't connect or be
able to find each other.

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
other components can find it when starting up.

Every other component will send its address/endpoint information
during registration. That makes the system dynamic, and new components
can be added at any point. They can run anywhere, and the other
components will still be able to find them.

The same mechanism ensures that components have been offline temporarily,
will be re-integrated into the system. This makes the system very
resilient and self-healing in case of failures, server reboots or loss
of connectivity.

Every component can be started as script. But it is also possible to
combine components by means of a glue script - in case of multiple
available CPU cores or when expecting low load/throughput.

### Project Status

This is a work in progress, not ready for production!