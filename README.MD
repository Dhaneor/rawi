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
git clone https://github.com/yourusername/rawi.git
```

### Usage

To get started, simply combine the modules according to your requirements. The modules communicate over ZeroMQ connections, providing you with the flexibility to run the system either on a single machine or as a distributed system of microservices.

The Configuration can be changed in zmq_config.py. Most importantly, you can change the static address for the service registry (amanya) there. Otherwise your components won't connect or be
able to find each other.


### Project Status

This is a work in progress, not ready for production!