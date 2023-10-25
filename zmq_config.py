#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides configuration information for components in data_sources.

The configuration objects are based on the BaseConfig class from
the zmqbricks package.

Created on Mon  Sep 18 19:17:23 2023

@author_ dhaneor
"""
from collections import namedtuple
from random import randint
from typing import Sequence

import config as cnf  # noqa: F401, E402

from zmqbricks.base_config import BaseConfig, ConfigT  # noqa: F401, E402
from zmqbricks.util.sockets import SockDef  # noqa: F401, E402
from util.random_names import random_elven_name as rand_name  # noqa: E402
from keys.amanya import public, private  # noqa: F401, E402

# encryption keys for the Central Service Registry (Amanya)
Keys = namedtuple("Keys", ["public", "private"])
amanya_keys = Keys(public=public, private=private)

BaseConfig.encrypt = True


# --------------------------------------------------------------------------------------
class Streamer(BaseConfig):
    """Configuration for the streamer component."""

    service_type: str = "streamer"
    port = randint(cnf.STREAMER_BASE_PORT, cnf.STREAMER_BASE_PORT + 50)

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.name = rand_name(gender='male')
        self.service_type = kwargs.get("service_type", Streamer.service_type)

        self.rgstr_with = ["csr", "collector"]

        self.publisher_addr = (f"tcp://127.0.0.1:{self.port}")

        self._endpoints = {
            "publisher": self.publisher_addr,
            "heartbeat": f"tcp://127.0.0.1:{self.port + 1}",
            "registration": f"tcp://127.0.0.1:{self.port + 2}",
        }

        self.service_registry = {
            "endpoint": "tcp://127.0.0.1:6000",
            "public_key": amanya_keys.public,
        }


class Collector(BaseConfig):
    """Configuration for the collector component."""

    service_type: str = "collector"
    no_consumer_no_subs: bool = False  # unsubscribe from upstream if no consumers
    max_cache_size: int = 1000  # for duplicate check
    kinsfolk_check_interval: int = 15  # seconds

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.name = rand_name(gender='male')
        self.service_type = kwargs.get("service_type", Collector.service_type)

        self.rgstr_with = ["csr"]

        self.service_registry = {
            "endpoint": "tcp://127.0.0.1:6000",
            "public_key": amanya_keys.public,
        }

        self.public_key, self.private_key = cnf.COLLECTOR_KEYS


class OhlcvRegistry(BaseConfig):
    """Configuration for the ohlcv registry"""

    service_type = "ohlcv_registry"

    PUBLISHER_ADDR = "inproc://craeft_pond"
    REPO_ADDR = cnf.ohlcv_repo_req
    cnf.collector_PUB_ADDR = cnf.collector_pub
    CONTAINER_SIZE_LIMIT = 1000

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = rand_name(gender="female")
        self.service_type = kwargs.get("service_type", OhlcvRegistry.service_type)
        self.rgstr_with = ["csr", "collector"]

        self.service_registry = {
            "endpoint": "tcp://127.0.0.1:6000",
            "public_key": amanya_keys.public,
        }

        self.port = randint(10000, 60000)

        self._endpoints = {
            "publisher": f"tcp://127.0.0.1:{self.port}",
            "heartbeat": f"tcp://127.0.0.1:{self.port + 1}",
            "registration": f"tcp://127.0.0.1:{self.port + 2}",
        }


class OhlcvRepository(BaseConfig):
    """Configuration for ohlcv_repository."""
    REQUESTS_ADDR = cnf.ohlcv_repo_req


class Amanya(BaseConfig):
    """Configuration for the Amanya component."""

    service_type: str = "Central Configuration Service"
    desc: str = "Central Configuration Service"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.name = "Amanya I."
        self.service_type = kwargs.get("service_type", Amanya.service_type)

        self.rgstr_with_csr = False  # this is the CSR
        self.rgstr_with = None

        self._endpoints: dict[str, str] = {
            "registration": "tcp://127.0.0.1:6000",
            "requests": "tcp://127.0.0.1:6001",
            "publisher": "tcp://127.0.0.1:6002",
            "heartbeat": "tcp://127.0.0.1:33333"
        }

        self.public_key = amanya_keys.public
        self.private_key = amanya_keys.private


# --------------------------------------------------------------------------------------
# ConfigT = TypeVar("ConfigT", ["BaseConfig"])

valid_service_types = {
    "streamer": Streamer,
    "collector": Collector,
    "ohlcv_registry": OhlcvRegistry,
    "ohlcv_repository": OhlcvRepository,
    "amanya": Amanya,
}


def get_config(
    service_type: str,
    exchange: str,
    markets: Sequence[str],
    sock_defs: Sequence[SockDef],
    **kwargs
) -> BaseConfig:
    """Get the configuration for a component.

    NOTE: Values for service_type, exchange, and market and sock_defs
    must be specified! See the definitions above for more information
    about possible options that can be provided as keyword argument!

    Parameters
    ----------
    service_type : str
        The type of the component.
    exchange : str
        The name of the exchange.
    market : str
        The name of the market.
    sock_defs : Sequence[SockDef]
        The socket definitions for the component.

    Returns
    -------
    ConfigT
        The configuration for the component.

    Raises
    ------
    ValueError
        If the service_type is not a valid service type.
    """
    if service_type not in valid_service_types:
        raise ValueError(f"Invalid service type: {service_type}")

    return valid_service_types[service_type](
        exchange=exchange or kwargs.pop("sock_defs"),
        markets=markets or kwargs.pop("markets"),
        sock_defs=sock_defs,
        **kwargs
    )


async def get_rgstr_info(
    service_type,
    exchange="kcuoin",
    market="spot"
) -> ConfigT | None:

    markets = [market] if isinstance(market, str) else market

    if service_type == "collector":
        cnf.collector_conf = Collector(exchange, markets)

        class C:
            service_name = "collector"
            endpoints = cnf.collector_conf.endpoints
            public_key = cnf.collector_conf.public_key

            def __repr__(self):
                return f"C(endpoint={self.endpoint}, public_key={self.public_key})"

        return C()


if __name__ == "__main__":
    # c = get_config("collector", "kucoin", ["spot"], [])

    # [print(f"{k} -> {v}") for k, v in vars(c).items()]

    # print(get_rgstr_info("collector", "kucoin", "spot"))

    print(cnf)
