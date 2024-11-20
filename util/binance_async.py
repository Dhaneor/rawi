#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Nov 20 16:33:20 2024

@author dhaneor
"""
import asyncio
import logging
from binance import AsyncClient
from datetime import datetime
from math import ceil

logger = logging.getLogger(f"main.{__name__}")


class Binance:
    def __init__(self):
        self.name = 'binance'
        self.client = None
        self.timeframes = [
            "1m",
            "3m",
            "5m",
            "15m",
            "30m",
            "1h",
            "2h",
            "4h",
            "6h",
            "8h",
            "12h",
            "1d",
            "3d",
            "1w",
            "1M"
        ]

    async def initialize(self):
        self.client = await AsyncClient.create()

    async def close(self):
        if self.client:
            await self.client.close_connection()

    async def fetch_ohlcv(self, symbol, interval, limit=1000):
        if limit <= 1000:
            return await self.client.get_klines(
                symbol=symbol, interval=interval, limit=limit
            )
        else:
            # Calculate the number of parallel calls needed
            num_calls = ceil(limit / 1000)
            tasks = []

            # Calculate the end time (now) and start time
            end_time = int(datetime.now().timestamp() * 1000)  # in milliseconds

            # Calculate interval in milliseconds
            interval_ms = self._interval_to_milliseconds(interval)
            total_duration_ms = interval_ms * limit

            start_time = end_time - total_duration_ms

            for i in range(num_calls):
                # Calculate start and end for each batch
                batch_end = end_time - (i * 1000 * interval_ms)
                batch_start = max(start_time, batch_end - (1000 * interval_ms) + 1)

                task = asyncio.create_task(self.client.get_klines(
                    symbol=symbol,
                    interval=interval,
                    limit=1000,
                    startTime=int(batch_start),
                    endTime=int(batch_end)
                ))
                tasks.append(task)

            results = await asyncio.gather(*tasks)

            # Flatten the results and trim to the requested limit
            flattened = [candle for batch in reversed(results) for candle in batch]
            return flattened[:limit]

    def _interval_to_milliseconds(self, interval):
        """Convert a Binance interval string to milliseconds"""
        unit = interval[-1]
        if unit == 'm':
            return int(interval[:-1]) * 60 * 1000
        elif unit == 'h':
            return int(interval[:-1]) * 60 * 60 * 1000
        elif unit == 'd':
            return int(interval[:-1]) * 24 * 60 * 60 * 1000
        elif unit == 'w':
            return int(interval[:-1]) * 7 * 24 * 60 * 60 * 1000
        else:
            raise ValueError(f"Invalid interval: {interval}")

    async def fetch_status(self):
        return await self.client.get_system_status()

    async def fetch_time(self):
        return await self.client.get_server_time()


async def main():
    binance = Binance()

    try:
        await binance.initialize()
        ohlcv = await binance.fetch_ohlcv("BTCUSDT", "1h", 5000)
        print(f"OHLCV Data - got {len(ohlcv) if ohlcv else 'NO'} candles")

        # Test with different intervals
        ohlcv_15m = await binance.fetch_ohlcv("BTCUSDT", "15m", 1500)
        print(f"15m OHLCV Data - got {len(ohlcv_15m) if ohlcv_15m else 'NO'} candles")

        ohlcv_1d = await binance.fetch_ohlcv("BTCUSDT", "1d", 500)
        print(f"1d OHLCV Data - got {len(ohlcv_1d) if ohlcv_1d else 'NO'} candles")

        status = await binance.fetch_status()
        time = await binance.fetch_time()

        print(f"Server Status: {status}")
        print(f"Server Time: {time}")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    finally:
        await binance.close()

if __name__ == "__main__":
    asyncio.run(main())
