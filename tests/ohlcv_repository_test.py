import os
import sys
import asyncio
import json
import unittest
import zmq
import zmq.asyncio
import logging

# -----------------------------------------------------------------------------
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
# -----------------------------------------------------------------------------

# Assuming your `ohlcv_repository` function is imported from the module
from ohlcv_repository import ohlcv_repository  # noqa: E402

# Configure logging for verbose output
logger = logging.getLogger("main")
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()

formatter = logging.Formatter(
    "%(asctime)s - %(name)s.%(funcName)s.%(lineno)d  - [%(levelname)s]: %(message)s"
)
ch.setFormatter(formatter)

logger.addHandler(ch)


class TestOHLCVRepository(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Start the OHLCV repository in an asyncio loop in the background
        cls.ctx = zmq.asyncio.Context()
        cls.addr = "tcp://127.0.0.1:5555"  # Bind address for the test
        cls.loop = asyncio.get_event_loop()

        cls.repo_task = cls.loop.create_task(ohlcv_repository(cls.ctx, cls.addr))

    @classmethod
    def tearDownClass(cls):
        # Stop the OHLCV repository and clean up
        cls.repo_task.cancel()
        cls.loop.run_until_complete(cls.repo_task)
        cls.ctx.term()

    async def zmq_client(self, requests):
        """Function to simulate a client sending requests via ZeroMQ."""
        socket = self.ctx.socket(zmq.REQ)  # REQ socket type for client
        socket.connect(self.addr)

        responses = []
        for request in requests:
            # Convert request to JSON string and then encode it to bytes
            request_json = json.dumps(request).encode('utf-8')

            await socket.send(request_json)
            logger.debug(f"Sent request: {request_json}")

            reply = await socket.recv_json()
            logger.debug(f"Received response: {reply}\n")

            responses.append(reply)

        socket.close()
        return responses

    def test_valid_requests(self):
        """Test sending valid requests for different cryptocurrency pairs."""
        logger.debug("----------------------------------------------------------------")
        logger.debug(">>>>> Test sending valid requests")

        async def run_test():
            requests = [
                {"exchange": "binance", "symbol": "BTC/USDT", "interval": "1d"},
                {"exchange": "kraken", "symbol": "ETH/USD", "interval": "4h"},
                {"exchange": "kucoin", "symbol": "LTC/USDT", "interval": "1h"},
            ]
            responses = await self.zmq_client(requests)

            for response in responses:
                self.assertTrue(
                    response["success"],
                    f"Valid request should return success=True - {response}"
                )
                self.assertIsInstance(
                    response["data"], list, "Response should be a list of OHLCV data"
                )

        self.loop.run_until_complete(run_test())

    def test_invalid_exchange(self):
        """Test sending requests with an invalid exchange name."""
        logger.debug("----------------------------------------------------------------")
        logger.debug(">>>>> Test sending requests with an invalid exchange")

        async def run_test():
            requests = [
                {"exchange": "fake_exchange", "symbol": "BTC/USDT", "interval": "1d"}
            ]
            responses = await self.zmq_client(requests)

            for response in responses:
                self.assertFalse(
                    response["success"], "Invalid exchange should return success=False"
                )
                self.assertTrue(
                    response["errors"]["exchange_error"],
                    "Invalid exchange should return an exchange_error",
                )

        self.loop.run_until_complete(run_test())

    def test_non_existent_pair(self):
        """Test sending requests with a non-existent trading pair."""
        logger.debug("----------------------------------------------------------------")
        logger.debug(">>>>> Test sending requests with a non-existent pair")

        async def run_test():
            requests = [
                {"exchange": "binance", "symbol": "FAKE/FAKE", "interval": "1d"}
            ]
            responses = await self.zmq_client(requests)

            for response in responses:
                self.assertFalse(
                    response["success"],
                    "Non-existent trading pair should return success=False"
                )
                self.assertTrue(
                    response["errors"]["symbol_error"],
                    "Non-existent pair should return a symbol_error",
                )

        self.loop.run_until_complete(run_test())

    def test_invalid_exchange_type(self):
        """Test sending a request where exchange is not a string."""
        async def run_test():
            requests = [
                {"exchange": 123, "symbol": "BTC/USDT", "interval": "1d"}
            ]
            responses = await self.zmq_client(requests)

            for response in responses:
                self.assertFalse(
                    response["success"],
                    "Invalid exchange type should return success=False"
                )
                self.assertTrue(
                    response["errors"]["exchange_error"],
                    "Invalid exchange type should trigger exchange_error"
                )

        self.loop.run_until_complete(run_test())

    def test_invalid_symbol_type(self):
        """Test sending a request where symbol is not a string."""
        async def run_test():
            requests = [
                {"exchange": "binance", "symbol": 12345, "interval": "1d"}
            ]
            responses = await self.zmq_client(requests)

            for response in responses:
                self.assertFalse(
                    response["success"],
                    "Invalid symbol type should return success=False"
                )
                self.assertTrue(
                    response["errors"]["symbol_error"],
                    "Invalid symbol type should trigger symbol_error"
                )

        self.loop.run_until_complete(run_test())

    def test_invalid_interval_type(self):
        """Test sending a request where interval is not a string."""
        async def run_test():
            requests = [
                {"exchange": "binance", "symbol": "BTC/USDT", "interval": 1}
            ]
            responses = await self.zmq_client(requests)

            for response in responses:
                self.assertFalse(
                    response["success"],
                    "Invalid interval type should return success=False"
                )
                self.assertTrue(
                    response["errors"]["interval_error"],
                    "Invalid interval type should trigger interval_error"
                )

        self.loop.run_until_complete(run_test())


if __name__ == '__main__':
    unittest.main()
