import os
import sys
import asyncio
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
from ..ohlcv_repository import ohlcv_repository  # noqa: E402

# Configure logging for verbose output
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("test_ohlcv_repository")


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
            logger.debug(f"Sending request: {request}")
            await socket.send_json(request)
            reply = await socket.recv_json()
            logger.debug(f"Received response: {reply}")
            responses.append(reply)

        socket.close()
        return responses

    def test_valid_requests(self):
        """Test sending valid requests for different cryptocurrency pairs."""
        async def run_test():
            requests = [
                {"action": "fetch_ohlcv", "exchange": "binance", "symbol": "BTC/USDT"},
                {"action": "fetch_ohlcv", "exchange": "kraken", "symbol": "ETH/USD"},
                {"action": "fetch_ohlcv", "exchange": "coinbasepro", "symbol": "LTC/USD"},
            ]
            responses = await self.zmq_client(requests)

            for response in responses:
                self.assertIsInstance(response, list, "Response should be a list of OHLCV data")

        self.loop.run_until_complete(run_test())

    def test_invalid_exchange(self):
        """Test sending requests with an invalid exchange name."""
        async def run_test():
            requests = [
                {"action": "fetch_ohlcv", "exchange": "fake_exchange", "symbol": "BTC/USDT"}
            ]
            responses = await self.zmq_client(requests)

            for response in responses:
                self.assertIn("error", response, "Invalid exchange should return an error")

        self.loop.run_until_complete(run_test())

    def test_non_existent_pair(self):
        """Test sending requests with a non-existent trading pair."""
        async def run_test():
            requests = [
                {"action": "fetch_ohlcv", "exchange": "binance", "symbol": "FAKE/FAKE"}
            ]
            responses = await self.zmq_client(requests)

            for response in responses:
                self.assertIn("error", response, "Non-existent pair should return an error")

        self.loop.run_until_complete(run_test())


if __name__ == '__main__':
    unittest.main()
