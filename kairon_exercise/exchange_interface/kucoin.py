import asyncio
import websockets
import json
import aiohttp
import uuid
from typing import Any
from datetime import datetime
from kairon_exercise.exchange_interface.common_functions import (
    MarketData,
    TickerData,
    TradeData,
    calculate_spread_and_slippage,
)


class Kucoin:
    def __init__(self, markets: list[str], update_interval: float = 1):
        self.markets = ",".join(markets)
        self.public_token: str = ""
        self.servers: list[dict[str, str]] = []
        self.websocket: websockets.websocket = None
        self.conn_id: str = str(uuid.uuid4())[:8]
        self.tunnel_id: str = str(uuid.uuid4())[:8]
        self.market_data = {market: MarketData(TickerData(0, 0), [], "") for market in markets}
        self.interval: float = update_interval

    async def _connect(self) -> bool:
        kucoin_api_url = "https://api.kucoin.com/api/v1/bullet-public"
        async with aiohttp.ClientSession() as session, session.post(kucoin_api_url) as response:
            if response.status == 200:
                data = await response.json()
                self.public_token = data["data"]["token"]
                self.servers = data["data"]["instanceServers"]
            else:
                print(f"Failed to obtain the public token. Status code: {response.status}")

        if not self.public_token:
            return False

        kucoin_endpoint = f"{self.servers[0]['endpoint']}?token={self.public_token}&connectId={self.conn_id}"

        self.websocket = await websockets.connect(kucoin_endpoint)
        # async with websockets.connect(kucoin_endpoint) as self.websocket:
        auth_payload = {
            "id": "auth",
            "type": "token",
            "token": self.public_token,
        }
        await self.websocket.send(json.dumps(auth_payload))
        welcome = await self._wait_for_message_type("welcome", 1)

        if welcome is None:
            print("Timed out waiting for 'welcome' message.")
            return False

        order_payload = {
            "id": self.conn_id,
            "type": "subscribe",
            "privateChannel": False,
            "response": True,
            "topic": f"/market/ticker:{self.markets}",
        }
        await self.websocket.send(json.dumps(order_payload))

        ack = await self._wait_for_message_type("ack", 1)

        if ack is None:
            print("Timed out waiting for 'ack' message.")
            return False

        trade_payload = {
            "id": self.conn_id,
            "type": "subscribe",
            "privateChannel": False,
            "response": True,
            "topic": f"/market/match:{self.markets}",
        }
        await self.websocket.send(json.dumps(trade_payload))

        ack = await self._wait_for_message_type("ack", 1)

        if ack is None:
            print("Timed out waiting for 'ack' message.")
            return False

        _ = asyncio.create_task(self._ticker_update())

        return True

    async def _wait_for_message_type(self, message_type: str, timeout: float) -> dict[str, Any] | None:
        try:
            while True:
                message = await asyncio.wait_for(self.websocket.recv(), timeout)
                data = json.loads(message)
                if data.get("type") == message_type:
                    return data
        except asyncio.TimeoutError:
            return None

    async def _ticker_update(self) -> None:
        while True:
            try:
                message = await self.websocket.recv()
            except websockets.exceptions.ConnectionClosedOK as e:
                print(f"Exiting application: {e}")
                break

            data = json.loads(message)

            match data:
                case {"topic": topic, "data": {"bestAsk": best_ask, "bestBid": best_bid}}:
                    market_name = topic.split(":")[1]
                    self.market_data[market_name].ticker = TickerData(float(best_ask), float(best_bid))
                case {"topic": topic, "data": {"price": price, "side": "buy"}}:
                    market_name = topic.split(":")[1]
                    self.market_data[market_name].trade_history.append(TradeData(float(price)))

    async def get_datapoint(self):
        while True:
            await asyncio.sleep(self.interval)
            timestamp = datetime.now().isoformat()
            for market_data in self.market_data.values():
                market_data.timestamp = timestamp
            yield {
                "source": "kucoin",
                "data": {
                    market: calculate_spread_and_slippage(market_data)
                    for market, market_data in self.market_data.items()
                },
            }
            [market_data.trade_history.clear() for market_data in self.market_data.values()]

    async def __aenter__(self):
        if await self._connect():
            return self
        raise Exception("Failed to connect to KuCoin WebSocket")

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self.websocket:
            print("Exiting")
            await self.websocket.close()
