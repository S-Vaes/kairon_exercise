import websockets
import json
import aiohttp
import uuid
from kairon_exercise.exchange_interface.common_functions import (
    TickerData,
    TradeData,
    ExchangeBase,
)


class Kucoin(ExchangeBase):
    """Kucoin exchange implementation."""

    def __init__(self, markets: list[str], interval: float, source: str):
        """Initialize a Kucoin instance.

        Parameters
        ----------
        markets (List[str]): List of market symbols.
        interval (float): Interval in seconds for updating data.
        source (str): Source name for the exchange.

        Attributes
        ----------
        markets (str): Comma-separated market symbols.
        public_token (str): Public token for authentication.
        conn_id (str): Connection ID.

        Methods
        -------
        _connect(self) -> bool:
            Connect to the Binance WebSocket.

        _update_market_data(self):
            Continuously update market data from WebSocket messages.

        Example
        -------
        async with Kucoin(["btc-usdt", "eth-usdt"], 5.0) as binance_ws:
            async for datapoint in kucoin_ws.get_datapoint():
                print(datapoint)
        """
        super().__init__(markets, interval, source)
        self.markets = ",".join(markets)
        self.public_token: str = ""
        self.conn_id: str = str(uuid.uuid4())[:8]

    async def _connect(self) -> bool:
        """Establish a WebSocket connection to Kucoin.

        Returns
        -------
        bool: True if the connection is successful, False otherwise.
        """
        kucoin_api_url = "https://api.kucoin.com/api/v1/bullet-public"
        async with aiohttp.ClientSession() as session, session.post(
            kucoin_api_url
        ) as response:
            if response.status == 200:
                data = await response.json()
                self.public_token = data["data"]["token"]
                servers = data["data"]["instanceServers"]
            else:
                print(
                    f"Failed to obtain the public token. Status code: {response.status}"
                )

        if not self.public_token:
            return False

        kucoin_endpoint = f"{servers[0]['endpoint']}?token={self.public_token}&connectId={self.conn_id}"

        self.websocket = await websockets.connect(kucoin_endpoint)

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

        return True

    async def _update_market_data(self) -> None:
        """Update market data from the WebSocket stream.

        Raises
        ------
        websockets.exceptions.ConnectionClosedOK: If the WebSocket connection is closed gracefully.
        """
        while True:
            try:
                message = await self.websocket.recv()
            except websockets.exceptions.ConnectionClosedOK as e:
                print(f"Exiting application: {e}")
                break

            data = json.loads(message)

            match data:
                case {
                    "topic": topic,
                    "data": {"bestAsk": best_ask, "bestBid": best_bid},
                }:
                    market_name = topic.split(":")[1]
                    self.market_data[market_name].ticker = TickerData(
                        float(best_ask), float(best_bid)
                    )
                case {"topic": topic, "data": {"price": price, "side": "buy"}}:
                    market_name = topic.split(":")[1]
                    self.market_data[market_name].trade_history.append(
                        TradeData(float(price))
                    )
