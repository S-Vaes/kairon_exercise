import websockets
import json
from kairon_exercise.exchange_interface.common_functions import (
    TickerData,
    TradeData,
    ExchangeBase,
)


class Binance(ExchangeBase):
    """Class representing a Binance WebSocket connection for market data.

    Args:
    ----
        markets (list[str]): List of market symbols to subscribe to.
        interval (float): Time interval in seconds between data updates.

    Attributes:
    ----------
        markets (list[str]): List of market symbols.
        websocket (websockets.websocket): WebSocket connection to Binance.
        market_data (dict[str, MarketData]): Dictionary to store market data.
        interval (float): Time interval in seconds between data updates.

    Methods:
    -------
        _connect(self) -> bool:
            Connect to the Binance WebSocket.

        _update_market_data(self):
            Continuously update market data from WebSocket messages.

    Example:
    -------
    async with Binance(["BTCUSDT", "ETHUSDT"], 5.0) as binance_ws:
        async for datapoint in binance_ws.get_datapoint():
            print(datapoint)
    """

    def __init__(self, markets: list[str], interval: float, source: str) -> None:
        super().__init__(markets, interval, source)
        self.markets = markets

    async def _connect(self) -> bool:
        """Connect to the Binance WebSocket.

        Returns
        -------
            bool: True if the connection is successful, False otherwise.

        """
        binance_url = "wss://stream.binance.com:9443/stream?streams="
        streams = []
        for market in self.markets:
            streams.append(f"{market.lower()}@ticker")
            streams.append(f"{market.lower()}@trade")
        streams_query = "/".join(streams)
        binance_url += streams_query
        self.websocket = await websockets.connect(binance_url)

        if not self.websocket:
            print("Failed to connect to Binance WebSocket.")
            return False

        return True

    async def _update_market_data(self):
        """Continuously update market data from WebSocket messages."""
        while True:
            try:
                message = await self.websocket.recv()
            except websockets.exceptions.ConnectionClosedOK as e:
                print(f"Exiting application: {e}")
                break

            data = json.loads(message)
            if "stream" in data:
                stream = data["stream"]
                if "@ticker" in stream:
                    market_name = stream.split("@")[0]
                    ticker_data = TickerData(
                        float(data["data"]["a"]), float(data["data"]["b"])
                    )
                    self.market_data[market_name].ticker = ticker_data
                elif "@trade" in stream:
                    market_name = stream.split("@")[0]
                    trade_data = TradeData(float(data["data"]["p"]))
                    self.market_data[market_name].trade_history.append(trade_data)
