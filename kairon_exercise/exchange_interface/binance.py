import asyncio
import websockets
import json
from datetime import datetime
from kairon_exercise.exchange_interface.common_functions import (
    MarketData,
    TickerData,
    TradeData,
    calculate_spread_and_slippage,
)


class Binance:
    def __init__(self, markets: list[str], update_interval: float = 1):
        self.markets = markets
        self.websocket: websockets.websocket = None
        self.market_data = {market: MarketData(TickerData(0, 0), [], "") for market in markets}
        self.interval: float = update_interval

    async def _connect(self) -> bool:
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

        _ = asyncio.create_task(self._update_market_data())
        return True

    async def _update_market_data(self):
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
                    ticker_data = TickerData(float(data["data"]["a"]), float(data["data"]["b"]))
                    self.market_data[market_name].ticker = ticker_data
                elif "@trade" in stream:
                    market_name = stream.split("@")[0]
                    trade_data = TradeData(float(data["data"]["p"]))
                    self.market_data[market_name].trade_history.append(trade_data)

    async def get_datapoint(self):
        while True:
            await asyncio.sleep(self.interval)
            timestamp = datetime.now().isoformat()
            for market_data in self.market_data.values():
                market_data.timestamp = timestamp
            yield {
                "source": "binance",
                "data": {
                    market: calculate_spread_and_slippage(market_data)
                    for market, market_data in self.market_data.items()
                },
            }
            [market_data.trade_history.clear() for market_data in self.market_data.values()]

    async def __aenter__(self):
        if await self._connect():
            return self
        raise Exception("Failed to connect to Binance WebSocket")

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self.websocket:
            print("Exiting")
            await self.websocket.close()
