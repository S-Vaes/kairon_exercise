"""Module for common exchange functions."""
from dataclasses import dataclass
import asyncio
import json
from datetime import datetime
from typing import Any
from collections.abc import Coroutine
import logging
import websockets


@dataclass
class TickerData:
    """Represents ticker data with best ask and best bid prices."""

    best_ask: float
    best_bid: float


@dataclass
class TradeData:
    """Represents trade data with a price."""

    price: float


@dataclass
class MarketData:
    """Represents market data with ticker, trade history, and a timestamp."""

    ticker: TickerData | None
    trade_history: list[TradeData]
    timestamp: str


@dataclass
class SpreadData:
    """Represents spread data with a spread value."""

    spread: float | None


@dataclass
class SlippageData:
    """Represents slippage data with a slippage value."""

    slippage: float | None


@dataclass
class CalcData:
    """Represents calculated data with spread, slippage, trading volume, and a timestamp."""

    spread: SpreadData
    slippage: SlippageData
    volume: int
    timestamp: str


def calculate_spread_and_slippage(
    market_data: MarketData, market: str, source: str
) -> CalcData:
    """Calculate the spread and slippage from market data.

    Args:
    ----
        market_data (MarketData): Market data containing ticker and trade history.
        market (str): Market name
        source (str): Source exchange name

    Returns:
    -------
        CalcData: Calculated spread and slippage data.
    """
    log = logging.getLogger("rich")
    ticker_data = market_data.ticker
    trade_history = market_data.trade_history

    if not ticker_data:
        # Handle the case where best_ask is zero, e.g., set spread and slippage to None
        spread = None
        slippage = None
        log.info(f"No ticker data for ({source}, {market}).")
        return CalcData(
            SpreadData(spread),
            SlippageData(slippage),
            len(trade_history),
            market_data.timestamp,
        )

    spread = (
        (ticker_data.best_ask - ticker_data.best_bid) / ticker_data.best_ask * 100
    )  # Spread in %
    # Calculate the slippage for trades within 2% of the ask price
    slippage_list = []
    slippage = None
    for trade in trade_history:
        slippage = (
            (ticker_data.best_ask - trade.price) / trade.price
        ) * 100  # Slippage in %
        if slippage <= 2.0:
            slippage_list.append(slippage)

    if slippage is not None and len(slippage_list) > 0:
        slippage = sum(slippage_list) / len(slippage_list)
    return CalcData(
        SpreadData(spread),
        SlippageData(slippage),
        len(trade_history),
        market_data.timestamp,
    )


class ExchangeBase:
    """Base class for exchange implementations."""

    def __init__(self, markets: list[str] | str, interval: float, source: str):
        """Initialize an ExchangeBase instance.

        Parameters
        ----------
            markets (Union[List[str], str]): List of markets or a single market as a string.
            interval (float): Interval in seconds for updating data.
            source (str): Source name for the exchange.

        Attributes
        ----------
            websocket (websockets.websocket): WebSocket connection object.
            market_data (Dict[str, MarketData]): Dictionary to store market data.
            interval (float): Update interval in seconds.
            source (str): Source name for the exchange.
        """
        self.websocket: websockets.websocket = None
        self.market_data = {market: MarketData(None, [], "") for market in markets}
        self.interval: float = interval
        self.source: str = source

    async def _connect(self) -> bool:
        """Establish a WebSocket connection.

        Raises
        ------
            NotImplementedError: Subclasses must implement _connect method.

        Returns
        -------
            bool: True if the connection is successful, False otherwise.
        """
        raise NotImplementedError("Subclasses must implement _connect method")

    async def _update_market_data(self) -> None:
        """Update market data from the WebSocket stream.

        Raise
        -----
            NotImplementedError: Subclasses must implement _update_market_data method.
        """
        raise NotImplementedError(
            "Subclasses must implement _update_market_data method"
        )

    async def _wait_for_message_type(
        self, message_type: str, timeout: float
    ) -> dict[str, Any] | None:
        """Wait for a specific message type from the WebSocket stream.

        Parameters
        ----------
            message_type (str): The type of message to wait for.
            timeout (float): Maximum time (in seconds) to wait for the message.

        Returns
        -------
            dict[str, Any] | None: The message data if received, None if timed out.
        """
        try:
            while True:
                message = await asyncio.wait_for(self.websocket.recv(), timeout)
                data = json.loads(message)
                if data.get("type") == message_type:
                    return data
        except asyncio.TimeoutError:
            return None

    async def get_datapoint(
        self,
    ) -> Coroutine[dict[str, str | dict[str, "CalcData"]], None, None]:
        """Yield market data points at regular intervals.

        Yields
        ------
            dict[str, dict[str, CalcData]]: A dictionary containing the source ("binance") and market data.
        """
        while True:
            await asyncio.sleep(self.interval)
            timestamp = datetime.now().isoformat()

            for market_data in self.market_data.values():
                market_data.timestamp = timestamp

            yield {
                "source": self.source,
                "data": {
                    market: calculate_spread_and_slippage(
                        market_data, market, self.source
                    )
                    for market, market_data in self.market_data.items()
                },
            }

            for market_data in self.market_data.values():
                market_data.ticker = None
                market_data.trade_history.clear()

    async def __aenter__(self) -> "ExchangeBase":
        """Enter the asynchronous context manager.

        Raises
        ------
            Exception: Raised if the connection to the WebSocket fails.

        Returns
        -------
            ExchangeBase: The instance of the ExchangeBase class.
        """
        if await self._connect():
            self._task = asyncio.create_task(self._update_market_data())
            return self
        raise Exception("Failed to connect to the WebSocket")

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Exit the asynchronous context manager and close the WebSocket connection.

        Parameters
        ----------
            exc_type: The type of exception (not used).
            exc_value: The exception value (not used).
            traceback: The traceback information (not used).
        """
        if self.websocket:
            print("Exiting")
            await self.websocket.close()
