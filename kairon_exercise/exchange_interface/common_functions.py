from dataclasses import dataclass


@dataclass
class TickerData:
    best_ask: float
    best_bid: float


@dataclass
class TradeData:
    price: float


@dataclass
class MarketData:
    ticker: TickerData
    trade_history: list[TradeData]
    timestamp: str


@dataclass
class SpreadData:
    spread: float | None


@dataclass
class SlippageData:
    slippage: float | None


@dataclass
class CalcData:
    spread: SpreadData
    slippage: SlippageData
    timestamp: str


def calculate_spread_and_slippage(market_data: MarketData) -> CalcData:
    ticker_data = market_data.ticker
    trade_history = market_data.trade_history

    if ticker_data.best_ask == 0:
        # Handle the case where best_ask is zero, e.g., set spread and slippage to None
        spread = None
        slippage = None
    else:
        spread: float = (ticker_data.best_ask - ticker_data.best_bid) / ticker_data.best_ask * 100  # Spread in %

    # Calculate the slippage for trades within 2% of the ask price
    slippage_list = []
    slippage: float | None = None
    for trade in trade_history:
        if trade.price == 0:
            continue
        slippage = ((ticker_data.best_ask - trade.price) / trade.price) * 100  # Slippage in %
        if slippage <= 2.0:
            slippage_list.append(slippage)

    if len(slippage_list):
        slippage = sum(slippage_list) / len(slippage_list)
        
    return CalcData(SpreadData(spread), SlippageData(slippage), market_data.timestamp)
