import pytest
from kairon_exercise.main import calculate_slippage, fetch_spread
import ccxt
from random import uniform, randint


# Fixture to generate mock order book data
@pytest.fixture
def mock_order_book():
    # Generate random bids and asks
    bids = [(uniform(0, 10000), uniform(0.1, 1000)) for _ in range(10)]
    asks = [(uniform(10000, 20000), uniform(0.1, 1000)) for _ in range(10)]
    return {"bids": bids, "asks": asks}


def test_fetch_spread(mock_order_book):
    exchange = ccxt.kucoin()
    exchange.fetch_order_book = lambda _: mock_order_book

    symbol = "BTC/USDT"
    _, spread = fetch_spread(exchange, symbol)

    # Assertions
    assert spread is None or spread >= 0, "Spread should be non-negative"


def test_calculate_slippage(mock_order_book):
    exchange = ccxt.kucoin()
    exchange.fetch_order_book = lambda _: mock_order_book

    symbol = "BTC/USDT"
    percentage_range = 0.02
    _, bid_slippage, ask_slippage = calculate_slippage(exchange, symbol, percentage_range)

    # Add assertions based on expected behavior
    assert bid_slippage is None or isinstance(bid_slippage, str), "Bid slippage should be valid"
    assert ask_slippage is None or isinstance(ask_slippage, str), "Ask slippage should be valid"


@pytest.fixture
def valid_mock_order_book():
    mid_price = 10000  # Example mid-price
    spread_percentage = 0.02  # 2% spread
    min_ask = mid_price * (1 + spread_percentage)
    max_bid = mid_price * (1 - spread_percentage)

    # Generate bids and asks
    bids = [(uniform(max_bid * 0.95, max_bid), randint(1, 10)) for _ in range(10)]
    asks = [(uniform(min_ask, min_ask * 1.05), randint(1, 10)) for _ in range(10)]

    return {"bids": sorted(bids, key=lambda x: x[0], reverse=True), "asks": sorted(asks, key=lambda x: x[0])}


def test_fetch_spread_with_valid_data(valid_mock_order_book):
    exchange = ccxt.kucoin()
    exchange.fetch_order_book = lambda _: valid_mock_order_book

    symbol = "BTC/USDT"
    _, spread = fetch_spread(exchange, symbol)

    # Validate the spread
    assert spread is not None, "Spread should not be None for valid data"
    assert spread >= 0, "Spread should be non-negative"
    # Check if spread is correctly calculated
    mid_price = (valid_mock_order_book["asks"][0][0] + valid_mock_order_book["bids"][0][0]) / 2
    assert (
        spread == ((valid_mock_order_book["asks"][0][0] - valid_mock_order_book["bids"][0][0]) / mid_price) * 100
    ), "Spread calculation error"


def test_calculate_slippage_with_valid_data(valid_mock_order_book):
    exchange = ccxt.kucoin()
    exchange.fetch_order_book = lambda _: valid_mock_order_book

    symbol = "BTC/USDT"
    percentage_range = 0.02  # 2% range
    _, bid_slippage, ask_slippage = calculate_slippage(exchange, symbol, percentage_range)

    # Validate the slippage
    # Assumptions: Bid slippage and ask slippage should not be None for a slippage larger than 2%
    assert bid_slippage is not None, "Bid slippage should not be None for valid data"
    assert ask_slippage is not None, "Ask slippage should not be None for valid data"
