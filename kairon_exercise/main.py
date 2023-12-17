import asyncio
from rich import print
from rich.pretty import pprint
import websockets
from kairon_exercise.exchange_interface.kucoin import Kucoin
from kairon_exercise.exchange_interface.binance import Binance
import typer
import tomllib
from aiostream import stream, streamcontext


async def binance_websocket(market_symbols):
    # url = f"wss://stream.binance.com:9443/ws/{market_symbol}@depth@1000ms"
    # Construct the combined URL for multiple streams
    streams = []
    for symbol in market_symbols:
        streams.append(f"{symbol}@trade")
        # streams.append(f"{symbol}@depth@1000ms")
    streams_query = "/".join(streams)
    url = f"wss://stream.binance.com:9443/stream?streams={streams_query}"

    async with websockets.connect(url) as ws:
        while True:
            message = await ws.recv()
            print(f"Binance: {message}")


def convert_to_kucoin_market(markets: list[str]) -> list[str]:
    return [market.replace("/", "-") for market in markets]


def convert_to_binance_market(markets: list[str]) -> list[str]:
    return [market.replace("/", "").lower() for market in markets]


async def start_sockets(markets: list[str], interval: float) -> None:
    async with Kucoin(
        convert_to_kucoin_market(markets),
        interval,
    ) as kucoin_ws, Binance(
        convert_to_binance_market(markets),
        interval,
    ) as binance_ws:
        zipped_points = stream.zip(kucoin_ws.get_datapoint(), binance_ws.get_datapoint())
        async with streamcontext(zipped_points) as zip_context:
            async for datapoints in zip_context:
                print("Received ticker data:")
                print(datapoints)
            # for market, datapoint in datapoints.items():
            #     print(f"Market: {market}")
            #     pprint(datapoint)


def _start(config_file: str):
    with open(config_file, "rb") as f:
        config = tomllib.load(f)
        interval: float = 5.0
        markets = []
        if "markets" not in config:
            print("Markets not in config!")
            return

        if "interval" in config:
            interval = config["interval"]

        markets = config["markets"]
    asyncio.run(start_sockets(markets, interval))


def start():
    typer.run(_start)


if __name__ == "__main__":
    start()
