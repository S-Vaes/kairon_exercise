import asyncio
from rich import print
import logging
from rich.logging import RichHandler
from kairon_exercise.exchange_interface.kucoin import Kucoin
from kairon_exercise.exchange_interface.binance import Binance
import typer
import tomllib
from aiostream import stream, streamcontext
from pathlib import Path
from kairon_exercise.database.db_functions import database_session, insert_calc_data


# Configure logging
FORMAT = "%(asctime)s: %(message)s"
logging.basicConfig(
    level="INFO", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)
log = logging.getLogger("rich")


def kucoin_market_translation(markets: list[str]) -> tuple[dict[str, str], list[str]]:
    """Translate Kucoin market names.

    Parameters
    ----------
    markets (list[str]): List of market names.

    Returns
    -------
    tuple[dict[str, str], list[str]]: A tuple containing a translation dictionary and translated market names.
    """
    translation_dict = {market.replace("/", "-"): market for market in markets}
    translated_markets = list(translation_dict.keys())
    return translation_dict, translated_markets


def binance_market_translation(markets: list[str]) -> tuple[dict[str, str], list[str]]:
    """Translate Binance market names.

    Parameters
    ----------
    markets (list[str]): List of market names.

    Returns
    -------
    tuple[dict[str, str], list[str]]: A tuple containing a translation dictionary and translated market names.
    """
    translation_dict = {market.replace("/", "").lower(): market for market in markets}
    translated_markets = list(translation_dict.keys())
    return translation_dict, translated_markets


async def start_sockets(markets: list[str], interval: float, db_file: str) -> None:
    """Start monitoring exchange WebSocket connections.

    Parameters
    ----------
    markets (list[str]): List of market names.
    interval (float): Update interval in seconds.
    db_file (str): Database file path.

    Notes
    -----
    This function establishes connections to Kucoin and Binance WebSocket APIs, retrieves market data, and inserts
    it into the database.
    """
    kucoin_translator, kucoin_markets = kucoin_market_translation(markets)
    binance_translator, binance_markets = binance_market_translation(markets)

    with database_session(f"sqlite:///{db_file}") as db_session:
        async with Kucoin(kucoin_markets, interval, "kucoin") as kucoin_ws, Binance(
            binance_markets, interval, "binance"
        ) as binance_ws:
            log.info("Connections successful, starting to monitor.")
            zipped_points = stream.zip(
                kucoin_ws.get_datapoint(), binance_ws.get_datapoint()
            )
            async with streamcontext(zipped_points) as zip_context:
                async for datapoints in zip_context:
                    kucoin_data, binance_data = datapoints
                    log.info("Adding new entry to the database.")
                    insert_calc_data(
                        db_session,
                        kucoin_data["source"],
                        kucoin_data["data"],
                        kucoin_translator,
                    )
                    insert_calc_data(
                        db_session,
                        binance_data["source"],
                        binance_data["data"],
                        binance_translator,
                    )

                    db_session.commit()


def _start(config_file: str):
    p = Path(config_file)
    with p.open(mode="rb") as f:
        config = tomllib.load(f)
        interval: float = 5.0
        markets = []
        if "markets" not in config:
            print("Markets not in config!")
            return

        if "db_file" not in config:
            print("Database file not in config!")
            return

        if "interval" in config:
            interval = config["interval"]

        markets = config["markets"]
        db_file = config["db_file"]
    asyncio.run(start_sockets(markets, interval, db_file))


def start():
    """Start the data monitoring process."""
    typer.run(_start)


if __name__ == "__main__":
    start()
