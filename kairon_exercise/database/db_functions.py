from sqlmodel import Field, SQLModel, create_engine, Session
from datetime import datetime
from contextlib import contextmanager
from kairon_exercise.exchange_interface.common_functions import CalcData
import logging


class CalcDataModel(SQLModel, table=True):
    """Model for storing spread and slippage calculations in the database."""

    db_id: int = Field(default=None, primary_key=True)
    exchange: str
    market: str
    spread: float | None
    slippage: float | None
    volume: int
    timestamp: datetime


@contextmanager
def database_session(database_url: str):
    """Create a context manager for database sessions.

    Parameters
    ----------
    database_url (str): URL to connect to the database.

    Yields
    ------
    Session: A database session.
    """
    engine = create_engine(database_url)
    SQLModel.metadata.create_all(engine)

    with Session(engine) as session:
        yield session


def insert_calc_data(
    session: Session,
    exchange: str | dict[str, CalcData],
    data: str | dict[str, CalcData],
    translation: dict[str, str],
) -> None:
    """Insert spread and slippage calculations into the database.

    Parameters
    ----------
    session (Session): Database session.
    exchange (str): Name of the exchange.
    data (Dict[str, CalcData]): Dictionary of calculation data.
    translation (Dict[str, str]): Mapping of market names.

    Notes
    -----
    This function inserts data into the `CalcDataModel` table in the database.
    """
    log = logging.getLogger("rich")

    if isinstance(data, str) or (
        isinstance(exchange, dict)
        and all(
            isinstance(key, str) and isinstance(value, CalcData)
            for key, value in exchange.items()
        )
    ):
        log.info("Incompatible exchange or data type.")
        return

    for market, calc_data in data.items():
        session.add(
            CalcDataModel(
                exchange=exchange,
                market=translation[market],
                spread=calc_data.spread.spread,
                slippage=calc_data.slippage.slippage if calc_data.slippage else None,
                volume=calc_data.volume,
                timestamp=datetime.fromisoformat(calc_data.timestamp),
            ),
        )
