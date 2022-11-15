from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from functools import cached_property
from typing import Callable
from typing import Sequence
from typing import Type
from typing import TypeAlias

import pandas as pd

from merchant.dataset import DataSource


Ticker: TypeAlias = str


class MarketOrderType(Enum):
    BUY = 'BUY'
    SELL = 'SELL'


@dataclass
class MarketOrder:
    ticker: Ticker
    quantity: float
    type: MarketOrderType


class MarketInterface:
    pass


class MarketError(Exception):
    ...


class SimulationNotRunning(MarketError):
    ...


class SimulationAlreadyRunning(MarketError):
    ...


class HistoricalMarket(MarketInterface):
    is_running: bool = False

    def __init__(
        self,
        start: int,
        end: int,
        tickers: Sequence[Ticker],
        datasource: type[DataSource] = DataSource,
    ) -> None:
        super().__init__()
        self._dataset = datasource()

    @cached_property
    def tickers(self) -> list[Ticker]:
        return []

    def get_quote(self, ticker: Ticker) -> float:
        ...

    def post_order(self, order: MarketOrder) -> bool:
        return False

    def step(self, timedelta: int | pd.Timedelta) -> None:
        if not self.is_running:
            raise SimulationNotRunning
        if isinstance(timedelta, int):
            timedelta = pd.Timedelta(timedelta, unit='D')
        self._timestamp += timedelta

    def start(self) -> None:
        if self.is_running:
            raise SimulationAlreadyRunning
        self.is_running = True

    def stop(self) -> None:
        if not self.is_running:
            raise SimulationNotRunning
        self.is_running = False

    def __enter__(self) -> HistoricalMarket:
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.stop()
