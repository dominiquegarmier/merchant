from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from functools import cached_property
from typing import Literal

import pandas as pd

from merchant.data.dataset import Dataset
from merchant.data.tickers import Ticker


@dataclass
class MarketOrder:
    ticker: Ticker
    quantity: float
    type: Literal['BUY', 'SELL']
    limit: float | None = None


@dataclass
class MarketOrderExecution:
    order: MarketOrder
    success: bool
    price: float | None
    quantity: float | None


class MarketError(Exception):
    ...


class SimulationNotRunning(MarketError):
    ...


class SimulationRunning(MarketError):
    ...


class HistoricalMarket:
    _is_running: bool = False

    _start: int
    _end: int
    _step: int
    _timestamp: int

    def __init__(
        self,
        start: int,
        end: int,
        step: int,
        tickers: Sequence[Ticker],
        dataset: type[Dataset] = Dataset,
    ) -> None:
        super().__init__()
        self._dataset = dataset(tickers=tickers)

        self._start = start
        self._end = end
        self._step = step

        self._timestamp = start

    @cached_property
    def tickers(self) -> list[Ticker]:
        return []

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def timestamp(self) -> int:
        '''
        returns:
            int: timestamp in nanoseconds of the current simulation step
        '''
        return self._timestamp

    def start(self) -> None:
        if self._is_running:
            raise SimulationRunning
        self._is_running = True

    def stop(self) -> None:
        if not self.is_running:
            raise SimulationNotRunning
        self._is_running = False

    def reset(self) -> None:
        if self._is_running:
            raise SimulationRunning
        self._timestamp = self._start

    def step(self) -> bool:
        self._timestamp += self._step
        return self._timestamp < self._end

    def get_quotes(self, tickers: Sequence[Ticker]) -> pd.DataFrame:
        raise NotImplementedError

    def _exectue_order(self, ord: MarketOrder) -> MarketOrderExecution:
        raise NotImplementedError

    def execute_orders(self, ords: Sequence[MarketOrder]) -> list[MarketOrderExecution]:
        '''
        execute a sequence of market orders

        sell orders are executed at or above the limit price
        buy orders are executed at or below the limit price
        '''
        return [self._exectue_order(ord) for ord in ords]
