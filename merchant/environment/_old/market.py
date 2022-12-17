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
    quantity: int
    type: Literal['BUY', 'SELL']
    limit: float | None = None


@dataclass
class MarketOrderExecution:
    order: MarketOrder
    success: bool
    price: float | None
    quantity: int | None


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

    _dataframe: pd.DataFrame
    _step_idx: int

    def __init__(
        self,
        start: int,
        end: int,
        tickers: Sequence[Ticker],
        dataset: type[Dataset] = Dataset,
    ) -> None:
        super().__init__()
        self._dataset = dataset(tickers=tickers)

        self._start = start
        self._end = end

        self._dataframe = self._dataset[start:end]
        self._step_idx = 0

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
        return self._dataframe.index[self._step_idx].value  # type: ignore

    def stop(self) -> None:
        if not self.is_running:
            raise SimulationNotRunning
        self._is_running = False

    def reset(self) -> None:
        if self._is_running:
            raise SimulationRunning
        self._is_running = True
        self._step_idx = 0

    def step(self) -> bool:
        self._step_idx += 1
        return self._step_idx < len(self._dataframe)

    def __getitem__(self, ticker: Ticker) -> float:
        '''get the current price of a ticker'''
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
