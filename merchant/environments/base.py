from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from datetime import datetime
from datetime import timedelta
from typing import Any
from typing import Protocol

import gym
import pandas as pd
from gym.spaces import MultiDiscrete

from merchant.datasources.base import DataSource
from merchant.environments.portfolio import BasePortfolio


class BaseMarket:
    ...


class Symbol:
    ...


class Action(ABC):

    _dispatch_timestamp: int
    _execution_timestamp: None | int = None

    @abstractmethod
    def execute(self, pf: BasePortfolio, mt: BaseMarket) -> None:
        ...


class BuyAction(Action):

    _symbol: Symbol
    _amount: float

    def __init__(self, ts: int, symbol: Symbol, amount: float) -> None:
        self._dispatch_timestamp = ts
        self._symbol = symbol
        self._amount = amount

    def execute(self, pf: BasePortfolio, mt: BaseMarket) -> None:
        raise NotImplementedError


class SellAction(Action):

    _symbol: Symbol
    _amount: float

    def __init__(self, ts: int, symbol: Symbol, amount: float) -> None:
        self._dispatch_timestamp = ts
        self._symbol = symbol
        self._amount = amount

    def execute(self, pf: BasePortfolio, mt: BaseMarket) -> None:
        raise NotImplementedError


class NoAction(Action):
    def __init__(self, ts: int) -> None:
        self._dispatch_timestamp = ts

    def execute(self, pf: BasePortfolio, mt: BaseMarket) -> None:
        return


class FeePolicy(Protocol):
    def __call__(self, price: float, amount: float) -> float:
        ...


def get_default_policy(pct: float) -> FeePolicy:
    def policy(price: float, amount: float) -> float:
        return price * amount * pct

    return policy


class HistoricMarketEngine:
    def __init__(
        self,
        data: dict[str, pd.DataFrame] | type[DataSource],
        *,
        start: datetime,
        end: datetime,
        tick_interval: timedelta,
        fee_policy: FeePolicy,
        symbols: list[str] | None = None,
    ) -> None:
        if isinstance(data, dict):
            self._data = data
        elif issubclass(data, DataSource):
            if symbols is None:
                raise ValueError('tickers must be provided if data is a DataSource')
            self._data = data(symbols=symbols).data
        else:
            raise ValueError('data must be a dict or a DataSource')

        self._start = start
        self._end = end

        self._tick_interval = tick_interval
        self._current_time = self._start

        self._fee_policy = fee_policy

    @property
    def observation(self) -> dict[str, pd.DataFrame]:
        # return histroical data up to current time
        return self._data

    def get_quote(self, symbol: str) -> float:
        # return nearest historical data point at current time
        return 0

    def buy(self, symbol: str, amount: float) -> float:
        '''returns the cost of the order'''
        return 0

    def sell(self, symbol: str, amount: float) -> float:
        '''returns the revenue of the order'''
        return 0

    def step(self) -> None:
        self._current_time += self._tick_interval


class BaseMarketEnvironment(gym.Env):
    _action_history: list[Action]
    _symbols: list[Symbol]

    # add realtime view with plotly?
    metadata: dict[str, Any] = {'render_modes': []}

    def __init__(self, symbols: list[Symbol]) -> None:
        self._symbols = symbols
        self._action_history = []
        self.action_space = MultiDiscrete([3] * len(symbols))

    def reset(self) -> None:
        self._action_history = []

    def step(self, action: list[int]) -> tuple[Any, float, bool, dict[str, Any]]:
        ...
