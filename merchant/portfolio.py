from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go

from merchant.dataset.datasource import Dataset
from merchant.utils.helpers import Ticker

CASH: Ticker = Ticker(ticker='$USD', exchange='__CASH__')


def bind_datasource(dataset: Dataset) -> None:
    Portfolio.Meta.datasource = dataset


class Position:
    ticker: Ticker
    quantity: float

    history: pd.DataFrame

    _datasource: Dataset | None

    def __init__(
        self, ticker: Ticker, quantity: float, datasource: Dataset | None
    ) -> None:
        self.ticker = ticker
        self.quantity = quantity

        self._datasource = datasource

        self.history = pd.DataFrame(
            columns=['quantity', 'unit_price'], index=pd.DatetimeIndex([])
        )

    def __repr__(self) -> str:
        return f'{self.ticker} {self.quantity}'

    @property
    def value(self) -> float:
        raise NotImplementedError


class Positions:
    _positions: dict[Ticker, Position] = {}
    _datasource: Dataset

    def __init__(self, datasource: Dataset) -> None:
        self._datasource = datasource

    def __getitem__(self, ticker: Ticker) -> Position:
        if ticker not in self._positions:
            self._positions[ticker] = Position(
                ticker=ticker, quantity=0, datasource=self._datasource
            )
        return self._positions[ticker]

    @property
    def value(self) -> float:
        return sum(position.value for position in self._positions.values())


class CashPosition(Position):
    def __init__(self, quantity: float) -> None:
        super().__init__(CASH, quantity, None)

    @property
    def value(self) -> float:
        return self.quantity


class Portfolio:
    class Meta:
        datasource: Dataset
        timekeeper: None

    _cash: CashPosition
    _positions: Positions

    _start_cash: float
    _history: pd.DataFrame  # value history

    def __init__(self, start_cash: float) -> None:
        self._start_cash = start_cash
        self._cash = CashPosition(quantity=start_cash)

        self._positions = Positions(datasource=self.Meta.datasource)
        self._history = pd.DataFrame(columns=['open', 'high', 'low', 'close'])

    @property
    def value(self) -> float:
        return self._cash.value + self._positions.value

    @property
    def cash_value(self) -> float:
        return self._cash.value

    @property
    def positions_value(self) -> float:
        return self._positions.value

    @property
    def pl_ratio(self) -> float | None:
        if self._start_cash == 0:
            return None
        return (self.value - self._start_cash) / self._start_cash

    @property
    def volatility(self) -> float:
        raise NotImplementedError

    @property
    def cagr(self) -> float:
        raise NotImplementedError

    @property
    def jensen_alpha(self) -> float:
        raise NotImplementedError

    @property
    def sharpe_ratio(self) -> float:
        raise NotImplementedError

    @property
    def calmar_ratio(self) -> float:
        raise NotImplementedError

    @property
    def sortino_ratio(self) -> float:
        raise NotImplementedError

    @property
    def treynor_ratio(self) -> float:
        raise NotImplementedError

    @property
    def max_drawdown_ratio(self) -> float:
        raise NotImplementedError

    def __repr__(self) -> str:
        return 'Portfolio: ...'

    def get_plot(self, *args, **kwargs) -> go.Figure:
        return go.Figure(
            data=go.Ohlc(
                open=self._history['open'],
                high=self._history['high'],
                low=self._history['low'],
                close=self._history['close'],
            )
        )
