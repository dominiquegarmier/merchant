from __future__ import annotations

from typing import Any
from typing import TypeAlias
from typing import TypedDict

import pandas as pd

Ticker: TypeAlias = str


class Position:
    ticker: Ticker
    quantity: float

    history: pd.DataFrame

    def __init__(self, ticker: Ticker, quantity: float) -> None:
        self.ticker = ticker
        self.quantity = quantity

        self.history = pd.DataFrame(
            columns=['quantity', 'unit_price'], index=pd.DatetimeIndex([])
        )

    @property
    def value(self) -> float:
        return 0.0

    @property
    def cost(self) -> float:
        return (self.history['quantity'] * self.history['unit_price']).sum()  # type: ignore

    @property
    def avg_cost(self) -> float:
        return self.cost / self.quantity

    @property
    def pl_ratio(self) -> float | None:
        if self.quantity is None:
            return None
        return self.value / self.cost


class Positions:
    _positions: dict[Ticker, Position] = {}

    def __getitem__(self, ticker: Ticker) -> Position:
        if ticker not in self._positions:
            self._positions[ticker] = Position(ticker, 0)
        return self._positions[ticker]

    @property
    def value(self) -> float:
        return sum(position.value for position in self._positions.values())

    @property
    def cost(self) -> float:
        return sum(position.cost for position in self._positions.values())

    @property
    def pl_ratio(self) -> float | None:
        if self.cost == 0:
            return None
        return self.value / self.cost
