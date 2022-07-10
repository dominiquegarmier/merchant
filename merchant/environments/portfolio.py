from __future__ import annotations

from collections import deque
from datetime import datetime
from typing import Any

import pandas as pd


class Position:
    __instances__: dict[str, Position] = {}

    _symbol: str
    _price: float
    _holding: float
    _profit: float  # realized running profit

    # used for p&l calculation
    _buy_history: deque[tuple[float, float]]  # tuple[quantity, price]
    _history: pd.DataFrame
    _open_date: datetime | None

    def __init__(self, symbol: str) -> None:
        self._symbol = symbol

        self._holding = 0
        self._history = pd.DataFrame(
            columns=['PRICE', 'HOLDING', 'PROFIT'], index=pd.DatetimeIndex(name='DATE')
        )
        self._buy_history = deque()
        self._total_profit = 0

    def __new__(cls) -> Position:
        '''only ever create on instance of Position per symbol'''
        if cls._symbol not in cls.__instances__:
            cls.__instances__[cls._symbol] = super().__new__(cls)
        return cls.__instances__[cls._symbol]

    def record_update(self, price: float, time: datetime) -> None:
        '''writes a new row to the history dataframe'''
        self._price = price
        self._history.loc[time] = [self._price, self._holding, self._profit]

    def buy(self, price: float, quantity: float, time: datetime) -> None:
        '''buy a given quantity of shares'''
        self._holding += quantity
        self.record_update(price=price, time=time)

        # record for average price calculation
        self._buy_history.append((quantity, price))

    def sell(self, price: float, quantity: float, time: datetime) -> None:
        '''sell a given quantity of shares'''
        self._holding -= quantity
        # average price recording stuff
        initial_cost: float = 0
        dropped: float = 0
        while dropped < quantity:
            q, p = self._buy_history.popleft()
            # if we removed too much from the queue, add back what is left
            if q > quantity - dropped:
                self._buy_history.appendleft((q - (quantity - dropped), p))
                initial_cost += (quantity - dropped) * p
                dropped += quantity - dropped
                break  # watch out for floating point math
            else:
                initial_cost += q * p
                dropped += q
        # calculate profit of this sale
        self._profit += price * quantity - initial_cost
        self.record_update(price=price, time=time)

    def close(self, price: float, time: datetime) -> None:
        '''sell everything'''
        self.sell(price=price, quantity=self._holding, time=time)

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def value(self) -> float:
        return self._holding * self._price

    @property
    def profit(self) -> float:
        '''get the running realized profit of this position'''
        return self._profit

    @property
    def is_open(self) -> bool:
        return self._holding > 0

    @property
    def is_closed(self) -> bool:
        return not self.is_open

    @property
    def average_price(self) -> float | None:
        '''returns the average price of the position'''
        if not self._holding:
            return None
        return sum(q * p for q, p in self._buy_history) / self._holding

    @property
    def unrealized_profit_and_loss(self) -> float:
        '''returns the unrealized profit of the position'''
        average_price = self.average_price
        if average_price is None:
            return 0
        return (self._price - average_price) / average_price

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Position):
            return self._symbol == other._symbol
        return False

    def __hash__(self) -> int:
        return hash(self._symbol)


class Portfolio:
    positions: set[Position]
