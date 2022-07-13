from __future__ import annotations

from collections import deque
from datetime import datetime
from typing import Any

import pandas as pd

from merchant.datasources.base import DataSource
from merchant.environments.market import MarketSimulation

_DEFAULT_STARTING_CAPITAL = 10_000


class InvalidAction(Exception):
    pass


class Position:
    __instances__: dict[str, Position] = {}

    _symbol: str
    _price: float
    _holding: float
    _profit: float  # realized running profit

    # used for p&l calculation
    _buy_history: deque[tuple[float, float]]  # tuple[quantity, price]
    _history: pd.DataFrame

    def __init__(self, symbol: str) -> None:
        self._symbol = symbol

        self._holding = 0
        self._history = pd.DataFrame(
            columns=['PRICE', 'HOLDING', 'PROFIT'], index=pd.DatetimeIndex(name='DATE')
        )
        self._buy_history = deque()
        self._total_profit = 0

    def record_update(self, *, price: float, time: datetime) -> None:
        '''writes a new row to the history dataframe'''
        self._price = price
        self._history.loc[time] = [self._price, self._holding, self._profit]

    def buy(self, *, price: float, quantity: float, time: datetime) -> None:
        '''buy a given quantity of shares'''
        self._holding += quantity
        self.record_update(price=price, time=time)

        # record for average price calculation
        self._buy_history.append((quantity, price))

    def sell(self, *, price: float, quantity: float, time: datetime) -> None:
        '''\
        sell a given quantity of shares
        raises an InvalidAction if there are not enough shares to sell
        '''
        if quantity > self._holding:
            raise InvalidAction('not enough shares to sell')

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

    def close(self, *, price: float, time: datetime) -> None:
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


class PortfolioSimulation:
    _done: bool | None

    _start_data: datetime | None
    _end_data: datetime | None

    _timestamp: datetime | None
    _value: float
    _liquidity: float

    _history: pd.DataFrame
    _positions: dict[str, Position]

    def __init__(self, *, starting_liquity: float | None = None) -> None:
        self._history = pd.DataFrame(
            columns=['LIQUIDITY', 'VALUE'], index=pd.DatetimeIndex(name='DATE')
        )
        self._positions = {}

        # time stuff, will be set when start() is called
        self._timestamp = None
        self._start_data = None
        self._end_data = None
        self._done = None

        if starting_liquity is None:
            starting_liquity = _DEFAULT_STARTING_CAPITAL
        self._liquidity = starting_liquity
        self._value = self._liquidity

    def start(self, time: datetime) -> None:
        if self._done is not None:
            raise ValueError('portfolio already started')
        self._start_data = time
        self._done = False
        self.record_update(time=time, propagate=False)

    def finish(self, time: datetime, market: MarketSimulation) -> None:
        self._assert_running()
        self._end_data = time
        self._done = True
        self.record_update(time=time, market=market)

    def _porpagate_updates(self, *, market: MarketSimulation, time: datetime) -> None:
        '''update the positions based on the market'''
        for symbol, position in self._positions.items():
            if not position.is_open and symbol in market:
                position.record_update(price=market[symbol], time=time)

    def _assert_done(self) -> None:
        if self._done is None:
            raise ValueError('portfolio not started')
        if self._done is False:
            raise ValueError('portfolio not finished')

    def _assert_running(self) -> None:
        if self._done is None:
            raise ValueError('portfolio not started')
        if self._done is True:
            raise ValueError('portfolio already finished')

    def _assert_started(self) -> None:
        if self._done is None:
            raise ValueError('portfolio not started')

    def record_update(
        self,
        *,
        time: datetime,
        market: MarketSimulation | None = None,
        propagate: bool = True,
    ) -> None:
        '''\
        handle an update to the portfolio,
        skips propagation if propagate is False or if market is None
        '''
        if self._done is not False:
            raise ValueError('portfolio is not running')
        assert self._start_data is not None
        assert time > self._start_data

        # update the positions based on the market
        if market is not None and propagate:
            self._porpagate_updates(market=market, time=time)

        self._timestamp = time
        self._value = sum(p.value for p in self._positions.values()) + self._liquidity
        self._history.loc[time] = [self._liquidity, self._value]

    def buy(
        self, *, symbol: str, price: float, quantity: float, time: datetime
    ) -> None:
        '''buy a given quantity of shares'''
        if self._done is not False:
            raise ValueError('portfolio is not running')

        if self.liquidity < quantity * price:
            raise InvalidAction('not enough liquidity')

        if symbol not in self._positions:
            self._positions[symbol] = Position(symbol=symbol)
        position = self._positions[symbol]
        position.buy(price=price, quantity=quantity, time=time)
        self._liquidity -= price * quantity

        self.record_update(time=time, propagate=False)

    def sell(
        self, *, symbol: str, price: float, quantity: float, time: datetime
    ) -> None:
        '''sell a given quantity of shares'''

        if self._done is not False:
            raise ValueError('portfolio is not running')

        if symbol not in self._positions:
            raise InvalidAction('no position to sell')
        position = self._positions[symbol]
        position.sell(price=price, quantity=quantity, time=time)
        self._liquidity += price * quantity

        self.record_update(time=time, propagate=False)

    @property
    def liquidity(self) -> float:
        return self._liquidity

    @property
    def value(self) -> float:
        self._assert_started()
        return self._value

    @property
    def is_done(self) -> bool:
        return self._done is True

    @property
    def is_running(self) -> bool:
        return self._done is False
