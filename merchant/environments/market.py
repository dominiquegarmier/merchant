from __future__ import annotations

from collections import OrderedDict
from datetime import datetime
from datetime import timedelta
from typing import cast
from typing import NoReturn
from typing import Protocol

import pandas as pd


class OrderFailed(Exception):
    pass


class _FeeMethod(Protocol):
    def __call__(self, *, price: float, quantity: float) -> float:
        ...


class MarketSimulation:
    '''market simulation'''

    _done: bool | None = None
    _timestamp: datetime | None = None

    _symbols: set[str]
    _historic_data: pd.DataFrame = None
    _start_date: datetime | None = None
    _end_date: datetime | None = None

    _fee_method: _FeeMethod

    def __init__(
        self,
        *,
        historic_data: pd.DataFrame,
        fee_pct: float = 0.01,
        fee_method: _FeeMethod | None = None,
    ) -> None:
        self._historic_data = historic_data
        self._symbols = set()

        # see the fee method
        if fee_method is None:
            self._fee_method = lambda *, price, quantity: fee_pct * price * quantity
        else:
            self._fee_method = fee_method

    def start(self, time: datetime) -> None:
        '''start the simulation'''
        if self._done is not None:
            raise ValueError('simulation has already started')
        self._done = False
        self._timestamp = time
        self._start_date = time

    def record_update(self, time: datetime) -> None:
        '''advance the simulation by one timestep'''
        if self._done is not False:
            ValueError('simulation is not running')
        assert self._timestamp is not None
        assert time > self._timestamp
        self._timestamp = time

    def stop(self) -> None:
        '''stop the simulation'''
        if self._done is not False:
            raise ValueError('simulation is not running')
        self._done = True

    @property
    def market_view(self) -> pd.DataFrame:
        '''return past market data'''
        if self._done is None:
            raise ValueError('simulation has not started')
        return self._historic_data[:][self._historic_data.index < self._timestamp]

    @property
    def is_done(self) -> bool:
        return self._done is True

    @property
    def is_running(self) -> bool:
        return self._done is False

    @property
    def timestamp(self) -> datetime:
        '''return current timestamp of market simulation'''
        if self._timestamp is None:
            raise ValueError('simulation has not started')
        return self._timestamp

    def __getitem__(self, symbol: str) -> float:
        '''get the price of a given symbol'''
        return self.quote(symbol=symbol)

    def __contains__(self, symbol: str) -> bool:
        '''check if a given symbol is in the market environment'''
        return symbol in self._symbols

    def fees(self, *, price: float, quantity: float) -> float:
        '''calcuate broker fees and return the price of a given quote'''
        return self._fee_method(price=price, quantity=quantity)

    def quote(self, *, symbol: str) -> float:
        '''get the fair price of a given symbol'''
        # TODO fix this
        stock_data = self._historic_data[symbol]
        o, h, l, c, _ = stock_data.iloc[
            stock_data.index.get_loc(self._timestamp, method='nearest')
        ]
        return cast(float, (o + h + l + c) / 4)

    def buy(self, *, symbol: str, quantity: int) -> float:
        '''\
        place a buy order
        returns the money spent on the purchase
        '''
        quote = self.quote(symbol=symbol)
        fees = self.fees(price=quote, quantity=quantity)
        return quote * quantity + fees

    def sell(self, *, symbol: str, quantity: int) -> float:
        '''\
        place a sell order
        returns the money gained from the sale
        '''
        quote = self.quote(symbol=symbol)
        fees = self.fees(price=quote, quantity=quantity)
        return quote * quantity - fees
