from __future__ import annotations

from collections.abc import Collection
from collections.abc import Sequence
from decimal import Decimal
from typing import overload

from merchant.core.clock import HasInternalClock
from merchant.core.clock import NSClock
from merchant.trading.action.order import Order
from merchant.trading.action.order import OrderExecution
from merchant.trading.portfolio import Portfolio
from merchant.trading.tools.instrument import Instrument
from merchant.trading.tools.pair import TradingPair
from merchant.trading.tools.pair import VirtualTradingPair


class Market:
    '''
    a list of (real) trading pairs, forming an arbitrary graph
    '''

    _buy: dict[Instrument, set[TradingPair]]
    _sell: dict[Instrument, set[TradingPair]]
    _pairs: set[TradingPair]
    _instruments: set[Instrument]

    def __init__(self, /, *, pairs: Collection[TradingPair]) -> None:
        self._buy = {}
        self._sell = {}

        for p in pairs:
            if p._buy not in self._buy:
                self._buy[p._buy] = set()
            self._buy[p._buy].add(p)
            if p._sell not in self._sell:
                self._sell[p._sell] = set()
            self._sell[p._sell].add(p)

        self._pairs = set(pairs)
        self._instruments = set(self._buy.keys()) | set(self._sell.keys())

    def __contains__(self, __o: Instrument) -> bool:
        '''check if an instrument is in the market'''
        if __o in self._instruments:
            return True
        return False

    def __getitem__(
        self,
        __o: Instrument | VirtualTradingPair,
    ) -> set[TradingPair]:
        '''get the trading pairs for an instrument'''
        if isinstance(__o, VirtualTradingPair):
            if __o._sell is not None:
                return self._sell[__o._sell]
            if __o._buy is not None:
                return self._buy[__o._buy]
            raise KeyError(f'invalid virtual trading pair {__o} in {self}')

        if __o in self._instruments:
            return self._buy[__o] | self._sell[__o]
        raise KeyError(f'no trading pairs for {__o} in {self}')

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, Market):
            return False
        if self._pairs != __o._pairs:
            return False
        return True

    def __ne__(self, __o: object) -> bool:
        return not self.__eq__(__o)

    def __repr__(self) -> str:
        return f'{type(self)}({self._pairs!r})'

    def portfolio_value(self, currency: Instrument) -> Decimal:
        '''get the value of the portfolio in a given currency'''
        raise NotImplementedError


class MarketSimulation(HasInternalClock[NSClock]):
    '''
    a simulation of a market, with a portfolio
    '''

    _market: Market

    def __init__(self, market: Market, clock: NSClock) -> None:
        self._clock = clock
        self._market = market

    @overload
    def execute(self, /, *, order: Order, portfolio: Portfolio) -> OrderExecution:
        ...

    @overload
    def execute(
        self, /, *, order: Sequence[Order], portfolio: Portfolio
    ) -> list[OrderExecution]:
        ...

    def execute(
        self, /, *, order: Order | Sequence[Order], portfolio: Portfolio
    ) -> OrderExecution | list[OrderExecution]:
        if isinstance(order, Order):
            return self._execute(order)
        return self._execute_many(order)

    def _execute(self, order: Order) -> OrderExecution:
        '''execute a single order'''
        raise NotImplementedError

    def _execute_many(self, orders: Sequence[Order]) -> list[OrderExecution]:
        return [self._execute(o) for o in orders]
