from __future__ import annotations

from abc import ABCMeta
from abc import abstractmethod
from abc import abstractproperty
from collections.abc import Collection
from collections.abc import Sequence
from decimal import Decimal
from typing import overload

from merchant.core.numeric import DEFAULT_CONTEXT
from merchant.core.numeric import Numeric
from merchant.trading.asset import Asset
from merchant.trading.direction import TradingDirection
from merchant.trading.instrument import Instrument
from merchant.trading.pair import TradingPair
from merchant.trading.portfolio import Portfolio


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
            if p.is_virtual:
                raise TypeError(f'cannot add {p} to {self}: virtual trading pair')

            assert p._buy is not None
            assert p._sell is not None

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
        __o: Instrument | TradingPair,
    ) -> set[TradingPair]:
        '''get the trading pairs for an instrument'''
        if isinstance(__o, TradingPair):
            if not __o.is_virtual:
                raise TypeError(
                    f'cannot get trading pairs for {__o}: not a virtual trading pair'
                )
            if __o._sell is not None:
                return self._sell[__o._sell]
            if __o._buy is not None:
                return self._buy[__o._buy]
            assert False, 'unreachable'

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


class Order:
    _direction: TradingDirection
    _quantity: Decimal

    def __init__(self, direction: TradingDirection, quantity: Numeric) -> None:
        self._direction = direction
        self._quantity = Decimal(quantity, context=DEFAULT_CONTEXT)

    @property
    def direction(self) -> TradingDirection:
        return self._direction

    @property
    def quantity(self) -> Decimal:
        return self._quantity


class OrderExecution:
    _order: Order
    _success: bool
    _movements: tuple[Asset, Asset]

    def __init__(
        self, order: Order, success: bool, movements: tuple[Asset, Asset]
    ) -> None:
        self._order = order
        self._success = success

        if not all(a.instrument in self._order.direction for a in movements):
            raise TypeError(f'assets {movements} do not match order {order}')
        self._movements = movements

    @property
    def order(self) -> Order:
        return self._order

    @property
    def success(self) -> bool:
        return self._success

    @property
    def assets(self) -> tuple[Asset, Asset]:
        return self._movements


class MarketSimulation:
    '''
    a simulation of a market, with a portfolio
    '''

    _market: Market
    _portfolio: Portfolio

    def __init__(self, market: Market, portfolio: Portfolio) -> None:
        self._market = market
        self._portfolio = portfolio

    @overload
    def execute(self, order: Order) -> OrderExecution:
        ...

    @overload
    def execute(self, order: Sequence[Order]) -> list[OrderExecution]:
        ...

    def execute(
        self, order: Order | Sequence[Order]
    ) -> OrderExecution | list[OrderExecution]:
        if isinstance(order, Order):
            return self._execute(order)
        return self._execute_many(order)

    def _execute(self, order: Order) -> OrderExecution:
        '''execute a single order'''
        raise NotImplementedError

    def _execute_many(self, orders: Sequence[Order]) -> list[OrderExecution]:
        return [self._execute(o) for o in orders]