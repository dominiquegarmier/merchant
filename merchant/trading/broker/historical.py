from __future__ import annotations

from collections.abc import Collection
from collections.abc import Sequence
from decimal import Decimal
from typing import overload

from merchant.core.clock import HasInternalClock
from merchant.core.clock import NSClock
from merchant.trading.action.order import Order
from merchant.trading.action.order import OrderExecution
from merchant.trading.broker.base import Broker
from merchant.trading.portfolio import Portfolio
from merchant.trading.tools.instrument import Instrument
from merchant.trading.tools.market import Market
from merchant.trading.tools.pair import TradingPair


class HistoricalBroker(Broker, HasInternalClock[NSClock]):
    ...


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
