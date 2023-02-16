from __future__ import annotations

from abc import ABCMeta
from abc import abstractmethod
from abc import abstractproperty
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pandas as pd

from merchant.core.abstract import Observable
from merchant.core.abstract import TimeDependant
from merchant.core.numeric import NormedDecimal
from merchant.trading.tools.asset import Asset
from merchant.trading.tools.instrument import Instrument
from merchant.trading.tools.pair import TradingPair


if TYPE_CHECKING:
    from merchant.trading.market.portfolio import Portfolio


@dataclass(frozen=True)
class Order:
    pair: TradingPair
    quantity: NormedDecimal
    timestamp: pd.Timestamp


@dataclass(frozen=True)
class OrderExecution:
    order: Order
    timestamp: pd.Timestamp
    rate: NormedDecimal
    fees: Asset


@dataclass(frozen=True)
class Trade:
    sold: Asset
    bought: Asset
    fees: Asset
    execution: OrderExecution


@dataclass(frozen=True)
class Quote:
    pair: TradingPair
    rate: NormedDecimal
    time: pd.Timestamp


class BrokerExceptions(Exception):
    ...


class InsufficientAssets(BrokerExceptions):
    _order: Order

    def __init__(self, order: Order) -> None:
        super().__init__(f'Insufficient balance to execute order: {order}')
        self._order = order


class OrderAfterValuation(BrokerExceptions):
    _valuation: pd.Timestamp
    _order: Order

    def __init__(self, order: Order, valuation: pd.Timestamp) -> None:
        super().__init__(f'Order {order} placed after valuation at {valuation}')
        self._valuation = valuation
        self._order = order


class _MarketMixin:
    @abstractproperty
    def instruments(self) -> list[Instrument]:
        ...

    @abstractproperty
    def trading_pairs(self) -> list[TradingPair]:
        ...


class BaseMarketObserver(TimeDependant, Observable, _MarketMixin, metaclass=ABCMeta):
    def __init__(self) -> None:
        super().__init__()


class BaseBroker(TimeDependant, Observable, _MarketMixin, metaclass=ABCMeta):
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def execute_order(self, order: Order) -> OrderExecution | None:
        ...

    @abstractproperty
    def portfolio(self) -> Portfolio:
        ...

    @abstractproperty
    def minimum_timestep(self) -> pd.Timedelta:
        '''
        this timestep must ensure that the broker can execute orders again after a valuation
        it also makes sure that execution prices are consistent between two steps (i.e when
        using historical data this must be greater than the aggregates durations)

        example:
        - a realtime broker might have a minimum timestep of 0 seconds
        - a (historical data) broker using 1 minute candles must have a minimum timestep of 1 minute or greater
        '''
        ...
