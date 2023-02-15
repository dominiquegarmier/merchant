from __future__ import annotations

from abc import ABCMeta
from abc import abstractmethod
from abc import abstractproperty
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pandas as pd

from merchant.core.abstract import TimeDependant
from merchant.core.numeric import NormedDecimal
from merchant.trading.tools.asset import Asset
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


class BaseMarketData(TimeDependant, metaclass=ABCMeta):
    def __init__(self) -> None:
        super().__init__()

    @abstractproperty
    def trading_pairs(self) -> set[TradingPair]:
        ...

    @abstractmethod
    def get_quote(self, pair: TradingPair) -> Quote:
        ...

    @abstractmethod
    def get_history(self, window: int, pair: TradingPair | None = None) -> pd.DataFrame:
        ...


class BaseBroker(TimeDependant, metaclass=ABCMeta):
    def __init__(self) -> None:
        super().__init__()

    @abstractproperty
    def trading_pairs(self) -> set[TradingPair]:
        ...

    @abstractmethod
    def execute_order(self, order: Order) -> OrderExecution | None:
        ...

    @abstractmethod
    def update_portfolio(self) -> Portfolio:
        ...
