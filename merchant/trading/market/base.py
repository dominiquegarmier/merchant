from __future__ import annotations

from abc import ABCMeta
from abc import abstractmethod
from abc import abstractproperty
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

import pandas as pd

from merchant.core.clock import TimeDependant
from merchant.core.numeric import NormedDecimal
from merchant.core.utils import AggType
from merchant.trading.portfolio import Portfolio
from merchant.trading.tools.pair import TradingPair


@dataclass
class Order:
    '''only supports market orders for now'''

    pair: TradingPair
    quantity: NormedDecimal
    timestamp: pd.Timestamp


@dataclass
class OrderExecution:
    order: Order
    timestamp: pd.Timestamp
    rate: NormedDecimal


@dataclass
class Quote:
    pair: TradingPair
    rate: NormedDecimal
    time: pd.Timestamp


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
    def get_portfolio(self) -> Portfolio:
        ...
