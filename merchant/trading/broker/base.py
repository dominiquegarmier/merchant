from __future__ import annotations

from abc import ABCMeta
from abc import abstractmethod

from merchant.trading.action.order import Order
from merchant.trading.action.order import OrderExecution
from merchant.trading.portfolio import Portfolio


class Broker(metaclass=ABCMeta):
    @abstractmethod
    def execute_order(self, order: Order) -> OrderExecution:
        ...

    @abstractmethod
    def get_portfolio(self) -> Portfolio:
        ...
