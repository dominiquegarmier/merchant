from __future__ import annotations

from decimal import Decimal

from merchant.core.numeric import NormedDecimal
from merchant.trading.action.base import Action
from merchant.trading.action.base import ActionExecution
from merchant.trading.tools.asset import Asset
from merchant.trading.tools.pair import TradingPair


class Order(Action):
    _pair: TradingPair
    _quantity: NormedDecimal

    def __init__(self, pair: TradingPair, quantity: Decimal) -> None:
        if pair.buy is None or pair.sell is None:
            raise ValueError(f'need a real trading pair, not {pair}')
        self._pair = pair
        self._quantity = NormedDecimal(quantity, prec=pair.buy.precision)

    @property
    def direction(self) -> TradingPair:
        return self._pair

    @property
    def quantity(self) -> NormedDecimal:
        return self._quantity


class OrderExecution(ActionExecution):
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
