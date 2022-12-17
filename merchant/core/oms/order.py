from __future__ import annotations

from decimal import Decimal

from merchant.core.numeric import DEFAULT_CONTEXT
from merchant.core.numeric import Numeric
from merchant.core.oms.direction import TradingDirection
from merchant.core.pms.asset import Asset


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
