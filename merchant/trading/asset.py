from __future__ import annotations

from decimal import Context
from decimal import Decimal
from decimal import ROUND_DOWN
from functools import total_ordering

from merchant.trading.instrument import Instrument


@total_ordering
class Asset:
    '''
    an asset is an instrument multiplied by a quantity
    it can represent a dept or a credit
    '''

    _context: Context
    _instrument: Instrument
    _quantity: Decimal
    _precision: int

    def __init__(self, instrument: Instrument, quantity: float | Decimal) -> None:
        self._instrument = instrument
        self._precision = instrument.precision
        self._context = Context(prec=self._precision, rounding=ROUND_DOWN)
        self._quantity = Decimal(value=quantity, context=self._context)

    @property
    def instrument(self) -> Instrument:
        return self._instrument

    @property
    def quantity(self) -> Decimal:
        return self._quantity

    def __float__(self) -> float:
        return float(self._quantity)

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, Asset):
            return False
        if (self._instrument, self._quantity) != (__o._instrument, __o._quantity):
            return False
        return True

    def __lt__(self, __o: object) -> bool:
        if not isinstance(__o, Asset):
            raise TypeError(f'cannot compare {type(self)} with {type(__o)}')
        if self._instrument != __o._instrument:
            raise TypeError(f'cannot compare {self} with {__o}: different instruments')
        return self._quantity < __o._quantity

    def __add__(self, __o: Asset) -> Asset:
        if self._instrument != __o._instrument:
            raise TypeError(f'cannot add {self} with {__o}: different instruments')
        return Asset(self._instrument, self._quantity + __o._quantity)

    def __neg__(self) -> Asset:
        return Asset(self._instrument, -self._quantity)

    def __sub__(self, __o: Asset) -> Asset:
        if self._instrument != __o._instrument:
            raise TypeError(f'cannot subtract {self} with {__o}: different instruments')
        return self.__add__(-__o)

    def __repr__(self) -> str:
        return f'{float(self._quantity):.{self._precision}} {self._instrument}'
