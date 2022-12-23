from __future__ import annotations

from decimal import Decimal
from functools import total_ordering
from typing import NewType

from merchant.core.numeric import NormedDecimal
from merchant.trading.tools.instrument import Instrument


@total_ordering
class Asset:
    '''
    an asset is an instrument multiplied by a quantity
    it can represent a dept or a credit
    '''

    _instrument: Instrument
    _quantity: NormedDecimal
    _precision: int

    def __init__(self, instrument: Instrument, /, *, quantity: Decimal) -> None:
        self._instrument = instrument
        self._precision = instrument.precision
        self._quantity = NormedDecimal(quantity, prec=self._precision)

    @property
    def instrument(self) -> Instrument:
        return self._instrument

    @property
    def quantity(self) -> NormedDecimal:
        return self._quantity

    def __float__(self) -> float:
        return float(self._quantity)

    def __hash__(self) -> int:
        return hash((self._instrument, self._quantity))

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
        return Asset(self._instrument, quantity=self._quantity + __o._quantity)

    def __neg__(self) -> Asset:
        return Asset(self._instrument, quantity=-self._quantity)

    def __sub__(self, __o: Asset) -> Asset:
        if self._instrument != __o._instrument:
            raise TypeError(f'cannot subtract {self} with {__o}: different instruments')
        return self.__add__(-__o)

    def __mul__(self, __o: Decimal) -> Asset:
        return Asset(self._instrument, quantity=self._quantity * __o)

    def __rmul__(self, __o: Decimal) -> Asset:
        return self.__mul__(__o)

    def __truediv__(self, __o: Asset | Decimal) -> Asset:
        if isinstance(__o, Decimal):
            return Asset(self._instrument, quantity=self._quantity / __o)
        if self._instrument != __o._instrument:
            raise TypeError(f'cannot divide {self} with {__o}: different instruments')
        if __o._quantity == 0:
            raise ZeroDivisionError(f'cannot divide {self} by {__o}')
        return Asset(self._instrument, quantity=self._quantity / __o._quantity)

    def __str__(self) -> str:
        return f'{self._quantity:.{self._precision}} {self._instrument}'

    def __repr__(self) -> str:
        return str(self)


# representing the value of another asset wrt. a given benchmark instrument
Valuation = NewType('Valuation', Asset)
