from __future__ import annotations

from decimal import Decimal
from decimal import ROUND_DOWN
from typing import TypeAlias


Number: TypeAlias = Decimal | float | int


class NormedDecimal(Decimal):
    '''
    a decimal with a fixed precision
    '''

    def __new__(cls, value: Number, /, *, prec: int = 2) -> NormedDecimal:
        if prec < 0:
            raise ValueError('precision must be greater than 0')
        if not isinstance(value, Decimal):
            value = Decimal(value)
        return super().__new__(
            cls, value.quantize(Decimal(f'1e-{prec}'), rounding=ROUND_DOWN)
        )


__all__ = [
    'NormedDecimal',
]
