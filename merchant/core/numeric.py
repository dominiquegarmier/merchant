from __future__ import annotations

from decimal import Context
from decimal import Decimal
from decimal import ROUND_DOWN
from typing import NewType


class NormedDecimal(Decimal):
    '''
    a decimal with a fixed precision
    '''

    def __new__(cls, value: Decimal, /, *, prec: int = 2) -> NormedDecimal:
        if prec < 0:
            raise ValueError('precision must be greater than 0')
        return super().__new__(
            cls, value.quantize(Decimal(f'1e-{prec}'), rounding=ROUND_DOWN)
        )


__all__ = [
    'NormedDecimal',
]
