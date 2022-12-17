from __future__ import annotations

from decimal import Context
from decimal import Decimal
from decimal import ROUND_DOWN
from typing import NewType


NormedDecimal = NewType(
    'NormedDecimal', Decimal
)  # a decimal normalized to a given precision


def normalize(value: Decimal, /, *, prec: int = 2) -> NormedDecimal:
    '''
    normalize a decimal value to a given precision,
    '''
    if prec < 0:
        raise ValueError('precision must be greater than 0')
    return NormedDecimal(value.quantize(Decimal(f'1e-{prec}'), rounding=ROUND_DOWN))


__all__ = [
    'normalize',
    'NormedDecimal',
]
