from __future__ import annotations

from decimal import Context
from decimal import Decimal
from decimal import ROUND_HALF_DOWN

Numeric = float | int | Decimal

DEFAULT_CONTEXT = Context(prec=28, rounding=ROUND_HALF_DOWN)


def to_decimal(value: Numeric, context: Context = DEFAULT_CONTEXT) -> Decimal:
    if isinstance(value, Decimal):
        return value.quantize(Decimal('1E-28'), context=context)
    return Decimal(value, context=context)


__all__ = [
    'Numeric',
    'DEFAULT_CONTEXT',
]
