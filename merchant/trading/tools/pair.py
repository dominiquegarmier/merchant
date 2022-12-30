from __future__ import annotations

from abc import ABCMeta
from typing import overload
from typing import TYPE_CHECKING
from typing import TypeVar


if TYPE_CHECKING:
    from merchant.trading.tools.instrument import Instrument


T = TypeVar('T', bound='_TradingPair')


class _TradingPair(metaclass=ABCMeta):
    '''
    a pair of trading instruments, with a sell and a buy side
    if one of the instruments is None, it indicates a virtual trading pair, which is only used for pattern matching
    '''

    _buy: Instrument | None
    _sell: Instrument | None

    def __init__(self, buy: Instrument | None, sell: Instrument | None) -> None:
        if buy is None and sell is None:
            raise TypeError(f'cannot create {type(self)}: both instruments are None')
        self._buy = buy
        self._sell = sell

    @property
    def buy(self) -> Instrument | None:
        return self._buy

    @property
    def sell(self) -> Instrument | None:
        return self._sell

    def __contains__(self, __o: object) -> bool:
        return __o in (self._buy, self._sell)

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, _TradingPair):
            return False
        if (self._buy, self._sell) != (__o._buy, __o._sell):
            return False
        return True

    def __invert__(self: T) -> T:
        return type(self)(self._sell, self._buy)

    def __hash__(self) -> int:
        return hash((type(self), self._buy, self._sell))

    def __repr__(self) -> str:
        return f'{type(self)}(buy={self._buy!r}, sell={self._sell!r})'


class TradingPair(_TradingPair):
    _buy: Instrument
    _sell: Instrument

    def __init__(self, buy: Instrument, sell: Instrument) -> None:
        super().__init__(buy, sell)


class VirtualTradingPair(_TradingPair):
    @overload
    def __init__(self, buy: None, sell: Instrument) -> None:
        ...

    @overload
    def __init__(self, buy: Instrument, sell: None) -> None:
        ...

    def __init__(self, buy, sell) -> None:
        if len([_ for _ in (buy, sell) if _ is None]) != 1:
            raise TypeError(
                f'cannot create {type(self)}: exactly one instrument must be None'
            )
        super().__init__(buy, sell)
