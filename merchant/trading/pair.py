from __future__ import annotations

from abc import ABCMeta
from collections.abc import Collection
from typing import Any
from typing import Generic
from typing import Literal
from typing import overload

from merchant.trading.instrument import Instrument


class TradingPair:
    '''
    a pair of trading instruments, with a sell and a buy side
    if one of the instruments is None, it indicates a virtual trading pair, which is only used for pattern matching

    Parameters
    ----------

    buy: Instrument | None
        the instrument on the buy side

    sell: Instrument | None
        the instrument on the sell side

    Note: only one of the instruments can be None, this will make the trading pair virtual

    Properties
    ----------

    is_virtual: bool
        whether the trading pair is virtual (exactly one of the instruments is None)
    '''

    _buy: Instrument | None
    _sell: Instrument | None

    @overload
    def __init__(self, buy: Instrument, sell: Instrument) -> None:
        ...

    @overload
    def __init__(self, buy: Instrument, sell: None) -> None:
        ...

    @overload
    def __init__(self, buy: None, sell: Instrument) -> None:
        ...

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

    @property
    def is_virtual(self) -> bool:
        return self._buy is None or self._sell is None

    def __contains__(self, __o: object) -> bool:
        if not isinstance(__o, Instrument):
            return False
        return __o in (self._buy, self._sell)

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, TradingPair):
            return False
        if (self._buy, self._sell) != (__o._buy, __o._sell):
            return False
        return True

    def __hash__(self) -> int:
        return hash((self._buy, self._sell))

    def __repr__(self) -> str:
        return f'{type(self)}(buy={self._buy!r}, sell={self._sell!r})'
