from __future__ import annotations

from abc import ABCMeta
from collections.abc import Collection
from typing import Any
from typing import Generic
from typing import Literal
from typing import overload

from merchant.trading.dataclasses.instrument import Instrument


class TradingPair:
    '''
    a pair of trading instruments, with a sell and a buy side
    if one of the instruments is None, it indicates a virtual trading pair, which is only used for pattern matching

    Parameters
    ----------

    buy: Instrument
        the instrument on the buy side

    sell: Instrument
        the instrument on the sell side
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
        self._buy = buy
        self._sell = sell

    @property
    def is_virtual(self) -> bool:
        return self._buy is None or self._sell is None

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


class Index:
    '''
    an index is a list of trading pairs, with pairwise different buy instruments
    (i.e an index can only contain one pair with a given instrument on the buy side)
    '''

    _instruments: dict[Instrument, TradingPair]

    def __init__(self, /, *, pairs: Collection[TradingPair]) -> None:
        self._instruments = {}
        for p in pairs:
            if p.is_virtual:
                raise ValueError(f'cannot add virtual trading pair {p} to {self}')
            assert p._buy is not None
            if p._buy in self._instruments:
                raise ValueError(
                    f'cannot add {p} to {self}: {p._buy} already in {self}'
                )
            self._instruments[p._buy] = p

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, Index):
            return False
        if self._instruments != __o._instruments:
            return False
        return True

    def __repr__(self) -> str:
        return f'{type(self)}({self._instruments!r})'

    def __contains__(self, __o: Instrument) -> bool:
        '''check if an instrument is in the index (on the buy side)'''
        if __o in self._instruments:
            return True
        return False

    def __getitem__(self, __o: Instrument) -> TradingPair:
        '''get the trading pair for an instrument (on the buy side)'''
        if __o in self._instruments:
            return self._instruments[__o]
        raise KeyError(f'no trading pair for {__o} in {self}')


class Market:
    '''
    a list of (real) trading pairs, forming an arbitrary graph
    '''

    _buy: dict[Instrument, set[TradingPair]]
    _sell: dict[Instrument, set[TradingPair]]
    _pairs: set[TradingPair]
    _instruments: set[Instrument]

    def __init__(self, /, *, pairs: Collection[TradingPair]) -> None:
        self._buy = {}
        self._sell = {}

        for p in pairs:
            if p.is_virtual:
                raise TypeError(f'cannot add {p} to {self}: virtual trading pair')

            assert p._buy is not None
            assert p._sell is not None

            if p._buy not in self._buy:
                self._buy[p._buy] = set()
            self._buy[p._buy].add(p)
            if p._sell not in self._sell:
                self._sell[p._sell] = set()
            self._sell[p._sell].add(p)

        self._pairs = set(pairs)
        self._instruments = set(self._buy.keys()) | set(self._sell.keys())

    def __contains__(self, __o: Instrument) -> bool:
        '''check if an instrument is in the market'''
        if __o in self._instruments:
            return True
        return False

    def __getitem__(
        self,
        __o: Instrument | TradingPair,
    ) -> set[TradingPair]:
        '''get the trading pairs for an instrument'''
        if isinstance(__o, TradingPair):
            if not __o.is_virtual:
                raise TypeError(
                    f'cannot get trading pairs for {__o}: not a virtual trading pair'
                )
            if __o._sell is not None:
                return self._sell[__o._sell]
            if __o._buy is not None:
                return self._buy[__o._buy]
            assert False, 'unreachable'

        if __o in self._instruments:
            return self._buy[__o] | self._sell[__o]
        raise KeyError(f'no trading pairs for {__o} in {self}')

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, Market):
            return False
        if self._pairs != __o._pairs:
            return False
        return True

    def __ne__(self, __o: object) -> bool:
        return not self.__eq__(__o)

    def __repr__(self) -> str:
        return f'{type(self)}({self._pairs!r})'
