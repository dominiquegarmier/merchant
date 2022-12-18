from __future__ import annotations

from abc import ABCMeta
from abc import abstractproperty
from typing import Literal

import pandas as pd

from merchant.trading.tools.instrument import Instrument


class _Base(metaclass=ABCMeta):
    _instrument: Instrument
    _benchmark: Instrument
    _open: pd.Timestamp

    def __init__(self, instrument: Instrument, /, *, benchmark: Instrument) -> None:
        raise NotImplementedError

    @abstractproperty
    def is_open(self) -> bool:
        ...

    @abstractproperty
    def is_closed(self) -> bool:
        ...

    @property
    def instrument(self) -> Instrument:
        '''get the instrument'''
        return self._instrument

    @property
    def benchmark(self) -> Instrument:
        '''get the benchmark'''
        return self._benchmark

    def __str__(self) -> str:
        return f'{type(self)}(instrument={self.instrument}, benchmark={self.benchmark}, ...)'

    def __repr__(self) -> str:
        return str(self)


class Position(_Base):
    '''
    a 'Position' is an open 'Trade'
    '''

    @property
    def is_open(self) -> Literal[True]:
        return True

    @property
    def is_closed(self) -> Literal[False]:
        return False

    def close(self, close: pd.Timestamp) -> Trade:
        '''close the position'''
        return Trade(self.instrument, benchmark=self.benchmark)


class Trade(_Base):
    '''
    a 'Trade' consists of two consecutive successful market orders
    (one at the start, one at the end of the trade period).
    It is implicitly assumed that after each 'Trade' the whole asset is liquidated
    (and potentially reacquired (partially)).
    '''

    _close: pd.Timestamp

    def __init__(self, instrument: Instrument, /, *, benchmark: Instrument) -> None:
        super().__init__(instrument, benchmark=benchmark)
        raise NotImplementedError

    @property
    def is_open(self) -> Literal[False]:
        return False

    @property
    def is_closed(self) -> Literal[True]:
        return True

    @property
    def open(self) -> pd.Timestamp:
        return self._open

    @property
    def close(self) -> pd.Timestamp:
        return self._close
