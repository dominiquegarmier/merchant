from __future__ import annotations

from abc import ABCMeta
from abc import abstractmethod
from abc import abstractproperty
from logging import getLogger
from typing import Generic
from typing import TypeVar

import pandas as pd

logger = getLogger(__name__)


T = TypeVar('T')
U = TypeVar('U')


class InternalClockBase(Generic[T, U], metaclass=ABCMeta):
    @abstractproperty
    def time(self) -> T:
        ...

    @abstractmethod
    def reset(self) -> None:
        ...

    @abstractproperty
    def increment(self) -> U:
        ...

    @increment.setter
    def increment(self, incr: U) -> None:
        ...

    @abstractmethod
    def step(self, /, *, to: T | None = None, incr: U | None = None) -> None:
        ...

    def __str__(self) -> str:
        return f'<{type(self)}: time={self.time!r}, increment={self.increment!r}>'

    def __repr__(self) -> str:
        return str(self)


class NSClock(InternalClockBase[pd.Timestamp, pd.Timedelta], metaclass=ABCMeta):
    '''
    clock object that
    '''

    _start: pd.Timestamp
    _time: pd.Timestamp
    _incr: pd.Timedelta

    def __init__(
        self, /, *, start: pd.Timestamp, incr: pd.Timedelta | None = None
    ) -> None:
        self._start = start
        self._time = start
        if incr is None:
            incr = pd.Timedelta(1, unit='s')
        self._incr = incr

    @property
    def time(self) -> pd.Timestamp:
        return self._time

    @property
    def increment(self) -> pd.Timedelta:
        return self._incr

    @increment.setter
    def increment(self, incr: pd.Timedelta) -> None:
        self._incr = incr

    def step(
        self, incr: pd.Timedelta | None = None, to: pd.Timestamp | None = None
    ) -> None:
        if to is not None and incr is not None:
            raise ValueError('Cannot specify both `to` and `incr`')
        if to is not None:
            logger.debug(f'{self!r}: stepping to {to!r}...')
            self._time = to
        else:
            logger.debug(f'{self!r}: stepping increment: {incr or self._incr!r}...')
            self._time += incr or self._incr

    def reset(self) -> None:
        logger.debug(f'{self!r}: resetting back to {self._start!r}...')
        self._time = self._start


C = TypeVar('C', bound=InternalClockBase)


class HasInternalClock(Generic[C], metaclass=ABCMeta):
    _clock: C

    @property
    def clock(self) -> C:
        return self._clock

    @clock.setter
    def clock(self, clock: C) -> None:
        self._clock = clock
