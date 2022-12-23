from __future__ import annotations

from abc import ABCMeta
from abc import abstractmethod
from abc import abstractproperty
from collections.abc import Callable
from logging import getLogger
from typing import Generic
from typing import TypeVar

import pandas as pd

from merchant.core.observable import AbstractHook
from merchant.core.observable import Observable
from merchant.core.observable import reset_hooks
from merchant.core.observable import run_hooks

logger = getLogger(__name__)


T = TypeVar('T')
U = TypeVar('U')


class AbstractClock(Observable, Generic[T, U], metaclass=ABCMeta):
    @abstractproperty
    def time(self) -> T:
        ...

    @abstractmethod
    @reset_hooks
    def reset(self) -> None:
        ...

    @abstractproperty
    def increment(self) -> U:
        ...

    @increment.setter
    def increment(self, incr: U) -> None:
        ...

    @abstractmethod
    @run_hooks
    def step(self, /, *, to: T | None = None, incr: U | None = None) -> None:
        ...

    def __str__(self) -> str:
        return f'<{type(self)}: time={self.time!r}, increment={self.increment!r}>'

    def __repr__(self) -> str:
        return str(self)


class NSClock(AbstractClock[pd.Timestamp, pd.Timedelta]):
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

    @run_hooks
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

    @reset_hooks
    def reset(self) -> None:
        logger.debug(f'{self!r}: resetting back to {self._start!r}...')
        self._time = self._start


class ClockHook(AbstractHook[NSClock]):
    def __init__(self, func: Callable[[], None]):
        self._func: Callable[[], None] = func

    def __call__(self, clock: AbstractClock) -> None:
        self._func()


TClock = TypeVar('TClock', bound=AbstractClock)


class HasInternalClock(Generic[TClock], metaclass=ABCMeta):
    _clock: TClock

    @property
    def clock(self) -> TClock:
        return self._clock

    @clock.setter
    def clock(self, clock: TClock) -> None:
        self._clock = clock
