from __future__ import annotations

import functools
from abc import ABCMeta
from abc import abstractmethod
from abc import abstractproperty
from collections.abc import Callable
from logging import getLogger
from typing import Concatenate
from typing import Generic
from typing import Literal
from typing import overload
from typing import ParamSpec
from typing import TypeVar

import pandas as pd

logger = getLogger(__name__)


T = TypeVar('T')
U = TypeVar('U')


class Hook(Generic[T]):
    _func: Callable[[], None]
    _event_type: Literal['tick', 'time']
    _time: T | None = None
    _active: bool = False

    def __init__(
        self,
        /,
        *,
        func: Callable[[], None],
        event_type: Literal['tick', 'time'],
        time: T | None = None,
    ) -> None:
        self._func = func
        self._event_type = event_type
        if event_type == 'time' and time is None:
            raise ValueError('Must specify time for time-based hook')
        self._time = time

    def reset(self) -> None:
        self._active = True

    def check_hook(self, time: T) -> bool:
        if self._event_type == 'tick':
            return True
        elif self._event_type == 'time' and self._active:
            if time >= self._time:  # type: ignore
                self._active = False
                return True
            return False
        return False

    def run(self) -> None:
        self._func()

    @property
    def is_active(self) -> bool:
        return self._active

    def __hash__(self) -> int:
        return super().__hash__()

    def __eq__(self, __o: object) -> bool:
        return self is __o

    def __str__(self) -> str:
        return f'{type(self)}(func={self._func!r}, hook_type={self._event_type!r})'

    def __repr__(self) -> str:
        return str(self)


TClock = TypeVar('TClock', bound='InternalClockBase')
P = ParamSpec('P')
R = TypeVar('R')


@overload
def trigger_hooks(
    *, reset: Literal[True]
) -> Callable[
    [Callable[Concatenate[TClock, P], R]], Callable[Concatenate[TClock, P], R]
]:
    ...


@overload
def trigger_hooks(
    func: Callable[Concatenate[TClock, P], R]
) -> Callable[Concatenate[TClock, P], R]:
    ...


def trigger_hooks(
    func: Callable[Concatenate[TClock, P], R] | None = None, *, reset: bool = False
) -> Callable[Concatenate[TClock, P], R] | Callable[
    [Callable[Concatenate[TClock, P], R]], Callable[Concatenate[TClock, P], R]
]:
    def decorator(
        func: Callable[Concatenate[TClock, P], R]
    ) -> Callable[Concatenate[TClock, P], R]:
        @functools.wraps(func)
        def wrapper(self: TClock, *args: P.args, **kwargs: P.kwargs) -> R:
            ret = func(self, *args, **kwargs)
            if reset:
                for hook in self._hooks:
                    hook.reset()
            self._run_hooks()
            return ret

        return wrapper

    if func is None:
        return decorator
    return decorator(func)


class InternalClockBase(Generic[T, U], metaclass=ABCMeta):

    _hooks: set[Hook[T]]

    def _run_hooks(self) -> None:
        def run_hook(hook: Hook[T], time: T) -> None:
            if not hook.is_active:
                return
            if hook.check_hook(time):
                hook.run()

        for hook in self._hooks:
            run_hook(hook, self.time)

    @overload
    def attach(
        self, func: Callable[[], None], /, *, event_type: Literal['tick']
    ) -> Hook[T]:
        ...

    @overload
    def attach(
        self, func: Callable[[], None], /, *, event_type: Literal['time'], time: T
    ) -> Hook[T]:
        ...

    def attach(
        self,
        func: Callable[[], None],
        /,
        *,
        event_type: Literal['tick', 'time'],
        time: T | None = None,
    ) -> Hook[T]:
        if event_type == 'time' and time is None:
            raise ValueError('Must specify time for time-based hook')
        hook = Hook(func=func, event_type=event_type, time=time)
        self._hooks.add(hook)
        return hook

    def detach(self, hook: Hook) -> None:
        self._hooks.remove(hook)

    @abstractproperty
    def time(self) -> T:
        ...

    @abstractmethod
    @trigger_hooks(reset=True)
    def reset(self) -> None:
        ...

    @abstractproperty
    def increment(self) -> U:
        ...

    @increment.setter
    def increment(self, incr: U) -> None:
        ...

    @abstractmethod
    @trigger_hooks
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
    _hooks: set[Hook[pd.Timestamp]]

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

    @trigger_hooks
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

    @trigger_hooks(reset=True)
    def reset(self) -> None:
        logger.debug(f'{self!r}: resetting back to {self._start!r}...')
        self._time = self._start


class HasInternalClock(Generic[TClock], metaclass=ABCMeta):
    _clock: TClock

    @property
    def clock(self) -> TClock:
        return self._clock

    @clock.setter
    def clock(self, clock: TClock) -> None:
        self._clock = clock
