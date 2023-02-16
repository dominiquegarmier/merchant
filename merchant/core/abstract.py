from __future__ import annotations

from abc import ABCMeta
from abc import abstractmethod
from abc import abstractproperty
from logging import getLogger
from types import TracebackType

import numpy as np
import pandas as pd

logger = getLogger(__name__)


class Observable(metaclass=ABCMeta):
    '''
    an object that can yield an observation
    '''

    @abstractproperty
    def observation_shape(self) -> tuple[int, ...]:
        ...

    @abstractmethod
    def get_observation(self) -> np.ndarray:
        ...


class Identifiable(metaclass=ABCMeta):
    def __hash__(self) -> int:
        return hash(self)

    def __eq__(self, __o: object) -> bool:
        return self is __o


class Clock(metaclass=ABCMeta):
    '''
    attributes:
        - time: the current time
        - started_at: the time the clock was started
        - stopped_at: the time the clock was stopped
    '''

    _stopped: bool = False

    _started_at: pd.Timestamp
    _stopped_at: None | pd.Timestamp = None

    def __init__(self) -> None:
        self._started_at = self._time()

    @property
    def time(self) -> pd.Timestamp:
        if self._stopped is not None:
            return self._stopped_at  # type: ignore
        return self._get_time()

    @property
    def started_at(self) -> pd.Timestamp:
        return self._started_at

    @property
    def stopped_at(self) -> None | pd.Timestamp:
        return self._stopped_at

    @property
    def stopped(self) -> bool:
        return self._stopped

    @abstractmethod
    def _time(self) -> pd.Timestamp:
        ...

    def __enter__(self) -> Clock:
        global _CONTEXT_CLOCK
        if _CONTEXT_CLOCK is not None:
            raise RuntimeError('Clock context already entered')
        _CONTEXT_CLOCK = self
        return self

    def __exit__(
        self,
        tp: type[BaseException] | None,
        inst: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        # mark the clock as stopped
        self._stopped_at = self.time
        self._stopped = True

        # remove the clock from the context
        global _CONTEXT_CLOCK
        _CONTEXT_CLOCK = None
        return None


class RealClock(Clock):
    '''
    clock that proxies pd.Timestamp.now()
    use this for real time trading
    '''

    def __init__(self) -> None:
        super().__init__()

    def _time(self) -> pd.Timestamp:
        return pd.Timestamp.now()


class VirutalClock(Clock):
    '''
    clock that can be stepped forward manually
    use this to simulate historical data
    '''

    _virtual_time: pd.Timestamp

    def __init__(self, start: pd.Timestamp) -> None:
        self._virtual_time = start
        super().__init__()

    def _time(self) -> pd.Timestamp:
        return self._virtual_time

    def step(self, delta: pd.Timedelta) -> None:
        self._virtual_time += delta


_CONTEXT_CLOCK: Clock | None = None
_DEFAULT_CLOCK = RealClock()


class TimeDependant(metaclass=ABCMeta):
    '''
    objects that exist inside a clock context, do not create outisde a 'with' statment
    make sure to call super().__init__() in subclasses
    '''

    _clock: Clock

    def __init__(self) -> None:
        if _CONTEXT_CLOCK is None:
            logger.warning(
                'TimeIdentifiable Object was created outside of a clock context, falling back to default clock'
            )
        self._clock = _CONTEXT_CLOCK or _DEFAULT_CLOCK

    @property
    def clock(self):
        return self._clock
