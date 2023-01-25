from __future__ import annotations

import time
from abc import ABCMeta
from abc import abstractmethod
from logging import getLogger
from types import TracebackType

logger = getLogger(__name__)


class Clock(metaclass=ABCMeta):
    _stopped: bool = False
    _stopped_at: None | int = None
    _started_at: int

    def __init__(self) -> None:
        self._started_at = self._get_time()

    @property
    def time(self) -> int:
        if self._stopped is not None:
            return self._stopped_at  # type: ignore
        return self._get_time()

    @property
    def stopped(self) -> bool:
        return self._stopped

    @abstractmethod
    def _get_time(self) -> int:
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
    def _get_time(self) -> int:
        return int(time.time_ns())


class VirutalClock(Clock):
    _time = 0

    def __init__(self, start: int) -> None:
        self._time = start
        super().__init__()

    def _get_time(self) -> int:
        return self._time

    def step(self, ns: int) -> None:
        self._time += ns


_CONTEXT_CLOCK: Clock | None = None
_DEFAULT_CLOCK = RealClock()


class TimeIdentifiable(metaclass=ABCMeta):
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


class MarketData(metaclass=ABCMeta):
    pass
