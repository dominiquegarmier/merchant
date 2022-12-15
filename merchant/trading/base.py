from __future__ import annotations

from abc import ABCMeta
from abc import abstractmethod
from abc import abstractproperty
from typing import TypeVar


Time = int


class InternalClockMeta(metaclass=ABCMeta):
    @abstractproperty
    def time(self) -> Time:
        ...

    @abstractmethod
    def reset(self) -> None:
        ...

    @abstractmethod
    def step(self) -> None:
        ...


class HasInternalClock(metaclass=ABCMeta):
    _clock: InternalClockMeta

    @property
    def time(self) -> Time:
        return self._clock.time

    def reset(self) -> None:
        self._clock.reset()

    def step(self) -> None:
        self._clock.step()
