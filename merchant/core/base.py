from __future__ import annotations

from abc import ABCMeta
from abc import abstractmethod
from abc import abstractproperty
from typing import Generic
from typing import TypeVar


T = TypeVar('T')


class InternalClockMeta(Generic[T], metaclass=ABCMeta):
    @abstractproperty
    def time(self) -> T:
        ...

    @abstractmethod
    def reset(self) -> None:
        ...

    @abstractmethod
    def step(self) -> None:
        ...


class HasInternalClock(Generic[T], metaclass=ABCMeta):
    _clock: InternalClockMeta[T]

    @property
    def time(self) -> T:
        return self._clock.time

    def reset(self) -> None:
        self._clock.reset()

    def step(self) -> None:
        self._clock.step()
