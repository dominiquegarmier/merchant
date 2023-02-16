from __future__ import annotations

from abc import ABCMeta
from abc import abstractmethod
from typing import Generic
from typing import TypeVar

ObsType = TypeVar('ObsType')


class BaseObservationScheme(Generic[ObsType], metaclass=ABCMeta):
    @abstractmethod
    def observe(self) -> ObsType:
        ...

    @abstractmethod
    def reset(self) -> None:
        ...
