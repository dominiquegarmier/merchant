from __future__ import annotations

from abc import ABCMeta
from abc import abstractmethod
from typing import Generic
from typing import TypeVar

from merchant.environment.base.env import TradingEnvAbstract

ObsType = TypeVar('ObsType')


class BaseObservationScheme(Generic[ObsType], metaclass=ABCMeta):
    @abstractmethod
    def __call__(self, env: TradingEnvAbstract) -> ObsType:
        raise NotImplementedError

    @abstractmethod
    def reset(self) -> None:
        raise NotImplementedError
