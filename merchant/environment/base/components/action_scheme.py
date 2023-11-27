from __future__ import annotations

from abc import ABCMeta
from abc import abstractmethod
from typing import Generic
from typing import TypeVar

from merchant.environment.base.env import TradingEnvAbstract

ActType = TypeVar('ActType')


class BaseActionScheme(Generic[ActType], metaclass=ABCMeta):
    @abstractmethod
    def __call__(self, env: TradingEnvAbstract, action: ActType) -> None:
        raise NotImplementedError

    @abstractmethod
    def reset(self) -> None:
        raise NotImplementedError
