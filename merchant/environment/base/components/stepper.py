from __future__ import annotations

from abc import ABCMeta
from abc import abstractmethod

from merchant.environment.base.env import TradingEnvAbstract


class BaseStepper(metaclass=ABCMeta):
    @abstractmethod
    def __call__(self, env: TradingEnvAbstract) -> None:
        raise NotImplementedError
