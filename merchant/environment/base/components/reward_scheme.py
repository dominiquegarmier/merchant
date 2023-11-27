from __future__ import annotations

from abc import ABCMeta
from abc import abstractmethod
from typing import Generic
from typing import TypeVar

from merchant.environment.base.env import TradingEnvAbstract

RewardType = TypeVar('RewardType')
ObsType = TypeVar('ObsType')


class BaseRewardScheme(Generic[RewardType], metaclass=ABCMeta):
    @abstractmethod
    def __call__(self, env: TradingEnvAbstract) -> RewardType:
        raise NotImplementedError

    @abstractmethod
    def reset(self) -> None:
        raise NotImplementedError
