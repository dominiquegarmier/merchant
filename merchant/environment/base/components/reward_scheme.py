from __future__ import annotations

from abc import ABCMeta
from abc import abstractmethod
from typing import Generic
from typing import TypeVar

RewardType = TypeVar('RewardType')


class BaseRewardScheme(Generic[RewardType], metaclass=ABCMeta):
    @abstractmethod
    def reward(self) -> RewardType:
        ...

    @abstractmethod
    def reset(self) -> None:
        ...
