from __future__ import annotations

from abc import ABCMeta
from abc import abstractmethod
from typing import Generic
from typing import TypeVar

ActType = TypeVar('ActType')


class BaseActionScheme(Generic[ActType], metaclass=ABCMeta):
    @abstractmethod
    def do_action(self, action: ActType) -> None:
        ...

    @abstractmethod
    def reset(self) -> None:
        ...
