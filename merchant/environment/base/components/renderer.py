from __future__ import annotations

from abc import ABCMeta
from abc import abstractmethod


class BaseRenderer(metaclass=ABCMeta):
    @abstractmethod
    def render(self) -> None:
        ...

    @abstractmethod
    def reset(self) -> None:
        ...
