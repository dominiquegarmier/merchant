from __future__ import annotations

from abc import ABCMeta
from abc import abstractmethod


class Stepper(metaclass=ABCMeta):
    @abstractmethod
    def step(self) -> bool:
        raise NotImplementedError
