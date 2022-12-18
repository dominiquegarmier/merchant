from __future__ import annotations

from abc import ABCMeta
from typing import TypeVar

import gym
import numpy as np

ObsType = np.ndarray
ActType = np.ndarray


class TradingEnvAbstract(gym.Env[ObsType, ActType], metaclass=ABCMeta):
    pass
