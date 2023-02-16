from __future__ import annotations

from abc import ABCMeta
from abc import abstractmethod
from typing import TYPE_CHECKING
from typing import TypeAlias
from typing import TypeVar

import gym
import numpy as np

if TYPE_CHECKING:
    from merchant.environment.base.components import (
        BaseRewardScheme,
        BaseObservationScheme,
        BaseActionScheme,
        BaseRenderer,
    )

ObsType: TypeAlias = np.ndarray
ActType: TypeAlias = np.ndarray


class TradingEnvAbstract(gym.Env[ObsType, ActType], metaclass=ABCMeta):
    _reward_scheme: BaseRewardScheme
    _action_scheme: BaseActionScheme[ActType]
    _observer: BaseObservationScheme[ObsType]
    _renderer: BaseRenderer

    def __init__(
        self,
        reward_schema: type[BaseRewardScheme],
        observer: type[BaseObservationScheme],
        action_scheme: type[BaseActionScheme],
        renderer: type[BaseRenderer],
    ) -> None:
        super().__init__()
