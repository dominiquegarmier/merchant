from __future__ import annotations

from abc import ABCMeta
from abc import abstractmethod
from abc import abstractproperty
from typing import TYPE_CHECKING
from typing import TypeAlias
from typing import TypeVar

import gymnasium as gym
import numpy as np

from merchant.core.abstract import Clock

if TYPE_CHECKING:
    from merchant.environment.base.components import BaseRewardScheme
    from merchant.environment.base.components import BaseObservationScheme
    from merchant.environment.base.components import BaseActionScheme
    from merchant.environment.base.components import BaseRenderer


ObsType: TypeAlias = np.ndarray
ActType: TypeAlias = np.ndarray


class TradingEnvAbstract(gym.Env[ObsType, ActType], metaclass=ABCMeta):
    _reward_scheme: BaseRewardScheme
    _action_scheme: BaseActionScheme[ActType]
    _observer: BaseObservationScheme[ObsType]
    _renderer: BaseRenderer

    @abstractmethod
    def __init__(
        self,
        reward_schema: type[BaseRewardScheme],
        observer: type[BaseObservationScheme],
        action_scheme: type[BaseActionScheme],
        renderer: type[BaseRenderer],
    ) -> None:
        super().__init__()

    @abstractproperty
    def clock(self) -> Clock:
        raise NotImplementedError
