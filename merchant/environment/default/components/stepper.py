from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

import pandas as pd

from merchant.environment.base.components.stepper import BaseStepper

if TYPE_CHECKING:
    from merchant.environment.base.env import TradingEnvAbstract
    from merchant.environment.base.components.action_scheme import BaseActionScheme


class DefaultStepper(BaseStepper):
    _step: pd.Timedelta
    _action_scheme: BaseActionScheme

    def __init__(self, step: pd.Timedelta, action_scheme: BaseActionScheme) -> None:
        super().__init__()

        self._step = step
        self._action_scheme = action_scheme

    # TODO this is shit
    def __call__(self, env: TradingEnvAbstract) -> None:
        env.clock.step(delta=self._step)
