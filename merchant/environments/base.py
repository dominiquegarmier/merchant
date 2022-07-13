from __future__ import annotations

from datetime import datetime
from typing import Any
from typing import Literal
from typing import NamedTuple

import gym
import pandas as pd
from gym.spaces import MultiDiscrete

from merchant.environments.market import MarketSimulation
from merchant.environments.portfolio import PortfolioSimulation


class Action(NamedTuple):
    type: Literal['BUY', 'SELL']
    symbol: str
    quantity: int
    price: float


class BaseMarketEnvironment(gym.Env):
    _action_history: list[Action]

    _portfolio_simulation: PortfolioSimulation
    _market_simulation: MarketSimulation

    def __init__(self) -> None:
        super().__init__()

        self._action_history = []

        self._portfolio_simulation = PortfolioSimulation()
        self._market_simulation = MarketSimulation()  # type: ignore

        # start the market simulation
        self._portfolio_simulation.start(datetime(2020, 1, 1))
        self._market_simulation.start(datetime(2020, 1, 1))

    def reset(self, seed=None) -> Any:
        super().reset(seed=seed)

        self._action_history = []

        self._portfolio_simulation = PortfolioSimulation()
        self._market_simulation = MarketSimulation()  # type: ignore

        # start the market simulation
        self._portfolio_simulation.start(datetime(2020, 1, 1))
        self._market_simulation.start(datetime(2020, 1, 1))
