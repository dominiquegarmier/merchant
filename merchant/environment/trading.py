from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any
from typing import Literal
from typing import NamedTuple
from typing import TypeAlias

import gym
import numpy as np
import torch
from gym.spaces import Box
from ray.rllib.env.env_context import EnvContext

from merchant.data.tickers import Ticker
from merchant.environment.market import HistoricalMarket
from merchant.environment.market import MarketOrder


ObsType: TypeAlias = np.ndarray
ActType: TypeAlias = np.ndarray


class TradingEnvironment(gym.Env[ObsType, ActType]):

    action_space: Box
    observation_space: Box

    _tickers: Sequence[Ticker]
    _market_simulation: HistoricalMarket

    def __init__(self, config: EnvContext) -> None:
        self._tickers = config['tickers']
        self._market_simulation = HistoricalMarket(
            start=1, end=2, step=1, tickers=self._tickers
        )

        self.action_space = Box(low=-1, high=1, shape=(len(self._tickers),))
        self.observation_space = Box(
            low=0, high=np.inf, shape=(len(self._tickers), 1000, 5)
        )

    def reset(
        self,
        seed: int | None = None,
        return_info: bool = False,
        options: dict | None = None,
    ) -> tuple[ObsType, dict]:
        self._market_simulation.reset()
        self._market_simulation.start()

        return self._get_observation(), {}

    def step(self, action) -> tuple[ObsType, float, bool, dict]:
        orders = self._get_orders(action)
        self._market_simulation.execute_orders(orders)

        done = not self._market_simulation.step()
        return self._get_observation(), 0, done, {}

    def _get_observation(self) -> ObsType:
        if not self._market_simulation.is_running:
            raise RuntimeError('simulation not running')
        return self.observation_space.sample()

    def _get_orders(self, action: ActType) -> Sequence[MarketOrder]:
        EPSILON = 0.1
        ret: list[MarketOrder] = []

        for index, ticker in enumerate(self._tickers):
            if abs(action[index]) > EPSILON:
                ord = MarketOrder(
                    ticker=ticker,
                    limit=0,
                    quantity=action[index],  # TODO: scale quantity
                    type='BUY' if action[index] > 0 else 'SELL',
                )
                ret.append(ord)
        return ret
