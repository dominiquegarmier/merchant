from __future__ import annotations

from typing import Any
from typing import TypeAlias

import gymnasium as gym
import numpy as np
import pandas as pd

from merchant.core.abstract import VirutalClock
from merchant.core.numeric import NormedDecimal
from merchant.data.dataset import SyntheticDataset
from merchant.environment.base.env import TradingEnvAbstract
from merchant.trading.market.base import Order
from merchant.trading.market.historical import HistoricalMarketBroker
from merchant.trading.market.historical import HistoricalMarketObserver
from merchant.trading.tools.instrument import Instrument
from merchant.trading.tools.instrument import USD

ObsType: TypeAlias = np.ndarray
ActType: TypeAlias = np.ndarray


class TradingEnv(gym.Env[ObsType, ActType]):
    action_space: gym.spaces.Box
    observation_space: gym.spaces.Box

    _clock: VirutalClock
    _start: pd.Timestamp
    _end: pd.Timestamp
    _initial_money: int

    _dataset: SyntheticDataset
    _broker: HistoricalMarketBroker
    _observer: HistoricalMarketObserver
    _instrument: Instrument
    _previous_value: float

    def __init__(
        self,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
        dataset: SyntheticDataset,
        initial_money: int = 10_000,
    ) -> None:
        super().__init__()

        self._start = start_time
        self._end = end_time
        self._initial_money = initial_money
        self._dataset = dataset

        # setup action and observation spaces
        obs_size = 512 * 7 + 2
        self.observation_space = gym.spaces.Box(
            low=-np.inf, high=np.inf, shape=(obs_size,)
        )
        self.action_space = gym.spaces.Box(low=-np.inf, high=np.inf, shape=(1,))
        self._clock = VirutalClock(start=self._start)
        self._clock.__enter__()  # TODO change this API

        self._broker = HistoricalMarketBroker(
            dataset=self._dataset, assets=(self._initial_money * USD,)
        )
        self._observer = HistoricalMarketObserver(dataset=self._dataset)
        self._instrument = self._broker.instruments[0]
        self._previous_value = initial_money

    def step(
        self, action: ActType
    ) -> tuple[ObsType, float, bool, bool, dict[str, Any]]:
        timestamp = self._clock.time
        order: Order | None = None
        if float(action) < 0:
            order = Order(
                pair=USD / self._instrument,
                quantity=NormedDecimal(-float(action)),
                timestamp=timestamp,
            )
        elif float(action) > 0:
            order = Order(
                pair=self._instrument / USD,
                quantity=NormedDecimal(float(action)),
                timestamp=timestamp,
            )

        if order is not None:
            self._broker.execute_order(order=order)

        portfolio_obs = self._broker.get_observation()
        market_obs = self._observer.get_observation()

        observation = np.concatenate((portfolio_obs.flatten(), market_obs.flatten()))
        self._clock.step(delta=self._broker.skip + self._broker.resolution)

        # TODO very naiv reward function
        new_value = self._broker.portfolio.value
        if new_value is not None:
            reward = float(new_value) - self._previous_value
            self._previous_value = float(new_value)
        else:
            reward = 0.0

        completed = self._clock.time >= self._end
        truncated = False  # early termination
        info = {
            'timestamp': timestamp,
            'orders': [order] if order is not None else [],
        }

        return (observation, reward, completed, truncated, info)

    def reset(
        self, *, seed: int | None = None, options: dict[str, Any] | None = None
    ) -> tuple[ObsType, dict[str, Any]]:
        # exit old clock
        self._clock.__exit__()

        # create and enter new clock
        self._clock = VirutalClock(start=self._start)
        self._clock.__enter__()  # TODO change this API

        self._broker = HistoricalMarketBroker(
            dataset=self._dataset, assets=(self._initial_money * USD,)
        )
        self._observer = HistoricalMarketObserver(dataset=self._dataset)
        self._instrument = self._broker.instruments[0]
        self._previous_value = self._initial_money

        portfolio_obs = self._broker.get_observation()
        market_obs = self._observer.get_observation()
        observation = np.concatenate((portfolio_obs.flatten(), market_obs.flatten()))
        self._clock.step(delta=self._broker.skip + self._broker.resolution)

        return (observation, {})
