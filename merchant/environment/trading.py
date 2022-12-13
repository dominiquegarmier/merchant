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
from merchant.environment.portfolio import Portfolio


ObsType: TypeAlias = np.ndarray
ActType: TypeAlias = np.ndarray


class TradingEnvironment(gym.Env[ObsType, ActType]):

    action_space: Box
    observation_space: Box

    _tickers: Sequence[Ticker]
    _market_simulation: HistoricalMarket
    _portfolio_simulation: Portfolio

    def __init__(self, config: EnvContext) -> None:
        self._tickers = config['tickers']
        self._market_simulation = HistoricalMarket(
            start=1, end=2, step=1, tickers=self._tickers
        )
        self._portfolio_simulation = Portfolio(
            start_cash=config['start_cash'], market=self._market_simulation
        )

        self.action_space = Box(low=0, high=np.inf, shape=(len(self._tickers),))
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

    def step(self, action: ActType) -> tuple[ObsType, float, bool, dict]:
        try:
            sell_action = 1 - np.maximum(action, 1)  # value in [0, 1]
            self._exectue_sell_side(action=sell_action)

            buy_action = np.minimum(action, 1) - 1  # value between in [0, inf)
            buy_action = self._check_adj_buy_side(action=buy_action)
            self._exectue_buy_side(action=buy_action)
        except Exception:  # TODO catch market order exception
            raise

        done = not self._market_simulation.step()
        return self._get_observation(), 0, done, {}

    def _get_observation(self) -> ObsType:
        raise NotImplementedError

    def _exectue_sell_side(self, action: ActType) -> None:
        orders: list[MarketOrder] = []
        for stock_idx, signal in enumerate(action):
            if signal == 0:
                continue
            ticker = self._tickers[stock_idx]
            position = self._portfolio_simulation[ticker]
            if position.quantity == 0:
                continue

            sell_amount = int(signal * position.quantity)
            order = MarketOrder(ticker=ticker, quantity=sell_amount, type='SELL')
            orders.append(order)

        excs = self._market_simulation.execute_orders(ords=orders)  # noqa
        return None

    def _check_adj_buy_side(self, action: ActType) -> ActType:
        return action

    def _exectue_buy_side(self, action: ActType) -> None:
        return None
