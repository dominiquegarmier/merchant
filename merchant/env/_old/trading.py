from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from logging import getLogger
from typing import Any
from typing import Literal
from typing import NamedTuple
from typing import TYPE_CHECKING
from typing import TypeAlias

import gym
import numpy as np
import torch
from gym.spaces import Box
from ray.rllib.env.env_context import EnvContext

from merchant.data.tickers import Ticker
from merchant.env._old.market import HistoricalMarket
from merchant.env._old.market import MarketOrder
from merchant.env._old.market import MarketOrderExecution
from merchant.env._old.portfolio import Portfolio

logger = getLogger(__name__)

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
            start=1, end=2, tickers=self._tickers
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

        return self._get_observation(), {}

    def step(self, action: ActType) -> tuple[ObsType, float, bool, dict]:
        try:
            sell_action = 1 - np.maximum(action, 1)  # value in [0, 1]
            sell_excs = self._exectue_sell_side(action=sell_action)  # noqa

            buy_action = np.minimum(action, 1) - 1  # value between in [0, inf)
            buy_excs = self._exectue_buy_side(action=buy_action)  # noqa
        except Exception:  # TODO catch market order exception
            raise

        done = not self._market_simulation.step()
        return self._get_observation(), 0, done, {}

    def _get_observation(self) -> ObsType:
        '''Observation contains:

        - 1. OHLCV over some time period
        - 2. Portfolio information
            - i. Cash
            - ii. Value
            - iii. Positions
        '''
        raise NotImplementedError

    def _exectue_sell_side(self, action: ActType) -> list[MarketOrderExecution]:
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

        return self._market_simulation.execute_orders(ords=orders)  # type: ignore

    def _exectue_buy_side(self, action: ActType) -> list[MarketOrderExecution]:
        orders: list[MarketOrder] = []
        total_cost = 0.0

        for stock_idx, signal in enumerate(action):
            if signal == 0:
                continue
            ticker = self._tickers[stock_idx]
            unit_price = self._market_simulation[ticker]
            amount = int(signal * self._portfolio_simulation[ticker].quantity)
            total_cost += unit_price * amount

        if total_cost > self._portfolio_simulation.cash:
            adj_ratio = self._portfolio_simulation.cash / total_cost
            for order in orders:
                order.quantity = int(order.quantity * adj_ratio)

        return self._market_simulation.execute_orders(ords=orders)  # type: ignore

    def _update_portfolio(self, executions: list[MarketOrderExecution]) -> None:
        for exc in executions:
            if not exc.success:
                logger.debug(f'order failed: {exc.order}')
                continue

            if TYPE_CHECKING:
                assert exc.price is not None
                assert exc.quantity is not None

            self._portfolio_simulation.perform_action(
                action=exc.order.type,
                ticker=exc.order.ticker,
                quantity=exc.quantity,
                price=exc.price,
            )
