from __future__ import annotations

import numpy as np

from merchant.data.dataset import Dataset
from merchant.trading.market.base import BaseMarketObserver
from merchant.trading.tools.instrument import Instrument
from merchant.trading.tools.pair import TradingPair


class HistoricalMarketObserver(BaseMarketObserver):
    _dataset: Dataset

    def __init__(self, dataset: Dataset) -> None:
        super().__init__()
        self._dataset = dataset

    @property
    def trading_pairs(self) -> set[TradingPair]:
        raise NotImplementedError

    @property
    def instruments(self) -> set[Instrument]:
        raise NotImplementedError

    @property
    def observation_shape(self) -> tuple[int, ...]:
        raise NotImplementedError

    def get_observation(self) -> np.ndarray:
        raise NotImplementedError
