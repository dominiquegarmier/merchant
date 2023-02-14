from __future__ import annotations

import random
from abc import ABCMeta
from abc import abstractmethod
from collections.abc import Collection
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Literal
from typing import Protocol
from typing import TypeAlias

import pandas as pd
from pyarrow import dataset as ds

from merchant.core.numeric import NormedDecimal
from merchant.data.dataset import Dataset
from merchant.trading.market.base import BaseBroker
from merchant.trading.market.base import BaseMarketData
from merchant.trading.market.base import InsufficientAssets
from merchant.trading.market.base import Order
from merchant.trading.market.base import OrderExecution
from merchant.trading.market.base import Quote
from merchant.trading.market.portfolio import Portfolio
from merchant.trading.tools.asset import Asset
from merchant.trading.tools.pair import TradingPair

SUPPORTED_AGGREGATES = ['1s', '1min', '5min', '15min', '30min', '1h', '1d', '1w', '1m']
AggregateType = Literal['1s', '1min', '5min', '15min', '30min', '1h', '1d', '1w', '1m']

SLIPPAGE_RATIO = 0.0001


_TCandle: TypeAlias = tuple[float, float, float, float, int, int, float]


# TODO: improve this
def _slippage_model(price: float) -> float:
    return price * (1 + random.uniform(-SLIPPAGE_RATIO, SLIPPAGE_RATIO))


class SlippageModel(Protocol):
    def __call__(
        self, order: Order, quote: NormedDecimal, candle: _TCandle
    ) -> NormedDecimal:
        # retuns adjusted (virtual) quote after slippage
        ...


class FeeModel(Protocol):
    def __call__(self, order: Order, quote: NormedDecimal) -> Asset:
        # returns the base asset costs due to fees
        ...


class MarketEngine(BaseBroker):
    _dataset: Dataset
    _portfolio: Portfolio
    _pairs: set[TradingPair]

    _slippage: SlippageModel
    _fees: FeeModel

    def __init__(
        self,
        dataset: Dataset,
        assets: Collection[Asset],
        slippage: SlippageModel,
        fees: FeeModel,
    ) -> None:
        super().__init__()
        self._dataset = dataset
        self._portfolio = Portfolio(assets=assets)

        self._fees = fees
        self._slippage = slippage

    def execute_order(self, order: Order) -> OrderExecution:
        timestamp = self.clock.time
        quote = _get_context_consistent_quote(
            self._dataset, timestamp=timestamp, pair=order.pair
        )
        candle = _get_containing_candle(
            self._dataset, timestamp=timestamp, pair=order.pair
        )

        # calculate finaly execution price after slippage and fees
        slippage_adj_quote = self._slippage(order=order, quote=quote, candle=candle)
        fees = self._fees(order=order, quote=slippage_adj_quote)
        price_after_fees = order.pair.sell * slippage_adj_quote * order.quantity + fees

        if self._portfolio.assets[order.pair.sell] < price_after_fees:
            raise InsufficientAssets(order=order)

        # keep track of transaction in portfolio
        self._portfolio._increase_holdings(asset=order.pair.buy * order.quantity)
        self._portfolio._decrease_holdings(asset=price_after_fees)

        return OrderExecution(
            order=order,
            timestamp=self.clock.time,
            rate=slippage_adj_quote,
            fees=fees,
        )

    def update_portfolio(self) -> Portfolio:
        '''update value of portfolio and record history'''
        raise NotImplementedError


# TODO
class HistoricalMarketData(BaseMarketData):
    _dataset: Dataset
    _portfolio: Portfolio

    def __init__(self, dataset: Dataset) -> None:
        super().__init__()
        self._dataset = dataset


def _unpack_ohlcv(df: pd.DataFrame) -> _TCandle:
    return (  # type: ignore
        df['OPEN'],
        df['HIGH'],
        df['LOW'],
        df['CLOSE'],
        df['VOLUME'],
        df['TRADES'],
        df['VW_PRICE'],
    )


_CACHED_TIMESTAMP = None
_QUOTE_CACHE: dict[TradingPair, float] = {}

_AGG_SIZE = 60
_WINDOW_SIZE = 100
_CACHED_WINDOW_SLICE: pd.DataFrame | None = None
_CACHED_WINDOW: tuple[pd.Timestamp, pd.Timestamp] | None = None


def _get_window_slice(timestamp: pd.Timestamp) -> tuple[pd.Timestamp, pd.Timestamp]:
    # multiply by aggregate size
    window = _WINDOW_SIZE * _AGG_SIZE * 1_000_000_000
    start = timestamp.value // window * window
    end = start + window

    return pd.Timestamp(start), pd.Timestamp(end)


def _get_rolling_window_cached(
    dataset: Dataset, timestamp: pd.Timestamp
) -> pd.DataFrame:
    global _CACHED_WINDOW
    global _CACHED_WINDOW_SLICE
    if (
        _CACHED_WINDOW is None
        or _CACHED_WINDOW_SLICE is None
        or _CACHED_WINDOW[0] > timestamp
        or _CACHED_WINDOW[1] < timestamp
    ):
        _CACHED_WINDOW = _get_window_slice(timestamp)
        _CACHED_WINDOW_SLICE = dataset[
            _CACHED_WINDOW[0].value : _CACHED_WINDOW[1].value  # noqa: E203
        ]
    return _CACHED_WINDOW_SLICE


def _get_containing_candle(
    dataset: Dataset, timestamp: pd.Timestamp, pair: TradingPair
) -> _TCandle:
    window = _get_rolling_window_cached(dataset=dataset, timestamp=timestamp)
    pair_df = window[str(pair)]  # TODO: cast to propper ticker
    index = pair_df.index.searchsorted(timestamp, side='left')

    return _unpack_ohlcv(pair_df.iloc[index])  # type: ignore


def _randomized_interpolated_quote(candle: _TCandle, timestamp: pd.Timestamp) -> float:
    '''
    get a random value of a realistic quote, based on a candle and the relative time rel_time in [0, 1]
    '''
    ts_start, ts_end, open, high, low, close, _ = candle
    rel_time = (timestamp.value - ts_start) / (ts_end - ts_start)

    WINDOW_RATIO = 2  # maximal slope of the piecewise affine function
    h_open = max(0, 1 - WINDOW_RATIO * rel_time)
    h_close = max(0, WINDOW_RATIO * rel_time - 1)
    h_mid = max(0, min(1, (1 - abs(rel_time - 0.5) * 2) * WINDOW_RATIO))

    return h_open * open + h_close * close + h_mid * random.uniform(low, high)


def _get_context_consistent_quote(
    dataset: Dataset,
    timestamp: pd.Timestamp,
    pair: TradingPair,
) -> NormedDecimal:
    global _QUOTE_CACHE
    global _CACHED_TIMESTAMP

    if _CACHED_TIMESTAMP != timestamp:
        _CACHED_TIMESTAMP = timestamp
        _QUOTE_CACHE.clear()

    if pair not in _QUOTE_CACHE:
        _QUOTE_CACHE[pair] = _randomized_interpolated_quote(
            _get_containing_candle(dataset, timestamp, pair), timestamp
        )
    return NormedDecimal(_QUOTE_CACHE[pair])
