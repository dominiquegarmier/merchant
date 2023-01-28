from __future__ import annotations

import random
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Literal
from typing import TypeAlias

import pandas as pd
from pyarrow import dataset as ds

from merchant.core.numeric import NormedDecimal
from merchant.trading.market.base import BaseBroker
from merchant.trading.market.base import BaseMarketData
from merchant.trading.market.base import Order
from merchant.trading.market.base import OrderExecution
from merchant.trading.market.base import Quote
from merchant.trading.tools.pair import TradingPair


SUPPORTED_AGGREGATES = ['1s', '1min', '5min', '15min', '30min', '1h', '1d', '1w', '1m']
AggregateType = Literal['1s', '1min', '5min', '15min', '30min', '1h', '1d', '1w', '1m']


class Dataset:
    _pairs: set[TradingPair]
    _aggregates: dict[TradingPair, set[AggregateType]]
    _dataset: ds.Dataset

    def __init__(self, path_to_dataset: str | Path) -> None:
        raise NotImplementedError

    def __getitem__(self, s: slice) -> pd.DataFrame:
        raise NotImplementedError


class HistoricalBroker(BaseBroker):
    def __init__(self) -> None:
        super().__init__()


class HistoricalMarketData(BaseMarketData):
    _dataset: Dataset

    def __init__(self, dataset: Dataset) -> None:
        super().__init__()

        # TODO: handle time from internal clock
        time = self.clock.time
        self._dataset = dataset

    def get_quote(self, pair: TradingPair) -> Quote:
        quote = _get_context_consistent_quote(
            self._dataset, pair, self.clock.time, rel_time=0
        )


_TCandle: TypeAlias = tuple[int, int, float, float, float, float, float]


def _unpack_ohlcv(df: pd.DataFrame) -> _TCandle:
    return (  # type: ignore
        df['timestamp_start'].astype(int),
        df['timestamp_end'].astype(int),
        df['open'],
        df['high'],
        df['low'],
        df['close'],
        df['volume'],
    )


_CACHED_TIMESTAMP = None
_QUOTE_CACHE: dict[TradingPair, NormedDecimal] = {}

_WINDOW_SIZE = 100
_CACHED_WINDOW_SLICE: pd.DataFrame | None = None
_CACHED_WINDOW: tuple[pd.Timestamp, pd.Timestamp] | None = None


def _get_window_slice(timestamp: pd.Timestamp) -> tuple[pd.Timestamp, pd.Timestamp]:
    # multiply by aggregate size
    window = _WINDOW_SIZE * 60 * 1000_000_000
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
            _CACHED_WINDOW[0] : _CACHED_WINDOW[1]  # noqa: E203
        ]
    return _CACHED_WINDOW_SLICE


def _get_containing_candle(
    dataset: Dataset, timestamp: pd.Timestamp, pair: TradingPair
) -> _TCandle:
    window = _get_rolling_window_cached(dataset=dataset, timestamp=timestamp)
    # TODO: fix this once final dataframe format is decided
    pair_df = window[str(pair)]
    return _unpack_ohlcv(pair_df.iloc[pair_df.index.get_loc(timestamp, method='nearest')])  # type: ignore


def _randomized_interpolated_quote(
    candle: _TCandle, timestamp: pd.Timestamp
) -> NormedDecimal:
    '''
    get a random value of a realistic quote, based on a candle and the relative time rel_time in [0, 1]
    '''
    ts_start, ts_end, open, high, low, close, _ = candle
    rel_time = (timestamp.value - ts_start) / (ts_end - ts_start)

    WINDOW_RATIO = 2  # maximal slope of the piecewise affine function
    h_open = max(0, 1 - WINDOW_RATIO * rel_time)
    h_close = max(0, WINDOW_RATIO * rel_time - 1)
    h_mid = max(0, min(1, (1 - abs(rel_time - 0.5) * 2) * WINDOW_RATIO))

    return NormedDecimal(
        h_open * open + h_close * close + h_mid * random.uniform(low, high)
    )


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
    return _QUOTE_CACHE[pair]
