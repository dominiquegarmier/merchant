from __future__ import annotations

import os
import random
from abc import ABCMeta
from abc import abstractmethod
from abc import abstractproperty
from collections.abc import Collection
from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from pathlib import Path
from typing import cast
from typing import Literal

import exchange_calendars as xcals
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

from merchant.data.constants import Aggregates
from merchant.data.synthetic import synthetic_intraday

_DATASET_REGISTRY: dict[str, DatasetSpec] = {}


@dataclass(frozen=True)
class DatasetSpec:
    path: Path | str | bytes | os.PathLike
    schema: pa.Schema
    partitioning: ds.Partitioning


class Dataset(metaclass=ABCMeta):
    @abstractproperty
    def tickers(self) -> list[str]:
        ...

    @abstractmethod
    def slice(
        self, from_: pd.Timestamp | None = None, to_: pd.Timestamp | None = None
    ) -> pd.DataFrame:
        ...

    @abstractmethod
    def range(
        self, from_: pd.Timestamp | None = None, to_: pd.Timestamp | None = None
    ) -> Generator[tuple[pd.Timestamp, pd.Series], None, None]:
        ...

    @abstractmethod
    def get(
        self,
        timestamp: pd.Timestamp,
        num: int = 1,
        method: Literal['left', 'right'] = 'left',
    ) -> pd.DataFrame:
        ...


# TODO make this a Protocol
class OHLCVDataset(Dataset, metaclass=ABCMeta):
    @abstractproperty
    def aggregate_type(self) -> Aggregates:
        ...


OHLCV_SCHEMA = pa.schema(
    [
        ('TIMESTAMP', pa.timestamp('ns')),  # default timezone is UTC
        ('TICKER', pa.string()),
        ('OPEN', pa.float64()),
        ('HIGH', pa.float64()),
        ('LOW', pa.float64()),
        ('CLOSE', pa.float64()),
        ('VOLUME', pa.uint64()),
        ('TRADES', pa.uint64()),
        ('VW_PRICE', pa.float64()),
    ]
)

OHLCV_PARTITIONING = ds.partitioning(
    schema=pa.schema(
        [
            ('TICKER', pa.string()),
        ]
    ),
)
# TODO
TRADES_SCHEMA = pa.schema(
    [
        ('TIMESTAMP', pa.timestamp('ns')),  # default timezone is UTC
        ('TICKER', pa.string()),
        ('EXCHANGE', pa.string()),
        ('TAKER', pa.string()),
        ('ID', pa.string()),
        ('PRICE', pa.float64()),
        ('SIZE', pa.uint64()),
        ('CONDITION', pa.string()),
        ('CORRECTION', pa.string()),
    ]
)

TRADES_PARTITIONING = ds.partitioning(
    schema=pa.schema(
        [
            ('TICKER', pa.string()),
            ('EXCHANGE', pa.string()),
        ]
    ),
)


def register_dataset(name: str, metadata: DatasetSpec) -> None:
    if name in _DATASET_REGISTRY:
        raise ValueError(f'Dataset {name} already registered')
    _DATASET_REGISTRY[name] = metadata


def _validate_datetime_slice(s: slice) -> tuple[datetime, datetime]:
    start, stop = s.start, s.stop
    if s.step is not None:
        raise KeyError('step is not supported')
    if start is None or stop is None:
        raise KeyError('start and stop must be specified')

    # supports anything that is accepted by pd.Timestamp
    if not isinstance(start, datetime):
        start = pd.Timestamp(start).to_pydatetime()
    if not isinstance(stop, datetime):
        stop = pd.Timestamp(stop).to_pydatetime()

    return start, stop


# TODO update this to use the new dataset API
class ParquetDataset(Dataset):
    _pyarrow_dataset: ds.Dataset
    _filter: pa.Expression | None = None
    _dataset_spec: DatasetSpec

    def __init__(self, metadata: DatasetSpec) -> None:
        # infer the schema to preserve schema.metadata
        self._pyarrow_dataset = ds.dataset(
            metadata.path,
            format='parquet',
            partitioning=metadata.partitioning,
        )

        if not self.schema.equals(metadata.schema, check_metadata=False):
            raise ValueError(
                f'schema mismatch: {self.schema.metadata} != {metadata.schema.metadata} (metadata is ignored)'
            )

        # TODO what about non ticker datasets?
        if not all(name in self.schema.names for name in ('TICKER', 'TIMESTAMP')):
            raise ValueError('schema must contain `TICKER` and `TIMESTAMP` fields')

    @property
    def schema(self) -> pa.Schema:
        return self._pyarrow_dataset.schema

    @property
    def metadata(self) -> dict[bytes, bytes]:
        return cast(dict[bytes, bytes], self.schema.metadata)

    @cached_property
    def tickers(self) -> list[str]:
        return cast(
            list[str],
            pc.unique(self._pyarrow_dataset.scanner(columns=['TICKER'])).to_pylist(),
        )

    def set_filter(
        self, include=Collection[str] | None, exclude=Collection[str] | None
    ) -> None:
        if (include is None) == (exclude is None):
            raise ValueError('either include or exclude must be specified')
        if include is not None:
            self._filter = pc.field('TICKER').isin(include)
        else:
            self._filter = ~pc.field('TICKER').isin(exclude)

    def _get_table_slice(self, s: slice) -> pa.Table:
        start, stop = _validate_datetime_slice(s)
        expr = pc.field('TIMESTAMP') >= pa.scalar(start, type=pa.timestamp('ns'))
        expr &= pc.field('TIMESTAMP') <= pa.scalar(stop, type=pa.timestamp('ns'))

        if self._filter is not None:
            expr &= self._filter

        return self._pyarrow_dataset.to_table(filter=expr)

    def __getitem__(self, s: slice) -> pd.DataFrame:
        '''
        return a DataFrame with multiindex columns and timestamp as rows index
        the outer column level is for the ticker
        '''
        dataframe = cast(pd.DataFrame, self._get_table_slice(s).to_pandas())
        dataframe.pivot(index='TIMESTAMP', columns='TICKER').swaplevel(
            axis=1
        ).sort_index(axis=1)

        return dataframe


# Defaults for synthetic dataset
_SYNTHETIC_DEFAULT_N_TICKERS = 10
_SYNTHETIC_DEFAULT_START_PRICE_RANGE = (100, 1000)
_SYNTHETIC_DEFAULT_DAILY_VOLATILITY = 0.01
_SYNTHETIC_DEFAULT_DAILY_SHIFT = 0
_SYNTHETIC_DEFAULT_MEAN_VOLUME = 10_000_000
_SYNTHETIC_DEFAULT_MEAN_TRADE_SIZE = 1_000


class SyntheticDataset(Dataset):
    _tickers: list[str]
    _data: pd.DataFrame

    _start: datetime
    _end: datetime
    _aggregate: Aggregates

    def __init__(
        self,
        start: datetime | str,
        end: datetime | str,
        tickers: list[str] | dict[str, float] | None = None,
        aggregate: Aggregates = Aggregates.MIN,
    ):
        if tickers is None:
            tickers = [
                f'SYN{chr(i) if i <= ord("Z") else i - ord("Z")}'
                for i in range(ord('A'), ord('A') + _SYNTHETIC_DEFAULT_N_TICKERS)
            ]
        self._tickers = list(tickers)
        self._aggregate = aggregate

        if isinstance(start, str):
            start = datetime.strptime(start, '%Y-%m-%d')
        if isinstance(end, str):
            end = datetime.strptime(end, '%Y-%m-%d')

        self._start = start
        self._end = end
        self._data = self._gen_data(tickers if isinstance(tickers, dict) else None)

    def slice(
        self, from_: pd.Timestamp | None = None, to_: pd.Timestamp | None = None
    ) -> pd.DataFrame:
        from_ts = from_ if from_ is not None else pd.Timestamp.min
        to_ts = to_ if to_ is not None else pd.Timestamp.max
        return cast(
            pd.DataFrame,
            self._data.loc[(self._data.index >= from_ts) & (self._data.index <= to_ts)],
        )

    def range(
        self, from_: pd.Timestamp | None = None, to_: pd.Timestamp | None = None
    ) -> Generator[tuple[pd.Timestamp, pd.Series], None, None]:
        for index, row in self.slice(from_, to_).iterrows():
            yield cast(pd.Timestamp, index), row

    def get(
        self,
        timestamp: pd.Timestamp,
        num: int = 1,
        method: Literal['left', 'right'] = 'left',
    ) -> pd.DataFrame:
        if method == 'left':
            return cast(
                pd.DataFrame, self._data.loc[self._data.index <= timestamp].tail(num)
            )
        elif method == 'right':
            return cast(
                pd.DataFrame, self._data.loc[self._data.index >= timestamp].head(num)
            )

    @property
    def tickers(self) -> list[str]:
        return self._tickers

    def _gen_data(self, start_prices: dict[str, float] | None = None) -> pd.DataFrame:
        xnys = xcals.get_calendar('XNYS')  # NYSE
        trading_days: list[str] = xnys.sessions_in_range(
            str(self._start.date()), str(self._end.date())
        ).to_list()

        ticker_data: dict[str, pd.DataFrame] = {}
        for ticker in self._tickers:
            if start_prices is None:
                a, b = _SYNTHETIC_DEFAULT_START_PRICE_RANGE
                start_price = random.random() * (b - a) + a
            else:
                start_price = start_prices[ticker]

            days: list[pd.DataFrame] = []
            for day in trading_days:
                sigma = _SYNTHETIC_DEFAULT_DAILY_VOLATILITY
                mu = _SYNTHETIC_DEFAULT_DAILY_SHIFT
                data = synthetic_intraday(
                    start_price,
                    day,
                    timeframe=self._aggregate,
                    mean_volume=_SYNTHETIC_DEFAULT_MEAN_VOLUME,
                    mean_trade_size=_SYNTHETIC_DEFAULT_MEAN_TRADE_SIZE,
                    sigma=sigma,
                    mu=mu,
                )
                assert data is not None, 'data is None'

                start_price = data['CLOSE'].iloc[-1]
                days.append(data)

            ticker_data[ticker] = pd.concat(days)
        return pd.concat(ticker_data, axis=1)


def registerd_datasets() -> list[str]:
    return list(_DATASET_REGISTRY.keys())


def load_dataset(name: str) -> Dataset:
    if name not in _DATASET_REGISTRY:
        raise ValueError(f'dataset {name} not registered')
    return ParquetDataset(metadata=_DATASET_REGISTRY[name])  # type: ignore


__all__ = [
    'Dataset',
    'DatasetSpec',
    'load_dataset',
]
