from __future__ import annotations

import os
from collections.abc import Collection
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import cast

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

_DATASET_REGISTRY: dict[str, DatasetSpec] = {}


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


# UTP sale conditions
SALE_CONDITIONS = {
    '@': 'Regular Trade',
    'A': 'Acquisition',
    'B': 'Bunched Trade',
    'C': 'Cash Sale',
    'D': 'Distribution',
    'E': 'Placeholder',
    'F': 'Intermarket Sweep',
    'G': 'Bunched Sold Trade',
    'H': 'Price Variation Trade',
    'I': 'Odd Lot Trade',
    'K': 'Rule 155 Trade (AMEX)',
    'L': 'Sold Last',
    'M': 'Market Center Official Close',
    'N': 'Next Day',
    'O': 'Opening Prints',
    'P': 'Prior Reference Price',
    'Q': 'Market Center Official Open',
    'R': 'Seller',
    'S': 'Split Trade',
    'T': 'Form T',
    'U': 'Extended trading hours (Sold Out of Sequence)',
    'V': 'Contingent Trade',
    'W': 'Average Price Trade',
    'X': 'Cross/Periodic Auction Trade',
    'Y': 'Yellow Flag Regular Trade',
    'Z': 'Sold (out of sequence)',
    '1': 'Stopped Stock (Regular Trade)',
    '4': 'Derivatively priced',
    '5': 'Re-Opening Prints',
    '6': 'Closing Prints',
    '7': 'Qualified Contingent Trade (“QCT”)',
    '8': 'Placeholder For 611 Exempt',
    '9': 'Corrected Consolidated Close (per listing market)',
}

# Correction satus https://alpaca.markets/docs/api-references/market-data-api/stock-pricing-data/historical/#trade
CORRECTION_STATUS = {
    'normal': 'normal trade',
    'corrected': 'trade was corrected',
    'cancelled': 'trade was cancelled',
    'incorrect': 'trade was incorrectly reported',
}


class Aggregates(Enum):
    SEC = 1
    MIN = 60
    HOUR = 60 * 60
    DAY = 60 * 60 * 24


@dataclass(frozen=True)
class DatasetSpec:
    path: Path | str | bytes | os.PathLike
    schema: pa.Schema
    partitioning: ds.Partitioning


def register_dataset(name: str, metadata: DatasetSpec) -> None:
    if name in _DATASET_REGISTRY:
        raise ValueError(f'Dataset {name} already registered')
    _DATASET_REGISTRY[name] = metadata


class Dataset:
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
        start, stop = s.start, s.stop
        if s.step is not None:
            raise KeyError('step is not supported')
        if start is None or stop is None:
            raise KeyError('start and stop must be specified')

        expr = pc.field('TIMESTAMP') >= pa.scalar(start, type=pa.timestamp('ns'))
        expr &= pc.field('TIMESTAMP') <= pa.scalar(stop, type=pa.timestamp('ns'))

        if self._filter is not None:
            expr &= self._filter

        return self._pyarrow_dataset.to_table(filter=expr)

    def __getitem__(self, s: slice) -> pd.DataFrame:
        '''
        return a DataFrame with multiindex columns and timestamp as rows index
        the outer column level is for the ticker

        if you want the ohlcv for a specific ticker and time do:

        >>> df['AAPL'].loc[timestamp]

        or to find nearest timestamp:
        >>> idx = df['AAPL'].index.searchsorted(timestamp, side='left')]
        >>> df['AAPL'].iloc[idx]
        '''
        dataframe = cast(pd.DataFrame, self._get_table_slice(s).to_pandas())
        dataframe.pivot(index='TIMESTAMP', columns='TICKER').swaplevel(
            axis=1
        ).sort_index(axis=1)

        return dataframe


def registerd_datasets() -> list[str]:
    return list(_DATASET_REGISTRY.keys())


def load_dataset(name: str) -> Dataset:
    if name not in _DATASET_REGISTRY:
        raise ValueError(f'dataset {name} not registered')
    return Dataset(metadata=_DATASET_REGISTRY[name])


__all__ = [
    'Dataset',
    'DatasetSpec',
    'load_dataset',
]
