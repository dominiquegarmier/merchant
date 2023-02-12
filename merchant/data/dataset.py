from __future__ import annotations

import os
from collections.abc import Collection
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockTradesRequest
from selenium import webdriver

_DATASET_REGISTRY: dict[str, DatasetMetadata] = {}


OHLCV_SCHEMA = pa.schema(
    [
        ('timestamp', pa.timestamp('ns')),  # default timezone is UTC
        ('ticker', pa.string()),
        ('open', pa.float64()),
        ('high', pa.float64()),
        ('low', pa.float64()),
        ('close', pa.float64()),
        ('volume', pa.uint64()),
        ('trades', pa.uint64()),
        ('vw_price', pa.float64()),
    ]
)

OHLCV_PARTITIONING = ds.partitioning(
    schema=pa.schema(
        [
            ('ticker', pa.string()),
        ]
    ),
)

# TODO
# 'taker' is dictionary encoded see https://arrow.apache.org/docs/cpp/parquet.html#encodings
TRADES_SCHEMA = pa.schema(
    [
        ('timestamp', pa.timestamp('ns')),  # default timezone is UTC
        ('ticker', pa.string()),
        ('exchange', pa.string()),
        ('taker', pa.string()),
        ('id', pa.string()),
        ('price', pa.float64()),
        ('size', pa.uint64()),
        ('condition', pa.string()),
        ('correction', pa.string()),
    ]
)

TRADES_PARTITIONING = ds.partitioning(
    schema=pa.schema(
        [
            ('ticker', pa.string()),
            ('exchange', pa.string()),
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


@dataclass
class DatasetMetadata:
    path: Path | str | bytes | os.PathLike
    schema: pa.Schema
    partitioning: ds.Partitioning


def register_dataset(name: str, metadata: DatasetMetadata) -> None:
    if name in _DATASET_REGISTRY:
        raise ValueError(f'Dataset {name} already registered')
    _DATASET_REGISTRY[name] = metadata


class Dataset:
    _pyarrow_dataset: ds.Dataset
    _filter: pa.Expression | None = None

    def __init__(self, metadata: DatasetMetadata) -> None:
        self._pyarrow_dataset = ds.dataset(
            metadata.path,
            schema=metadata.schema,
            format='parquet',
            partitioning=metadata.partitioning,
        )

    def set_filter(
        self, include=Collection[str] | None, exclude=Collection[str] | None
    ) -> None:
        if (include is None) == (exclude is None):
            raise ValueError('either include or exclude must be specified')
        if include is not None:
            self._filter = pc.field('ticker').isin(include)
        else:
            self._filter = ~pc.field('ticker').isin(exclude)

    def _get_table_slice(self, s: slice) -> pa.Table:
        start, stop = s.start, s.stop
        if s.step is not None:
            raise KeyError('step is not supported')
        if start is None or stop is None:
            raise KeyError('start and stop must be specified')

        expr = pc.field('timestamp') >= pa.scalar(start, type=pa.timestamp('ns'))
        expr &= pc.field('timestamp') <= pa.scalar(stop, type=pa.timestamp('ns'))

        if self._filter is not None:
            expr &= self._filter

        return self._pyarrow_dataset.to_table(filter=expr)

    def __getitem__(self, s: slice) -> pd.DataFrame:
        return cast(pd.DataFrame, self._get_table_slice(s).to_pandas())


def registerd_datasets() -> list[str]:
    return list(_DATASET_REGISTRY.keys())


def load_dataset(name: str) -> Dataset:
    if name not in _DATASET_REGISTRY:
        raise ValueError(f'Dataset {name} not registered')
    return Dataset(metadata=_DATASET_REGISTRY[name])


__all__ = [
    'Dataset',
    'DatasetMetadata',
    'load_dataset',
]
