from __future__ import annotations

from collections.abc import Iterator
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from pathlib import Path
from typing import Literal
from typing import TypeAlias

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from polygon import RESTClient
from urllib3 import HTTPResponse

from merchant.data.tickers import Ticker


AggIntervall: TypeAlias = Literal['1s', '1m', '5m', '15m', '1h', '1d']

AGG_INTERVAL_MAPPING: dict[str, str] = {
    's': 'second',
    'm': 'minute',
    'h': 'hour',
    'd': 'day',
}


def set_data_path(path: Path | str) -> None:
    DatasetMetaData.data_path = Path(path)


@dataclass
class APISettings:
    api_key: str
    api_cooldown: int


def _interval_to_tuple(interval: AggIntervall) -> tuple[int, str]:
    return int(interval[:-1]), AGG_INTERVAL_MAPPING[interval[-1]]


def _get_agg_interval(
    settings: APISettings, ticker: Ticker, interval: AggIntervall
) -> pd.DataFrame:
    client = RESTClient(settings.api_key)

    multiplier, timespan = _interval_to_tuple(interval=interval)

    resp = client.get_aggs(
        ticker=ticker.ticker,
        multiplier=multiplier,
        timespan=timespan,
        from_=datetime(2022, 1, 1),
        to=datetime(2022, 2, 1),
        limit=50000,
        sort='asc',
        adjusted=True,
    )

    if isinstance(resp, HTTPResponse):
        raise RuntimeError(f'HTTP Error: {resp.status}')

    ret = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
    for agg in resp:
        ret.loc[agg.timestamp * 1_000_000] = [
            agg.open,
            agg.high,
            agg.low,
            agg.close,
            agg.volume,
        ]  # convert ms timestamp to ns
    raise NotImplementedError


def _normalize_data(data: pd.DataFrame) -> pa.Table:
    raise NotImplementedError


def drop_duplicates() -> None:
    '''drop duplicate timestamp / ticker combinations'''
    raise NotImplementedError


def download_dataset(
    settings: APISettings, tickers: Sequence[Ticker], interval: AggIntervall
) -> Dataset:
    data = pd.DataFrame()
    for ticker in tickers:
        df = _get_agg_interval(settings=settings, ticker=ticker, interval=interval)
        df.columns = pd.MultiIndex.from_product(
            [[f'{ticker.exchange}:{ticker.ticker}'], df.columns]
        )
        data = pd.concat([data, df], axis=1)

    normalized = _normalize_data(data=data)

    ds.write_dataset(
        data=normalized,
        base_dir=DatasetMetaData.data_path,
        partitioning=DatasetMetaData.partitioning,
    )

    raise NotImplementedError


class DatasetMetaData:
    data_path: Path = Path(__file__).parent.parent.parent / 'data'
    schema: pa.Schema = pa.schema(
        [
            ('timestamp', pa.uint64()),
            ('exchange:ticker', pa.string()),
            ('open', pa.float64()),
            ('high', pa.float64()),
            ('low', pa.float64()),
            ('close', pa.float64()),
            ('volume', pa.float64()),
        ]
    )
    partitioning: ds.Paritioning = ds.partitioning(
        schema=pa.schema([('ticker', pa.string())]),
        flavor='hive',
    )


class Dataset:

    _tickers: list[Ticker]
    _dataset: ds.Dataset
    _dataset_path: Path
    _intervall: AggIntervall

    def __init__(
        self,
        tickers: Sequence[Ticker],
        intervall: AggIntervall = '1s',
    ) -> None:
        self._tickers = list(tickers)
        self._dataset_path = DatasetMetaData.data_path / intervall.upper()

        self._dataset = ds.dataset(
            self._dataset_path,
            schema=DatasetMetaData.schema,
            partitioning=DatasetMetaData.partitioning,
            format='parquet',
        )

    @property
    def intervall(self) -> AggIntervall:
        return self._intervall

    @property
    def tickers(self) -> list[Ticker]:
        return self._tickers

    @cached_property
    def _exchange_ticker_pairs(self) -> list[str]:
        return [f'{ticker.exchange}:{ticker.ticker}' for ticker in self.tickers]

    def __contains__(self, ticker: Ticker) -> bool:
        return ticker in self._tickers

    def __len__(self) -> int:
        return len(self.tickers)

    def __iter__(self) -> Iterator[Ticker]:
        return self.tickers.__iter__()

    def __getitem__(self, s: slice) -> pd.DataFrame:
        ts_from, ts_to = pd.Timestamp(s.start).value, pd.Timestamp(s.stop).value

        if ts_from is None:
            ts_from = pd.Timestamp.min.value
        if ts_to is None:
            ts_to = pd.Timestamp.max.value

        if s.step is not None:
            raise ValueError('step not supported')

        table = self._dataset.to_table(
            filter=(
                (ds.field('exchange_ticker').isin(self._exchange_ticker_pairs))
                & (ds.field('timestamp') >= ts_from)
                & (ds.field('timestamp') <= ts_to)
            ),
        )
        raise NotImplementedError
        return (
            table.to_pandas()
            .pivot(index='timestamp', columns='exchange:ticker')
            .swaplevel(axis=1)
            .sort_index(axis=1)
        )
