from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Sequence

import pandas as pd
import pyarrow as pa
import ray.data as da
from oakstore import Store

from merchant.utils.helpers import Ticker


def set_data_path(path: Path | str) -> None:
    Dataset.MetaData.data_path = Path(path)


def download_from_polygon_api(tickers: Sequence[Ticker]) -> Dataset:
    raise NotImplementedError


def load_dataset(tickers: Sequence[Ticker]) -> Dataset | None:
    try:
        ds = Dataset(tickers=tickers)
    except Exception:
        return None
    return ds


class Dataset:
    class MetaData:
        data_path: Path = Path(__file__).parent.parent.parent / 'data'
        schema: pa.Schema = pa.schema(
            [
                ('timestamp', pa.uint64()),
                ('open', pa.float64()),
                ('high', pa.float64()),
                ('low', pa.float64()),
                ('close', pa.float64()),
                ('volume', pa.float64()),
            ]
        )

    _store: Store
    _tickers: list[Ticker]
    _ticker_datasets: dict[Ticker, da.Dataset]

    def __init__(
        self,
        tickers: Sequence[Ticker],
    ) -> None:
        self._tickers = list(tickers)

        for ticker in self._tickers:
            parquet_path = self.MetaData.data_path / f'{ticker.ticker}.parquet'
            try:
                self._ticker_datasets[ticker] = da.read_parquet(
                    paths=parquet_path, schema=self.MetaData.schema
                )
            except FileNotFoundError:
                raise ValueError(f'Ticker {ticker} not found in dataset')

    @property
    def tickers(self) -> list[Ticker]:
        return self._tickers

    def __contains__(self, ticker: Ticker) -> bool:
        return ticker in self._tickers

    def __getitem__(
        self, s: slice | int | datetime | pd.Timestamp | tuple[slice, Sequence[Ticker]]
    ) -> pd.DataFrame:
        if isinstance(s, tuple):
            s, tickers = s
        elif not isinstance(s, slice):
            pass  # get nearest to date
        elif s.step is not None:
            raise ValueError('step not supported')

        ret = []
        tickers = self._tickers if tickers is None else tickers
        for ticker in tickers:
            if ticker not in self:
                raise ValueError(f'Ticker {ticker} not found in dataset')
            ret.append(self._store[ticker][s])
