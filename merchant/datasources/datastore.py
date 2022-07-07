from __future__ import annotations

from datetime import datetime
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pystore
import yfinance as yf

from merchant.config import get_config
from merchant.datasources.base import DataSource

COLUMN_NAMES = ('Date', 'Open', 'High', 'Low', 'Close', 'Volume')
PYSTORE_NAME = 'datastore'
COLLECTIONS = ('EOD',)


class DataStore:

    _path: Path
    _store: pystore.store

    def __init__(self) -> None:
        self._path = get_config().DATA_PATH
        if not self._path.exists():
            self._path.mkdir(parents=True)
        pystore.set_path(str(self._path))
        self._store = pystore.store(PYSTORE_NAME)

    def query(
        self,
        col: str,
        symbol: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> pd.DataFrame:
        if col not in COLLECTIONS:
            raise ValueError(
                f'{col} is not a valid collection, must be one of {COLLECTIONS}'
            )
        if end is None:
            end = datetime.now()

        collection = self._store.collection(col)
        key = f'{symbol.upper()}'
        if key in collection.list_items():
            # partition filtering
            filters = [('Close', '<=', 1000)]
            if start is not None:
                filters.append(('Close', '>=', 100))
            ddf = collection.item(key, filters=filters).data

            # fine grained filtering within the partition
            cond = ddf.Date <= end
            if start is not None:
                cond &= ddf.Date >= start
            return ddf[cond].reset_index(drop=True).compute()

        raise ValueError(f'{symbol.upper()} not DataStore')

    def write(
        self,
        col: str,
        symbol: str,
        data: pd.DataFrame,
        *,
        unsafe_append: bool = False,
    ) -> None:
        if data.columns.to_list() != list(COLUMN_NAMES):
            raise ValueError(f'{data} has invalid format, must be {COLUMN_NAMES}')
        if col not in COLLECTIONS:
            raise ValueError(
                f'{col} is not a valid collection, must be one of {COLLECTIONS}'
            )

        collection = self._store.collection(col)
        key = f'{symbol.upper()}'
        if key in collection.list_items():
            if not unsafe_append:
                raise ValueError(f'{symbol.upper()} already exists')
            ddf = collection.item(key).data
            concat_ddf = dd.concat([ddf, data], axis=0).drop_duplicates(subset='Date')
            concat_ddf = concat_ddf.sort_values(by='Date').reset_index(drop=True)
            collection.write(key, concat_ddf, overwrite=True)
        else:
            collection.write(key, data)


def download_symbols(symbols: list[str]) -> None:
    _symbols_string = ' '.join(symbols)
    data = yf.download(_symbols_string, period='max', group_by='ticker')

    ds = DataStore()
    for symbol in symbols:
        if len(symbols) > 1:
            symbol_data = data[symbol]
        else:
            symbol_data = data

        symbol_data.drop(columns=['Adj Close'], inplace=True)
        symbol_data.reset_index(inplace=True)
        ds.write('EOD', symbol, symbol_data)


def read_symbol() -> None:
    ds = DataStore()
    data = ds.query('EOD', 'AAPL', start=datetime(2020, 1, 1))
    print(data)


download_symbols(['AAPL'])
read_symbol()
