from __future__ import annotations

from merchant.datasources.base import DataSource
from merchant.config import get_config
from pathlib import Path
import pandas as pd
from datetime import datetime
import pystore
import yfinance as yf


class DataStore:
    PYSTORE_NAME = "merchant.datasources.datastore"
    COLUMN_NAMES = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']

    _path: Path
    _store: pystore.store

    def __init__(self) -> None:
        self._path = get_config().DATA_PATH
        if not self._path.exists():
            self._path.mkdir(parents=True)
        pystore.set_path(str(self._path))
        self._store = pystore.store(self.PYSTORE_NAME)

    def query(
        self, symbol: str, start: datetime | None = None, end: datetime | None = None
    ) -> pd.DataFrame:
        if start is None:
            start = datetime(1, 1, 1)
        if end is None:
            end = datetime.now()
        raw_file_path = self._path / "OHLCV" / f"{symbol}.csv"
        if not raw_file_path.exists():
            empty = pd.DataFrame(columns=COLUMN_NAMES)
            empty.index.name = INDEX_NAME
            return empty
        full_data = pd.read_csv(raw_file_path, index_col=INDEX_NAME)
        print(full_data.index)
        data = full_data[pd.Timestamp(start) <= full_data.index <= pd.Timestamp(end)]
        data.index.name = INDEX_NAME
        return data

    def write(self, symbol: str, data: pd.DataFrame) -> None:
        folder_path = self._path / "OHLCV"
        if not folder_path.exists():
            folder_path.mkdir(parents=True)
        raw_file_path = folder_path / f"{symbol}.csv"
        data = data[COLUMN_NAMES]
        if raw_file_path.exists():
            full_dataframe = pd.read_csv(
                raw_file_path,
                index_col=INDEX_NAME,
                parse_dates=True,
            )
            data = pd.concat([full_dataframe, data])
            # TODO this doesn't work
            data.index.drop_duplicates()
            data.sort_index()
        data.to_csv(raw_file_path, index_label=INDEX_NAME)


def download_symbols(symbols: list[str]) -> None:
    _symbols_string = ' '.join(symbols)
    data = yf.download(_symbols_string, period='max', group_by='ticker')

    ds = DataStore()
    for symbol in symbols:
        if len(symbols) > 1:
            symbol_data = data[symbol]
        else:
            symbol_data = data
        symbol_data = symbol_data[COLUMN_NAMES].dropna()
        ds.write(symbol, symbol_data)


DOW_30_SYMBOLS = [
    "AXP",
    "AMGN",
    "AAPL",
    "BA",
    "CAT",
    "CSCO",
    "CVX",
    "GS",
    "HD",
    "HON",
    "IBM",
    "INTC",
    "JNJ",
    "KO",
    "JPM",
    "MCD",
    "MMM",
    "MRK",
    "MSFT",
    "NKE",
    "PG",
    "TRV",
    "UNH",
    "CRM",
    "VZ",
    "V",
    "WBA",
    "WMT",
    "DIS",
    "DOW",
]

download_symbols(['AXP'])
