from __future__ import annotations

from pathlib import Path
from typing import Iterable
from typing import TypeAlias

import pandas as pd
from oakstore import Series
from oakstore import Store
from torch.utils.data import Dataset as TorchDataset

from merchant.helpers import Ticker


def set_data_path(path: Path) -> None:
    DataSetConfig.DATA_PATH = path


class DataSetConfig:
    DATA_PATH: Path | None = None


class DataSet(Store):
    _data_path: Path
    _tickers: list[Ticker]

    def __init__(self, data_path: Path | str | None = None) -> None:
        if data_path is not None:
            self._data_path = Path(data_path)
        elif DataSetConfig.DATA_PATH is None:
            raise RuntimeError('data path not set')
        else:
            self._data_path = DataSetConfig.DATA_PATH

        super().__init__(base_path=self._data_path, read_only=True)

    def __getitem__(self, ticker: Ticker) -> Series:
        return super().__getitem__(repr(ticker))

    def __contains__(self, ticker: Ticker) -> bool:
        return ticker in self._tickers

    def __iter__(self) -> Iterable[Ticker]:
        return iter(self._tickers)

    @property
    def tickers(self) -> list[Ticker]:
        return self._tickers

    def sample(self, ticker: Ticker, size: int) -> None:
        ...

    def batch(self) -> Batch:
        ...


class Batch(TorchDataset):
    def __iter__(self) -> Iterable[Batch]:
        return self

    def __next__(self) -> Batch:
        raise StopIteration
