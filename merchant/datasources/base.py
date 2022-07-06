from __future__ import annotations

from abc import ABC
from abc import abstractproperty

import pandas as pd


class DataSource(ABC):
    _symbols: list[str]

    def __init__(self, *, symbols: list[str] | str) -> None:
        '''DataSource wrapper for different data apis
        data source will lazy load data when retrieved
        it will also first try to load from cache_dir
        '''
        ...

    @abstractproperty
    def data(self) -> dict[str, pd.DataFrame]:
        ...
