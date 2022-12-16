from __future__ import annotations

from abc import ABCMeta
from abc import abstractmethod
from abc import abstractproperty
from collections.abc import Collection

import pandas as pd

from merchant.trading.asset import Asset
from merchant.trading.instrument import Instrument
from merchant.trading.market import Market


class HasValueHistory(metaclass=ABCMeta):
    @abstractproperty
    def value_history(self) -> pd.Series:
        pass


class Portfolio:
    _assets: dict[Instrument, Asset]
    _connected: Market | None = None

    def __init__(self, assets: Collection[Asset] | None = None) -> None:
        self._assets = {asset.instrument: asset for asset in assets or []}

    @property
    def assets(self) -> list[Asset]:
        return list(self._assets.values())

    @property
    def connected(self) -> bool:
        return self._connected is not None

    def connect(self, market: Market) -> None:
        self._market = market

    def __getitem__(self, instrument: Instrument) -> Asset:
        if instrument not in self._assets:
            return Asset(instrument, quantity=0)
        return self._assets[instrument]

    def add_asset(self, asset: Asset) -> None:
        owned = self[asset.instrument]
        self._assets[owned.instrument] = owned + asset

    def remove_asset(self, asset: Asset) -> None:
        owned = self[asset.instrument]
        if self.has_asset(asset) is False:
            raise ValueError(f'Cannot remove {asset!r} from {self!r}')
        new = owned - asset
        if new.quantity > 0:
            del self._assets[owned.instrument]
        else:
            self._assets[owned.instrument] = new

    def has_asset(self, asset: Asset) -> bool:
        return self[asset.instrument].quantity >= asset.quantity

    def __iadd__(self, asset: Asset) -> Portfolio:
        self.add_asset(asset)
        return self

    def __isub__(self, asset: Asset) -> Portfolio:
        self.remove_asset(asset)
        return self

    def __contains__(self, instrument: Instrument) -> bool:
        return self[instrument].quantity > 0

    def __str__(self) -> str:
        return f'{type(self)}({self._assets!r})'

    def __repr__(self) -> str:
        return str(self)
