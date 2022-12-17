from __future__ import annotations

import functools
from abc import ABCMeta
from collections.abc import Collection
from decimal import Decimal

import pandas as pd

from merchant.core.clock import HasInternalClock
from merchant.core.clock import NSClock
from merchant.core.numeric import normalize
from merchant.core.numeric import NormedDecimal
from merchant.meta.oms.market import MarketSimulation
from merchant.meta.pms.asset import Asset
from merchant.meta.pms.instrument import Instrument


class _StaticPortfolio(metaclass=ABCMeta):
    '''
    'StaticPortfolio' represents the state of a trading portfolio a single point in time
    '''

    _assets: dict[Instrument, Asset]

    def __init__(self, /, *, assets: Collection[Asset] | None = None) -> None:
        self._assets = {asset.instrument: asset for asset in assets or []}

    @property
    def assets(self) -> list[Asset]:
        return list(self._assets.values())

    def __getitem__(self, instrument: Instrument) -> Asset:
        if instrument not in self._assets:
            return Asset(
                instrument, quantity=normalize(Decimal(0), prec=instrument.precision)
            )
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

    def __iadd__(self, asset: Asset) -> _StaticPortfolio:
        self.add_asset(asset)
        return self

    def __isub__(self, asset: Asset) -> _StaticPortfolio:
        self.remove_asset(asset)
        return self

    def __contains__(self, instrument: Instrument) -> bool:
        return self[instrument].quantity > 0

    def __str__(self) -> str:
        return f'{type(self)}({self._assets!r})'

    def __repr__(self) -> str:
        return str(self)


class Portfolio(HasInternalClock[NSClock], _StaticPortfolio):
    '''
    'Portfolio' represents a trading portfolio, this includes:
        - the current state of the portfolio
        - performance (relative to a connected market)
        - value and trading history
    '''

    _performance_metric: Instrument
    _market: MarketSimulation

    _value_history: pd.Series[NormedDecimal]

    def __init__(
        self, /, *, market: MarketSimulation, assets: Collection[Asset] | None = None
    ) -> None:
        self._market = market
        self._clock = self._market.clock
        super().__init__(assets=assets)

    @property
    def market(self) -> MarketSimulation:
        return self._market

    @property
    def value(self) -> NormedDecimal:
        raise NotImplementedError
