from __future__ import annotations

from collections import defaultdict
from collections.abc import Collection
from typing import Any

import pandas as pd

from merchant.core.abstract import TimeDependant
from merchant.trading.tools.asset import Asset
from merchant.trading.tools.asset import Valuation
from merchant.trading.tools.instrument import Instrument
from merchant.trading.tools.instrument import USD


class Portfolio(TimeDependant):
    _assets: dict[Instrument, Asset]
    _primary_instrument: Instrument

    _value: Valuation | None
    _value_history: pd.DataFrame

    def __init__(
        self, assets: Collection[Asset], primary_instrument: Instrument = USD
    ) -> None:
        self._assets = {asset.instrument: asset for asset in assets}
        self._primary_instrument = primary_instrument
        self._value = None

    @property
    def assets(self) -> dict[Instrument, Asset]:
        return self._assets

    @property
    def value(self) -> Valuation | None:
        return self._value

    @property
    def performance(self) -> pd.DataFrame:
        raise NotImplementedError

    @property
    def balance(self) -> Asset:
        return self._assets[self._primary_instrument]

    def _increase_holdings(self, asset: Asset) -> None:
        balance = self._get_holding(asset.instrument)
        self._set_holding(balance + asset)

    def _decrease_holdings(self, asset: Asset) -> None:
        balance = self._get_holding(asset.instrument)
        self._set_holding(balance - asset)

    def _get_holding(self, instrument: Instrument) -> Asset:
        if instrument not in self._assets:
            return 0 * instrument
        return self._assets[instrument]

    def _set_holding(self, asset: Asset) -> None:
        self._assets[asset.instrument] = asset
