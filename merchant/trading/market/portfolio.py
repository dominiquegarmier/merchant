from __future__ import annotations

from collections.abc import Collection

import numpy as np
import pandas as pd

from merchant.core.abstract import TimeDependant
from merchant.trading.market.base import Trade
from merchant.trading.tools.asset import Asset
from merchant.trading.tools.asset import Valuation
from merchant.trading.tools.instrument import Instrument
from merchant.trading.tools.instrument import USD


class Portfolio(TimeDependant):
    _assets: dict[Instrument, Asset]
    _primary_instrument: Instrument

    _value: Valuation | None
    _value_history: pd.DataFrame
    _trade_histroy: list[Trade]

    def __init__(
        self, assets: Collection[Asset], primary_instrument: Instrument = USD
    ) -> None:
        self._assets = {asset.instrument: asset for asset in assets}
        self._primary_instrument = primary_instrument

        # data that gets set by the market engine
        self._value = None
        self._value_history = pd.DataFrame(
            index=pd.DatetimeIndex([]), columns=['VALUE']
        )
        self._trade_histroy = []

    @property
    def assets(self) -> dict[Instrument, Asset]:
        return self._assets

    @property
    def value(self) -> Valuation | None:
        return self._value

    @property
    def performance(self) -> pd.DataFrame:
        return self._value_history

    @property
    def balance(self) -> Asset:
        return self._assets[self._primary_instrument]
