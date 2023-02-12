from __future__ import annotations

from collections.abc import Collection
from typing import Any

import pandas as pd

from merchant.core.clock import TimeDependant
from merchant.trading.tools.asset import Asset
from merchant.trading.tools.asset import Valuation
from merchant.trading.tools.instrument import Instrument
from merchant.trading.tools.instrument import USD


class Portfolio(TimeDependant):
    _assets: dict[Instrument, Asset]
    _value: Valuation | None
    _history: Any
    _money_instrument: Instrument

    def __init__(
        self, assets: Collection[Asset], money_instrument: Instrument = USD
    ) -> None:
        self._assets = {asset.instrument: asset for asset in assets}
        self._money_instrument = money_instrument
        self._value = None

    @property
    def assets(self) -> dict[Instrument, Asset]:
        return self._assets

    @property
    def value(self) -> Valuation:
        raise NotImplementedError
