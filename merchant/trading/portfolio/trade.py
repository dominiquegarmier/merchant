from __future__ import annotations

import pandas as pd

from merchant.core.base import Identifiable
from merchant.trading.tools.asset import Asset
from merchant.trading.tools.asset import Valuation
from merchant.trading.tools.pair import TradingPair


class Trade(Identifiable):
    _buy: Asset
    _sell: Asset
    _time: pd.Timestamp
    _pair: TradingPair

    def __init__(self, pair: TradingPair, buy: Asset, sell: Asset) -> None:
        if pair.buy != buy.instrument:
            raise ValueError(f'buy asset {buy} does not match direction {pair}')
        if pair.sell != sell.instrument:
            raise ValueError(f'sell asset {sell} does not match direction {pair}')

        self._pair = pair
        self._buy = buy
        self._sell = sell


class ValuedTrade(Trade):
    # this assumes that the market is efficient and the valuation is the same
    # on the buy and sell side
    _valuation: Valuation

    def __init__(
        self, pair: TradingPair, buy: Asset, sell: Asset, valuation: Valuation
    ) -> None:
        super().__init__(pair, buy, sell)


class ClosedPosition(Identifiable):
    _amount: Asset
    _open: ValuedTrade
    _close: ValuedTrade

    _open_valuation: Valuation
    _close_valuation: Valuation

    def __init__(
        self, /, *, amount: Asset, open: ValuedTrade, close: ValuedTrade
    ) -> None:
        if amount.instrument != open._pair.buy:
            raise ValueError(f'instrument {amount} does not match open trade {open}')
        if amount.instrument != close._pair.sell:
            raise ValueError(f'instrument {amount} does not match close trade {close}')
        self._amount = amount
        self._open = open
        self._close = close

        open_ratio = self._amount / open._buy
        close_ratio = self._amount / close._sell

        self._open_valuation = Valuation(open_ratio * open._valuation)
        self._close_valuation = Valuation(close_ratio * close._valuation)
