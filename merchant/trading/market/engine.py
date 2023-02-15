from __future__ import annotations

import random
from collections import defaultdict
from collections.abc import Collection
from typing import cast
from typing import NamedTuple
from typing import Protocol

import pandas as pd
from pyarrow import dataset as ds

from merchant.core.numeric import NormedDecimal
from merchant.data.constants import Aggregates
from merchant.data.dataset import Dataset
from merchant.trading.market.base import BaseBroker
from merchant.trading.market.base import BaseMarketData
from merchant.trading.market.base import InsufficientAssets
from merchant.trading.market.base import Order
from merchant.trading.market.base import OrderExecution
from merchant.trading.market.base import Trade
from merchant.trading.market.portfolio import Portfolio
from merchant.trading.tools.asset import Asset
from merchant.trading.tools.instrument import Instrument
from merchant.trading.tools.instrument import USD
from merchant.trading.tools.pair import TradingPair


SLIPPAGE_RATIO = 0.0001


# methods to edit portfolio
def _increase_holdings(asset: Asset, portfolio: Portfolio) -> None:
    balance = _get_holding(asset.instrument, portfolio)
    _set_holding(balance + asset, portfolio)


def _decrease_holdings(asset: Asset, portfolio: Portfolio) -> None:
    balance = _get_holding(asset.instrument, portfolio)
    _set_holding(balance - asset, portfolio)


def _get_holding(instrument: Instrument, portfolio: Portfolio) -> Asset:
    if instrument not in portfolio._assets:
        return 0 * instrument
    return portfolio._assets[instrument]


def _set_holding(asset: Asset, portfolio: Portfolio) -> None:
    portfolio._assets[asset.instrument] = asset


def _log_new_trade(
    execution: OrderExecution,
    sold: Asset,
    bought: Asset,
    fees: Asset,
    portfolio: Portfolio,
) -> None:
    trade = Trade(
        sold=sold,
        bought=bought,
        fees=fees,
        execution=execution,
    )
    portfolio._trade_histroy.append(trade)


def _raise_if_insufficient_assets(
    assets: Collection[Asset], order: Order, portfolio: Portfolio
) -> None:
    asset_dict: dict[Instrument, list[Asset]] = defaultdict(list)
    for asset in assets:
        asset_dict[asset.instrument].append(asset)
    for instrument, assets in asset_dict.items():
        if portfolio.assets[instrument] < sum(assets, start=0 * instrument):
            raise InsufficientAssets(order=order)


class Candle(NamedTuple):
    TIMESTAMP: pd.Timestamp
    OPEN: float
    HIGH: float
    LOW: float
    CLOSE: float
    VOLUME: float
    TRADES: int
    VW_PRICE: float


class SlippageModel(Protocol):
    def __call__(
        self, order: Order, quote: NormedDecimal, candle: Candle
    ) -> NormedDecimal:
        # retuns adjusted (virtual) quote after slippage
        ...


class FeeModel(Protocol):
    def __call__(self, order: Order, quote: NormedDecimal) -> Asset:
        # returns the base asset costs due to fees
        ...


class MarketEngine(BaseBroker):
    _dataset: Dataset
    _portfolio: Portfolio
    _pairs: set[TradingPair]

    _slippage: SlippageModel
    _fees: FeeModel
    _aggregate_type: Aggregates

    def __init__(
        self,
        dataset: Dataset,
        assets: Collection[Asset],
        slippage: SlippageModel,
        fees: FeeModel,
    ) -> None:
        super().__init__()
        self._dataset = dataset
        self._portfolio = Portfolio(assets=assets)
        self._aggregate_type = Aggregates.MIN

        self._fees = fees
        self._slippage = slippage

    def execute_order(self, order: Order) -> OrderExecution:
        timestamp = self.clock.time

        # TODO support more complex pairs
        if order.pair.sell != USD and order.pair.buy != USD:
            raise NotImplementedError

        # TODO: support more complex pairs
        is_sell = order.pair.buy == USD
        instrument = order.pair.sell if is_sell else order.pair.buy

        # get quote and candle to calculate slippage
        quote = _get_context_consistent_quote(
            self._dataset,
            timestamp=timestamp,
            instrument=instrument,
            agg=self._aggregate_type,
            reversed=is_sell,
        )
        candle = _get_containing_candle(
            self._dataset, timestamp=timestamp, instrument=instrument
        )

        # calculate finaly execution price after slippage and fees
        slippage_adj_quote = self._slippage(order=order, quote=quote, candle=candle)
        fees = self._fees(order=order, quote=slippage_adj_quote)

        # check asset balance
        bought_assets = order.pair.buy * order.quantity
        sold_assets = order.pair.sell * slippage_adj_quote * order.quantity
        _raise_if_insufficient_assets(
            assets=(sold_assets, fees), order=order, portfolio=self._portfolio
        )

        # create order execution
        order_execution = OrderExecution(
            order=order, timestamp=timestamp, rate=slippage_adj_quote, fees=fees
        )

        # update portfolio
        # keep track of transaction in portfolio
        _increase_holdings(asset=bought_assets, portfolio=self._portfolio)
        _decrease_holdings(asset=sold_assets, portfolio=self._portfolio)
        _decrease_holdings(asset=fees, portfolio=self._portfolio)
        # log the trade in the portfolio history
        _log_new_trade(
            execution=order_execution,
            sold=sold_assets,
            bought=bought_assets,
            fees=fees,
            portfolio=self._portfolio,
        )

        return order_execution

    def update_portfolio(self) -> Portfolio:
        '''update value of portfolio and record history'''
        raise NotImplementedError


# TODO remove this
class HistoricalMarketData(BaseMarketData):
    _dataset: Dataset
    _portfolio: Portfolio

    def __init__(self, dataset: Dataset) -> None:
        super().__init__()
        self._dataset = dataset


def _get_candle_from_slice(data: pd.DataFrame) -> Candle:
    return Candle(
        TIMESTAMP=cast(pd.Timestamp, data.index[0]),
        OPEN=float(data['OPEN']),
        HIGH=float(data['HIGH']),
        LOW=float(data['LOW']),
        CLOSE=float(data['CLOSE']),
        VOLUME=float(data['VOLUME']),
        TRADES=int(data['TRADES']),
        VW_PRICE=float(data['VW_PRICE']),
    )


def _inverse_candle(candle: Candle) -> Candle:
    return Candle(
        TIMESTAMP=candle.TIMESTAMP,
        OPEN=1 / candle.CLOSE,
        HIGH=1 / candle.LOW,
        LOW=1 / candle.HIGH,
        CLOSE=1 / candle.OPEN,
        VOLUME=candle.VOLUME / candle.VW_PRICE,
        TRADES=candle.TRADES,
        VW_PRICE=1 / candle.VW_PRICE,
    )


_CACHED_TIMESTAMP = None
_QUOTE_CACHE: dict[Instrument, float] = {}

_AGG_SIZE = 60
_WINDOW_SIZE = 100
_CACHED_WINDOW_SLICE: pd.DataFrame | None = None
_CACHED_WINDOW: tuple[pd.Timestamp, pd.Timestamp] | None = None


def _get_window_slice(timestamp: pd.Timestamp) -> tuple[pd.Timestamp, pd.Timestamp]:
    # multiply by aggregate size
    window = _WINDOW_SIZE * _AGG_SIZE * 1_000_000_000
    start = timestamp.value // window * window
    end = start + window

    return pd.Timestamp(start), pd.Timestamp(end)


def _get_rolling_window_cached(
    dataset: Dataset, timestamp: pd.Timestamp
) -> pd.DataFrame:
    global _CACHED_WINDOW
    global _CACHED_WINDOW_SLICE
    if (
        _CACHED_WINDOW is None
        or _CACHED_WINDOW_SLICE is None
        or _CACHED_WINDOW[0] > timestamp
        or _CACHED_WINDOW[1] < timestamp
    ):
        _CACHED_WINDOW = _get_window_slice(timestamp)
        _CACHED_WINDOW_SLICE = dataset[
            _CACHED_WINDOW[0].value : _CACHED_WINDOW[1].value  # noqa: E203
        ]
    return _CACHED_WINDOW_SLICE


def _get_containing_candle(
    dataset: Dataset,
    timestamp: pd.Timestamp,
    instrument: Instrument,
    reversed: bool = False,
) -> Candle:
    window = _get_rolling_window_cached(dataset=dataset, timestamp=timestamp)
    pair_df = window[instrument.symbol]  # TODO support more complex pairs
    index = pair_df.index.searchsorted(timestamp, side='left')

    candle = _get_candle_from_slice(pair_df.iloc[[index - 1]])  # type: ignore
    if reversed:
        candle = _inverse_candle(candle)
    return candle


def _randomized_interpolated_quote(
    candle: Candle, timestamp: pd.Timestamp, agg: Aggregates
) -> float:
    '''
    get a random value of a realistic quote, based on a candle and the relative time rel_time in [0, 1]
    '''
    start, open, high, low, close, _, _, _ = candle
    ts_start = start.value
    ts_end = start.value + agg.value * 1_000_000_000

    rel_time = (timestamp.value - ts_start) / (ts_end - ts_start)

    WINDOW_RATIO = 2  # maximal slope of the piecewise affine function
    h_open = max(0, 1 - WINDOW_RATIO * rel_time)
    h_close = max(0, WINDOW_RATIO * rel_time - 1)
    h_mid = max(0, min(1, (1 - abs(rel_time - 0.5) * 2) * WINDOW_RATIO))

    return h_open * open + h_close * close + h_mid * random.uniform(low, high)


def _get_context_consistent_quote(
    dataset: Dataset,
    timestamp: pd.Timestamp,
    instrument: Instrument,
    agg: Aggregates,
    reversed: bool = False,
) -> NormedDecimal:
    global _QUOTE_CACHE
    global _CACHED_TIMESTAMP

    if _CACHED_TIMESTAMP != timestamp:
        _CACHED_TIMESTAMP = timestamp
        _QUOTE_CACHE.clear()

    if instrument not in _QUOTE_CACHE:
        _QUOTE_CACHE[instrument] = _randomized_interpolated_quote(
            _get_containing_candle(dataset, timestamp, instrument), timestamp, agg
        )
    if reversed:
        return NormedDecimal(1 / _QUOTE_CACHE[instrument])
    return NormedDecimal(_QUOTE_CACHE[instrument])
