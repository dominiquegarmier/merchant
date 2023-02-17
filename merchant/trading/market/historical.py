from __future__ import annotations

import random
from abc import abstractmethod
from collections import defaultdict
from collections.abc import Collection
from collections.abc import Iterator
from functools import cached_property
from typing import cast
from typing import NamedTuple
from typing import Protocol

import numpy as np
import pandas as pd
from pyarrow import dataset as ds

from merchant.core.numeric import NormedDecimal
from merchant.data.constants import Aggregates
from merchant.data.dataset import Dataset
from merchant.trading.market.base import BaseBroker
from merchant.trading.market.base import BaseMarketObserver
from merchant.trading.market.base import InsufficientAssets
from merchant.trading.market.base import Order
from merchant.trading.market.base import OrderAfterValuation
from merchant.trading.market.base import OrderExecution
from merchant.trading.market.base import Trade
from merchant.trading.market.portfolio import Portfolio
from merchant.trading.tools.asset import Asset
from merchant.trading.tools.asset import Valuation
from merchant.trading.tools.instrument import Instrument
from merchant.trading.tools.instrument import USD
from merchant.trading.tools.pair import TradingPair


class Candle(NamedTuple):
    TIMESTAMP: pd.Timestamp
    OPEN: float
    HIGH: float
    LOW: float
    CLOSE: float
    VOLUME: float
    TRADES: int
    VW_PRICE: float


def _get_candle_from_slice(timestamp: pd.Timestamp, data: pd.Series) -> Candle:
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


_CACHED_TIMESTAMP: pd.Timestamp | None = None
_QUOTE_CACHE: dict[Instrument, float] = {}


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
    candle: Candle,
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
            candle, timestamp, agg
        )
    if reversed:
        return NormedDecimal(1 / _QUOTE_CACHE[instrument])
    return NormedDecimal(_QUOTE_CACHE[instrument])


# methods to edit portfolio
def _get_holding(instrument: Instrument, portfolio: Portfolio) -> Asset:
    if instrument not in portfolio._assets:
        return 0 * instrument
    return portfolio._assets[instrument]


def _set_holding(asset: Asset, portfolio: Portfolio) -> None:
    portfolio._assets[asset.instrument] = asset


def _increase_holdings(asset: Asset, portfolio: Portfolio) -> None:
    balance = _get_holding(asset.instrument, portfolio)
    _set_holding(balance + asset, portfolio)


def _decrease_holdings(asset: Asset, portfolio: Portfolio) -> None:
    balance = _get_holding(asset.instrument, portfolio)
    _set_holding(balance - asset, portfolio)


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


class SlippageModel(Protocol):
    @abstractmethod
    def __call__(
        self, order: Order, quote: NormedDecimal, candle: Candle
    ) -> NormedDecimal:
        # retuns adjusted (virtual) quote after slippage
        ...


class FeeModel(Protocol):
    @abstractmethod
    def __call__(self, order: Order, quote: NormedDecimal) -> Asset:
        # returns the base asset costs due to fees
        ...


def _no_slippage(order: Order, quote: NormedDecimal, candle: Candle) -> NormedDecimal:
    return quote


def _no_fees(order: Order, quote: NormedDecimal) -> Asset:
    return 0 * USD


_NO_FEES = _no_fees
_NO_SLIPPAGE = _no_slippage


class HistoricalMarketBroker(BaseBroker):
    _dataset: Dataset
    _portfolio: Portfolio

    _slippage: SlippageModel
    _fees: FeeModel
    _aggregate_type: Aggregates

    _locked: pd.Timestamp | None = None

    _slice_iter: Iterator[tuple[pd.Timestamp, pd.Series]]
    _timestamp_slice: tuple[pd.Timestamp, pd.Series]
    _next_timestamp_slice: tuple[pd.Timestamp, pd.Series] | None

    def __init__(
        self,
        dataset: Dataset,
        assets: Collection[Asset],
        slippage: SlippageModel = _NO_SLIPPAGE,
        fees: FeeModel = _NO_FEES,
    ) -> None:
        super().__init__()
        self._dataset = dataset
        self._portfolio = Portfolio(assets=assets)

        self._fees = fees
        self._slippage = slippage

        # get the first data slice
        self._aggregate_type = Aggregates.MIN
        self._slice_iter = self._dataset.range(from_=self.clock.time - self.resolution)  # type: ignore
        try:
            self._timestamp_slice = next(self._slice_iter)
        except StopIteration:
            raise ValueError('dataset is empty')
        self._next_timestamp_slice = self._next_slice()

    @property
    def resolution(self) -> pd.Timedelta:
        return pd.Timedelta(self._aggregate_type.value, unit='s')

    @property
    def portfolio(self) -> Portfolio:
        return self._portfolio

    @property
    def skip(self) -> pd.Timedelta:
        if self._next_timestamp_slice is None:
            return pd.Timedelta.max
        return self._next_timestamp_slice[0] - self._timestamp_slice[0]

    @cached_property
    def trading_pairs(self) -> list[TradingPair]:
        pairs: list[TradingPair] = []
        for instrument in self.instruments:
            if instrument != USD:
                pairs += [instrument / USD, USD / instrument]
        return pairs

    @cached_property
    def instruments(self) -> list[Instrument]:
        tickers = set(self._dataset.tickers)
        instruments = [Instrument(symbol=ticker, precision=4) for ticker in tickers]
        instruments.append(USD)
        return instruments

    def execute_order(self, order: Order) -> OrderExecution:
        timestamp = self.clock.time

        # TODO support more complex pairs
        if order.pair.sell != USD and order.pair.buy != USD:
            raise NotImplementedError

        # TODO: support more complex pairs
        is_sell = order.pair.buy == USD
        instrument = order.pair.sell if is_sell else order.pair.buy

        # get candle and turn into a quote
        candle_ts, data = self._get_slice()
        candle = _get_candle_from_slice(timestamp=candle_ts, data=data)
        quote = _get_context_consistent_quote(
            candle=candle,
            timestamp=timestamp,
            instrument=instrument,
            agg=self._aggregate_type,
            reversed=is_sell,
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

    @property
    def observation_shape(self) -> tuple[int, int]:
        return (len(self.instruments), 1)

    def get_observation(self) -> np.ndarray:
        # compute portfolio value
        self._update_portfolio_value()

        # TODO more complex observations
        obs = np.zeros(shape=self.observation_shape)
        for i, instrument in enumerate(self.instruments):
            obs[i, :] = [float(self._portfolio.assets[instrument].quantity)]
        return obs

    def _check_lock_or_raise(self, order: Order) -> None:
        if self._locked is not None and self._locked > self.clock.time:
            raise OrderAfterValuation(order=order, valuation=self._locked)
        self._locked = None

    def _next_slice(self) -> tuple[pd.Timestamp, pd.Series] | None:
        try:
            return next(self._slice_iter)
        except StopIteration:
            return None

    def _get_slice(self) -> tuple[pd.Timestamp, pd.Series]:
        while self.clock.time >= self._timestamp_slice[0] + self.resolution:
            if self._next_timestamp_slice is None:
                raise ValueError('dataset is empty')
            self._timestamp_slice = self._next_timestamp_slice
            self._next_timestamp_slice = self._next_slice()
        return self._timestamp_slice

    def _calculate_value(self) -> tuple[Valuation, pd.Timestamp]:
        value = 0.0
        timestamp, data = self._get_slice()
        for instrument, asset in self._portfolio.assets.items():
            candle = _get_candle_from_slice(
                timestamp=timestamp, data=data[instrument.symbol]
            )
            value += float(asset.quantity) * candle.CLOSE
            timestamp = candle.TIMESTAMP

        # timestamp of end of candle
        timestamp = pd.Timestamp(
            timestamp.value + self._aggregate_type.value * 1_000_000_000
        )
        return Valuation(NormedDecimal(value) * USD), timestamp

    def _update_portfolio_value(self) -> Portfolio:
        '''
        update value of portfolio and record history
        this locks the portfolio to trading until the next candle
        '''

        value, timestamp = self._calculate_value()
        self._locked = timestamp

        self._portfolio._value = value
        self._portfolio._value_history[timestamp] = float(value)

        return self._portfolio


_N_CANDLE_FEATURES = 7
_DEFAULT_OBSERVATION_WINDOW = 390
_DEFAULT_ATTENTION_WINDOW = pd.Timedelta(7, unit='d')  # 1 week


class HistoricalMarketObserver(BaseMarketObserver):
    _dataset: Dataset

    _attention_window: pd.Timedelta
    _attention: pd.DataFrame

    _observation_window: int  # size of the observation sample

    _aggregate_type: Aggregates

    def __init__(
        self,
        dataset: Dataset,
        observation_window: int = _DEFAULT_OBSERVATION_WINDOW,
        attention_window: pd.Timedelta = _DEFAULT_ATTENTION_WINDOW,
    ) -> None:
        super().__init__()
        self._dataset = dataset

        # sample the initial attention
        start_time = self.clock.time
        self._attention_window = attention_window
        self._attention = self._dataset[
            start_time - self._attention_window : start_time  # type: ignore
        ]

        self._observation_window = observation_window
        self._aggregate_type = Aggregates.MIN

    @property
    def resolution(self) -> pd.Timedelta:
        return pd.Timedelta(self._aggregate_type.value, unit='s')

    @cached_property
    def trading_pairs(self) -> list[TradingPair]:
        pairs: list[TradingPair] = []
        for instrument in self.instruments:
            if instrument != USD:
                pairs += [instrument / USD, USD / instrument]
        return pairs

    @cached_property
    def instruments(self) -> list[Instrument]:
        tickers = set(self._dataset.tickers)
        instruments = [Instrument(symbol=ticker, precision=4) for ticker in tickers]
        instruments.append(USD)
        return instruments

    @property
    def observation_shape(self) -> tuple[int, int, int]:
        return (
            _N_CANDLE_FEATURES,
            len(self._dataset.tickers),
            self._observation_window,
        )

    def get_observation(self) -> np.ndarray:
        # update attention
        epsilon = pd.Timedelta(1, unit='ns')
        self._attention = self._attention.loc[
            self._attention.index > self.clock.time - self._attention_window
        ]
        attn = [
            self._attention,
            self._dataset[self._attention.index[-1] + epsilon : self.clock.time],  # type: ignore
        ]
        self._attention = pd.concat(attn, axis=0)

        # TODO more sophisticated sampling (exponential distribution?)
        # sample the attention to get the correct shape for the observation
        observation = self._attention.tail(self._observation_window)
        arrs = [observation[ticker].to_numpy().T for ticker in self._dataset.tickers]
        return np.stack(arrs, axis=1)
