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

import exchange_calendars as xcals
import numpy as np
import pandas as pd
from exchange_calendars import ExchangeCalendar

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
    candle: Candle, timestamp: pd.Timestamp, timeframe: pd.Timedelta
) -> float:
    '''
    get a random value of a realistic quote, based on a candle and the relative time rel_time in [0, 1]
    '''
    start, open, high, low, close, _, _, _ = candle
    ts_start = start.value
    ts_end = start.value + timeframe.value

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
    timeframe: pd.Timedelta,
    reversed: bool = False,
) -> NormedDecimal:
    global _QUOTE_CACHE
    global _CACHED_TIMESTAMP

    if _CACHED_TIMESTAMP != timestamp:
        _CACHED_TIMESTAMP = timestamp
        _QUOTE_CACHE.clear()

    if instrument not in _QUOTE_CACHE:
        _QUOTE_CACHE[instrument] = _randomized_interpolated_quote(
            candle, timestamp, timeframe
        )
    if reversed:
        return NormedDecimal(1 / _QUOTE_CACHE[instrument])
    return NormedDecimal(_QUOTE_CACHE[instrument])


# methods to edit portfolio


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


_NO_SLIPPAGE: SlippageModel = _no_slippage
_NO_FEES: FeeModel = _no_fees
_XNYS_CALENDAR = xcals.get_calendar('XNYS')  # NYSE


class HistoricalMarketBroker(BaseBroker):
    _dataset: Dataset
    _portfolio: Portfolio

    _slippage: SlippageModel
    _fees: FeeModel
    _timeframe: pd.Timedelta

    _calendar: ExchangeCalendar  # TODO support more than one calendar, first we need to support more than one exchange

    _locked: pd.Timestamp | None = None

    _slice_iter: Iterator[tuple[pd.Timestamp, pd.Series]]
    _timestamp_slice: tuple[pd.Timestamp, pd.Series]
    _next_timestamp_slice: tuple[pd.Timestamp, pd.Series] | None

    def __init__(
        self,
        dataset: Dataset,
        assets: Collection[Asset],
        calendar: ExchangeCalendar = _XNYS_CALENDAR,
        slippage: SlippageModel = _NO_SLIPPAGE,
        fees: FeeModel = _NO_FEES,
    ) -> None:
        super().__init__()
        self._dataset = dataset
        self._portfolio = Portfolio(assets=assets)

        self._fees = fees
        self._slippage = slippage

        # get the first data slice
        self._calendar = calendar
        self._timeframe = pd.Timedelta(60, unit='s')  # TODO generalize

    @property
    def portfolio(self) -> Portfolio:
        return self._portfolio

    @property
    def skip(self) -> pd.Timedelta:
        if self._next_timestamp_slice is None:
            return pd.Timedelta.max
        return self._next_timestamp_slice[0] - self._timestamp_slice[0]

    @cached_property
    def pairs(self) -> list[TradingPair]:
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
    def open(self) -> bool:
        return self._calendar.is_open_at_time(  # type: ignore
            timestamp=self.clock.time, side='neither'
        )

    @property
    def open_pairs(self) -> list[TradingPair]:
        if self.open:
            return self.pairs
        return []

    @property
    def next_open(self) -> pd.Timestamp | None:
        if not self.open:
            return self._calendar.next_open(self.clock.time)  # type: ignore
        return None

    def execute_order(self, order: Order) -> OrderExecution:
        # check if locked
        self._raise_if_locked(order=order)

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
            timeframe=self._timeframe,
            reversed=is_sell,
        )

        # calculate finaly execution price after slippage and fees
        slippage_adj_quote = self._slippage(order=order, quote=quote, candle=candle)
        fees = self._fees(order=order, quote=slippage_adj_quote)

        # check asset balance
        bought_assets = order.pair.buy * order.quantity
        sold_assets = order.pair.sell * slippage_adj_quote * order.quantity
        self._raise_if_insufficient_assets(assets=(sold_assets, fees), order=order)

        # create order execution
        order_execution = OrderExecution(
            order=order, timestamp=timestamp, rate=slippage_adj_quote, fees=fees
        )

        # update portfolio
        # keep track of transaction in portfolio
        self._increase_holdings(asset=bought_assets)
        self._decrease_holdings(asset=sold_assets)
        self._decrease_holdings(asset=fees)
        # log the trade in the portfolio history
        trade = Trade(
            sold=sold_assets,
            bought=bought_assets,
            fees=fees,
            execution=order_execution,
        )
        self._portfolio._trade_histroy.append(trade)

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

    def _raise_if_locked(self, order: Order) -> None:
        if self._locked is not None and self._locked > self.clock.time:
            raise OrderAfterValuation(order=order, valuation=self._locked)
        self._locked = None

    def _raise_if_insufficient_assets(
        self, assets: Collection[Asset], order: Order
    ) -> None:
        asset_dict: dict[Instrument, list[Asset]] = defaultdict(list)
        for asset in assets:
            asset_dict[asset.instrument].append(asset)
        for instrument, assets in asset_dict.items():
            if self._portfolio.assets[instrument] < sum(assets, start=0 * instrument):
                raise InsufficientAssets(order=order)

    def _get_slice(self) -> tuple[pd.Timestamp, pd.Series]:
        data_slice = self._dataset.get(self._clock.time)
        return (
            cast(pd.Timestamp, data_slice.index[0]),
            cast(pd.Series, data_slice.iloc[0]),
        )

    def _calculate_value(self) -> tuple[Valuation, pd.Timestamp]:
        value = 0.0
        timestamp, data = self._get_slice()
        for instrument, asset in self._portfolio.assets.items():
            candle = _get_candle_from_slice(
                timestamp=timestamp, data=data[instrument.symbol]
            )
            value += float(asset.quantity) * candle.CLOSE
            timestamp = candle.TIMESTAMP + self._timeframe  # end of candle

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

    def _get_holding(self, instrument: Instrument) -> Asset:
        if instrument not in self._portfolio._assets:
            return 0 * instrument
        return self._portfolio._assets[instrument]

    def _set_holding(self, asset: Asset) -> None:
        self._portfolio._assets[asset.instrument] = asset

    def _increase_holdings(self, asset: Asset) -> None:
        balance = self._get_holding(asset.instrument)
        self._set_holding(balance + asset)

    def _decrease_holdings(self, asset: Asset) -> None:
        balance = self._get_holding(asset.instrument)
        self._set_holding(balance - asset)


_N_CANDLE_FEATURES = 7
_DEFAULT_OBSERVATION_WINDOW = 512


class HistoricalMarketObserver(BaseMarketObserver):
    _dataset: Dataset
    _observation_window: int
    _aggregate_type: Aggregates
    _timeframe: pd.Timedelta

    def __init__(
        self,
        dataset: Dataset,
        observation_window: int = _DEFAULT_OBSERVATION_WINDOW,
    ) -> None:
        super().__init__()
        self._dataset = dataset
        self._timeframe = pd.Timedelta(60, unit='s')  # TODO generalize
        self._observation_window = observation_window

    @cached_property
    def pairs(self) -> list[TradingPair]:
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
        window = self._dataset.get(
            timestamp=self.clock.time - self._timeframe,  # type: ignore
            num=self._observation_window,
            method='left',
        )
        arrs = [window[ticker].to_numpy().T for ticker in self._dataset.tickers]
        return np.stack(arrs, axis=1)
