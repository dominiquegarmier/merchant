from __future__ import annotations

import functools
import inspect
from abc import ABCMeta
from collections import defaultdict
from collections.abc import Callable
from collections.abc import Collection
from decimal import Decimal
from typing import Any
from typing import Concatenate
from typing import NewType
from typing import ParamSpec
from typing import TypeVar

import pandas as pd

from merchant.core.base import Identifiable
from merchant.core.clock import ClockHook
from merchant.core.clock import HasInternalClock
from merchant.core.clock import NSClock
from merchant.core.numeric import NormedDecimal
from merchant.trading.broker import MarketSimulation
from merchant.trading.portfolio.benchmarks import Benchmark
from merchant.trading.portfolio.benchmarks import BETA
from merchant.trading.portfolio.benchmarks import BoundBenchmark
from merchant.trading.portfolio.benchmarks import CALMAR_RATIO
from merchant.trading.portfolio.benchmarks import COMPOUND_ANNUAL_GROWTH_RATE
from merchant.trading.portfolio.benchmarks import GAIN_TO_PAIN_RATIO
from merchant.trading.portfolio.benchmarks import INFORMATION_RATIO
from merchant.trading.portfolio.benchmarks import JENSEN_ALPHA
from merchant.trading.portfolio.benchmarks import KELLY_CRITERION
from merchant.trading.portfolio.benchmarks import MAX_DRAWDOWN_RATIO
from merchant.trading.portfolio.benchmarks import ROLLING_BETA
from merchant.trading.portfolio.benchmarks import ROLLING_SHARPE_RATIO
from merchant.trading.portfolio.benchmarks import ROLLING_SORTINO_RATIO
from merchant.trading.portfolio.benchmarks import ROLLING_VOLATILITY
from merchant.trading.portfolio.benchmarks import SHARPE_RATIO
from merchant.trading.portfolio.benchmarks import SORTINO_RATIO
from merchant.trading.portfolio.benchmarks import TRACKING_ERROR
from merchant.trading.portfolio.benchmarks import TREYNOR_RATIO
from merchant.trading.portfolio.benchmarks import VOLATILITY
from merchant.trading.portfolio.trade import ClosedPosition
from merchant.trading.portfolio.trade import Trade
from merchant.trading.portfolio.trade import ValuedTrade
from merchant.trading.tools.asset import Asset
from merchant.trading.tools.asset import Valuation
from merchant.trading.tools.instrument import Instrument
from merchant.trading.tools.instrument import USD

DEFAULT_BENCHMARKS: dict[Benchmark, ArgsKwargs | None] = {
    VOLATILITY: None,
    ROLLING_VOLATILITY: None,
    SHARPE_RATIO: None,
    ROLLING_SHARPE_RATIO: None,
    SORTINO_RATIO: None,
    ROLLING_SORTINO_RATIO: None,
    BETA: None,
    ROLLING_BETA: None,
    MAX_DRAWDOWN_RATIO: None,
    GAIN_TO_PAIN_RATIO: None,
    JENSEN_ALPHA: None,
    CALMAR_RATIO: None,
    KELLY_CRITERION: None,
    TRACKING_ERROR: None,
    INFORMATION_RATIO: None,
    TREYNOR_RATIO: None,
    COMPOUND_ANNUAL_GROWTH_RATE: None,
}


class _StaticPortfolio(Identifiable, metaclass=ABCMeta):
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
                instrument,
                quantity=NormedDecimal(Decimal(0), prec=instrument.precision),
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
        return f'{type(self)}(...)'

    def __repr__(self) -> str:
        return str(self)


# buy amount, valuation wrt. benchmark, trade
TradeTuple = tuple[Asset, ValuedTrade]


class _OpenPositionStack:
    _positions: dict[Instrument, list[TradeTuple]]
    _benchmark: Instrument  # we don't track positions on the benchmark (since their performance is always trivial)

    def __init__(self, /, *, benchmark: Instrument) -> None:
        self._positions = defaultdict(list)
        self._benchmark = benchmark

    def _push(self, trade: ValuedTrade) -> None:
        trade_tuple: TradeTuple = (trade._buy, 0, trade)  # type: ignore
        self._positions[trade._buy.instrument].append(trade_tuple)

    def _pop(self, trade: ValuedTrade) -> list[ClosedPosition]:
        ret: list[ClosedPosition] = []
        sold_asset = trade._sell
        while sold_asset > 0:
            asset, open = self._positions[trade._sell.instrument].pop()

            if sold_asset <= asset:
                self._positions[trade._sell.instrument].append(
                    (asset - trade._sell, open)
                )
                ret.append(ClosedPosition(amount=trade._sell, open=open, close=trade))
                continue

            ret.append(ClosedPosition(amount=asset, open=open, close=trade))
            sold_asset -= asset
        return ret

    def handle_trade(self, trade: ValuedTrade) -> list[ClosedPosition]:
        if trade._buy.instrument != self._benchmark:
            self._push(trade)
        if trade._sell.instrument != self._benchmark:
            return self._pop(trade)
        return []


TPortfolio = TypeVar('TPortfolio', bound='Portfolio')
P = ParamSpec('P')
R = TypeVar('R')


def invalidates_cache(
    func: Callable[Concatenate[TPortfolio, P], R]
) -> Callable[Concatenate[TPortfolio, P], R]:
    @functools.wraps(func)
    def wrapper(self: TPortfolio, *args: P.args, **kwargs: P.kwargs) -> R:
        self._invalidate_caches()
        return func(self, *args, **kwargs)

    return wrapper


FnCache = dict[int, Any]


def iv_cached(
    func: Callable[Concatenate[TPortfolio, P], R]
) -> Callable[Concatenate[TPortfolio, P], R]:
    fn_key = hash(func)

    @functools.wraps(func)
    def wrapper(self: TPortfolio, *args: P.args, **kwargs: P.kwargs) -> R:
        if fn_key not in self._cache:
            self._cache[fn_key] = {}

        key = hash(args + tuple(kwargs.items()))
        if key not in self._cache[fn_key]:
            self._cache[fn_key][key] = func(self, *args, **kwargs)
        return self._cache[fn_key][key]  # type: ignore

    return wrapper


ArgsKwargs = tuple[tuple[Any], dict[str, Any]]


class Portfolio(HasInternalClock[NSClock], _StaticPortfolio):
    '''
    'Portfolio' represents a trading portfolio, this includes:
        - the current state of the portfolio
        - performance (relative to a connected market)
        - value and trading history
    '''

    _valuation: Instrument  # with respect to which instrument the portfolio is valued
    _benchmarks: dict[Benchmark, ArgsKwargs | None] = DEFAULT_BENCHMARKS
    _bound_benchmarks: dict[Benchmark, BoundBenchmark]

    _cache: dict[int, FnCache]  # cache that is invalidated on every step
    _clock_hook: ClockHook
    _market: MarketSimulation

    _open_positions: _OpenPositionStack
    _value_history: pd.Series  # values are stored as floats
    _trade_history: list[Trade]
    _position_histroy: list[ClosedPosition]

    def __init__(
        self,
        /,
        *,
        market: MarketSimulation,
        assets: Collection[Asset] | None = None,
        valuation: Instrument = USD,
    ) -> None:
        super().__init__(assets=assets)

        self._market = market
        self._clock = self._market.clock
        self._valuation = valuation

        for benchmark, arg_tuple in self._benchmarks.items():
            bound_benchmark = benchmark(self)
            self._bound_benchmarks[benchmark] = bound_benchmark
            # check types
            sig = inspect.signature(bound_benchmark)

            if arg_tuple is None:
                args: tuple = ()
                kwargs: dict[str, Any] = {}
            else:
                args, kwargs = arg_tuple

            try:
                sig.bind(*args, **kwargs)
            except TypeError as e:
                raise TypeError(
                    f'Invalid arguments for {benchmark!r} bound to {self!r}'
                ) from e

        # attach _step to clock
        hook = ClockHook(lambda: self._step())
        self._clock_hook = hook
        self._clock_hook = self.clock.attach(hook=hook)

    def _step(
        self,
    ) -> None:
        # trigger atleast once per step
        self._update_history()
        self._invalidate_caches()

    def _update_history(self) -> None:
        current_ts = self.clock.time
        if current_ts not in self._value_history:
            self._value_history[current_ts] = float(self.value)

    def _invalidate_caches(self) -> None:
        self._value_cache = None
        for benchmark in self._bound_benchmarks.values():
            benchmark.invalidate_cache()
        for ch in self._cache.values():
            ch.clear()

    @property
    @iv_cached
    def value(self) -> Valuation:
        raise NotImplementedError

    @property
    def value_history(self) -> pd.Series:
        self._update_history()
        return self._value_history

    @property
    @iv_cached
    def trade_history(self) -> tuple[Trade, ...]:
        return tuple(self._trade_history)

    @property
    def position_history(self) -> tuple[ClosedPosition, ...]:
        return tuple(self._position_histroy)

    def _append_trade_to_history(self, trade: ValuedTrade) -> None:
        self._trade_history.append(trade)
        closed_positions = self._open_positions.handle_trade(trade)
        self._position_histroy.extend(closed_positions)

    @property
    def valuation(self) -> Instrument:
        return self._valuation

    @property
    def benchmarks(self) -> tuple[Benchmark, ...]:
        return tuple(self._benchmarks.keys())

    @property
    def market(self) -> MarketSimulation:
        return self._market

    # add cache invalidation to all mutating methods
    @invalidates_cache
    def add_asset(self, asset: Asset) -> None:
        super().add_asset(asset)

    @invalidates_cache
    def remove_asset(self, asset: Asset) -> None:
        super().remove_asset(asset)

    @invalidates_cache
    def __iadd__(self, asset: Asset) -> _StaticPortfolio:
        return super().__iadd__(asset)

    @invalidates_cache
    def __isub__(self, asset: Asset) -> _StaticPortfolio:
        return super().__isub__(asset)
