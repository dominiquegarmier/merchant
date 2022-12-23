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
from merchant.trading.market import MarketSimulation
from merchant.trading.portfolio.benchmarks import Benchmark
from merchant.trading.portfolio.benchmarks import BoundBenchmark
from merchant.trading.portfolio.trade import ClosedPosition
from merchant.trading.portfolio.trade import Trade
from merchant.trading.portfolio.trade import ValuedTrade
from merchant.trading.tools.asset import Asset
from merchant.trading.tools.asset import Valuation
from merchant.trading.tools.instrument import Instrument
from merchant.trading.tools.instrument import USD


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


ArgsKwargs = tuple[tuple[Any], dict[str, Any]]


class Portfolio(HasInternalClock[NSClock], _StaticPortfolio):
    '''
    'Portfolio' represents a trading portfolio, this includes:
        - the current state of the portfolio
        - performance (relative to a connected market)
        - value and trading history
    '''

    _valuation: Instrument  # with respect to which instrument the portfolio is valued
    _benchmarks: dict[Benchmark, tuple[ArgsKwargs, ...]]
    _bound_benchmarks: dict[Benchmark, BoundBenchmark]
    _value_cache: Valuation | None

    _clock_hook: ClockHook

    _market: MarketSimulation

    _open_positions: _OpenPositionStack
    _value_history: pd.Series
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
            for args, kwargs in arg_tuple:
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

    def _get_obs(self) -> Any:
        raise NotImplementedError

    def _step(self) -> None:
        _ = self._get_obs()
        self._invalidate_caches()

    @property
    def market(self) -> MarketSimulation:
        return self._market

    @property
    def value(self) -> Valuation:
        if self._value_cache is None:
            raise NotImplementedError
        return self._value_cache

    @property
    def valuation(self) -> Instrument:
        return self._valuation

    @property
    def benchmarks(self) -> tuple[Benchmark, ...]:
        return tuple(self._benchmarks.keys())

    @property
    def trade_history(self) -> tuple[Trade, ...]:
        return tuple(self._trade_history)

    @property
    def value_history(self) -> pd.Series:
        return self._value_history

    @property
    def position_history(self) -> tuple[ClosedPosition, ...]:
        return tuple(self._position_histroy)

    def _invalidate_caches(self) -> None:
        self._value_cache = None
        for benchmark in self._bound_benchmarks.values():
            benchmark.invalidate_cache()

    def _append_trade_to_history(self, trade: ValuedTrade) -> None:
        self._trade_history.append(trade)
        closed_positions = self._open_positions.handle_trade(trade)
        self._position_histroy.extend(closed_positions)

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
