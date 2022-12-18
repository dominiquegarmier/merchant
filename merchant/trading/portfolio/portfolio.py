from __future__ import annotations

import functools
from abc import ABCMeta
from collections.abc import Callable
from collections.abc import Collection
from decimal import Decimal
from functools import cache
from typing import Any
from typing import Concatenate
from typing import ParamSpec
from typing import TypeVar

import pandas as pd

from merchant.core.clock import HasInternalClock
from merchant.core.clock import NSClock
from merchant.core.numeric import NormedDecimal
from merchant.trading.market import MarketSimulation
from merchant.trading.portfolio.benchmarks import _Benchmark
from merchant.trading.tools.asset import Asset
from merchant.trading.tools.instrument import Instrument


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
        return f'{type(self)}({self._assets!r})'

    def __repr__(self) -> str:
        return str(self)


P = ParamSpec('P')
R = TypeVar('R')


class _BenchmarkCache:
    _hooked: bool = False
    _cache: dict[_Benchmark, Any] = {}

    def __contains__(self, benchmark: _Benchmark[P, R]) -> bool:
        return benchmark in self._cache

    def __getitem__(self, benchmark: _Benchmark[P, R]) -> R:
        return self._cache[benchmark]  # type: ignore

    def __setitem__(self, benchmark: _Benchmark[P, R], value: R) -> None:
        self._cache[benchmark] = value

    def _invalidate(self) -> None:
        self._cache.clear()


def cached_benchmark(
    func: Callable[[Portfolio, _Benchmark[P, R]], R]
) -> Callable[[Portfolio, _Benchmark[P, R]], R]:
    @functools.wraps(func)
    def wrapper(self: Portfolio, bm: _Benchmark[P, R]) -> R:
        if bm not in self._benchmark_cache:
            self._benchmark_cache[bm] = func(self, bm)
        return self._benchmark_cache[bm]

    return wrapper


class Portfolio(HasInternalClock[NSClock], _StaticPortfolio):
    '''
    'Portfolio' represents a trading portfolio, this includes:
        - the current state of the portfolio
        - performance (relative to a connected market)
        - value and trading history
    '''

    _benchmark_instrument: Instrument
    _benchmark_cache: _BenchmarkCache
    _benchmarks: tuple[str, ...] = (
        'pl_ratio',
        'volatility',
        'cagr',
        'jensen_alpha',
        'sharpe_ratio',
        'calmar_ratio',
        'sortino_ratio',
        'treynor_ratio',
        'max_drawdown_ratio',
    )

    _market: MarketSimulation
    _value_history: pd.Series

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

    @property
    def benchmark_instrument(self) -> Instrument:
        return self._benchmark

    @benchmark_instrument.setter
    def benchmark_instrument(self, instrument: Instrument) -> None:
        self._benchmark = instrument

    @property
    def benchmarks(self) -> tuple[str, ...]:
        return self._benchmarks

    def __getattr__(self, name: str) -> object:
        if name in self._benchmarks:
            raise NotImplementedError
        return getattr(self, name)

    @cached_benchmark
    def _get_benchmark(self, bm: _Benchmark) -> object:
        raise NotImplementedError
