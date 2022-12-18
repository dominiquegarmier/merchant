from __future__ import annotations

import functools
import inspect
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
from merchant.trading.portfolio.benchmarks import Benchmark
from merchant.trading.portfolio.benchmarks import BoundBenchmark
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
        return f'{type(self)}(...)'

    def __repr__(self) -> str:
        return str(self)


ArgsKwargs = tuple[tuple[Any], dict[str, Any]]


class Portfolio(HasInternalClock[NSClock], _StaticPortfolio):
    '''
    'Portfolio' represents a trading portfolio, this includes:
        - the current state of the portfolio
        - performance (relative to a connected market)
        - value and trading history
    '''

    _benchmark_instrument: Instrument
    _benchmarks: dict[Benchmark, tuple[ArgsKwargs, ...]]
    _bound_benchmarks: dict[Benchmark, BoundBenchmark]

    _market: MarketSimulation
    _value_history: pd.Series

    def __init__(
        self, /, *, market: MarketSimulation, assets: Collection[Asset] | None = None
    ) -> None:
        super().__init__(assets=assets)

        self._market = market
        self._clock = self._market.clock

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
    def benchmarks(self) -> tuple[Benchmark, ...]:
        return tuple(self._benchmarks.keys())

    def __getattr__(self, name: str) -> object:
        if name in self._benchmarks:
            raise NotImplementedError
        return getattr(self, name)
