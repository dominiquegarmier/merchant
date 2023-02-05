from __future__ import annotations

import functools
from abc import ABCMeta
from abc import abstractmethod
from collections.abc import Callable
from functools import wraps
from typing import Any
from typing import Concatenate
from typing import NewType
from typing import ParamSpec
from typing import Protocol
from typing import TypeVar

import pandas as pd

from merchant.trading.portfolio.portfolio import Portfolio
from merchant.trading.portfolio.trade import ClosedPosition
from merchant.trading.portfolio.trade import Trade
from merchant.trading.tools.asset import Valuation


class Benchmarkable(Protocol):
    @property
    def value(self) -> Valuation:
        ...

    @property
    def value_history(self) -> pd.Series:
        ...

    @property
    def trade_history(self) -> list[Trade]:
        ...

    @property
    def position_history(self) -> list[ClosedPosition]:
        ...


TResult = float
TConfig = dict[str, Any]

TBenchmark = Callable[[Benchmarkable, TConfig], TResult]


class _benchmark:
    _func: TBenchmark
    _description: str
    _name: str

    def __init__(self, func: TBenchmark) -> None:
        self._func = func
        self._name = func.__name__
        self._description = func.__doc__ or ''

    def __call__(self, benchmarkable: Benchmarkable, config: TConfig) -> TResult:
        return self._func(benchmarkable, config)

    @property
    def description(self) -> str:
        return self._description

    def __hash__(self) -> int:
        return super().__hash__()

    def __eq__(self, __o: object) -> bool:
        return self is __o

    def __str__(self) -> str:
        return f'Benchmark: {self._name}'

    def __repr__(self) -> str:
        return str(self)


def benchmark(func: TBenchmark) -> _benchmark:
    warpper = _benchmark(func)
    functools.update_wrapper(warpper, func)
    return warpper


@benchmark
def _volatility(pf: Benchmarkable, config: TConfig) -> TResult:
    raise NotImplementedError


@benchmark
def _rolling_volatility(pf: Benchmarkable, config: TConfig) -> TResult:
    raise NotImplementedError


@benchmark
def _sharpe_ratio(pf: Benchmarkable, config: TConfig) -> TResult:
    raise NotImplementedError


@benchmark
def _rolling_sharpe_ratio(pf: Benchmarkable, config: TConfig) -> TResult:
    raise NotImplementedError


@benchmark
def _sortino_ratio(pf: Benchmarkable, config: TConfig) -> TResult:
    raise NotImplementedError


@benchmark
def _rolling_sortino_ratio(pf: Benchmarkable, config: TConfig) -> TResult:
    raise NotImplementedError


@benchmark
def _beta(pf: Benchmarkable, config: TConfig) -> TResult:
    raise NotImplementedError


@benchmark
def _rolling_beta(pf: Benchmarkable, config: TConfig) -> TResult:
    raise NotImplementedError


@benchmark
def _max_drawdown_ratio(pf: Benchmarkable, config: TConfig) -> TResult:
    raise NotImplementedError


@benchmark
def _gain_to_pain_ratio(pf: Benchmarkable, config: TConfig) -> TResult:
    raise NotImplementedError


@benchmark
def _jensen_alpha(pf: Benchmarkable, config: TConfig) -> TResult:
    raise NotImplementedError


@benchmark
def _calmar_ratio(pf: Benchmarkable, config: TConfig) -> TResult:
    raise NotImplementedError


@benchmark
def _kelly_criterion(pf: Benchmarkable, config: TConfig) -> TResult:
    raise NotImplementedError


@benchmark
def _tracking_error(pf: Benchmarkable, config: TConfig) -> TResult:
    raise NotImplementedError


@benchmark
def _information_ratio(pf: Benchmarkable, config: TConfig) -> TResult:
    raise NotImplementedError


@benchmark
def _treynor_ratio(pf: Benchmarkable, config: TConfig) -> TResult:
    raise NotImplementedError


@benchmark
def _compound_annual_growthRate(pf: Benchmarkable, config: TConfig) -> TResult:
    raise NotImplementedError


Benchmark = _benchmark

VOLATILITY = _volatility
ROLLING_VOLATILITY = _rolling_volatility
SHARPE_RATIO = _sharpe_ratio
ROLLING_SHARPE_RATIO = _rolling_sharpe_ratio
SORTINO_RATIO = _sortino_ratio
ROLLING_SORTINO_RATIO = _rolling_sortino_ratio
BETA = _beta
ROLLING_BETA = _rolling_beta
MAX_DRAWDOWN_RATIO = _max_drawdown_ratio
GAIN_TO_PAIN_RATIO = _gain_to_pain_ratio
JENSEN_ALPHA = _jensen_alpha
CALMAR_RATIO = _calmar_ratio
KELLY_CRITERION = _kelly_criterion
TRACKING_ERROR = _tracking_error
INFORMATION_RATIO = _information_ratio
TREYNOR_RATIO = _treynor_ratio
COMPOUND_ANNUAL_GROWTH_RATE = _compound_annual_growthRate

__all__ = [
    'benchmark',
    'Benchmark',
    'VOLATILITY',
    'ROLLING_VOLATILITY',
    'SHARPE_RATIO',
    'ROLLING_SHARPE_RATIO',
    'SORTINO_RATIO',
    'ROLLING_SORTINO_RATIO',
    'BETA',
    'ROLLING_BETA',
    'MAX_DRAWDOWN_RATIO',
    'GAIN_TO_PAIN_RATIO',
    'JENSEN_ALPHA',
    'CALMAR_RATIO',
    'KELLY_CRITERION',
    'TRACKING_ERROR',
    'INFORMATION_RATIO',
    'TREYNOR_RATIO',
    'COMPOUND_ANNUAL_GROWTH_RATE',
]
