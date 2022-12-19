from __future__ import annotations

from abc import ABCMeta
from abc import abstractmethod
from collections.abc import Callable
from collections.abc import Hashable
from collections.abc import Sequence
from functools import wraps
from typing import Any
from typing import Concatenate
from typing import Generic
from typing import Literal
from typing import NewType
from typing import overload
from typing import ParamSpec
from typing import TYPE_CHECKING
from typing import TypeVar

import pandas as pd

from merchant.trading.portfolio.portfolio import Portfolio
from merchant.trading.portfolio.trade import Trade


P = ParamSpec('P')
T = TypeVar('T', bound='AbstractBenchmark')
R = TypeVar('R')

BenchmarkResult = float
# force the use of 'chached_benchmark' decorator on '__call__' method
# the only way to return _BenchmarkResult is to use the decorator
# TODO: should this be done like that?
_BenchmarkResult = NewType('_BenchmarkResult', BenchmarkResult)


def benchmark(
    func: Callable[Concatenate[T, P], BenchmarkResult]
) -> Callable[Concatenate[T, P], _BenchmarkResult]:
    @wraps(func)
    def inner(self: T, *args: P.args, **kwargs: P.kwargs) -> _BenchmarkResult:
        key = hash((args, tuple(kwargs.items())))
        if key not in self._cache:
            self._cache[key] = func(self, *args, **kwargs)
        return _BenchmarkResult(self._cache[key])

    return inner


class BenchmarkMeta(ABCMeta):
    def __hash__(self) -> int:
        return super().__hash__()

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, BenchmarkMeta):
            return False
        return self is __o

    def __repr__(self) -> str:
        return f'Benchmark {self.__name__}'


class AbstractBenchmark(metaclass=BenchmarkMeta):
    _description: str = ''
    _name: str = ''

    _portfolio: Portfolio
    _cache: dict[int, BenchmarkResult]

    def __init__(self, portfolio: Portfolio) -> None:
        self._name = self._name or type(self).__name__
        self._description = self._description or self.__doc__ or self._name
        self._portfolio = portfolio

    @abstractmethod
    @benchmark
    def __call__(self, *args: Any, **kwargs: Any) -> BenchmarkResult:
        ...

    def invalidate_cache(self) -> None:
        self._cache = {}

    def __hash__(self) -> int:
        return hash((type(self), self._portfolio))

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, AbstractBenchmark):
            return False
        return self._portfolio == __o._portfolio and type(self) == type(__o)

    def __str__(self) -> str:
        return f'{self._name}: {self._description}, {self._portfolio!r}'

    def __repr__(self) -> str:
        return str(self)


class _Volatility(AbstractBenchmark):
    '''Volatility of the portfolio'''

    def __init__(self) -> None:
        super().__init__(self)  # type: ignore

    @benchmark
    def __call__(self, period: str | int = 'ytd') -> BenchmarkResult:
        raise NotImplementedError


class _RollingVolatility(AbstractBenchmark):
    '''Volatility of the portfolio'''

    @benchmark
    def __call__(self, window: str | int = '1d') -> BenchmarkResult:
        raise NotImplementedError


class _SharpeRatio(AbstractBenchmark):
    '''Sharpe ratio of the portfolio'''

    @benchmark
    def __call__(self, period: str | int = 'ytd') -> BenchmarkResult:
        raise NotImplementedError


class _RollingSharpeRatio(AbstractBenchmark):
    '''Sharpe ratio of the portfolio'''

    @benchmark
    def __call__(self, window: str | int = '1d') -> BenchmarkResult:
        raise NotImplementedError


class _SortinoRatio(AbstractBenchmark):
    '''Sortino ratio of the portfolio'''

    @benchmark
    def __call__(self, period: str | int = 'ytd') -> BenchmarkResult:
        raise NotImplementedError


class _RollingSortinoRatio(AbstractBenchmark):
    '''Sortino ratio of the portfolio'''

    @benchmark
    def __call__(self, window: str | int = '1d') -> BenchmarkResult:
        raise NotImplementedError


class _Beta(AbstractBenchmark):
    '''Beta of the portfolio'''

    @benchmark
    def __call__(self, period: str | int = 'ytd') -> BenchmarkResult:
        raise NotImplementedError


class _RollingBeta(AbstractBenchmark):
    '''Beta of the portfolio'''

    @benchmark
    def __call__(self, window: str | int = '1d') -> BenchmarkResult:
        raise NotImplementedError


class _MaxDrawdownRatio(AbstractBenchmark):
    '''Max drawdown ratio of the portfolio'''

    @benchmark
    def __call__(self, period: str | int = 'ytd') -> BenchmarkResult:
        raise NotImplementedError


class _GainToPainRatio(AbstractBenchmark):
    '''Gain to pain ratio of the portfolio'''

    @benchmark
    def __call__(self, period: str | int = 'ytd') -> BenchmarkResult:
        raise NotImplementedError


class _JensenAlpha(AbstractBenchmark):
    '''Jensen alpha of the portfolio'''

    @benchmark
    def __call__(self, period: str | int = 'ytd') -> BenchmarkResult:
        raise NotImplementedError


class _CalmarRatio(AbstractBenchmark):
    '''Calmar ratio of the portfolio'''

    @benchmark
    def __call__(self, period: str | int = 'ytd') -> BenchmarkResult:
        raise NotImplementedError


class _KellyCriterion(AbstractBenchmark):
    '''Kelly criterion of the portfolio'''

    @benchmark
    def __call__(self, period: str | int = 'ytd') -> BenchmarkResult:
        raise NotImplementedError


class _TrackingError(AbstractBenchmark):
    '''Tracking error of the portfolio'''

    @benchmark
    def __call__(self, period: str | int = 'ytd') -> BenchmarkResult:
        raise NotImplementedError


class _InformationRatio(AbstractBenchmark):
    '''Information ratio of the portfolio'''

    @benchmark
    def __call__(self, period: str | int = 'ytd') -> BenchmarkResult:
        raise NotImplementedError


class _TreynorRatio(AbstractBenchmark):
    '''Treynor ratio of the portfolio'''

    @benchmark
    def __call__(self, period: str | int = 'ytd') -> BenchmarkResult:
        raise NotImplementedError


class _CompoundAnnualGrowthRate(AbstractBenchmark):
    '''Compound annual growth rate of the portfolio'''

    @benchmark
    def __call__(self, period: str | int = 'ytd') -> BenchmarkResult:
        raise NotImplementedError


# type of a benchmark class
Benchmark = type[AbstractBenchmark]
BoundBenchmark = AbstractBenchmark

VOLATILITY = _Volatility
ROLLING_VOLATILITY = _RollingVolatility
SHARPE_RATIO = _SharpeRatio
ROLLING_SHARPE_RATIO = _RollingSharpeRatio
SORTINO_RATIO = _SortinoRatio
ROLLING_SORTINO_RATIO = _RollingSortinoRatio
BETA = _Beta
ROLLING_BETA = _RollingBeta
MAX_DRAWDOWN_RATIO = _MaxDrawdownRatio
GAIN_TO_PAIN_RATIO = _GainToPainRatio
JENSEN_ALPHA = _JensenAlpha
CALMAR_RATIO = _CalmarRatio
KELLY_CRITERION = _KellyCriterion
TRACKING_ERROR = _TrackingError
INFORMATION_RATIO = _InformationRatio
TREYNOR_RATIO = _TreynorRatio
COMPOUND_ANNUAL_GROWTH_RATE = _CompoundAnnualGrowthRate

__all__ = [
    'AbstractBenchmark',
    'benchmark',
    'Benchmark',
    'BoundBenchmark',
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
