from __future__ import annotations

from collections.abc import Callable
from collections.abc import Sequence
from functools import wraps
from typing import Generic
from typing import NewType
from typing import overload
from typing import ParamSpec
from typing import TypeVar

import pandas as pd

from merchant.trading.portfolio.trade import Trade


P = ParamSpec('P')
R = TypeVar('R')


class _Benchmark(Generic[P, R]):
    _name: str
    _description: str
    _func: Callable[P, R]

    def __init__(self, /, *, name: str, description: str, func: Callable[P, R]) -> None:
        self._name = name
        self._description = description
        self._func = func

    def __hash__(self) -> int:
        return super().__hash__()

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, _Benchmark):
            return NotImplemented
        return self is __o

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return self._func(*args, **kwargs)


@overload
def benchmark(func: Callable[P, R], /, *, desc: None = None) -> _Benchmark[P, R]:
    ...


@overload
def benchmark(
    func: None = None, /, *, desc: str = ''
) -> Callable[[Callable[P, R]], _Benchmark[P, R]]:
    ...


def benchmark(
    func: Callable[P, R] | None = None, desc: str | None = None
) -> _Benchmark[P, R] | Callable[[Callable[P, R]], _Benchmark[P, R]]:
    if func is None:

        def inner_dec(inner_func: Callable[P, R]) -> _Benchmark[P, R]:
            return _Benchmark(
                name=inner_func.__name__, description=desc or '', func=inner_func
            )

        return inner_dec
    return _Benchmark(name=func.__name__, description=desc or '', func=func)


Benchmarkable = NewType('Benchmarkable', pd.Series)
BenchmarkResult = NewType('BenchmarkResult', object)


def _get_winning_trades(positions: Sequence[Trade]) -> list[Trade]:
    raise NotImplementedError


def _get_losing_trades(positions: Sequence[Trade]) -> list[Trade]:
    raise NotImplementedError


@benchmark(desc='')
def volatility(value: Benchmarkable, _period: str | int = 'ytd') -> float:
    raise NotImplementedError


@benchmark(desc='')
def rolling_valatility(value: Benchmarkable, _window: str | int = '1d') -> float:
    raise NotImplementedError


@benchmark(desc='')
def sharpe_ratio(value: Benchmarkable, _period: str | int = 'ytd') -> float:
    raise NotImplementedError


@benchmark(desc='')
def rolling_sharpe_ratio(value: Benchmarkable, _window: str | int = '1d') -> float:
    raise NotImplementedError


@benchmark(desc='')
def sortino_ratio(value: Benchmarkable, _period: str | int = 'ytd') -> float:
    raise NotImplementedError


@benchmark(desc='')
def rolling_sortino_ratio(value: Benchmarkable, _window: str | int = '1d') -> float:
    raise NotImplementedError


@benchmark(desc='')
def beta(value: Benchmarkable, _period: str | int = 'ytd') -> float:
    raise NotImplementedError


@benchmark(desc='')
def rolling_beta(value: Benchmarkable, _window: str | int = '1d') -> float:
    raise NotImplementedError


@benchmark(desc='')
def max_drawdown_ratio(value: Benchmarkable, _period: str | int = 'ytd') -> float:
    raise NotImplementedError


@benchmark(desc='')
def gain_to_pain_ratio(value: Benchmarkable, _period: str | int = 'ytd') -> float:
    raise NotImplementedError


@benchmark(desc='')
def jensen_alpha(value: Benchmarkable, _period: str | int = 'ytd') -> float:
    raise NotImplementedError


@benchmark(desc='')
def calmar_ratio(value: Benchmarkable, _period: str | int = 'ytd') -> float:
    raise NotImplementedError


@benchmark(desc='')
def kelly_criterion(value: Benchmarkable, _period: str | int = 'ytd') -> float:
    raise NotImplementedError


@benchmark(desc='')
def tracking_error(value: Benchmarkable, _period: str | int = 'ytd') -> float:
    raise NotImplementedError


@benchmark(desc='')
def information_ratio(value: Benchmarkable, _period: str | int = 'ytd') -> float:
    raise NotImplementedError
