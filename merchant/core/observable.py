from __future__ import annotations

import functools
import itertools
from abc import ABCMeta
from abc import abstractmethod
from collections.abc import Callable
from typing import Any
from typing import Concatenate
from typing import Generic
from typing import Literal
from typing import overload
from typing import ParamSpec
from typing import TypeVar

TObservable = TypeVar('TObservable', bound='Observable')


class AbstractHook(Generic[TObservable], metaclass=ABCMeta):
    _signal: str | None = None
    _func: Callable[[], None]
    _active: bool = True

    @abstractmethod
    def __init__(self, func: Callable[[], None], **kwargs: Any):
        ...

    def reset(self) -> None:
        self._active = True

    @abstractmethod
    def __call__(self, obs: TObservable) -> None:
        ...

    def __hash__(self) -> int:
        return super().__hash__()

    def __eq__(self, __o: object) -> bool:
        return self is __o

    def __str__(self) -> str:
        return f'{type(self)}(func={self._func!r})'

    def __repr__(self) -> str:
        return str(self)


P = ParamSpec('P')
R = TypeVar('R')


@overload
def run_hooks(
    *, signal: str | tuple[str]
) -> Callable[
    [Callable[Concatenate[TObservable, P], R]], Callable[Concatenate[TObservable, P], R]
]:
    ...


@overload
def run_hooks(
    func: Callable[Concatenate[TObservable, P], R]
) -> Callable[Concatenate[TObservable, P], R]:
    ...


def run_hooks(
    func: Callable[Concatenate[TObservable, P], R] | None = None,
    *,
    signal: str | tuple[str] | None = None,
) -> (
    Callable[Concatenate[TObservable, P], R]
    | Callable[
        [Callable[Concatenate[TObservable, P], R]],
        Callable[Concatenate[TObservable, P], R],
    ]
):
    def decorator(_func: Callable[Concatenate[TObservable, P], R]):
        @functools.wraps(_func)
        def wrapper(self: TObservable, *args: P.args, **kwargs: P.kwargs) -> R:
            ret = _func(self, *args, **kwargs)
            self.run_hooks(signal=signal)
            return ret

        return wrapper

    if func is None:
        return decorator
    return decorator(func)  # type: ignore  # this is a mypy bug@overload


@overload
def reset_hooks(
    *, signal: str | tuple[str]
) -> Callable[
    [Callable[Concatenate[TObservable, P], R]], Callable[Concatenate[TObservable, P], R]
]:
    ...


@overload
def reset_hooks(
    func: Callable[Concatenate[TObservable, P], R]
) -> Callable[Concatenate[TObservable, P], R]:
    ...


def reset_hooks(
    func: Callable[Concatenate[TObservable, P], R] | None = None,
    *,
    signal: str | tuple[str] | None = None,
) -> (
    Callable[Concatenate[TObservable, P], R]
    | Callable[
        [Callable[Concatenate[TObservable, P], R]],
        Callable[Concatenate[TObservable, P], R],
    ]
):
    def decorator(_func: Callable[Concatenate[TObservable, P], R]):
        @functools.wraps(_func)
        def wrapper(self: TObservable, *args: P.args, **kwargs: P.kwargs) -> R:
            ret = _func(self, *args, **kwargs)
            self.reset_hooks(signal=signal)
            return ret

        return wrapper

    if func is None:
        return decorator
    return decorator(func)  # type: ignore  # this is a mypy bug


def _signaled_hooks(hooks: set[AbstractHook[Any]], signal: str | tuple[str] | None):
    if isinstance(signal, str):
        signal = (signal,)
    for hook in hooks:
        if signal is None or hook._signal in signal:
            yield hook


class Observable(metaclass=ABCMeta):
    _hooks: set[AbstractHook[Any]]  # use Self type here once it's supported

    def reset_hooks(self: TObservable, /, *, signal: str | tuple[str] | None = None):
        for hook in _signaled_hooks(self._hooks, signal):
            hook.reset()

    def run_hooks(self: TObservable, /, *, signal: str | tuple[str] | None = None):
        for hook in _signaled_hooks(self._hooks, signal):
            hook(obs=self)

    def attach(self: TObservable, /, *, hook: AbstractHook[TObservable]):
        self._hooks.add(hook)

    def detach(self: TObservable, /, *, hook: AbstractHook[TObservable]):
        self._hooks.remove(hook)


__all__ = [
    'AbstractHook',
    'Observable',
    'run_hooks',
    'reset_hooks',
]
