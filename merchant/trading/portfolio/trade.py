from __future__ import annotations

from collections import defaultdict

import pandas as pd

from merchant.trading.tools.asset import Asset
from merchant.trading.tools.asset import Valuation
from merchant.trading.tools.instrument import Instrument
from merchant.trading.tools.pair import TradingPair


class OpenPositionStack:
    _positions: dict[Instrument, list[TradeTuple]]
    _benchmark: Instrument  # we don't track positions on the benchmark (since their performance is always trivial)

    def __init__(self, /, *, benchmark: Instrument) -> None:
        self._positions = defaultdict(list)
        self._benchmark = benchmark

    def handle_trade(self, trade: Trade) -> list[ClosedPosition]:
        raise NotImplementedError


class Trade:
    _buy: Asset
    _sell: Asset
    _time: pd.Timestamp
    _direction: TradingPair

    def __init__(self, direction: TradingPair, buy: Asset, sell: Asset) -> None:
        if direction.buy != buy.instrument:
            raise ValueError(f'buy asset {buy} does not match direction {direction}')
        if direction.sell != sell.instrument:
            raise ValueError(f'sell asset {sell} does not match direction {direction}')

        self._direction = direction
        self._buy = buy
        self._sell = sell

    def __hash__(self) -> int:
        return super().__hash__()

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, Trade):
            return False
        return self is __o


# buy amount, valuation wrt. benchmark, trade
TradeTuple = tuple[Asset, Valuation, Trade]


class ClosedPosition:
    _instrument: Instrument
    _amount: Asset
    _open: Trade
    _close: Trade
    _open_value: Valuation
    _close_value: Valuation

    def __init__(
        self,
        instrument: Instrument,
        /,
        *,
        amount: Asset,
        open: Trade,
        close: Trade,
        open_value: Valuation,
        close_value: Valuation,
    ) -> None:
        self._instrument = instrument
        self._amount = amount
        self._open = open
        self._close = close
        self._open_value = open_value
        self._close_value = close_value


# class _Base(metaclass=ABCMeta):
#     _instrument: Instrument
#     _benchmark: Instrument
#     _open: pd.Timestamp

#     def __init__(self, instrument: Instrument, /, *, benchmark: Instrument) -> None:
#         raise NotImplementedError

#     @abstractproperty
#     def is_open(self) -> bool:
#         ...

#     @abstractproperty
#     def is_closed(self) -> bool:
#         ...

#     @property
#     def instrument(self) -> Instrument:
#         '''get the instrument'''
#         return self._instrument

#     @property
#     def benchmark(self) -> Instrument:
#         '''get the benchmark'''
#         return self._benchmark

#     def __str__(self) -> str:
#         return f'{type(self)}(instrument={self.instrument}, benchmark={self.benchmark}, ...)'

#     def __repr__(self) -> str:
#         return str(self)


# class Position(_Base):
#     '''
#     a 'Position' is an open 'Trade'
#     '''

#     _open_value: Decimal
#     _volume: Decimal

#     @property
#     def is_open(self) -> Literal[True]:
#         return True

#     @property
#     def is_closed(self) -> Literal[False]:
#         return False

#     @property
#     def volume(self) -> Decimal:
#         '''get the volume'''
#         return self._volume

#     @volume.setter
#     def volume(self, volume: Decimal) -> None:
#         '''set the volume'''
#         self._volume = volume

#     def close(self, close: pd.Timestamp) -> Trade:
#         '''close the position'''
#         return Trade(self.instrument, benchmark=self.benchmark)


# class Trade(_Base):
#     '''
#     a 'Trade' consists of two consecutive successful market orders
#     (one at the start, one at the end of the trade period).
#     It is implicitly assumed that after each 'Trade' the whole asset is liquidated
#     (and potentially reacquired (partially)).
#     '''

#     _close: pd.Timestamp

#     def __init__(self, instrument: Instrument, /, *, benchmark: Instrument) -> None:
#         super().__init__(instrument, benchmark=benchmark)
#         raise NotImplementedError

#     @property
#     def is_open(self) -> Literal[False]:
#         return False

#     @property
#     def is_closed(self) -> Literal[True]:
#         return True

#     @property
#     def open(self) -> pd.Timestamp:
#         return self._open

#     @property
#     def close(self) -> pd.Timestamp:
#         return self._close
