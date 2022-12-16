from __future__ import annotations

from merchant.trading.instrument import Instrument
from merchant.trading.pair import TradingPair


class TradingDirection:
    '''
    trading pair combined with a direction

    Parameters
    ----------

    pair: TradingPair
        the trading pair on wich the a trade is made

    reversed: bool
        if true it indicates the sell direction, if false it indicates the buy direction, defaults to False

    Methods
    -------

    __not__(self)
        create a new 'TradingDirection' with the opposite direction
    '''

    _pair: TradingPair
    _reversed: bool

    def __init__(self, pair: TradingPair, reversed: bool = False) -> None:
        if pair.is_virtual:
            raise TypeError(f'cannot create {type(self)}: pair is virtual')

        self._pair = pair
        self._reversed = reversed

    @property
    def pair(self) -> TradingPair:
        return self._pair

    @property
    def reversed(self) -> bool:
        return self._reversed

    @property
    def buy(self) -> Instrument:
        if self._reversed:
            return self._pair._sell  # type: ignore
        return self._pair._buy  # type: ignore

    @property
    def sell(self) -> Instrument:
        if self._reversed:
            return self._pair._buy  # type: ignore
        return self._pair._sell  # type: ignore

    def __contains__(self, __o: object) -> bool:
        if not isinstance(__o, Instrument):
            return False
        return __o in self._pair

    def __not__(self) -> TradingDirection:
        return self.__class__(self._pair, not self._reversed)

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, TradingDirection):
            return False
        if (self._pair, self._reversed) != (__o._pair, __o._reversed):
            return False
        return True

    def __hash__(self) -> int:
        return hash((self._pair, self._reversed))

    def __repr__(self) -> str:
        return f'{type(self)}(pair={self._pair!r}, reversed={self._reversed!r})'
