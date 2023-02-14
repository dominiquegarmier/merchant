from __future__ import annotations

from decimal import Decimal
from logging import getLogger
from typing import overload

from merchant.trading.tools.asset import Asset
from merchant.trading.tools.pair import TradingPair
from merchant.trading.tools.pair import VirtualTradingPair


logger = getLogger(__name__)


class Instrument:
    '''
    An instrument is a unit that can be traded, in TradingPairs with other instruments
    '''

    _symbol: str
    _precision: int
    _description: str

    def __init__(
        self, symbol: str, precision: int, description: str | None = None
    ) -> None:
        self._symbol = symbol
        if precision < 0:
            raise ValueError('precision must be greater than 0')
        self._precision = precision
        self._description = description or ''

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, Instrument):
            return False
        if (self._symbol, self._precision) != (__o._symbol, __o._precision):
            if (
                all((self._description, __o._description))
                and self._description != __o._description
            ):
                logger.warning(
                    f'two instruments have the same symbol and precision but different descriptions: {self._symbol}'
                )
            return False
        return True

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def precision(self) -> int:
        return self._precision

    @property
    def description(self) -> str:
        return self._description

    @overload
    def __truediv__(self, __o: None) -> VirtualTradingPair:
        ...

    @overload
    def __truediv__(self, __o: Instrument) -> TradingPair:
        ...

    def __truediv__(self, __o: Instrument | None) -> TradingPair | VirtualTradingPair:
        '''allows you to create a 'TradingPair' with A / B'''
        if __o is None:
            return VirtualTradingPair(self, __o)
        return TradingPair(self, __o)

    @overload
    def __rtruediv__(self, __o: None) -> VirtualTradingPair:
        ...

    @overload
    def __rtruediv__(self, __o: Instrument) -> TradingPair:
        ...

    def __rtruediv__(self, __o: Instrument | None) -> TradingPair | VirtualTradingPair:
        '''allows you to create a 'TradingPair' with B / A'''
        if __o is None:
            return VirtualTradingPair(__o, self)
        return TradingPair(__o, self)

    def __mul__(self, __o: Decimal | int) -> Asset:
        '''allows you to create an 'Asset' with A * n'''
        if isinstance(__o, int):
            __o = Decimal(__o)
        return Asset(self, quantity=__o)

    def __rmul__(self, __o: Decimal | int) -> Asset:
        '''allows you to create an 'Asset' with n * A'''
        if isinstance(__o, int):
            __o = Decimal(__o)
        return self.__mul__(__o)

    def __hash__(self) -> int:
        return hash((type(self), self._symbol, self._precision))

    def __str__(self) -> str:
        return f'{type(self)}(symbol={self._symbol}, precision={self._precision})'

    def __repr__(self) -> str:
        return str(self)


# Currencies
USD = Instrument(symbol='USD', precision=2, description='US Dollar')
EUR = Instrument(symbol='EUR', precision=2, description='Euro')
GBP = Instrument(symbol='GBP', precision=2, description='British Pound')
CHF = Instrument(symbol='CHF', precision=2, description='Swiss Franc')

# Crypto
BTC = Instrument(symbol='BTC', precision=8, description='Bitcoin')
ETH = Instrument(symbol='ETH', precision=8, description='Ethereum')
