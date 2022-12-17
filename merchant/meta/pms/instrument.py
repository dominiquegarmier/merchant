from __future__ import annotations

from logging import getLogger

from merchant.meta.oms.direction import TradingDirection
from merchant.meta.pms.pair import TradingPair


logger = getLogger(__name__)


class Instrument:
    '''An instrument is a unit that can be traded, in TradingPairs with other instruments'''

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
            if self._description != __o._description:
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

    def __truediv__(self, __o: Instrument | None) -> TradingPair:
        '''allows you to create a 'TradingPair' with A / B'''
        return TradingPair(self, __o)

    def __rtruediv__(self, __o: Instrument | None) -> TradingPair:
        return TradingPair(__o, self)

    def __gt__(self, __o: Instrument) -> TradingDirection:
        '''allows you to create a 'TradingDirection' with A > B'''
        return TradingDirection(self / __o)

    def __lt__(self, __o: Instrument) -> TradingDirection:
        '''allows you to create a reversed 'TradingDirection' with A < B'''
        return TradingDirection(self / __o, reversed=True)

    def __hash__(self) -> int:
        return hash((self._symbol, self._precision))

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
