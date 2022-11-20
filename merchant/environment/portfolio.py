from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go

from merchant.data.tickers import CASH
from merchant.data.tickers import Ticker
from merchant.environment.market import HistoricalMarket


class Position:
    _market: HistoricalMarket | None

    ticker: Ticker
    quantity: float
    history: pd.DataFrame

    def __init__(
        self,
        ticker: Ticker,
        quantity: float,
        market: HistoricalMarket | None,
        *,
        _require_market: bool = True,
    ) -> None:
        self.ticker = ticker
        self.quantity = quantity

        if _require_market and market is None:
            raise ValueError('market must be provided')

        self.history = pd.DataFrame(
            columns=['quantity', 'unit_price'], index=pd.DatetimeIndex([])
        )

    def __repr__(self) -> str:
        return f'{self.ticker} {self.quantity}'

    @property
    def value(self) -> float:
        if self._market is None:
            raise ValueError('market not set')
        raise NotImplementedError


class CashPosition(Position):
    def __init__(self, quantity: float) -> None:
        super().__init__(CASH, quantity, None, _require_market=False)

    @property
    def value(self) -> float:
        return self.quantity


class Positions:
    _positions: dict[Ticker, Position] = {}
    _market: HistoricalMarket

    def __init__(self, market: HistoricalMarket) -> None:
        self._market = market

    def __getitem__(self, ticker: Ticker) -> Position:
        if ticker not in self._positions:
            self._positions[ticker] = Position(
                ticker=ticker, quantity=0, market=self._market
            )
        return self._positions[ticker]

    @property
    def value(self) -> float:
        return sum(position.value for position in self._positions.values())


class Portfolio:
    _market: HistoricalMarket

    _cash: CashPosition
    _positions: Positions

    _start_cash: float
    _history: pd.DataFrame  # value history
    _step_size: int = 1

    def __init__(self, start_cash: float, market: HistoricalMarket) -> None:
        self._start_cash = start_cash
        self._market = market
        self._cash = CashPosition(quantity=start_cash)

        self._positions = Positions(market=self._market)
        self._history = pd.DataFrame(columns=['open', 'high', 'low', 'close'])

    @property
    def value(self) -> float:
        return self._cash.value + self._positions.value

    @property
    def cash_value(self) -> float:
        return self._cash.value

    @property
    def positions_value(self) -> float:
        return self._positions.value

    @property
    def pl_ratio(self) -> float | None:
        if self._start_cash == 0:
            return None
        return (self.value - self._start_cash) / self._start_cash

    @property
    def volatility(self) -> float:
        '''
        returns:
            float: unormalized volatility
        '''
        return self._history['close'].pct_change().std()  # type: ignore

    @property
    def cagr(self) -> float:
        raise NotImplementedError

    @property
    def jensen_alpha(self) -> float:
        raise NotImplementedError

    @property
    def sharpe_ratio(self) -> float:
        raise NotImplementedError

    @property
    def calmar_ratio(self) -> float:
        raise NotImplementedError

    @property
    def sortino_ratio(self) -> float:
        raise NotImplementedError

    @property
    def treynor_ratio(self) -> float:
        raise NotImplementedError

    @property
    def max_drawdown_ratio(self) -> float:
        cum_max = self._history['high'].cummax()
        drawdown = cum_max - self._history.low
        drawdown_ratio = drawdown / cum_max
        return 1 - drawdown_ratio.max()  # type: ignore

    def __repr__(self) -> str:
        return 'Portfolio: ...'

    def get_plot(self, *args, **kwargs) -> go.Figure:
        return go.Figure(
            data=go.Ohlc(
                open=self._history['open'],
                high=self._history['high'],
                low=self._history['low'],
                close=self._history['close'],
            )
        )
