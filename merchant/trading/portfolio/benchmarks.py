from __future__ import annotations

from collections.abc import Sequence

import pandas as pd

from merchant.trading.portfolio.trade import Trade


def _get_winning_trades(positions: Sequence[Trade]) -> list[Trade]:
    raise NotImplementedError


def _get_losing_trades(positions: Sequence[Trade]) -> list[Trade]:
    raise NotImplementedError


def volatility(value: pd.Series, _period: str | int = 'ytd') -> float:
    raise NotImplementedError


def rolling_valatility(value: pd.Series, _window: str | int = '1d') -> float:
    raise NotImplementedError


def sharpe_ratio(value: pd.Series, _period: str | int = 'ytd') -> float:
    raise NotImplementedError


def rolling_sharpe_ratio(value: pd.Series, _window: str | int = '1d') -> float:
    raise NotImplementedError


def sortino_ratio(value: pd.Series, _period: str | int = 'ytd') -> float:
    raise NotImplementedError


def rolling_sortino_ratio(value: pd.Series, _window: str | int = '1d') -> float:
    raise NotImplementedError


def beta(value: pd.Series, _period: str | int = 'ytd') -> float:
    raise NotImplementedError


def rolling_beta(value: pd.Series, _window: str | int = '1d') -> float:
    raise NotImplementedError


def max_drawdown_ratio(value: pd.Series, _period: str | int = 'ytd') -> float:
    raise NotImplementedError


def gain_to_pain_ratio(value: pd.Series, _period: str | int = 'ytd') -> float:
    raise NotImplementedError


def jensen_alpha(value: pd.Series, _period: str | int = 'ytd') -> float:
    raise NotImplementedError


def calmar_ratio(value: pd.Series, _period: str | int = 'ytd') -> float:
    raise NotImplementedError


def kelly_criterion(value: pd.Series, _period: str | int = 'ytd') -> float:
    raise NotImplementedError


def tracking_error(value: pd.Series, _period: str | int = 'ytd') -> float:
    raise NotImplementedError


def information_ratio(value: pd.Series, _period: str | int = 'ytd') -> float:
    raise NotImplementedError
