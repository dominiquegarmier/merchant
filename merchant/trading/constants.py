from __future__ import annotations

from datetime import datetime


_now = datetime.now()
PERIODS_DAYS = {
    'mtd': (_now - datetime(_now.year, _now.month, 1)).days,
    'qtd': (
        _now
        - datetime(
            _now.year,
            1
            if _now.month < 4
            else 4
            if _now.month < 7
            else 7
            if _now.month < 10
            else 10,
            1,
        )
    ).days,
    'ytd': (_now - datetime(_now.year, 1, 1)).days,
    '3m': 3 * 21,
    '6m': 6 * 21,
    '1y': 12 * 21,
    '3y': 3 * 12 * 21,
    '5y': 5 * 12 * 21,
    '10y': 10 * 12 * 21,
}

PERIODS_SECONDS = {
    'mtd': PERIODS_DAYS['mtd'] * 24 * 60 * 60,
    'qtd': PERIODS_DAYS['qtd'] * 24 * 60 * 60,
    'ytd': PERIODS_DAYS['ytd'] * 24 * 60 * 60,
    '3m': PERIODS_DAYS['3m'] * 24 * 60 * 60,
    '6m': PERIODS_DAYS['6m'] * 24 * 60 * 60,
    '1y': PERIODS_DAYS['1y'] * 24 * 60 * 60,
    '3y': PERIODS_DAYS['3y'] * 24 * 60 * 60,
    '5y': PERIODS_DAYS['5y'] * 24 * 60 * 60,
    '10y': PERIODS_DAYS['10y'] * 24 * 60 * 60,
    '1min': 60,
    '5min': 5 * 60,
    '15min': 15 * 60,
    '1h': 60 * 60,
    '1d': 24 * 60 * 60,
    '1w': 5 * 24 * 60 * 60,
    '1m': 21 * 24 * 60 * 60,
}
