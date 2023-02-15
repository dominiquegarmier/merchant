from __future__ import annotations

import math
from datetime import datetime

import exchange_calendars as xcal
import mplfinance as mpf
import numpy as np
import pandas as pd

from merchant.data.dataset import Aggregates


def brownian_motion(n: int, n_samples: int = 1, sigma: int = 1) -> np.ndarray:
    '''
    generate n_samples standard brownian motion paths
    on the interval [0, 1] with n timesteps
    '''
    dt = 1 / (n - 1)
    diffs = np.sqrt(sigma) * np.random.normal(0, np.sqrt(dt), size=(n - 1, n_samples))
    return np.concatenate(
        (np.zeros(shape=(1, n_samples)), np.cumsum(diffs, axis=0)), axis=0
    )


def exp_brownian_motion(
    n: int, n_samples: int = 1, sigma: float = 1, mu: float = 0
) -> np.ndarray:
    dt = 1 / (n - 1)
    diffs = np.exp(
        (mu - sigma**2 / 2) * dt
        + sigma * np.random.normal(0, np.sqrt(dt), size=(n - 1, n_samples))
    )
    return np.concatenate(
        (np.ones(shape=(1, n_samples)), np.cumprod(diffs, axis=0)), axis=0
    )


SAMPLE_RATE = 10


def synthetic_intraday(
    initial_price: float = 100,
    date: datetime | str = datetime(2020, 1, 2),
    timeframe: Aggregates = Aggregates.MIN,
    sigma: float = 0.01,
    mu: float = 0,
) -> pd.DataFrame | None:
    if isinstance(date, str):
        date = datetime.strptime(date, '%Y-%m-%d')

    xnys = xcal.get_calendar('XNYS')  # NYSE
    try:
        open, _, _, close = xnys.schedule.loc[str(date.date())]
    except KeyError:
        return None
    duration = (close - open).seconds

    keep_last = False
    n_candles = math.ceil(duration / timeframe.value)
    if duration / timeframe.value < n_candles:
        keep_last = True

    n_samples = n_candles * SAMPLE_RATE + 1

    time_samples = np.linspace(open.value, close.value, n_samples)
    price_samples = initial_price * exp_brownian_motion(n_samples, sigma=sigma, mu=mu)

    df = pd.DataFrame(
        {
            'PRICE': price_samples.flatten(),
            'TIMESTAMP': time_samples,
        }
    )
    df['TIMESTAMP'] = df['TIMESTAMP'].astype('datetime64[ns]')
    df.set_index('TIMESTAMP', inplace=True)
    df = df['PRICE'].resample(f'{timeframe.value}S').ohlc()
    df.rename(
        columns={'open': 'OPEN', 'high': 'HIGH', 'low': 'LOW', 'close': 'CLOSE'},
        inplace=True,
    )
    if keep_last:
        return df
    return df[:-1]
