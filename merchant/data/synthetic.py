from __future__ import annotations

import math
from datetime import datetime

import exchange_calendars as xcals
import mplfinance as mpf
import numpy as np
import pandas as pd

from merchant.data.constants import Aggregates


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


_SYNTHETIC_SAMPLE_RATE = 10


def synthetic_intraday(
    initial_price: float = 100,
    date: datetime | str = datetime(2020, 1, 2),
    timeframe: Aggregates = Aggregates.MIN,
    mean_volume: int = 1_000_000_000,
    mean_trade_size: int = 100,
    sigma: float = 0.01,
    mu: float = 0,
) -> pd.DataFrame | None:
    if isinstance(date, str):
        date = datetime.strptime(date, '%Y-%m-%d')

    xnys = xcals.get_calendar('XNYS')  # NYSE
    try:
        open, _, _, close = xnys.schedule.loc[str(date.date())]
    except KeyError:
        return None
    duration = (close - open).seconds

    keep_last = False
    n_candles = math.ceil(duration / timeframe.value)
    if duration / timeframe.value < n_candles:
        keep_last = True

    n_samples = n_candles * _SYNTHETIC_SAMPLE_RATE + 1

    time_samples = np.linspace(open.value, close.value, n_samples)
    price_samples = initial_price * exp_brownian_motion(n_samples, sigma=sigma, mu=mu)

    volume_samples = np.random.poisson(
        mean_volume / (n_samples - 1), size=(n_samples - 1,)
    )
    trades_samples = np.round(
        volume_samples / np.random.poisson(mean_trade_size, size=(n_samples - 1,))
    )

    volume_chunks = volume_samples.reshape(-1, _SYNTHETIC_SAMPLE_RATE)
    trades_chunks = trades_samples.reshape(-1, _SYNTHETIC_SAMPLE_RATE)
    price_chunks = price_samples[:-1].reshape(-1, _SYNTHETIC_SAMPLE_RATE)

    volume = volume_chunks.sum(axis=1)
    trades = trades_chunks.sum(axis=1)
    vw_price = np.average(price_chunks * volume_chunks, axis=1) / volume

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
    if not keep_last:
        df = df[:-1]
    df['VOLUME'] = volume
    df['TRADES'] = trades.astype(int)
    df['VW_PRICE'] = vw_price
    return df
