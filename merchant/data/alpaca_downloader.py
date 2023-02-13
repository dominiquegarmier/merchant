from __future__ import annotations

import os
from collections.abc import Generator
from collections.abc import Iterable
from datetime import datetime
from logging import getLogger
from pathlib import Path
from typing import Any
from typing import cast

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from alpaca.common.exceptions import APIError
from alpaca.data.enums import Adjustment
from alpaca.data.enums import DataFeed
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.timeframe import TimeFrameUnit
from dotenv import load_dotenv
from tqdm import tqdm

from merchant.data.dataset import DatasetSpec
from merchant.data.dataset import OHLCV_PARTITIONING
from merchant.data.dataset import OHLCV_SCHEMA
from merchant.data.tickers import get_tickers


logger = getLogger(__name__)


HISTORICAL_DATA_START_YEAR = 2020
DATA_BASE_PATH = Path(__file__).parent.parent.parent / 'data'
DEFAULT_TIMEFRAME = TimeFrame(1, TimeFrameUnit.Minute)

ALPACA_BARS_REMAP = {
    't': 'TIMESTAMP',
    'o': 'OPEN',
    'h': 'HIGH',
    'l': 'LOW',
    'c': 'CLOSE',
    'v': 'VOLUME',
    'n': 'TRADES',
    'vw': 'VW_PRICE',
}

PANDAS_AGGS_DTYPES = {
    'TIMESTAMP': 'datetime64[ns]',
    'OPEN': 'float64',
    'HIGH': 'float64',
    'LOW': 'float64',
    'CLOSE': 'float64',
    'VOLUME': 'int64',
    'TRADES': 'int64',
    'VW_PRICE': 'float64',
}


def get_client() -> StockHistoricalDataClient:
    load_dotenv()
    return StockHistoricalDataClient(
        os.environ['ALPACA_API_KEY'], os.environ['ALPACA_API_SECRET'], raw_data=True
    )


def _trades_download_iter(
    ticker: str, start: datetime, end: datetime
) -> Generator[pa.Table, None, None]:
    raise NotImplementedError()


def trades_dowloader() -> DatasetSpec:
    raise NotImplementedError


def _to_alpaca_ticker(ticker: str) -> str:
    if ':' in ticker:
        _, ticker = ticker.split(':')
    return ticker.upper()


_TICKERS_BATCH_SIZE = 256


def supported_tickers(
    client: StockHistoricalDataClient, tickers: list[str]
) -> list[str]:
    ret: list[str] = []
    for batch_idx in tqdm(
        range(0, len(tickers), _TICKERS_BATCH_SIZE), desc='checking tickers ...'
    ):
        ret += _check_tickers_batch(
            client, list(tickers)[batch_idx : batch_idx + _TICKERS_BATCH_SIZE]
        )
    return ret


def _check_tickers_batch(
    client: StockHistoricalDataClient, tickers: list[str]
) -> list[str]:
    if len(tickers) > _TICKERS_BATCH_SIZE:
        raise ValueError('too many tickers, max {_TICKERS_BATCH_SIZE}')
    alpaca_tickers: dict[str, str] = {
        _to_alpaca_ticker(ticker): ticker for ticker in tickers
    }
    for a_tkr, tkr in alpaca_tickers.items():
        if not a_tkr.replace('.', '').isalnum():
            raise ValueError(
                f'Invalid ticker: {a_tkr} ({tkr}), must be alphanumeric (with exceptions of ".")'
            )
    request = StockBarsRequest(
        symbol_or_symbols=list(alpaca_tickers.keys()),
        limit=2
        * len(
            alpaca_tickers
        ),  # 2 bars per ticker, incase this runs at the beginning of a year
        timeframe=TimeFrame(12, TimeFrameUnit.Month),
        start=datetime(datetime.now().year - 1, 1, 1),
    )
    try:
        res = cast(dict[str, str], client.get_stock_bars(request))
    except APIError:
        raise
    return [alpaca_tickers[ticker] for ticker in res.keys() if res[ticker] != [None]]


def _parse_ohlcv_response(ticker: str, response: dict[str, Any]) -> pd.DataFrame | None:
    alpaca_ticker = _to_alpaca_ticker(ticker)
    if response[alpaca_ticker] == [None]:
        return None

    df = pd.DataFrame.from_records(response[alpaca_ticker])
    df.rename(
        columns=ALPACA_BARS_REMAP,
        inplace=True,
    )
    df = df.astype(dtype=PANDAS_AGGS_DTYPES)
    df['TICKER'] = ticker
    return df


def _year_request(year: int, ticker: str) -> StockBarsRequest:
    return StockBarsRequest(
        symbol_or_symbols=_to_alpaca_ticker(ticker),
        start=datetime(year, 1, 1),
        end=datetime(year, 12, 31),
        timeframe=DEFAULT_TIMEFRAME,
        adjustment=Adjustment.ALL,
        limit=None,
        feed=DataFeed.IEX,
        asof=datetime.now(),  # use the current ticker names
    )


def _ohlcv_download_years_iter(
    client: StockHistoricalDataClient,
    tickers: Iterable[str],
    years: Iterable[int],
) -> Generator[pa.RecordBatch, None, None]:
    for ticker in tqdm(tickers, desc='downloading ...', position=1):
        for year in tqdm(years, desc=f'{ticker}...', position=0, leave=False):
            try:
                response: dict[str, Any] = cast(
                    dict[str, Any], client.get_stock_bars(_year_request(year, ticker))
                )
            except APIError as e:
                logger.warning(f'skipping {ticker} ({e}) ...')
                continue
            data = _parse_ohlcv_response(ticker, response=response)
            # skip if no data was returned
            if data is None:
                continue
            yield pa.RecordBatch.from_pandas(data, schema=OHLCV_SCHEMA)


def ohlcv_downloader(
    dataset_name: str, start_year: int = HISTORICAL_DATA_START_YEAR
) -> DatasetSpec:
    path = DATA_BASE_PATH / 'datasets' / dataset_name

    client = get_client()
    all_tickers = get_tickers()['ticker'].to_list()
    tickers = supported_tickers(client, all_tickers)

    ds.write_dataset(
        data=_ohlcv_download_years_iter(
            client, tickers, range(start_year, datetime.now().year)
        ),
        base_dir=path,
        format='parquet',
        partitioning=OHLCV_PARTITIONING,
        existing_data_behavior='error',
        create_dir=True,
        schema=OHLCV_SCHEMA,
    )

    return DatasetSpec(
        path=path,
        schema=OHLCV_SCHEMA,
        partitioning=OHLCV_PARTITIONING,
    )
