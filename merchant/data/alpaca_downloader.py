from __future__ import annotations

import os
from collections.abc import Generator
from datetime import datetime
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

from merchant.data.dataset import DatasetMetadata
from merchant.data.dataset import OHLCV_PARTITIONING
from merchant.data.dataset import OHLCV_SCHEMA


HISTORICAL_DATA_START_YEAR = 2020
DATA_BASE_PATH = Path(__file__).parent.parent.parent / 'data'
DEFAULT_TIMEFRAME = TimeFrame(1, TimeFrameUnit.Day)


def get_client() -> StockHistoricalDataClient:
    load_dotenv()
    return StockHistoricalDataClient(
        os.environ['ALPACA_API_KEY'], os.environ['ALPACA_API_SECRET'], raw_data=True
    )


def _check_ticker_exists(client: StockHistoricalDataClient, ticker: str) -> bool:
    request = StockBarsRequest(
        symbol_or_symbols=ticker,
        limit=1,
        timeframe=DEFAULT_TIMEFRAME,
        start=datetime(2000, 1, 1),
        end=datetime(2000, 1, 1),
    )

    try:
        _ = client.get_stock_bars(request)
    except APIError:
        return False
    return True


def _trades_download_iter(
    ticker: str, start: datetime, end: datetime
) -> Generator[pa.Table, None, None]:
    raise NotImplementedError()


def trades_dowloader() -> DatasetMetadata:
    raise NotImplementedError


def _parse_ohlcv_response(ticker: str, response: dict[str, Any]) -> pd.DataFrame | None:
    if response[ticker] is None:
        return None

    df = pd.DataFrame.from_records(response[ticker])
    df.rename(
        columns={
            't': 'timestamp',
            'o': 'open',
            'h': 'high',
            'l': 'low',
            'c': 'close',
            'v': 'volume',
            'n': 'trades',
            'vw': 'vw_price',
        },
        inplace=True,
    )
    df = df.astype(
        dtype={
            'timestamp': 'datetime64[ns]',
            'open': 'float64',
            'high': 'float64',
            'low': 'float64',
            'close': 'float64',
            'volume': 'int64',
            'trades': 'int64',
            'vw_price': 'float64',
        },
    )
    df['ticker'] = ticker
    return df


def _ohlcv_download_years_iter(
    client: StockHistoricalDataClient,
    tickers: list[str],
    years: list[int],
) -> Generator[pa.RecordBatch, None, None]:
    for ticker in tqdm(tickers, desc='downloading ...', position=1):
        for year in tqdm(years, desc=f'{ticker}...', position=0):

            request = StockBarsRequest(
                symbol_or_symbols=ticker,
                start=datetime(year, 1, 1),
                end=datetime(year, 12, 31),
                timeframe=DEFAULT_TIMEFRAME,
                adjustment=Adjustment.ALL,
                limit=None,
                feed=DataFeed.IEX,
                asof=datetime.now(),  # use the current ticker names
            )

            response: dict[str, Any] = cast(
                dict[str, Any], client.get_stock_bars(request)
            )
            data = _parse_ohlcv_response(ticker, response=response)
            # skip if no data was returned
            if data is None:
                continue
            yield pa.RecordBatch.from_pandas(data, schema=OHLCV_SCHEMA)


def ohlcv_downloader(dataset_name: str) -> DatasetMetadata:
    client = get_client()

    years = [year for year in range(HISTORICAL_DATA_START_YEAR, 2023)]
    tables_iter = _ohlcv_download_years_iter(client, ['GSL-B'], years)

    path = DATA_BASE_PATH / dataset_name
    ds.write_dataset(
        data=tables_iter,
        base_dir=path,
        format='parquet',
        partitioning=OHLCV_PARTITIONING,
        existing_data_behavior='error',
        create_dir=True,
        schema=OHLCV_SCHEMA,
    )

    return DatasetMetadata(
        path=path,
        schema=OHLCV_SCHEMA,
        partitioning=OHLCV_PARTITIONING,
    )


if __name__ == '__main__':
    client = get_client()
    res = _check_ticker_exists(client, 'GSL.B')
    print(res)
