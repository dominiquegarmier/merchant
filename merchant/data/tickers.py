from __future__ import annotations

from collections.abc import Generator
from datetime import datetime
from pathlib import Path
from typing import Literal

import httpx
import pandas as pd
from tqdm import tqdm

BASE_WIKIPEADIA_URL = 'https://en.wikipedia.org/wiki'
SNP_500_COMPANIES_URL = f'{BASE_WIKIPEADIA_URL}/List_of_S%26P_500_companies'

NASDAQ_DATALINK_URL = 'https://api.nasdaq.com/api/screener'


def get_snp500_tickers(historical: bool = True) -> list[str]:
    '''
    scrapes the wikipedia page for the S&P 500 companies
    and returns a list of the tickers

    if historical is True, the list will include the tickers
    of companies that have been removed from the S&P 500
    '''
    tables = pd.read_html(SNP_500_COMPANIES_URL)
    tickers = set()
    tickers |= set(tables[0]['Symbol'].to_list())

    # add removed tickers from second table
    if historical:
        tickers |= set(tables[1]['Removed']['Ticker'].to_list())
    return list(tickers)


def nasdaq_stock_screener(
    asset_type: Literal['stocks', 'etf'],
    exchange: Literal['NASDAQ', 'NYSE', 'AMEX'] | None,
) -> Generator[tuple[str, str], None, None]:
    '''
    downlaod tickers from NASDAQ stock-screener for NASDAQ, NYSE, and AMEX stocks and etfs
    '''
    client = httpx.Client()
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/109.0'
    }
    params = {
        'tableonly': 'true',
        'offset': '0',
        'download': 'true',
    }
    if exchange is not None:
        params['exchange'] = exchange

    resp = client.get(
        f'{NASDAQ_DATALINK_URL}/{asset_type}',
        headers=headers,
        params=params,
    ).json()

    if asset_type == 'stocks':
        data = resp['data']['rows']
    elif asset_type == 'etf':
        data = resp['data']['data']['rows']

    for row in data:
        yield row['symbol'], row['name' if asset_type == 'stocks' else 'companyName']


def download_tickers(etfs: bool = True) -> pd.DataFrame:
    '''
    downloads all tickers found on the NASDAQ stock-screener
    includes stocks and etfs
    '''
    tickers: list[str] = []
    names: list[str] = []

    def _append_tickers(
        asset_type: Literal['stocks', 'etf'],
        exchange: Literal['NASDAQ', 'NYSE', 'AMEX'] | None,
    ) -> None:
        for ticker, name in nasdaq_stock_screener(asset_type, exchange):
            ticker = ''.join(c if c.isalnum() else '.' for c in ticker).strip('.')
            if exchange is not None:
                ticker = f'{exchange}:{ticker}'
            if asset_type == 'etf':
                ticker = f'ETF:{ticker}'
            tickers.append(ticker)
            names.append(name)

    base_message = 'downloading tickers'
    with tqdm(total=4, desc=f'{base_message}...') as pbar:
        for exchange in ['NASDAQ', 'NYSE', 'AMEX']:
            pbar.set_description(f'{base_message} ({exchange})...')
            _append_tickers('stocks', exchange)  # type: ignore
            pbar.update(1)
        if etfs:
            pbar.set_description(f'{base_message} (ETFs)...')
            _append_tickers('etf', None)
        pbar.update(1)

    return pd.DataFrame({'ticker': tickers, 'name': names})


TICKER_CACHE_PATH = Path(__file__).parent.parent.parent / 'data/tickers/US_EQUITIES.csv'


def get_tickers(cached: bool = True) -> pd.DataFrame:
    if not cached or not TICKER_CACHE_PATH.exists():
        tickers = download_tickers()
        tickers.to_csv(TICKER_CACHE_PATH, index=False)
    return pd.read_csv(TICKER_CACHE_PATH)
