from __future__ import annotations

import timeit
from datetime import datetime
from typing import Any

import httpx
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockTradesRequest


API_KEY = ''
API_SECRET = ''
BASE_URL = 'https://data.alpaca.markets/v2/stocks'
TICKER = 'AAPL'


def slow_processing(trades: list[dict[str, Any]]) -> None:
    for trade in trades:
        trade['timestamp'] = trade.pop('t')


def one_request(limit: int = 100) -> None:
    client = httpx.Client()
    headers = httpx.Headers(
        {'APCA-API-KEY-ID': API_KEY, 'APCA-API-SECRET-KEY': API_SECRET}
    )
    params: dict[str, int | str] = {
        'start': '2021-01-01',
        'end': '2022-01-01',
        'limit': limit,
    }
    res = client.get(f'{BASE_URL}/{TICKER}/trades', params=params, headers=headers)
    data = res.json()
    slow_processing(data['trades'])


def alpaca_sdk(limit: int = 100) -> None:
    client = StockHistoricalDataClient(API_KEY, API_SECRET, raw_data=True)
    request = StockTradesRequest(
        symbol_or_symbols='AAPL',
        start=datetime(2021, 1, 5),
        end=datetime(2021, 1, 7),
        limit=limit,
    )
    data = client.get_stock_trades(request)
    print(len(data['AAPL']))


def main() -> int:
    NUM = 1
    LIMIT = 1000000

    t = timeit.timeit(lambda: alpaca_sdk(LIMIT), number=NUM)
    print(f'{t / NUM:.3f} seconds per execution of {LIMIT} items')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
