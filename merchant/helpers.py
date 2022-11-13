from __future__ import annotations


class Ticker:
    _ticker: str
    _exchange: str

    _instances: dict[tuple[str, str], Ticker] = {}

    def __init__(self, ticker: str, exchange: str) -> None:
        self._ticker = ticker
        self._exchange = exchange

    def __new__(cls: type[Ticker], ticker: str, exchange: str) -> Ticker:
        if (ticker, exchange) not in cls._instances:
            cls._instances[(ticker, exchange)] = super().__new__(cls)
        return cls._instances[(ticker, exchange)]

    def __str__(self) -> str:
        return self._ticker

    def __repr__(self) -> str:
        return f'{self._exchange}_{self._ticker}'

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Ticker):
            return self._ticker == other._ticker and self._exchange == other._exchange
        return False

    def __hash__(self) -> int:
        return hash((self._ticker, self._exchange))

    @property
    def ticker(self) -> str:
        return self._ticker

    @property
    def exchange(self) -> str:
        return self._exchange
