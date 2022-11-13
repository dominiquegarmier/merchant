from __future__ import annotations

from typing import Callable
from typing import TypeAlias

Ticker: TypeAlias = str


class SimulationTime:
    '''TODO move this to environment'''

    time: int
    start: int
    end: int

    def __init__(self, start: int, end: int):
        self.start = start
        self.end = end

    def set_time(self, time: int):
        self.time = time


class MarketOrder:
    pass


class MarketInterface:
    pass


class HistoricalMarket(MarketInterface):
    is_running: bool = False

    def __init__(self, time_keeper: SimulationTime) -> None:
        super().__init__()

    def get_quote(self, ticker: Ticker) -> float:
        pass

    def post_order(self, order: MarketOrder) -> None:
        pass

    def _set_time(self, timestamp: int) -> None:
        pass

    def __enter__(self) -> HistoricalMarket:
        self.is_running = True
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.is_running = False
