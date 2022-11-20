from __future__ import annotations

from typing import TypeAlias

import pandas as pd

TSEmbedding: TypeAlias = tuple[float, float, float, float, float]


def timestamp_to_embedding(ts: int) -> TSEmbedding:
    """Converts a timestamp to an embedding.

    Args:
        ts: timestamp in nanoseconds

    Returns:
        minute of hour, hour of day, day of week, day of month, month of year
    """

    date = pd.to_datetime(ts, unit='ns')

    return (
        date.minute / 60,
        date.hour / 24,
        date.dayofweek / 7,
        date.day / date.daysinmonth,
        date.month / 12,
    )
