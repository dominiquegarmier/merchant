from __future__ import annotations

import yfinance as yf
import plotly.graph_objects as go


def plot_example() -> None:
    msft = yf.Ticker("MSFT")
    data = msft.history(period="2y")

    fig = go.Figure(
        data=go.Ohlc(
            open=data['Open'],
            high=data['High'],
            low=data['Low'],
            close=data['Close'],
        )
    )
    fig.show()
