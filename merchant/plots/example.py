from __future__ import annotations

import mplfinance as mplf
import yfinance as yf


MSFT = yf.Ticker('MSFT').history(period='1y')

mplf.plot(MSFT, type='candle', volume=True, mav=(3, 6, 9))
