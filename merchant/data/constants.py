from __future__ import annotations

from enum import Enum

# UTP sale conditions
SALE_CONDITIONS = {
    '@': 'Regular Trade',
    'A': 'Acquisition',
    'B': 'Bunched Trade',
    'C': 'Cash Sale',
    'D': 'Distribution',
    'E': 'Placeholder',
    'F': 'Intermarket Sweep',
    'G': 'Bunched Sold Trade',
    'H': 'Price Variation Trade',
    'I': 'Odd Lot Trade',
    'K': 'Rule 155 Trade (AMEX)',
    'L': 'Sold Last',
    'M': 'Market Center Official Close',
    'N': 'Next Day',
    'O': 'Opening Prints',
    'P': 'Prior Reference Price',
    'Q': 'Market Center Official Open',
    'R': 'Seller',
    'S': 'Split Trade',
    'T': 'Form T',
    'U': 'Extended trading hours (Sold Out of Sequence)',
    'V': 'Contingent Trade',
    'W': 'Average Price Trade',
    'X': 'Cross/Periodic Auction Trade',
    'Y': 'Yellow Flag Regular Trade',
    'Z': 'Sold (out of sequence)',
    '1': 'Stopped Stock (Regular Trade)',
    '4': 'Derivatively priced',
    '5': 'Re-Opening Prints',
    '6': 'Closing Prints',
    '7': 'Qualified Contingent Trade (“QCT”)',
    '8': 'Placeholder For 611 Exempt',
    '9': 'Corrected Consolidated Close (per listing market)',
}

# Correction satus https://alpaca.markets/docs/api-references/market-data-api/stock-pricing-data/historical/#trade
CORRECTION_STATUS = {
    'normal': 'normal trade',
    'corrected': 'trade was corrected',
    'cancelled': 'trade was cancelled',
    'incorrect': 'trade was incorrectly reported',
}


class Aggregates(Enum):
    SEC = 1
    MIN = 60
    HOUR = 60 * 60
    DAY = 60 * 60 * 24
