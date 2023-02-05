from __future__ import annotations

from decimal import Decimal

from merchant.trading import Asset
from merchant.trading import Instrument
from merchant.trading import TradingPair
from merchant.trading import VirtualTradingPair


def test_instrument():
    A = Instrument('A', precision=6, description='A desc')

    # properties
    assert A.symbol == 'A'
    assert A.precision == 6
    assert A.description == 'A desc'

    # comparison
    assert A == Instrument('A', precision=6)
    assert A != Instrument('B', precision=6)
    assert A != Instrument('A', precision=2)


def test_asset_operations():
    BTC = Instrument('BTC', precision=6, description='Bitcoin')
    asset = Decimal(8) * BTC

    assert isinstance(asset, Asset)
    assert asset.instrument == BTC
    assert asset.quantity == Decimal(8)
    assert float(asset) == 8.0

    # comparison
    assert asset == Decimal(8) * BTC
    assert asset > Decimal(7) * BTC
    assert asset < Decimal(9) * BTC

    # arithmetic
    assert asset + Decimal(2) * BTC == Decimal(10) * BTC
    assert asset - Decimal(2) * BTC == Decimal(6) * BTC
    assert asset * Decimal(2) == Decimal(16) * BTC
    assert asset / Decimal(2) == Decimal(4) * BTC


def test_trading_pair():
    BTC = Instrument('BTC', precision=6, description='Bitcoin')
    USD = Instrument('USD', precision=2, description='US Dollar')

    pair = BTC / USD
    assert isinstance(pair, TradingPair)
    assert pair.buy == BTC
    assert pair.sell == USD

    assert USD in pair
    assert BTC in pair

    assert hash(pair) == hash(BTC / USD)
    assert pair == BTC / USD

    assert ~pair == USD / BTC


def test_virtual_trading_pair():
    BTC = Instrument('BTC', precision=6, description='Bitcoin')

    p = BTC / None
    assert isinstance(p, VirtualTradingPair)
    assert p.buy == BTC
    assert p.sell is None

    q = None / BTC
    assert isinstance(q, VirtualTradingPair)
    assert q.buy is None
    assert q.sell == BTC
