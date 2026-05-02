from __future__ import annotations

import pytest

from vortex.trade import OrderIntent, PaperBrokerAdapter, PaperBrokerConfig, Quote


def test_paper_broker_buy_and_sell_records_cash_positions_orders_and_fills() -> None:
    broker = PaperBrokerAdapter(PaperBrokerConfig(initial_cash=100_000, lot_size=100, min_commission=5))
    broker.update_quotes([Quote(symbol="000001.SZ", open_price=10.0, last_price=10.0, volume=100_000)])

    buy = broker.submit_order(OrderIntent(symbol="000001.SZ", side="buy", shares=1_000, limit_price=10.1))

    assert buy.status == "filled"
    assert buy.filled_shares == 1_000
    assert broker.get_positions()[0].shares == 1_000
    assert broker.get_cash().available_cash < 90_000
    assert len(broker.get_orders()) == 1
    assert len(broker.get_fills()) == 1

    broker.update_quotes([Quote(symbol="000001.SZ", open_price=11.0, last_price=11.0, volume=100_000)])
    sell = broker.submit_order(OrderIntent(symbol="000001.SZ", side="sell", shares=500, limit_price=10.9))

    assert sell.status == "filled"
    assert sell.filled_shares == 500
    assert broker.get_positions()[0].shares == 500
    assert len(broker.get_fills()) == 2


def test_paper_broker_rejects_invalid_lot_missing_quote_and_limit_up_buy() -> None:
    broker = PaperBrokerAdapter(PaperBrokerConfig(initial_cash=100_000))

    odd_lot = broker.submit_order(OrderIntent(symbol="000001.SZ", side="buy", shares=99, limit_price=10.0))
    assert odd_lot.status == "rejected"
    assert "multiple" in odd_lot.message

    missing_quote = broker.submit_order(OrderIntent(symbol="000001.SZ", side="buy", shares=100, limit_price=10.0))
    assert missing_quote.status == "rejected"
    assert missing_quote.message == "missing quote"

    broker.update_quotes([Quote(symbol="000001.SZ", open_price=10.0, is_limit_up=True)])
    limit_up = broker.submit_order(OrderIntent(symbol="000001.SZ", side="buy", shares=100, limit_price=10.0))
    assert limit_up.status == "rejected"
    assert limit_up.message == "limit-up buy blocked"


def test_paper_broker_rejects_suspended_and_limit_down_sell() -> None:
    broker = PaperBrokerAdapter(PaperBrokerConfig(initial_cash=100_000))
    broker.update_quotes([Quote(symbol="000001.SZ", open_price=10.0, volume=100_000)])
    broker.submit_order(OrderIntent(symbol="000001.SZ", side="buy", shares=1_000, limit_price=10.0))

    broker.update_quotes([Quote(symbol="000001.SZ", open_price=10.0, is_suspended=True)])
    suspended = broker.submit_order(OrderIntent(symbol="000001.SZ", side="sell", shares=100, limit_price=10.0))
    assert suspended.status == "rejected"
    assert suspended.message == "symbol suspended"

    broker.update_quotes([Quote(symbol="000001.SZ", open_price=10.0, is_limit_down=True)])
    limit_down = broker.submit_order(OrderIntent(symbol="000001.SZ", side="sell", shares=100, limit_price=10.0))
    assert limit_down.status == "rejected"
    assert limit_down.message == "limit-down sell blocked"


def test_paper_broker_partially_fills_by_liquidity_cap() -> None:
    broker = PaperBrokerAdapter(
        PaperBrokerConfig(initial_cash=100_000, max_participation_rate=0.05, min_commission=5)
    )
    broker.update_quotes([Quote(symbol="000001.SZ", open_price=10.0, volume=5_000)])

    order = broker.submit_order(OrderIntent(symbol="000001.SZ", side="buy", shares=1_000, limit_price=10.0))

    assert order.status == "partial"
    assert order.filled_shares == 200
    assert order.remaining_shares == 800
    assert broker.get_positions()[0].shares == 200


def test_paper_broker_rejects_when_trading_disabled_or_bad_config() -> None:
    broker = PaperBrokerAdapter(PaperBrokerConfig(initial_cash=100_000, allow_trading=False))
    order = broker.submit_order(OrderIntent(symbol="000001.SZ", side="buy", shares=100, limit_price=10.0))
    assert order.status == "rejected"
    assert order.message == "trading disabled"

    with pytest.raises(ValueError, match="initial_cash"):
        PaperBrokerAdapter(PaperBrokerConfig(initial_cash=0))
