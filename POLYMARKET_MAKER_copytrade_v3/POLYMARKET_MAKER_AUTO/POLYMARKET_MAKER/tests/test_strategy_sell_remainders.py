import pytest

from Volatility_arbitrage_strategy import ActionType, StrategyConfig, VolArbStrategy


def test_on_sell_filled_treats_dust_as_flat():
    cfg = StrategyConfig(token_id="T", min_market_order_size=5.0)
    strategy = VolArbStrategy(cfg)

    strategy.on_buy_filled(avg_price=0.5, size=10.0)
    strategy.on_sell_filled(avg_price=0.55, size=9.0, remaining=4.0)

    status = strategy.status()
    assert status["state"] == "FLAT"
    assert status["awaiting"] is None
    assert status["position_size"] is None


def test_on_sell_filled_marks_remaining_sell_pending():
    cfg = StrategyConfig(token_id="T", min_market_order_size=5.0)
    strategy = VolArbStrategy(cfg)

    strategy.on_buy_filled(avg_price=0.5, size=10.0)
    strategy.on_sell_filled(avg_price=0.6, size=4.0, remaining=6.0)

    status = strategy.status()
    assert status["state"] == "LONG"
    assert status["awaiting"] == ActionType.SELL
    assert status["position_size"] == pytest.approx(6.0)


def test_mark_awaiting_allows_external_sell_flag():
    cfg = StrategyConfig(token_id="T")
    strategy = VolArbStrategy(cfg)

    strategy.on_buy_filled(avg_price=0.5, size=10.0)
    strategy.mark_awaiting(ActionType.SELL)

    status = strategy.status()
    assert status["state"] == "LONG"
    assert status["awaiting"] == ActionType.SELL
