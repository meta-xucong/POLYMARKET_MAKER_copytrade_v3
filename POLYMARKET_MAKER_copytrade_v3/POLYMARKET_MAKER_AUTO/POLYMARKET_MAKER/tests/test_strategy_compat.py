"""Compatibility tests for strategy callbacks used in the run loop."""

from Volatility_arbitrage_run import _strategy_accepts_total_position


class _OldStrategy:
    def __init__(self) -> None:
        self.calls = []

    def on_buy_filled(self, avg_price, size=None):  # pragma: no cover - signature only
        self.calls.append((avg_price, size))


class _NewStrategy:
    def __init__(self) -> None:
        self.calls = []

    def on_buy_filled(  # pragma: no cover - signature only
        self, avg_price, size=None, *, total_position=None
    ):
        self.calls.append((avg_price, size, total_position))


def test_strategy_without_total_position_is_detected() -> None:
    strategy = _OldStrategy()
    assert not _strategy_accepts_total_position(strategy)

    kwargs = {"avg_price": 0.66, "size": 10.0}
    if _strategy_accepts_total_position(strategy):
        kwargs["total_position"] = 10.0

    strategy.on_buy_filled(**kwargs)
    assert strategy.calls == [(0.66, 10.0)]


def test_strategy_with_total_position_is_detected() -> None:
    strategy = _NewStrategy()
    assert _strategy_accepts_total_position(strategy)

    kwargs = {"avg_price": 0.66, "size": 10.0}
    if _strategy_accepts_total_position(strategy):
        kwargs["total_position"] = 10.0

    strategy.on_buy_filled(**kwargs)
    assert strategy.calls == [(0.66, 10.0, 10.0)]
