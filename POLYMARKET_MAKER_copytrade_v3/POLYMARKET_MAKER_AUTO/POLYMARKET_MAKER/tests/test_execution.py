from collections import deque
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from enum import Enum

import pytest

from trading.execution import (
    ClobPolymarketAPI,
    ExecutionConfig,
    ExecutionEngine,
    ExecutionResult,
)


class FakeClock:
    def __init__(self) -> None:
        self._now = 0.0

    def now(self) -> float:
        return self._now

    def sleep(self, seconds: float) -> None:
        self._now += seconds


class MockAPI:
    def __init__(self, status_sequences=None, create_exceptions=None):
        self.create_calls = []
        self._templates = status_sequences or []
        self._sequences = []
        self._last_status = []
        self._create_exceptions = create_exceptions or {}

    def create_order(self, payload):
        order_id = str(len(self.create_calls))
        self.create_calls.append(payload)
        exc_factory = self._create_exceptions.get(len(self.create_calls) - 1)
        if exc_factory is not None:
            if isinstance(exc_factory, Exception):
                raise exc_factory
            raise exc_factory()
        template = self._templates[len(self._sequences)] if len(self._sequences) < len(self._templates) else []
        seq = deque(template)
        self._sequences.append(seq)
        last_status = template[-1] if template else {"status": "OPEN", "filledAmount": 0.0}
        self._last_status.append(last_status)
        return {"orderId": order_id}

    def get_order_status(self, order_id):
        idx = int(order_id)
        seq = self._sequences[idx]
        if seq:
            status = seq.popleft()
            self._last_status[idx] = status
            return status
        return self._last_status[idx]


def build_engine(config: ExecutionConfig, mock_api: MockAPI):
    clock = FakeClock()
    engine = ExecutionEngine(mock_api, config, clock=clock.now, sleep=clock.sleep)
    return engine, mock_api


def test_batch_scheduler_splits_quantity():
    config = ExecutionConfig(
        order_slice_min=1.0,
        order_slice_max=2.0,
        retry_attempts=0,
        wait_seconds=1.0,
        poll_interval_seconds=0.2,
        order_interval_seconds=0.0,
    )
    statuses = [
        [{"status": "FILLED", "filledAmount": 2.0}],
        [{"status": "FILLED", "filledAmount": 2.0}],
        [{"status": "FILLED", "filledAmount": 1.3}],
    ]
    mock_api = MockAPI(statuses)
    engine, mock = build_engine(config, mock_api)

    result = engine.execute_sell("token", price=0.6, quantity=5.3)

    assert isinstance(result, ExecutionResult)
    assert result.filled == pytest.approx(5.3)
    sizes = [call["size"] for call in mock.create_calls]
    assert len(sizes) == 3
    assert sizes[0] == pytest.approx(2.0)
    assert sizes[1] == pytest.approx(2.0)
    assert sizes[2] == pytest.approx(1.3)


def test_partial_fill_triggers_retry_and_allows_partial():
    config = ExecutionConfig(
        order_slice_min=1.0,
        order_slice_max=2.0,
        retry_attempts=2,
        wait_seconds=1.0,
        poll_interval_seconds=0.2,
        order_interval_seconds=0.0,
    )
    statuses = [
        [
            {"status": "OPEN", "filledAmount": 0.0},
            {"status": "PARTIAL", "filledAmount": 1.0},
        ],
        [{"status": "FILLED", "filledAmount": 2.0}],
    ]
    mock_api = MockAPI(statuses)
    engine, mock = build_engine(config, mock_api)

    result = engine.execute_sell("token", price=0.7, quantity=3.0)

    assert result.filled == pytest.approx(3.0)
    assert result.status == "FILLED"
    assert len(mock.create_calls) >= 2
    first_payload = mock.create_calls[0]
    assert first_payload["type"] == "GTC"
    assert first_payload["timeInForce"] == "GTC"
    assert first_payload["allowPartial"] is True
    assert first_payload["tokenId"] == "token"


def test_price_adjustment_after_timeout():
    config = ExecutionConfig(
        order_slice_min=0.5,
        order_slice_max=1.0,
        retry_attempts=1,
        price_tolerance_step=0.1,
        wait_seconds=1.0,
        poll_interval_seconds=0.2,
        order_interval_seconds=0.0,
    )
    statuses = [
        [{"status": "OPEN", "filledAmount": 0.0}],
        [{"status": "FILLED", "filledAmount": 1.0}],
    ]
    mock_api = MockAPI(statuses)
    engine, mock = build_engine(config, mock_api)

    result = engine.execute_sell("token", price=0.5, quantity=1.0)

    assert result.filled == pytest.approx(1.0)
    assert len(mock.create_calls) == 2
    first_price = mock.create_calls[0]["price"]
    second_price = mock.create_calls[1]["price"]
    assert second_price == pytest.approx(first_price * (1 - config.price_tolerance_step))


def test_buy_retries_raise_price():
    config = ExecutionConfig(
        order_slice_min=1.0,
        order_slice_max=1.0,
        retry_attempts=1,
        price_tolerance_step=0.1,
        wait_seconds=1.0,
        poll_interval_seconds=0.2,
        order_interval_seconds=0.0,
    )
    statuses = [
        [{"status": "OPEN", "filledAmount": 0.0}],
        [{"status": "FILLED", "filledAmount": 1.0}],
    ]
    mock_api = MockAPI(statuses)
    engine, mock = build_engine(config, mock_api)

    result = engine.execute_buy("token", price=0.5, quantity=1.0)

    assert result.status == "FILLED"
    assert len(mock.create_calls) == 2
    first_price = mock.create_calls[0]["price"]
    second_price = mock.create_calls[1]["price"]
    assert second_price == pytest.approx(first_price * (1 + config.price_tolerance_step))


def test_buy_handles_matched_status_without_reported_fill():
    config = ExecutionConfig(
        order_slice_min=1.0,
        order_slice_max=2.0,
        retry_attempts=2,
        wait_seconds=1.0,
        poll_interval_seconds=0.2,
        order_interval_seconds=0.0,
    )
    statuses = [[{"status": "MATCHED"}]]
    mock_api = MockAPI(statuses)
    engine, mock = build_engine(config, mock_api)

    result = engine.execute_buy("token", price=0.55, quantity=2.0)

    assert result.status == "FILLED"
    assert result.filled == pytest.approx(2.0)
    assert len(mock.create_calls) == 1


def test_buy_returns_partial_when_slice_not_fully_filled():
    config = ExecutionConfig(
        order_slice_min=1.0,
        order_slice_max=2.0,
        retry_attempts=0,
        wait_seconds=1.0,
        poll_interval_seconds=0.2,
        order_interval_seconds=0.0,
    )
    statuses = [
        [
            {"status": "PARTIAL", "filledAmount": 1.0},
            {"status": "CANCELLED", "filledAmount": 1.0},
        ]
    ]
    mock_api = MockAPI(statuses)
    engine, _ = build_engine(config, mock_api)

    result = engine.execute_buy("token", price=0.42, quantity=2.0)

    assert result.status == "PARTIAL"
    assert result.filled == pytest.approx(1.0)
    assert result.remaining == pytest.approx(1.0)


def test_buy_slicing_respects_min_quote_amount():
    config = ExecutionConfig(
        order_slice_min=1.0,
        order_slice_max=2.0,
        retry_attempts=0,
        wait_seconds=1.0,
        poll_interval_seconds=0.2,
        order_interval_seconds=0.0,
        min_quote_amount=1.0,
    )
    mock_api = MockAPI()
    engine, _ = build_engine(config, mock_api)

    slices = list(engine._slice_quantities(3.0, side="buy", price=0.49))

    assert len(slices) == 1
    assert slices[0] == pytest.approx(3.0)

    slices = list(engine._slice_quantities(6.2, side="buy", price=0.45))
    assert len(slices) >= 1
    for qty in slices:
        assert qty * 0.45 >= config.min_quote_amount - 1e-6


def test_buy_slicing_respects_market_min_order_size():
    config = ExecutionConfig(
        order_slice_min=1.0,
        order_slice_max=2.0,
        retry_attempts=0,
        wait_seconds=1.0,
        poll_interval_seconds=0.2,
        order_interval_seconds=0.0,
        min_market_order_size=5.0,
    )
    mock_api = MockAPI()
    engine, _ = build_engine(config, mock_api)

    slices = list(engine._slice_quantities(6.0, side="buy", price=0.75))
    assert len(slices) == 1
    assert slices[0] == pytest.approx(6.0)

    slices = list(engine._slice_quantities(12.1, side="buy", price=0.8))
    assert len(slices) == 2
    assert slices[0] == pytest.approx(5.0)
    assert slices[1] == pytest.approx(7.1)
    for qty in slices:
        assert qty >= config.min_market_order_size - 1e-6


def test_slicing_rolls_residual_into_last_order():
    config = ExecutionConfig(
        order_slice_min=1.0,
        order_slice_max=2.0,
        retry_attempts=0,
        wait_seconds=1.0,
        poll_interval_seconds=0.2,
        order_interval_seconds=0.0,
        min_market_order_size=5.0,
    )
    mock_api = MockAPI()
    engine, _ = build_engine(config, mock_api)

    slices = list(engine._slice_quantities(11.0, side="sell"))
    assert slices == pytest.approx([5.0, 6.0])


def test_buy_returns_partial_when_later_slice_fails():
    config = ExecutionConfig(
        order_slice_min=1.0,
        order_slice_max=2.0,
        retry_attempts=0,
        wait_seconds=1.0,
        poll_interval_seconds=0.2,
        order_interval_seconds=0.0,
    )
    statuses = [
        [{"status": "FILLED", "filledAmount": 2.0}],
        [{"status": "FILLED", "filledAmount": 2.0}],
    ]
    mock_api = MockAPI(statuses, create_exceptions={2: lambda: RuntimeError("boom")})
    engine, mock = build_engine(config, mock_api)

    result = engine.execute_buy("token", price=0.75, quantity=6.0)

    assert isinstance(result, ExecutionResult)
    assert result.status == "PARTIAL"
    assert result.filled == pytest.approx(4.0)
    assert "boom" in (result.message or "")
    assert len(mock.create_calls) == 3


def test_clob_adapter_honours_time_in_force(monkeypatch):
    import types
    import sys

    class DummyOrderType(Enum):
        FAK = "FAK"
        GTC = "GTC"

    class DummyOrderArgs:
        def __init__(self, token_id, side, price, size):
            self.token_id = token_id
            self.side = side
            self.price = price
            self.size = size

    clob_pkg = types.ModuleType("py_clob_client")
    clob_types = types.ModuleType("py_clob_client.clob_types")
    clob_types.OrderType = DummyOrderType
    clob_types.OrderArgs = DummyOrderArgs
    order_builder = types.ModuleType("py_clob_client.order_builder.constants")
    order_builder.BUY = "BUY"
    order_builder.SELL = "SELL"

    monkeypatch.setitem(sys.modules, "py_clob_client", clob_pkg)
    monkeypatch.setitem(sys.modules, "py_clob_client.clob_types", clob_types)
    monkeypatch.setitem(sys.modules, "py_clob_client.order_builder.constants", order_builder)

    class DummyClient:
        def __init__(self):
            self.post_calls = []

        def create_order(self, order_args):
            # Simulate SDK defaulting to FAK order type
            return {"orderType": "FAK", "allowPartial": False}

        def post_order(self, signed, order_type):
            self.post_calls.append((signed, order_type))
            return {"orderId": "abc123", "status": "OPEN"}

    adapter = ClobPolymarketAPI(DummyClient())
    payload = {
        "tokenId": "token",
        "side": "buy",
        "price": 0.45,
        "size": 1.0,
        "timeInForce": "GTC",
        "allowPartial": True,
    }

    response = adapter.create_order(payload)

    assert response["orderId"] == "abc123"
    assert adapter._client.post_calls[0][1] is DummyOrderType.GTC
    signed_payload = adapter._client.post_calls[0][0]
    assert signed_payload["orderType"] == "GTC"
    assert signed_payload["timeInForce"] == "GTC"
    assert signed_payload["allowPartial"] is True


def test_clob_adapter_skips_post_when_create_returns_order(monkeypatch):
    import types
    import sys

    class DummyOrderType(Enum):
        GTC = "GTC"

    class DummyOrderArgs:
        def __init__(self, *args, **kwargs):
            pass

    clob_pkg = types.ModuleType("py_clob_client")
    clob_types = types.ModuleType("py_clob_client.clob_types")
    clob_types.OrderType = DummyOrderType
    clob_types.OrderArgs = DummyOrderArgs
    order_builder = types.ModuleType("py_clob_client.order_builder.constants")
    order_builder.BUY = "BUY"
    order_builder.SELL = "SELL"

    monkeypatch.setitem(sys.modules, "py_clob_client", clob_pkg)
    monkeypatch.setitem(sys.modules, "py_clob_client.clob_types", clob_types)
    monkeypatch.setitem(sys.modules, "py_clob_client.order_builder.constants", order_builder)

    class DummyClient:
        def __init__(self):
            self.post_called = False

        def create_order(self, order_args):
            return {"orderId": "already-submitted"}

        def post_order(self, signed, order_type):
            self.post_called = True
            raise AssertionError("post_order should not be called when create_order returns orderId")

    adapter = ClobPolymarketAPI(DummyClient())
    payload = {"tokenId": "token", "side": "buy", "price": 0.5, "size": 1.0}

    response = adapter.create_order(payload)

    assert response["orderId"] == "already-submitted"


def test_clob_adapter_handles_nested_order_response(monkeypatch):
    import types
    import sys

    class DummyOrderType(Enum):
        GTC = "GTC"

    class DummyOrderArgs:
        def __init__(self, *args, **kwargs):
            pass

    clob_pkg = types.ModuleType("py_clob_client")
    clob_types = types.ModuleType("py_clob_client.clob_types")
    clob_types.OrderType = DummyOrderType
    clob_types.OrderArgs = DummyOrderArgs
    order_builder = types.ModuleType("py_clob_client.order_builder.constants")
    order_builder.BUY = "BUY"
    order_builder.SELL = "SELL"

    monkeypatch.setitem(sys.modules, "py_clob_client", clob_pkg)
    monkeypatch.setitem(sys.modules, "py_clob_client.clob_types", clob_types)
    monkeypatch.setitem(sys.modules, "py_clob_client.order_builder.constants", order_builder)

    class DummyClient:
        def create_order(self, order_args):
            return {}

        def post_order(self, signed, order_type):
            return {
                "data": {
                    "createOrder": {
                        "order": {"orderId": "nested-123"},
                        "status": "OPEN",
                    }
                }
            }

    adapter = ClobPolymarketAPI(DummyClient())
    payload = {"tokenId": "token", "side": "buy", "price": 0.5, "size": 1.0}

    response = adapter.create_order(payload)

    assert response["orderId"] == "nested-123"


def test_clob_adapter_handles_dataclass_response(monkeypatch):
    import sys
    import types
    from dataclasses import dataclass

    class DummyOrderType(Enum):
        GTC = "GTC"

    class DummyOrderArgs:
        def __init__(self, *args, **kwargs):
            pass

    clob_pkg = types.ModuleType("py_clob_client")
    clob_types = types.ModuleType("py_clob_client.clob_types")
    clob_types.OrderType = DummyOrderType
    clob_types.OrderArgs = DummyOrderArgs
    order_builder = types.ModuleType("py_clob_client.order_builder.constants")
    order_builder.BUY = "BUY"
    order_builder.SELL = "SELL"

    monkeypatch.setitem(sys.modules, "py_clob_client", clob_pkg)
    monkeypatch.setitem(sys.modules, "py_clob_client.clob_types", clob_types)
    monkeypatch.setitem(sys.modules, "py_clob_client.order_builder.constants", order_builder)

    @dataclass
    class Order:
        orderId: str

    @dataclass
    class CreateOrder:
        order: Order
        status: str

    @dataclass
    class ResponseWrapper:
        data: dict

    class DummyClient:
        def create_order(self, order_args):
            return {}

        def post_order(self, signed, order_type):
            return ResponseWrapper(data={"createOrder": CreateOrder(order=Order("dc-42"), status="OPEN")})

    adapter = ClobPolymarketAPI(DummyClient())
    payload = {"tokenId": "token", "side": "buy", "price": 0.5, "size": 1.0}

    response = adapter.create_order(payload)

    assert response["orderId"] == "dc-42"


def test_clob_adapter_raises_when_response_contains_only_error(monkeypatch):
    import sys
    import types

    class DummyOrderType(Enum):
        GTC = "GTC"

    class DummyOrderArgs:
        def __init__(self, *args, **kwargs):
            pass

    clob_pkg = types.ModuleType("py_clob_client")
    clob_types = types.ModuleType("py_clob_client.clob_types")
    clob_types.OrderType = DummyOrderType
    clob_types.OrderArgs = DummyOrderArgs
    order_builder = types.ModuleType("py_clob_client.order_builder.constants")
    order_builder.BUY = "BUY"
    order_builder.SELL = "SELL"

    monkeypatch.setitem(sys.modules, "py_clob_client", clob_pkg)
    monkeypatch.setitem(sys.modules, "py_clob_client.clob_types", clob_types)
    monkeypatch.setitem(sys.modules, "py_clob_client.order_builder.constants", order_builder)

    class DummyClient:
        def create_order(self, order_args):
            return {}

        def post_order(self, signed, order_type):
            return {
                "errors": [
                    {"message": "not enough balance / allowance"},
                ]
            }

    adapter = ClobPolymarketAPI(DummyClient())
    payload = {"tokenId": "token", "side": "buy", "price": 0.5, "size": 1.0}

    with pytest.raises(RuntimeError) as excinfo:
        adapter.create_order(payload)

    assert "not enough balance / allowance" in str(excinfo.value)
