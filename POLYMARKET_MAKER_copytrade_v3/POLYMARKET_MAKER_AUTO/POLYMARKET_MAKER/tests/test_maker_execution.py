import collections
from typing import Deque, Dict, List

import pytest

import maker_execution as maker


class StubAdapter:
    def __init__(self, client):
        self.client = client

    def create_order(self, payload: Dict[str, float]) -> Dict[str, object]:
        return self.client.create_order(payload)

    def get_order_status(self, order_id: str) -> Dict[str, object]:
        return self.client.get_order_status(order_id)


class DummyClient:
    def __init__(self, status_sequences: List[List[Dict[str, object]]]):
        self._status_sequences: Deque[Deque[Dict[str, object]]] = collections.deque(
            collections.deque(seq) for seq in status_sequences
        )
        self.order_status: Dict[str, Deque[Dict[str, object]]] = {}
        self.created_orders: List[Dict[str, object]] = []
        self.cancelled: List[str] = []
        self._counter = 0

    def create_order(self, payload: Dict[str, object]) -> Dict[str, object]:
        self._counter += 1
        order_id = f"order-{self._counter}"
        status_seq = self._status_sequences.popleft() if self._status_sequences else collections.deque(
            [{"status": "FILLED", "filledAmount": float(payload.get("size", 0.0)), "avgPrice": float(payload.get("price", 0.0))}]
        )
        self.order_status[order_id] = status_seq
        entry = dict(payload)
        entry["order_id"] = order_id
        self.created_orders.append(entry)
        return {"orderId": order_id}

    def get_order_status(self, order_id: str) -> Dict[str, object]:
        seq = self.order_status[order_id]
        if len(seq) > 1:
            return seq.popleft()
        return seq[0]

    def cancel_order(self, order_id: str) -> None:
        self.cancelled.append(order_id)
        seq = self.order_status.get(order_id)
        if seq is not None:
            last = seq[-1] if seq else {"filledAmount": 0.0}
            seq.append({"status": "CANCELLED", "filledAmount": last.get("filledAmount", 0.0)})


class InsufficientBalanceClient(DummyClient):
    def __init__(self, status_sequences: List[List[Dict[str, object]]], fail_threshold: float) -> None:
        super().__init__(status_sequences)
        self.fail_threshold = fail_threshold
        self.failures = 0

    def create_order(self, payload: Dict[str, object]) -> Dict[str, object]:
        size = float(payload.get("size", 0.0) or 0.0)
        if size >= self.fail_threshold and self.failures < 1:
            self.failures += 1
            raise RuntimeError("insufficient balance to place order")
        return super().create_order(payload)


@pytest.fixture(autouse=True)
def _patch_adapter(monkeypatch):
    monkeypatch.setattr(maker, "ClobPolymarketAPI", lambda client: StubAdapter(client))
    yield


def _stream(values: List[float]):
    dq = collections.deque(values)
    last_val = values[-1] if values else None

    def supplier():
        nonlocal last_val
        if dq:
            last_val = dq.popleft()
        return last_val

    return supplier


def test_maker_buy_immediate_fill():
    client = DummyClient(
        status_sequences=[[{"status": "FILLED", "filledAmount": 3.0, "avgPrice": 0.5}]]
    )
    result = maker.maker_buy_follow_bid(
        client,
        token_id="tkn",
        target_size=3.0,
        poll_sec=0.0,
        min_order_size=0.0,
        best_bid_fn=lambda: 0.5,
        sleep_fn=lambda _: None,
    )

    assert result["status"] == "FILLED"
    assert result["filled"] == pytest.approx(3.0)
    assert result["avg_price"] == pytest.approx(0.5)
    assert len(client.created_orders) == 1


def test_maker_buy_reprices_on_bid_rise():
    client = DummyClient(
        status_sequences=[
            [
                {"status": "OPEN", "filledAmount": 0.0},
                {"status": "OPEN", "filledAmount": 0.0},
            ],
            [
                {"status": "FILLED", "filledAmount": 2.0, "avgPrice": 0.52},
            ],
        ]
    )
    bid_supplier = _stream([0.50, 0.52, 0.52])

    result = maker.maker_buy_follow_bid(
        client,
        token_id="asset",
        target_size=2.0,
        poll_sec=0.0,
        min_order_size=0.0,
        best_bid_fn=bid_supplier,
        sleep_fn=lambda _: None,
    )

    assert result["filled"] == pytest.approx(2.0)
    assert client.cancelled, "Expected cancellation when bid moves higher"
    first_order = client.created_orders[0]
    assert first_order["price"] == pytest.approx(0.50, rel=0, abs=1e-9)


def test_maker_buy_handles_missing_fill_amount_on_match():
    client = DummyClient(status_sequences=[[{"status": "MATCHED"}]])

    result = maker.maker_buy_follow_bid(
        client,
        token_id="asset",
        target_size=5.0,
        poll_sec=0.0,
        min_order_size=0.0,
        best_bid_fn=lambda: 0.5,
        sleep_fn=lambda _: None,
    )

    assert result["status"] == "FILLED"
    assert result["filled"] == pytest.approx(5.0)
    assert client.created_orders, "expected order to be created"


def test_maker_buy_detects_precision_from_bid_stream():
    client = DummyClient(
        status_sequences=[
            [
                {"status": "OPEN", "filledAmount": 0.0},
                {"status": "OPEN", "filledAmount": 0.0},
            ],
            [
                {"status": "FILLED", "filledAmount": 1.0, "avgPrice": 0.984},
            ],
        ]
    )
    bids = _stream([0.98, 0.984, 0.984])

    result = maker.maker_buy_follow_bid(
        client,
        token_id="asset",
        target_size=1.0,
        poll_sec=0.0,
        min_quote_amt=0.0,
        min_order_size=0.0,
        best_bid_fn=bids,
        sleep_fn=lambda _: None,
    )

    assert result["filled"] == pytest.approx(1.0)
    assert len(client.created_orders) >= 2
    assert client.created_orders[0]["price"] == pytest.approx(0.98, rel=0, abs=1e-9)
    assert client.created_orders[1]["price"] == pytest.approx(0.984, rel=0, abs=1e-9)


def test_maker_buy_retries_after_invalid_status():
    client = DummyClient(
        status_sequences=[
            [{"status": "INVALID", "filledAmount": 0.0}],
            [{"status": "FILLED", "filledAmount": 0.2999, "avgPrice": 0.5}],
        ]
    )
    bids = _stream([0.5, 0.5, 0.5])

    result = maker.maker_buy_follow_bid(
        client,
        token_id="asset",
        target_size=0.3,
        poll_sec=0.0,
        min_quote_amt=0.0,
        min_order_size=0.0,
        best_bid_fn=bids,
        sleep_fn=lambda _: None,
    )

    assert result["status"] == "FILLED"
    assert result["filled"] == pytest.approx(0.2999, rel=0, abs=1e-9)
    assert len(client.created_orders) == 2
    assert client.cancelled, "Expected the INVALID order to be cancelled before retry"
    assert client.created_orders[1]["size"] < client.created_orders[0]["size"]


def test_maker_buy_shrinks_on_balance_error_during_create():
    client = InsufficientBalanceClient(
        status_sequences=[[{"status": "FILLED", "filledAmount": 0.2999, "avgPrice": 0.5}]],
        fail_threshold=0.29995,
    )
    bids = _stream([0.5, 0.5, 0.5])

    result = maker.maker_buy_follow_bid(
        client,
        token_id="asset",
        target_size=0.3,
        poll_sec=0.0,
        min_quote_amt=0.0,
        min_order_size=0.0,
        best_bid_fn=bids,
        sleep_fn=lambda _: None,
    )

    assert result["status"] == "FILLED"
    assert result["filled"] == pytest.approx(0.2999, rel=0, abs=1e-9)
    assert client.failures == 1, "expected one balance-related failure before retry"
    assert len(client.created_orders) == 1


def test_maker_buy_retries_on_insufficient_balance_status():
    client = DummyClient(
        status_sequences=[
            [{"status": "REJECTED", "message": "insufficient balance", "filledAmount": 0.0}],
            [{"status": "FILLED", "filledAmount": 0.1499, "avgPrice": 0.25}],
        ]
    )
    bids = _stream([0.25, 0.25, 0.25])

    result = maker.maker_buy_follow_bid(
        client,
        token_id="asset",
        target_size=0.15,
        poll_sec=0.0,
        min_quote_amt=0.0,
        min_order_size=0.0,
        best_bid_fn=bids,
        sleep_fn=lambda _: None,
    )

    assert result["status"] == "FILLED"
    assert result["filled"] == pytest.approx(0.1499, rel=0, abs=1e-9)


def test_maker_buy_respects_max_buy_price_cap_before_placing_order():
    client = DummyClient(status_sequences=[])

    call_count = {"n": 0}

    def _stop_after_three_checks() -> bool:
        call_count["n"] += 1
        return call_count["n"] >= 3

    result = maker.maker_buy_follow_bid(
        client,
        token_id="asset",
        target_size=1.0,
        poll_sec=0.0,
        min_quote_amt=0.0,
        min_order_size=0.0,
        best_bid_fn=lambda: 0.991,
        stop_check=_stop_after_three_checks,
        sleep_fn=lambda _: None,
        max_buy_price=0.98,
    )

    assert result["filled"] == pytest.approx(0.0)
    assert client.created_orders == []


def test_maker_sell_waits_for_floor_before_order():
    client = DummyClient(
        status_sequences=[[{"status": "FILLED", "filledAmount": 1.5, "avgPrice": 0.72}]]
    )
    asks = _stream([0.65, 0.65, 0.72, 0.72])

    result = maker.maker_sell_follow_ask_with_floor_wait(
        client,
        token_id="asset",
        position_size=1.5,
        floor_X=0.70,
        poll_sec=0.0,
        min_order_size=0.0,
        best_ask_fn=asks,
        sleep_fn=lambda _: None,
    )

    assert len(client.created_orders) == 1
    order = client.created_orders[0]
    assert order["price"] >= 0.70
    assert result["status"] == "FILLED"
    assert result["filled"] == pytest.approx(1.5)


def test_maker_sell_invokes_progress_probe():
    client = DummyClient(
        status_sequences=[
            [
                {"status": "OPEN", "filledAmount": 0.0},
                {"status": "FILLED", "filledAmount": 1.0, "avgPrice": 0.75},
            ]
        ]
    )
    asks = _stream([0.80, 0.80])
    probe_calls: List[int] = []

    def probe() -> None:
        probe_calls.append(len(probe_calls))

    result = maker.maker_sell_follow_ask_with_floor_wait(
        client,
        token_id="asset",
        position_size=1.0,
        floor_X=0.70,
        poll_sec=0.0,
        min_order_size=0.0,
        best_ask_fn=asks,
        sleep_fn=lambda _: None,
        progress_probe=probe,
        progress_probe_interval=0.0,
    )

    assert probe_calls, "expected sell progress probe to run at least once"
    assert result["filled"] == pytest.approx(1.0)


def test_maker_sell_expands_goal_via_position_fetcher():
    client = DummyClient(
        status_sequences=[[{"status": "FILLED", "filledAmount": 3.0, "avgPrice": 0.8}]]
    )
    asks = _stream([0.82, 0.82])
    fetch_calls: List[int] = []

    def _position_fetcher() -> float:
        fetch_calls.append(len(fetch_calls))
        return 3.0

    result = maker.maker_sell_follow_ask_with_floor_wait(
        client,
        token_id="asset",
        position_size=1.0,
        floor_X=0.75,
        poll_sec=0.0,
        min_order_size=0.0,
        best_ask_fn=asks,
        sleep_fn=lambda _: None,
        position_fetcher=_position_fetcher,
        position_refresh_interval=0.0,
    )

    assert fetch_calls, "expected position fetcher to be invoked"
    assert client.created_orders, "expected sell order"
    assert client.created_orders[0]["size"] == pytest.approx(3.0)
    assert result["filled"] == pytest.approx(3.0)


def test_maker_sell_shrinks_goal_and_cancels_active_order():
    client = DummyClient(
        status_sequences=[[{"status": "OPEN", "filledAmount": 0.0}]]
    )
    asks = _stream([0.9, 0.9])
    fetch_values = collections.deque([2.0, 0.0])

    def _position_fetcher() -> float:
        if fetch_values:
            return fetch_values.popleft()
        return 0.0

    result = maker.maker_sell_follow_ask_with_floor_wait(
        client,
        token_id="asset",
        position_size=2.0,
        floor_X=0.85,
        poll_sec=0.0,
        min_order_size=0.0,
        best_ask_fn=asks,
        sleep_fn=lambda _: None,
        position_fetcher=_position_fetcher,
        position_refresh_interval=0.0,
    )

    assert client.cancelled, "expected active order to be cancelled after shrink"
    assert result["status"] == "FILLED"
    assert result["remaining"] == pytest.approx(0.0)
