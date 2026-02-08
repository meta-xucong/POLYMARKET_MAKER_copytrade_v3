import sys
import types
from pathlib import Path

import pytest


class _RequestException(Exception):
    pass


class _Timeout(_RequestException):
    pass


class _HTTPError(_RequestException):
    pass


def _default_get(*args, **kwargs):  # pragma: no cover - defensive stub
    raise RuntimeError("requests stub should be patched in tests")


requests_stub = types.SimpleNamespace(
    RequestException=_RequestException,
    Timeout=_Timeout,
    HTTPError=_HTTPError,
    get=_default_get,
)

sys.modules["requests"] = requests_stub

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class _WebsocketStub(types.SimpleNamespace):
    def WebSocketApp(self, *args, **kwargs):  # pragma: no cover - defensive stub
        raise RuntimeError("websocket stub should not be used in tests")


sys.modules["websocket"] = _WebsocketStub()

from Volatility_arbitrage_run import (
    _extract_positions_from_data_api_response,
    _fetch_position_snapshot_with_cache,
    _fetch_positions_from_data_api,
    _lookup_position_avg_price,
    _merge_remote_position_size,
)


class DummyResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self):  # pragma: no cover - triggered only on errors
        if self.status_code >= 400:
            raise _HTTPError(f"status={self.status_code}")


class DummyClient(types.SimpleNamespace):
    pass


def test_extract_positions_handles_various_shapes():
    assert _extract_positions_from_data_api_response(None) == []
    sample_list = [{"asset": "1"}]
    assert _extract_positions_from_data_api_response(sample_list) == sample_list
    sample_dict = {"data": [{"asset": "2"}]}
    assert _extract_positions_from_data_api_response(sample_dict) == [{"asset": "2"}]
    assert _extract_positions_from_data_api_response({"unexpected": []}) is None


def test_fetch_positions_aggregates_pages(monkeypatch):
    module = __import__("Volatility_arbitrage_run")

    first_page = [{"asset": str(i), "size": "1"} for i in range(500)]
    second_page = [{"asset": "500", "size": "2"}]
    responses = [
        DummyResponse(200, {"data": first_page, "meta": {"total": 501}}),
        DummyResponse(200, {"data": second_page, "meta": {"total": 501}}),
    ]
    calls = []

    def fake_get(url, params=None, timeout=None):
        calls.append((url, dict(params or {}), timeout))
        return responses.pop(0)

    monkeypatch.setattr(module.requests, "get", fake_get)

    client = DummyClient(funder="0xabc")
    positions, ok, origin = _fetch_positions_from_data_api(client)
    assert ok is True
    assert origin.startswith("data-api positions(")
    assert len(positions) == 501
    assert calls[0][1]["user"] == "0xabc"
    assert calls[0][1]["offset"] == 0
    assert calls[1][1]["offset"] == 500


def test_fetch_positions_reports_404(monkeypatch):
    module = __import__("Volatility_arbitrage_run")

    responses = [
        DummyResponse(404, {}),
    ]
    calls = []

    def fake_get(url, params=None, timeout=None):
        calls.append((url, dict(params or {}), timeout))
        return responses.pop(0)

    monkeypatch.setattr(module.requests, "get", fake_get)

    client = DummyClient(funder="0xabc")
    positions, ok, info = _fetch_positions_from_data_api(client)

    assert ok is False
    assert positions == []
    assert calls[0][1]["user"] == "0xabc"
    assert "404" in info


def test_fetch_positions_missing_address(monkeypatch):
    for env_name in ("POLY_DATA_ADDRESS", "POLY_FUNDER", "POLY_WALLET", "POLY_ADDRESS"):
        monkeypatch.delenv(env_name, raising=False)
    client = DummyClient()
    positions, ok, info = _fetch_positions_from_data_api(client)
    assert positions == []
    assert ok is False
    assert "地址" in info


def test_fetch_positions_handles_http_error(monkeypatch):
    module = __import__("Volatility_arbitrage_run")

    def fake_get(url, params=None, timeout=None):  # pragma: no cover - simple stub
        raise module.requests.Timeout("boom")

    monkeypatch.setattr(module.requests, "get", fake_get)

    client = DummyClient(funder="0xabc")
    positions, ok, info = _fetch_positions_from_data_api(client)
    assert positions == []
    assert ok is False
    assert info.startswith("数据接口请求失败")


def test_fetch_positions_env_fallback(monkeypatch):
    module = __import__("Volatility_arbitrage_run")

    calls = []

    def fake_get(url, params=None, timeout=None):
        calls.append(dict(params or {}))
        raise module.requests.Timeout("stop after first call")

    monkeypatch.setattr(module.requests, "get", fake_get)
    monkeypatch.setenv("POLY_FUNDER", "0xfeed")

    client = DummyClient()
    positions, ok, info = _fetch_positions_from_data_api(client)
    assert positions == []
    assert ok is False
    assert "请求失败" in info
    assert calls[0]["user"] == "0xfeed"


def test_fetch_position_snapshot_uses_recent_cache(monkeypatch):
    module = __import__("Volatility_arbitrage_run")

    now = 1000.0
    cache = (0.5, 10.0, "cached")

    monkeypatch.setattr(module.time, "time", lambda: now)

    snapshot, ts = _fetch_position_snapshot_with_cache(
        client=DummyClient(),
        token_id="abc",
        cache=cache,
        cache_ts=now - 1.0,
        log_errors=False,
        force=False,
    )

    assert snapshot == cache
    assert ts == now - 1.0


def test_fetch_position_snapshot_falls_back_to_cache_on_error(monkeypatch):
    module = __import__("Volatility_arbitrage_run")

    now = 2000.0
    cache = (0.42, 5.0, "cached")

    monkeypatch.setattr(module.time, "time", lambda: now)
    monkeypatch.setattr(
        module,
        "_lookup_position_avg_price",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    snapshot, ts = _fetch_position_snapshot_with_cache(
        client=DummyClient(),
        token_id="abc",
        cache=cache,
        cache_ts=now - 100.0,
        log_errors=True,
        force=True,
    )

    assert snapshot == cache
    assert ts == now - 100.0


def test_lookup_position_avg_price_success(monkeypatch):
    module = __import__("Volatility_arbitrage_run")

    sample_positions = [
        {"asset": "123", "avg_price": "0.925", "size": "5"},
        {"tokenId": "456", "avg_price": "0.5", "size": "2"},
    ]

    def fake_fetch(client):
        return sample_positions, True, "mock-source"

    monkeypatch.setattr(module, "_fetch_positions_from_data_api", fake_fetch)

    avg_price, pos_size, origin = _lookup_position_avg_price(DummyClient(), "123")
    assert avg_price == 0.925
    assert pos_size == 5
    assert origin == "mock-source"


def test_lookup_position_avg_price_not_found(monkeypatch):
    module = __import__("Volatility_arbitrage_run")

    sample_positions = [{"tokenId": "999", "avg_price": "0.8", "size": "3"}]

    def fake_fetch(client):
        return sample_positions, True, "mock-origin"

    monkeypatch.setattr(module, "_fetch_positions_from_data_api", fake_fetch)

    avg_price, pos_size, origin = _lookup_position_avg_price(DummyClient(), "123")
    assert avg_price is None
    assert pos_size is None
    assert "123" in origin


def test_lookup_position_avg_price_zero_size(monkeypatch):
    module = __import__("Volatility_arbitrage_run")

    sample_positions = [{"tokenId": "123", "avg_price": "0.5", "size": 0}]

    def fake_fetch(client):
        return sample_positions, True, "mock-origin"

    monkeypatch.setattr(module, "_fetch_positions_from_data_api", fake_fetch)

    avg_price, pos_size, origin = _lookup_position_avg_price(DummyClient(), "123")
    assert avg_price == 0.5
    assert pos_size == 0
    assert origin == "mock-origin"


def test_merge_remote_position_size_detects_updates():
    new_size, changed = _merge_remote_position_size(None, 5.0)
    assert new_size == 5.0
    assert changed is True


def test_merge_remote_position_size_tolerates_noise():
    new_size, changed = _merge_remote_position_size(5.0, 5.0000004)
    assert abs((new_size or 0) - 5.0000004) < 1e-9
    assert changed is False


def test_merge_remote_position_size_detects_clear():
    new_size, changed = _merge_remote_position_size(5.0, 0.0)
    assert new_size is None
    assert changed is True


def test_merge_remote_position_size_ignores_dust(monkeypatch):
    # 远端仓位低于最小挂单量时应视为无仓位
    new_size, changed = _merge_remote_position_size(5.0, 2.0, dust_floor=5.0)
    assert new_size is None
    assert changed is True

    new_size, changed = _merge_remote_position_size(None, 2.0, dust_floor=5.0)
    assert new_size is None
    assert changed is False
