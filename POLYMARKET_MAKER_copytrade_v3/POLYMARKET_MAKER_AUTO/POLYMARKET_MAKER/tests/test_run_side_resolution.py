from pathlib import Path
import sys
import types

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

requests_stub = types.SimpleNamespace(RequestException=Exception, get=lambda *_, **__: None)
sys.modules.setdefault("requests", requests_stub)

ws_stub = types.SimpleNamespace(get_client=lambda: object(), ws_watch_by_ids=lambda *_, **__: None)
rest_stub = types.SimpleNamespace(get_client=lambda: object())
price_watch_stub = types.SimpleNamespace(resolve_token_ids=lambda *_, **__: ("YES", "NO", "", {}))

sys.modules.setdefault("Volatility_arbitrage_main_ws", ws_stub)
sys.modules.setdefault("Volatility_arbitrage_main_rest", rest_stub)
sys.modules.setdefault("Volatility_arbitrage_price_watch", price_watch_stub)

import Volatility_arbitrage_run as module


def test_resolve_side_accepts_side_field():
    cfg = {"side": "no"}
    assert module._resolve_side(cfg) == "NO"


def test_resolve_side_falls_back_to_preferred_side():
    cfg = {"preferred_side": "yes"}
    assert module._resolve_side(cfg) == "YES"


def test_resolve_side_uses_highlight_sides_when_missing():
    cfg = {"highlight_sides": ["no", "yes"]}
    assert module._resolve_side(cfg) == "NO"


def test_resolve_side_rejects_invalid_value():
    cfg = {"side": "maybe"}
    with pytest.raises(ValueError):
        module._resolve_side(cfg)


def test_resolve_side_requires_any_source():
    with pytest.raises(ValueError):
        module._resolve_side({})

