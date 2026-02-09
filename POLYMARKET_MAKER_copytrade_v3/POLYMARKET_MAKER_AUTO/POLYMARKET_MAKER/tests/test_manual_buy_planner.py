import math
import sys
import types


class _RequestException(Exception):
    pass


class _Timeout(_RequestException):
    pass


class _HTTPError(_RequestException):
    pass


def _default_get(*args, **kwargs):  # pragma: no cover - defensive stub
    raise RuntimeError("requests stub should not be used in planner tests")


requests_stub = types.SimpleNamespace(
    RequestException=_RequestException,
    Timeout=_Timeout,
    HTTPError=_HTTPError,
    get=_default_get,
)

sys.modules.setdefault("requests", requests_stub)


class _WebsocketStub(types.SimpleNamespace):
    def WebSocketApp(self, *args, **kwargs):  # pragma: no cover - defensive stub
        raise RuntimeError("websocket stub should not be used in planner tests")


sys.modules.setdefault("websocket", _WebsocketStub())

from Volatility_arbitrage_run import _plan_manual_buy_size


def test_manual_buy_as_target_skips_when_owned_enough():
    planned, skip = _plan_manual_buy_size(10, 10.0001, enforce_target=True)
    assert planned is None
    assert skip is True


def test_manual_buy_as_target_returns_gap():
    planned, skip = _plan_manual_buy_size(10, 4.2, enforce_target=True)
    assert skip is False
    assert math.isclose(planned, 5.8, rel_tol=1e-9)


def test_manual_buy_as_order_ignores_existing_position():
    planned, skip = _plan_manual_buy_size(3, 9.0, enforce_target=False)
    assert skip is False
    assert planned == 3
