"""Execution helpers for placing batched orders on Polymarket."""

from __future__ import annotations

import math
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Deque, Dict, Iterable, List, Optional, Set, Tuple

try:  # pragma: no cover - optional dependency
    import yaml
except ImportError:  # pragma: no cover - fallback to lightweight parser
    yaml = None


Number = float


@dataclass
class ExecutionConfig:
    """Configuration for slicing and retrying sell orders."""

    order_slice_min: Number = 1.0
    order_slice_max: Number = 2.0
    retry_attempts: int = 2
    price_tolerance_step: float = 0.01
    wait_seconds: float = 5.0
    poll_interval_seconds: float = 0.5
    order_interval_seconds: Optional[float] = None
    min_quote_amount: Number = 1.0
    min_market_order_size: Number = 0.0

    @classmethod
    def from_yaml(cls, path: str) -> "ExecutionConfig":
        with open(path, "r", encoding="utf-8") as fh:
            text = fh.read()
        if yaml is not None:
            data = yaml.safe_load(text) or {}
        else:
            data = cls._parse_simple_yaml(text)
        return cls(**data)

    @staticmethod
    def _parse_simple_yaml(text: str) -> Dict[str, object]:
        result: Dict[str, object] = {}
        for raw_line in text.splitlines():
            line = raw_line.split("#", 1)[0].strip()
            if not line or ":" not in line:
                continue
            key, value = line.split(":", 1)
            result[key.strip()] = ExecutionConfig._coerce_value(value.strip())
        return result

    @staticmethod
    def _coerce_value(value: str) -> object:
        lowered = value.lower()
        if lowered in {"true", "false"}:
            return lowered == "true"
        try:
            if value.startswith("0") and value not in {"0", "0.0"}:
                raise ValueError
            return int(value)
        except ValueError:
            pass
        try:
            return float(value)
        except ValueError:
            return value

    def __post_init__(self) -> None:
        if self.order_slice_min <= 0:
            raise ValueError("order_slice_min must be positive")
        if self.order_slice_max < self.order_slice_min:
            raise ValueError("order_slice_max must be >= order_slice_min")
        if self.retry_attempts < 0:
            raise ValueError("retry_attempts must be >= 0")
        if self.price_tolerance_step < 0:
            raise ValueError("price_tolerance_step must be >= 0")
        if self.wait_seconds <= 0:
            raise ValueError("wait_seconds must be positive")
        if self.poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        if self.order_interval_seconds is None:
            self.order_interval_seconds = self.wait_seconds
        if self.order_interval_seconds < 0:
            raise ValueError("order_interval_seconds must be >= 0")
        if self.min_quote_amount < 0:
            raise ValueError("min_quote_amount must be >= 0")
        if self.min_market_order_size < 0:
            raise ValueError("min_market_order_size must be >= 0")


@dataclass
class OrderRequest:
    token_id: str
    side: str
    price: float
    size: float


@dataclass
class ExecutionResult:
    """Summary returned by :class:`ExecutionEngine` after an order run."""

    side: str
    requested: float
    filled: float
    last_price: float
    attempts: int
    status: str
    message: Optional[str] = None
    avg_price: Optional[float] = None
    limit_price: Optional[float] = None

    @property
    def remaining(self) -> float:
        return max(self.requested - self.filled, 0.0)


class ExecutionEngine:
    """Batch scheduler for Polymarket orders with retry management."""

    def __init__(
        self,
        api_client: "PolymarketAPI",
        config: ExecutionConfig,
        clock: Optional[Callable[[], float]] = None,
        sleep: Optional[Callable[[float], None]] = None,
    ) -> None:
        self.api = api_client
        self.config = config
        self._clock = clock or time.monotonic
        self._sleep = sleep or time.sleep

    def execute_sell(
        self,
        token_id: str,
        price: float,
        quantity: float,
    ) -> ExecutionResult:
        """Submit a sell order broken into slices."""
        return self._execute_order("sell", token_id, price, quantity)

    def execute_buy(
        self,
        token_id: str,
        price: float,
        quantity: float,
    ) -> ExecutionResult:
        """Submit a buy order broken into slices."""
        return self._execute_order("buy", token_id, price, quantity)

    def _execute_order(
        self,
        side: str,
        token_id: str,
        price: float,
        quantity: float,
    ) -> ExecutionResult:
        if quantity <= 0:
            return ExecutionResult(
                side=side,
                requested=float(quantity),
                filled=0.0,
                last_price=float(price),
                attempts=0,
                status="SKIPPED",
                message="NON_POSITIVE_QUANTITY",
                avg_price=None,
                limit_price=float(price),
            )

        remaining = quantity
        filled_total = 0.0
        current_price = price
        last_submitted_price = price
        last_fill_price: Optional[float] = None
        weighted_price_sum: float = 0.0
        last_status_text: Optional[str] = None

        max_attempts = self.config.retry_attempts + 1
        attempt = 0

        aborted_due_to_error = False

        while remaining > 1e-9 and attempt < max_attempts:
            attempt += 1
            slice_queue: Deque[float] = deque(
                self._slice_quantities(remaining, side=side, price=current_price)
            )

            while slice_queue and remaining > 1e-9:
                slice_size = slice_queue.popleft()
                order_price = current_price
                last_submitted_price = order_price
                try:
                    order_id = self._create_order(
                        OrderRequest(
                            token_id=token_id,
                            side=side,
                            price=order_price,
                            size=slice_size,
                        )
                    )
                    filled, status_text, avg_price = self._await_fill(
                        order_id, slice_size
                    )
                except Exception as exc:
                    last_status_text = str(exc)
                    if filled_total > 1e-9:
                        remaining = max(quantity - filled_total, 0.0)
                        aborted_due_to_error = True
                        slice_queue.clear()
                        break
                    raise
                filled = min(filled, slice_size)
                filled_total += filled
                remaining = max(remaining - filled, 0.0)
                last_status_text = status_text
                if avg_price is not None:
                    avg_price_f = float(avg_price)
                    last_fill_price = avg_price_f
                    weighted_price_sum += avg_price_f * filled

                if filled < slice_size:
                    # keep the unfinished part for the next attempt
                    break

                if slice_queue and self.config.order_interval_seconds:
                    self._sleep(self.config.order_interval_seconds)

            if aborted_due_to_error:
                break

            if remaining <= 1e-9:
                break

            if attempt < max_attempts:
                current_price = self._adjust_price(side, current_price)
                last_submitted_price = current_price
                self._sleep(self.config.wait_seconds)

        if remaining <= 1e-9:
            status = "FILLED"
        elif filled_total > 1e-9:
            status = "PARTIAL"
        else:
            status = "REJECTED"

        message = last_status_text or status
        reported_price = last_fill_price if last_fill_price is not None else last_submitted_price

        avg_price_value: Optional[float]
        if filled_total > 1e-9 and weighted_price_sum > 0.0:
            avg_price_value = weighted_price_sum / filled_total
        elif last_fill_price is not None:
            avg_price_value = float(last_fill_price)
        else:
            avg_price_value = None

        return ExecutionResult(
            side=side,
            requested=float(quantity),
            filled=float(filled_total),
            last_price=float(reported_price),
            attempts=attempt,
            status=status,
            message=message,
            avg_price=avg_price_value,
            limit_price=float(last_submitted_price),
        )

    def _adjust_price(self, side: str, price: float) -> float:
        step = max(0.0, self.config.price_tolerance_step)
        if step == 0:
            return price
        if side.lower() == "sell":
            return max(0.0, price * (1 - step))
        return price * (1 + step)

    def _create_order(self, order: OrderRequest) -> str:
        payload: Dict[str, object] = {
            "tokenId": order.token_id,
            "side": order.side,
            "price": order.price,
            "size": order.size,
            "type": "GTC",
            "timeInForce": "GTC",
            "allowPartial": True,
        }
        response = self.api.create_order(payload)
        if "orderId" not in response:
            raise RuntimeError("Polymarket API did not return orderId")
        return str(response["orderId"])

    def _await_fill(
        self, order_id: str, target_size: float
    ) -> Tuple[float, str, Optional[float]]:
        deadline = self._clock() + self.config.wait_seconds
        filled = 0.0
        last_status = "OPEN"
        last_avg_price: Optional[float] = None
        final_statuses: Set[str] = {"FILLED", "CANCELLED", "CANCELED", "MATCHED", "COMPLETED", "EXECUTED"}

        while True:
            status = self.api.get_order_status(order_id)
            if not isinstance(status, dict):
                raise RuntimeError(
                    f"Order status response must be a mapping, got: {status!r}"
                )

            if "status" not in status:
                raise RuntimeError(
                    f"Order status payload missing 'status': {status!r}"
                )

            filled = float(status.get("filledAmount", filled))
            status_text = str(status.get("status", last_status))
            last_status = status_text
            avg_candidate: Optional[float] = None
            for key in (
                "avgPrice",
                "averagePrice",
                "avg_price",
                "filledAvgPrice",
                "filledAveragePrice",
                "executionPrice",
                "averageExecutionPrice",
                "fillPrice",
                "matchedPrice",
                "price",
            ):
                candidate = status.get(key)
                if candidate is None:
                    continue
                try:
                    avg_candidate = float(candidate)
                    break
                except (TypeError, ValueError):
                    continue
            if avg_candidate is not None:
                last_avg_price = avg_candidate

            if filled >= target_size - 1e-9:
                break

            status_upper = status_text.upper()

            if status_upper in final_statuses:
                if status_upper == "MATCHED" and filled < target_size - 1e-9:
                    filled = target_size
                break

            if self._clock() >= deadline:
                last_status = "TIMEOUT"
                break
            self._sleep(self.config.poll_interval_seconds)
        return min(filled, target_size), last_status, last_avg_price

    def _slice_quantities(
        self, total: float, side: Optional[str] = None, price: Optional[float] = None
    ) -> Iterable[float]:
        slices: List[float] = []
        remaining = float(total)
        min_size = float(self.config.order_slice_min)
        preferred_max = float(self.config.order_slice_max)

        enforced_min = min_size
        market_min = getattr(self.config, "min_market_order_size", 0.0) or 0.0

        if side and side.lower() == "buy" and price and price > 0:
            enforced_min = max(enforced_min, self._minimum_buy_size(price))
        elif market_min > 0:
            enforced_min = max(enforced_min, self._ceil_precision(market_min))

        preferred_max = max(preferred_max, min_size)
        preferred_max = max(preferred_max, enforced_min)

        if remaining <= 0:
            return []

        if remaining <= preferred_max:
            return [remaining]

        while remaining > 1e-9:
            if remaining <= preferred_max:
                if remaining < enforced_min and slices:
                    slices[-1] += remaining
                else:
                    slices.append(remaining)
                break

            residual = remaining - preferred_max

            if 0 < residual < enforced_min:
                slices.append(remaining)
                break

            slices.append(preferred_max)
            remaining = residual

        return slices

    def _minimum_buy_size(self, price: float) -> float:
        min_quote = getattr(self.config, "min_quote_amount", 0.0) or 0.0
        market_min = getattr(self.config, "min_market_order_size", 0.0) or 0.0
        if market_min > 0:
            market_min = self._ceil_precision(market_min)
        base_min = max(self.config.order_slice_min, market_min)
        if price <= 0:
            return base_min
        if min_quote <= 0:
            return base_min
        quote_min = self._ceil_precision(min_quote / price)
        return max(base_min, quote_min)

    @staticmethod
    def _ceil_precision(value: float, decimals: int = 4) -> float:
        factor = 10 ** decimals
        scaled = value * factor
        return math.ceil(scaled - 1e-12) / factor


class PolymarketAPI:
    """Minimal client protocol used by :class:`ExecutionEngine`."""

    def create_order(self, payload: Dict[str, object]) -> Dict[str, object]:  # pragma: no cover - interface only
        raise NotImplementedError

    def get_order_status(self, order_id: str) -> Dict[str, object]:  # pragma: no cover - interface only
        raise NotImplementedError


class ClobPolymarketAPI(PolymarketAPI):
    """Adapter that bridges :class:`py_clob_client.client.ClobClient` to ``PolymarketAPI``."""

    def __init__(self, client) -> None:  # type: ignore[override]
        self._client = client
        self._min_interval_seconds = 1.0
        self._last_call_ts = 0.0
        self._rate_lock = threading.Lock()

    def _enforce_rate_limit(self) -> None:
        if self._min_interval_seconds <= 0:
            return
        with self._rate_lock:
            now = time.monotonic()
            elapsed = now - self._last_call_ts
            remaining = self._min_interval_seconds - elapsed
            if remaining > 0:
                time.sleep(remaining)
            self._last_call_ts = time.monotonic()

    def create_order(self, payload: Dict[str, object]) -> Dict[str, object]:
        try:
            from py_clob_client.clob_types import OrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY, SELL
        except ImportError as exc:  # pragma: no cover - runtime dependency
            raise RuntimeError("py_clob_client is required to submit orders") from exc

        side_raw = str(payload.get("side", "")).upper()
        if side_raw not in {"BUY", "SELL"}:
            raise ValueError(f"Unsupported side: {side_raw}")

        token_id = str(payload.get("tokenId"))
        if not token_id:
            raise ValueError("tokenId is required")

        price = float(payload.get("price"))
        size = float(payload.get("size"))

        side_const = SELL if side_raw == "SELL" else BUY
        order_args = OrderArgs(token_id=token_id, side=side_const, price=price, size=size)

        order_type = self._resolve_order_type(payload, OrderType)

        self._enforce_rate_limit()
        signed_or_response = self._client.create_order(order_args)

        order_id = self._extract_order_id(signed_or_response)
        if order_id is not None:
            raw_response = signed_or_response
        else:
            self._apply_order_metadata(signed_or_response, order_type, payload)
            self._enforce_rate_limit()
            raw_response = self._client.post_order(signed_or_response, order_type)
            order_id = self._extract_order_id(raw_response)
            if order_id is None:
                raise RuntimeError(
                    f"Order response missing order id: {raw_response!r}"
                )

        if isinstance(raw_response, dict):
            response = dict(raw_response)
            response.setdefault("orderId", order_id)
            return response
        return {"orderId": order_id, "rawResponse": raw_response}

    @staticmethod
    def _resolve_order_type(payload: Dict[str, object], order_type_cls) -> object:
        desired = str(
            payload.get("type")
            or payload.get("timeInForce")
            or "GTC"
        ).upper()

        aliases = {
            "IOC": "FAK",  # py_clob_client historically uses FAK to emulate IOC behaviour
        }

        candidates = [desired, aliases.get(desired), "GTC", "FAK"]
        for cand in candidates:
            if not cand:
                continue
            member = getattr(order_type_cls, cand, None)
            if member is not None:
                return member
        # Fallback to the first enum entry to avoid crashing; actual value will be overwritten by metadata
        try:
            return next(iter(order_type_cls))
        except Exception:  # pragma: no cover - defensive
            return desired

    @staticmethod
    def _apply_order_metadata(order, order_type, payload: Dict[str, object]) -> None:
        order_type_str = ClobPolymarketAPI._order_type_to_str(order_type)
        allow_partial = payload.get("allowPartial")

        for key in ("orderType", "timeInForce", "type"):
            ClobPolymarketAPI._maybe_assign(order, key, order_type_str)

        if allow_partial is not None:
            for key in ("allowPartial", "allowTaker"):
                ClobPolymarketAPI._maybe_assign(order, key, bool(allow_partial))

    @staticmethod
    def _order_type_to_str(order_type: object) -> str:
        if hasattr(order_type, "name"):
            try:
                return str(order_type.name)
            except Exception:  # pragma: no cover - defensive
                pass
        if hasattr(order_type, "value"):
            try:
                return str(order_type.value)
            except Exception:  # pragma: no cover - defensive
                pass
        return str(order_type)

    @staticmethod
    def _maybe_assign(target: object, key: str, value: object) -> None:
        if target is None:
            return
        if isinstance(target, dict):
            target[key] = value
            return
        try:
            setattr(target, key, value)
            return
        except Exception:
            pass
        try:
            target[key] = value  # type: ignore[index]
        except Exception:
            pass

    def get_order_status(self, order_id: str) -> Dict[str, object]:
        candidate_methods = []
        for attr in ("get_order_status", "order_status", "get_order"):
            method = getattr(self._client, attr, None)
            if callable(method):
                candidate_methods.append(method)
        private = getattr(self._client, "private", None)
        if private is not None:
            for attr in ("get_order_status", "get_order", "order_status"):
                method = getattr(private, attr, None)
                if callable(method):
                    candidate_methods.append(method)

        last_error: Optional[Exception] = None
        for method in candidate_methods:
            try:
                self._enforce_rate_limit()
                raw = method(order_id)
                normalized = self._normalize_status(raw)
                if normalized:
                    return normalized
            except Exception as exc:  # pragma: no cover - depends on runtime client
                last_error = exc
                continue

        if last_error is not None:  # pragma: no cover - depends on runtime client
            raise last_error
        raise RuntimeError("Unable to fetch order status from client")

    @staticmethod
    def _extract_order_id(response: object) -> Optional[str]:
        candidates = (
            "order_id",
            "orderId",
            "orderID",
            "id",
            "orderHash",
            "order_hash",
            "hash",
        )

        visited: set[int] = set()

        def walk(obj: object, allow_plain_string: bool = False) -> Optional[str]:
            if obj is None:
                return None

            if isinstance(obj, (str, bytes, bytearray)):
                if not allow_plain_string:
                    return None
                text = obj.decode() if isinstance(obj, (bytes, bytearray)) else obj
                text = text.strip()
                return text or None

            obj_id = id(obj)
            if obj_id in visited:
                return None
            visited.add(obj_id)

            if isinstance(obj, dict):
                for key in candidates:
                    cand = obj.get(key)
                    if cand not in (None, ""):
                        return str(cand)
                for value in obj.values():
                    found = walk(value, allow_plain_string=False)
                    if found:
                        return found
                return None

            if isinstance(obj, (list, tuple, set)):
                for item in obj:
                    found = walk(item, allow_plain_string=False)
                    if found:
                        return found
                return None

            for key in candidates:
                try:
                    cand = getattr(obj, key)
                except AttributeError:
                    continue
                if cand not in (None, ""):
                    return str(cand)

            try:
                from dataclasses import asdict, is_dataclass

                if is_dataclass(obj):
                    return walk(asdict(obj), allow_plain_string=False)
            except Exception:
                pass

            to_dict = getattr(obj, "_asdict", None)
            if callable(to_dict):
                try:
                    return walk(to_dict(), allow_plain_string=False)
                except Exception:
                    pass

            if hasattr(obj, "__dict__"):
                return walk(vars(obj), allow_plain_string=False)

            return None

        return walk(response, allow_plain_string=True)

    @staticmethod
    def _normalize_status(raw: object) -> Dict[str, object]:
        def locate_payload(obj: object, visited: Set[int]) -> Optional[Dict[str, object]]:
            if obj is None:
                return None

            obj_id = id(obj)
            if obj_id in visited:
                return None
            visited.add(obj_id)

            if isinstance(obj, dict):
                status = obj.get("status") or obj.get("state") or obj.get("orderStatus")
                filled_keys = (
                    "filledAmount",
                    "filled",
                    "filledQuantity",
                    "filledSize",
                    "filledAmountQuote",
                    "filled_amount",
                    "totalFilled",
                )
                has_filled = any(key in obj for key in filled_keys) or isinstance(
                    obj.get("fills"), (list, tuple)
                )

                if status is not None or has_filled:
                    return obj

                nested_keys = (
                    "data",
                    "order",
                    "result",
                    "response",
                    "value",
                    "payload",
                )
                for key in nested_keys:
                    if key in obj:
                        payload = locate_payload(obj[key], visited)
                        if payload is not None:
                            return payload

                for value in obj.values():
                    payload = locate_payload(value, visited)
                    if payload is not None:
                        return payload
                return None

            if isinstance(obj, (list, tuple, set)):
                for item in obj:
                    payload = locate_payload(item, visited)
                    if payload is not None:
                        return payload
            return None

        payload = locate_payload(raw, set())
        if payload is None:
            raise RuntimeError(f"Unable to locate order status payload: {raw!r}")

        status_value = (
            payload.get("status")
            or payload.get("state")
            or payload.get("orderStatus")
        )
        if status_value is None:
            raise RuntimeError(f"Order status payload missing status: {payload!r}")
        def coerce_float(value: object) -> Optional[float]:
            if value in (None, ""):
                return None
            try:
                return float(value)
            except (TypeError, ValueError):
                return None

        primary_filled_keys = (
            "filledAmount",
            "filled",
            "filledQuantity",
            "filledSize",
            "filledAmountQuote",
            "filled_amount",
            "totalFilled",
            "matchedShares",
            "shares",
            "baseAmount",
        )

        filled_amount: Optional[float] = None
        filled_amount_quote: Optional[float] = None
        for key in primary_filled_keys:
            candidate = coerce_float(payload.get(key))
            if candidate is None:
                continue
            if key == "filledAmountQuote" and filled_amount_quote is None:
                filled_amount_quote = candidate
                continue
            filled_amount = candidate
            break

        price_keys = (
            "avgPrice",
            "averagePrice",
            "avg_price",
            "filledAvgPrice",
            "filledAveragePrice",
            "executionPrice",
            "averageExecutionPrice",
            "fillPrice",
            "matchedPrice",
            "price",
            "lastPrice",
            "lastTradePrice",
            "markPrice",
        )

        size_keys = (
            "size",
            "quantity",
            "qty",
            "amount",
            "filledAmount",
            "filled",
            "filledQuantity",
            "filledSize",
            "matchedShares",
            "shares",
            "baseAmount",
            "takingAmount",
            "takerAmount",
            "taker_amount",
        )

        fills_payload = payload.get("fills")
        fills_sequence = fills_payload if isinstance(fills_payload, (list, tuple)) else None

        total_from_fills = 0.0
        total_notional = 0.0

        if fills_sequence is not None:
            for entry in fills_sequence:
                if not isinstance(entry, dict):
                    continue
                size_val: Optional[float] = None
                for key in size_keys:
                    size_val = coerce_float(entry.get(key))
                    if size_val is not None and size_val > 0:
                        break
                if size_val is None or size_val <= 0:
                    continue
                total_from_fills += size_val

                price_val: Optional[float] = None
                for key in price_keys:
                    price_val = coerce_float(entry.get(key))
                    if price_val is not None:
                        break
                if price_val is not None:
                    total_notional += price_val * size_val

        if filled_amount is None:
            if total_from_fills > 0:
                filled_amount = total_from_fills
            else:
                filled_amount = 0.0

        status_upper = str(status_value).upper()

        average_price: Optional[float] = None

        for key in price_keys:
            candidate = coerce_float(payload.get(key))
            if candidate is not None:
                average_price = candidate
                break

        if average_price is None and total_from_fills > 0 and total_notional > 0:
            average_price = total_notional / total_from_fills

        if (filled_amount is None or filled_amount <= 1e-12) and average_price is not None:
            if filled_amount_quote is not None and filled_amount_quote > 0:
                filled_amount = filled_amount_quote / max(average_price, 1e-12)

        if (filled_amount is None or filled_amount <= 1e-12) and status_upper in {
            "FILLED",
            "MATCHED",
            "COMPLETED",
            "EXECUTED",
        }:
            fallback_keys = (
                "takingAmount",
                "takerAmount",
                "taker_amount",
                "size",
                "quantity",
                "qty",
                "matchedShares",
                "shares",
                "baseAmount",
            )
            for key in fallback_keys:
                candidate = coerce_float(payload.get(key))
                if candidate is not None:
                    filled_amount = candidate
                    break

        if filled_amount is None:
            raise RuntimeError(
                f"Unable to coerce filled amount to float from payload: {payload!r}"
            )

        result: Dict[str, object] = {
            "status": str(status_value),
            "filledAmount": filled_amount,
        }
        if average_price is not None:
            result["avgPrice"] = average_price
        return result


def load_default_config(path: Optional[str] = None) -> ExecutionConfig:
    """Load execution config from YAML, defaulting to ``config/trading.yaml``."""

    if path is None:
        path = str(Path(__file__).resolve().parents[1] / "config" / "trading.yaml")
    return ExecutionConfig.from_yaml(path)


__all__ = [
    "ExecutionConfig",
    "ExecutionEngine",
    "ExecutionResult",
    "PolymarketAPI",
    "ClobPolymarketAPI",
    "load_default_config",
]
