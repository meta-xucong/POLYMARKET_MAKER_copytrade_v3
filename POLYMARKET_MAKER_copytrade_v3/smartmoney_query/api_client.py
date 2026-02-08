from __future__ import annotations

import datetime as dt
import logging
from typing import Any, Dict, List

import requests

from .models import Trade

logger = logging.getLogger(__name__)


def _parse_datetime(value: Any) -> dt.datetime | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value
    try:
        if isinstance(value, (int, float)):
            if value > 1e12:
                value = value / 1000.0
            return dt.datetime.fromtimestamp(float(value), tz=dt.timezone.utc)
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            parsed = dt.datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=dt.timezone.utc)
            return parsed
    except Exception:
        return None
    return None


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _pick_first(raw: Dict[str, Any], keys: List[str]) -> Any:
    for key in keys:
        if key in raw and raw[key] is not None:
            return raw[key]
    return None


class DataApiClient:
    def __init__(
        self,
        host: str = "https://data-api.polymarket.com",
        session: requests.Session | None = None,
        timeout: float = 15.0,
    ) -> None:
        self.host = host.rstrip("/")
        self.session = session or requests.Session()
        self.timeout = timeout

    def fetch_trades(
        self,
        user: str,
        *,
        start_time: dt.datetime,
        page_size: int = 500,
        max_pages: int = 20,
        taker_only: bool = False,
    ) -> List[Trade]:
        start_ts = int(start_time.timestamp())
        end_ts = int(dt.datetime.now(tz=dt.timezone.utc).timestamp())
        url = f"{self.host}/activity"
        page_size = max(1, min(int(page_size), 500))
        max_pages = max(1, int(max_pages))
        offset = 0
        pages = 0
        trades: List[Trade] = []

        while pages < max_pages:
            params = {
                "user": user,
                "type": "TRADE",
                "limit": page_size,
                "offset": offset,
                "sortBy": "TIMESTAMP",
                "sortDirection": "DESC",
                "start": start_ts,
                "end": end_ts,
            }
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                resp.raise_for_status()
                payload = resp.json()
            except Exception as exc:
                logger.warning(
                    "fetch_trades request failed: user=%s offset=%s page_size=%s error=%s",
                    user,
                    offset,
                    page_size,
                    exc,
                )
                break

            items: List[Dict[str, Any]] = []
            if isinstance(payload, list):
                items = payload
            elif isinstance(payload, dict):
                items = payload.get("data") or payload.get("activity") or payload.get("results") or []
            if not isinstance(items, list) or not items:
                break

            for raw in items:
                if not isinstance(raw, dict):
                    continue
                if taker_only and str(raw.get("role") or "").lower() not in ("taker", ""):
                    continue
                trade = self._to_trade(raw)
                if trade is not None:
                    trades.append(trade)

            pages += 1
            offset += page_size
            if len(items) < page_size:
                break

        return trades

    def _to_trade(self, raw: Dict[str, Any]) -> Trade | None:
        side = _pick_first(raw, ["side", "takerSide", "tradeSide"])
        size = _pick_first(raw, ["size", "qty", "amount", "shares"])
        price = _pick_first(raw, ["price", "fillPrice", "avgPrice"])
        timestamp_raw = _pick_first(raw, ["timestamp", "time", "createdAt"])
        timestamp = _parse_datetime(timestamp_raw)
        if timestamp is None:
            return None
        market_id = _pick_first(raw, ["marketId", "market_id", "conditionId", "condition_id"])

        return Trade(
            side=str(side) if side is not None else None,
            size=_coerce_float(size),
            price=_coerce_float(price),
            timestamp=timestamp,
            raw=raw,
            market_id=str(market_id) if market_id is not None else None,
        )
