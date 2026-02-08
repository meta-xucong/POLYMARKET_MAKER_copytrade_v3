from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from smartmoney_query.api_client import DataApiClient

DEFAULT_CONFIG_PATH = Path(__file__).with_name("copytrade_config.json")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _setup_logger(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"copytrade_{time.strftime('%Y%m%d')}.log"

    logger = logging.getLogger("copytrade_run")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.handlers.clear()

    formatter = logging.Formatter("[%(asctime)s][%(levelname)s] %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def _deep_find_first(value: Any, keys: Tuple[str, ...], max_depth: int = 4) -> Any:
    if max_depth < 0:
        return None
    if isinstance(value, dict):
        for key in keys:
            if key in value and value[key] is not None:
                return value[key]
        for child in value.values():
            found = _deep_find_first(child, keys, max_depth=max_depth - 1)
            if found is not None:
                return found
    elif isinstance(value, (list, tuple)):
        for child in value:
            found = _deep_find_first(child, keys, max_depth=max_depth - 1)
            if found is not None:
                return found
    return None


def _normalize_trade(trade: Any) -> Optional[Dict[str, Any]]:
    raw_side = str(getattr(trade, "side", "") or "").upper()
    side = raw_side if raw_side in {"BUY", "SELL"} else None
    if side is None:
        return None
    size = float(getattr(trade, "size", 0.0) or 0.0)
    if size <= 0:
        return None

    raw = getattr(trade, "raw", {}) or {}
    token_id = (
        raw.get("tokenId")
        or raw.get("token_id")
        or raw.get("clobTokenId")
        or raw.get("clob_token_id")
        or raw.get("asset")
        or raw.get("assetId")
        or raw.get("asset_id")
        or raw.get("outcomeTokenId")
        or raw.get("outcome_token_id")
    )
    if token_id is None:
        token_id = _deep_find_first(
            raw,
            (
                "tokenId",
                "token_id",
                "clobTokenId",
                "clob_token_id",
                "asset",
                "assetId",
                "asset_id",
                "outcomeTokenId",
                "outcome_token_id",
            ),
        )
    if token_id is None:
        return None
    timestamp = getattr(trade, "timestamp", None)
    if timestamp is None:
        return None

    return {
        "token_id": str(token_id) if token_id is not None else None,
        "side": side,
        "size": size,
        "timestamp": timestamp,
    }


def _parse_last_seen(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            return None
    return None


def _load_token_map(path: Path) -> Dict[str, Dict[str, Any]]:
    payload = _load_json(path)
    tokens = payload.get("tokens") if isinstance(payload, dict) else []
    mapping: Dict[str, Dict[str, Any]] = {}
    if not isinstance(tokens, list):
        return mapping
    for item in tokens:
        if not isinstance(item, dict):
            continue
        token_id = item.get("token_id") or item.get("tokenId")
        if not token_id:
            continue
        key = str(token_id)
        entry = dict(item)
        entry.setdefault("introduced_by_buy", False)
        mapping[key] = entry
    return mapping


def _load_sell_signals(path: Path) -> Dict[str, Dict[str, Any]]:
    payload = _load_json(path)
    signals = payload.get("sell_tokens") if isinstance(payload, dict) else []
    mapping: Dict[str, Dict[str, Any]] = {}
    if not isinstance(signals, list):
        return mapping
    for item in signals:
        if not isinstance(item, dict):
            continue
        token_id = item.get("token_id") or item.get("tokenId")
        if not token_id:
            continue
        entry = dict(item)
        entry.setdefault("introduced_by_buy", False)
        mapping[str(token_id)] = entry
    return mapping


def _write_sell_signals(path: Path, mapping: Dict[str, Dict[str, Any]]) -> None:
    def _sort_key(entry: Dict[str, Any]) -> float:
        ts = _parse_last_seen(entry.get("last_seen"))
        return ts.timestamp() if ts else 0.0

    tokens = sorted(mapping.values(), key=_sort_key, reverse=True)
    payload = {
        "updated_at": _utc_now_iso(),
        "sell_tokens": tokens,
    }
    _write_json(path, payload)


def _write_tokens(path: Path, mapping: Dict[str, Dict[str, Any]]) -> None:
    def _sort_key(entry: Dict[str, Any]) -> float:
        ts = _parse_last_seen(entry.get("last_seen"))
        return ts.timestamp() if ts else 0.0

    tokens = sorted(mapping.values(), key=_sort_key, reverse=True)
    payload = {
        "updated_at": _utc_now_iso(),
        "tokens": tokens,
    }
    _write_json(path, payload)


def _collect_trades(
    client: DataApiClient,
    account: str,
    since_ms: int,
    min_size: float,
    logger: logging.Logger,
) -> Tuple[List[Dict[str, Any]], int]:
    start_dt = datetime.fromtimestamp(max(since_ms, 0) / 1000.0, tz=timezone.utc)
    trades = client.fetch_trades(account, start_time=start_dt, page_size=500, max_pages=5)
    actions: List[Dict[str, Any]] = []
    latest_ms = since_ms

    for trade in trades:
        raw_side = str(getattr(trade, "side", "") or "").upper()
        if raw_side and raw_side not in {"BUY", "SELL"}:
            logger.warning(
                "skip trade with unsupported side: account=%s side=%s",
                account,
                raw_side,
            )
        normalized = _normalize_trade(trade)
        if normalized is None:
            continue
        if normalized["size"] < min_size:
            continue
        ts_ms = int(normalized["timestamp"].timestamp() * 1000)
        if ts_ms <= since_ms:
            continue
        latest_ms = max(latest_ms, ts_ms)
        actions.append(normalized)

    logger.info(
        "account=%s trades=%s normalized=%s since_ms=%s",
        account,
        len(trades),
        len(actions),
        since_ms,
    )
    return actions, latest_ms


def run_once(
    config: Dict[str, Any],
    *,
    base_dir: Path,
    client: DataApiClient,
    logger: logging.Logger,
) -> None:
    poll_targets = config.get("targets") or []
    if not isinstance(poll_targets, list):
        raise ValueError("targets 必须是数组")

    token_output_path = base_dir / "tokens_from_copytrade.json"
    sell_signal_path = base_dir / "copytrade_sell_signals.json"
    state_path = base_dir / "copytrade_state.json"

    state = _load_json(state_path)
    if not isinstance(state, dict):
        state = {}
    state.setdefault("targets", {})

    token_map = _load_token_map(token_output_path)
    sell_map = _load_sell_signals(sell_signal_path)

    now_ms = int(time.time() * 1000)
    changed = False
    sell_changed = False

    for token_id, entry in list(sell_map.items()):
        token_entry = token_map.get(token_id)
        if not token_entry or not token_entry.get("introduced_by_buy", False):
            del sell_map[token_id]
            sell_changed = True
            continue
        if not entry.get("introduced_by_buy", False):
            entry["introduced_by_buy"] = True
            sell_changed = True

    for target in poll_targets:
        if not isinstance(target, dict):
            continue
        if target.get("enabled", True) is False:
            continue
        account = str(target.get("account") or "").strip()
        if not account:
            continue
        min_size = float(target.get("min_size", 0.0) or 0.0)
        target_state = state["targets"].get(account, {})
        since_ms = int(target_state.get("last_timestamp_ms") or 0)
        if since_ms <= 0:
            init_ms = now_ms
            state["targets"][account] = {
                "last_timestamp_ms": init_ms,
                "updated_at": _utc_now_iso(),
            }
            logger.info("初始化目标账户状态，忽略已有仓位: account=%s", account)
            continue

        actions, latest_ms = _collect_trades(client, account, since_ms, min_size, logger)
        if latest_ms > since_ms:
            state["targets"][account] = {
                "last_timestamp_ms": latest_ms,
                "updated_at": _utc_now_iso(),
            }
        for action in actions:
            token_id = action.get("token_id")
            if not token_id:
                continue
            key = str(token_id)

            last_seen = action["timestamp"].astimezone(timezone.utc).isoformat().replace(
                "+00:00", "Z"
            )
            existing = token_map.get(key)
            new_entry = {
                "token_id": token_id,
                "source_account": account,
                "last_seen": last_seen,
            }
            # 保留 existing 的 introduced_by_buy 标记，避免被覆盖丢失
            if existing and existing.get("introduced_by_buy", False):
                new_entry["introduced_by_buy"] = True

            if existing:
                existing_ts = _parse_last_seen(existing.get("last_seen"))
                new_ts = _parse_last_seen(last_seen)
                if existing_ts is None or (new_ts and new_ts >= existing_ts):
                    token_map[key] = new_entry
                    changed = True
            else:
                token_map[key] = new_entry
                changed = True

            if action.get("side") == "BUY":
                if not token_map[key].get("introduced_by_buy", False):
                    token_map[key]["introduced_by_buy"] = True
                    changed = True

            if action.get("side") == "SELL":
                if not token_map.get(key, {}).get("introduced_by_buy", False):
                    logger.info(
                        "skip sell signal before buy introduction: account=%s token=%s",
                        account,
                        token_id,
                    )
                    continue
                sell_entry = {
                    "token_id": token_id,
                    "source_account": account,
                    "last_seen": last_seen,
                    "introduced_by_buy": True,
                }
                existing_sell = sell_map.get(key)
                existing_ts = _parse_last_seen(
                    existing_sell.get("last_seen") if existing_sell else None
                )
                new_ts = _parse_last_seen(last_seen)
                if existing_ts is None or (new_ts and new_ts >= existing_ts):
                    sell_map[key] = sell_entry
                    sell_changed = True

    if changed:
        _write_tokens(token_output_path, token_map)
        logger.info("tokens output updated: %s (total=%s)", token_output_path, len(token_map))
    else:
        logger.info("no token updates, total=%s", len(token_map))

    if sell_changed:
        _write_sell_signals(sell_signal_path, sell_map)
        logger.info(
            "sell signal output updated: %s (total=%s)",
            sell_signal_path,
            len(sell_map),
        )

    _write_json(state_path, state)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Copytrade watcher")
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="copytrade_config.json 路径",
    )
    parser.add_argument("--once", action="store_true", help="仅执行一次抓取")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    config_path = args.config
    if not config_path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    config = _load_json(config_path)
    if not isinstance(config, dict):
        raise ValueError("配置文件必须是 JSON 对象")

    base_dir = config_path.parent
    log_dir = base_dir / "logs"
    logger = _setup_logger(log_dir)

    poll_interval = float(config.get("poll_interval_sec", 30))
    client = DataApiClient()

    logger.info("copytrade 启动 | poll_interval=%ss", poll_interval)
    while True:
        try:
            run_once(config, base_dir=base_dir, client=client, logger=logger)
        except Exception as exc:
            logger.exception("copytrade 运行异常: %s", exc)
        if args.once:
            break
        time.sleep(max(1.0, poll_interval))


if __name__ == "__main__":
    main()
