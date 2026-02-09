"""工具脚本：自动检测并 claim Polymarket 账户中可结算的仓位。

运行前请确保已设置以下环境变量：
- POLY_KEY / POLY_FUNDER：用于构造 CLOB 客户端的私钥与资金地址；
- （可选）POLY_API_KEY / POLY_API_SECRET：如需走 HTTP 签名兜底。

脚本逻辑：
1. 复用 `Volatility_arbitrage_main_rest.get_client()` 获取已鉴权的 `ClobClient`；
2. 先尝试调用客户端自带的 positions 查询接口，失败则回落到 HTTP 查询；
3. 从返回的仓位列表中筛选出可 claim 的市场；
4. 依次尝试通过 `client.claim_positions` 或 HTTP 接口发起 claim；
5. 打印每个市场的处理结果及累计 claim 金额。

执行方式：
>>> python Volatility_arbitrage_claim.py
"""
from __future__ import annotations

import json
import time
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode

import requests

_CLAIM_RATE_LIMIT_SEC = 1.0
_last_claim_http_ts = 0.0

def _enforce_claim_rate_limit() -> None:
    global _last_claim_http_ts
    now = time.monotonic()
    elapsed = now - _last_claim_http_ts
    remaining = _CLAIM_RATE_LIMIT_SEC - elapsed
    if remaining > 0:
        time.sleep(remaining)
    _last_claim_http_ts = time.monotonic()

from Volatility_arbitrage_main_rest import get_client
from Volatility_arbitrage_run import (
    _extract_api_creds,
    _resolve_client_host,
    _sign_payload,
)


# ---------------------------- 通用工具函数 ----------------------------


def _as_list(raw: Any) -> List[Any]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, tuple):
        return list(raw)
    return [raw]


def _to_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, str):
        val = val.strip()
        if not val:
            return None
        try:
            return float(val)
        except ValueError:
            return None
    return None


def _pick_first(mapping: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _normalize_positions(raw: Any) -> List[Dict[str, Any]]:
    """尝试将任意返回结构整理为仓位字典列表。"""
    if raw is None:
        return []
    if isinstance(raw, dict):
        for key in ("positions", "data", "results", "items", "list"):
            val = raw.get(key)
            if isinstance(val, list):
                return [item for item in val if isinstance(item, dict)]
        if all(k in raw for k in ("market", "token_id")):
            return [raw]
        # 某些接口返回 {"YES": {...}, "NO": {...}}
        if all(isinstance(v, dict) for v in raw.values()):
            return [dict(v, **{"token_side": k}) for k, v in raw.items()]
        return []
    if isinstance(raw, Iterable):
        items: List[Dict[str, Any]] = []
        for item in raw:
            if isinstance(item, dict):
                items.append(item)
        return items
    return []


def _is_claimable(position: Dict[str, Any]) -> bool:
    bool_keys = [
        "claimable",
        "isClaimable",
        "claimable_flag",
        "canClaim",
        "payoutClaimable",
        "claimableShares",
    ]
    for key in bool_keys:
        val = position.get(key)
        if isinstance(val, bool) and val:
            return True
        if isinstance(val, (int, float)) and val > 0:
            return True
        if isinstance(val, str) and val.strip().lower() in {"true", "yes", "1"}:
            return True
    numeric_keys = [
        "claimableAmount",
        "claimable_amount",
        "pendingPayout",
        "pending_payout",
        "payout",
        "amount",
        "value",
    ]
    for key in numeric_keys:
        num = _to_float(position.get(key))
        if num and num > 0:
            return True
    status = str(position.get("status") or position.get("state") or "").lower()
    if status in {"claimable", "unclaimed", "awaiting_claim", "awaiting claim"}:
        return True
    return False


def _extract_market_id(position: Dict[str, Any]) -> Optional[str]:
    market = _pick_first(
        position,
        "market",
        "market_id",
        "marketId",
        "marketSlug",
        "marketSlugId",
        "eventMarketId",
    )
    if isinstance(market, dict):
        return _pick_first(market, "id", "_id", "market", "market_id")
    return str(market) if market else None


def _extract_token_id(position: Dict[str, Any]) -> Optional[str]:
    token = _pick_first(position, "token_id", "tokenId", "token", "asset", "asset_id")
    if isinstance(token, dict):
        return _pick_first(token, "id", "token_id", "tokenId")
    if token:
        return str(token)
    side = position.get("outcome") or position.get("token_side")
    if side and isinstance(side, str):
        side = side.upper()
    yes = _pick_first(position, "yesToken", "yes_token", "yesTokenId")
    no = _pick_first(position, "noToken", "no_token", "noTokenId")
    if side == "YES" and yes:
        return str(yes)
    if side == "NO" and no:
        return str(no)
    return None


def _extract_claim_amount(position: Dict[str, Any]) -> Optional[float]:
    keys = [
        "claimableAmount",
        "claimable_amount",
        "pendingPayout",
        "pending_payout",
        "payout",
        "amount",
        "value",
    ]
    for key in keys:
        val = _to_float(position.get(key))
        if val is not None:
            return val
    return None


# ---------------------------- HTTP 请求封装 ----------------------------


def _signed_request(
    client,
    method: str,
    path: str,
    *,
    payload: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> Tuple[int, Any]:
    creds = _extract_api_creds(client)
    if not creds:
        raise RuntimeError("缺少 API Key/Secret，无法签名 HTTP 请求。")

    host = _resolve_client_host(client)
    query = ""
    if params:
        query = "?" + urlencode(params, doseq=True)
    url = f"{host}{path}{query}"

    body = ""
    if payload is not None:
        body = json.dumps(payload, separators=(",", ":"))

    ts = str(int(time.time() * 1000))
    signature_path = f"{path}{query}" if query else path
    signature = _sign_payload(creds["secret"], ts, method, signature_path, body)

    headers = {
        "Content-Type": "application/json",
        "X-API-Key": creds["key"],
        "X-API-Signature": signature,
        "X-API-Timestamp": ts,
    }

    request_fn = getattr(requests, method.lower())
    try:
        _enforce_claim_rate_limit()
        resp = request_fn(url, data=body or None, headers=headers, timeout=10)
    except Exception as exc:
        raise RuntimeError(f"请求 {url} 失败：{exc}") from exc

    try:
        data = resp.json()
    except ValueError:
        data = resp.text
    return resp.status_code, data


def _http_fetch_positions(client) -> List[Dict[str, Any]]:
    paths = [
        ("/v1/user/clob/positions", {}),
        ("/v1/user/positions", {}),
        ("/v2/user/clob/positions", {}),
        ("/v2/user/positions", {}),
    ]
    for path, params in paths:
        try:
            status, data = _signed_request(client, "GET", path, params=params)
        except RuntimeError as exc:
            print(f"[CLAIM] 通过 HTTP 获取仓位失败：{exc}")
            return []
        if status == 404:
            continue
        if status >= 500:
            print(f"[CLAIM] 服务端错误 {status}：{data}")
            continue
        if status in (401, 403):
            print(f"[CLAIM] 权限不足 {status}：{data}")
            return []
        positions = _normalize_positions(data)
        if positions:
            print(f"[CLAIM] 通过 {path} 获取到 {len(positions)} 条仓位数据。")
            return positions
        if isinstance(data, dict) and data.get("error"):
            print(f"[CLAIM] 接口 {path} 返回错误：{data}")
    return []


def _parse_claim_response(resp: Any) -> Tuple[bool, Optional[float]]:
    if resp is None:
        return False, None
    if isinstance(resp, dict):
        success_flags = {
            str(resp.get("success")).lower(),
            str(resp.get("status")).lower(),
        }
        if "true" in success_flags or "ok" in success_flags or "success" in success_flags:
            amount = _extract_claim_amount(resp)
            if amount is None:
                amount = _to_float(_pick_first(resp, "claimedAmount", "amountClaimed", "payout"))
            return True, amount
        if resp.get("error"):
            return False, None
        if "positions" in resp:
            try:
                totals = []
                for pos in _normalize_positions(resp["positions"]):
                    amt = _extract_claim_amount(pos)
                    if amt:
                        totals.append(amt)
                return bool(totals), sum(totals) if totals else None
            except Exception:
                pass
    if isinstance(resp, list):
        totals: List[float] = []
        for item in resp:
            amt = None
            if isinstance(item, dict):
                amt = _extract_claim_amount(item)
            amt = amt or _to_float(item)
            if amt:
                totals.append(amt)
        if totals:
            return True, sum(totals)
    return False, None


def _http_claim(
    client,
    market_id: str,
    token_id: Optional[str],
) -> Tuple[bool, Optional[float]]:
    payload = {"market": market_id}
    if token_id:
        payload["tokenIds"] = [token_id]

    paths = [
        "/v1/user/clob/positions/claim",
        "/v1/user/positions/claim",
        "/v2/user/clob/positions/claim",
        "/v2/user/positions/claim",
    ]
    for path in paths:
        try:
            status, data = _signed_request(client, "POST", path, payload=payload)
        except RuntimeError as exc:
            print(f"[CLAIM] 请求 {path} 失败：{exc}")
            return False, None
        if status == 404:
            continue
        print(f"[CLAIM] HTTP {path} → {status}，响应：{data}")
        if status >= 500:
            return False, None
        if status in (401, 403):
            return False, None
        success, amount = _parse_claim_response(data)
        if success:
            return True, amount
        if isinstance(data, dict) and data.get("error"):
            return False, None
    print("[CLAIM] 所有 HTTP claim 接口均不可用或返回 404。")
    return False, None


# ---------------------------- 主流程逻辑 ----------------------------


def _fetch_positions(client) -> List[Dict[str, Any]]:
    method_candidates = [
        ("list_positions", {}),
        ("get_positions", {}),
        ("fetch_positions", {}),
        ("get_user_positions", {}),
        ("list_user_positions", {}),
    ]
    for name, kwargs in method_candidates:
        fn = getattr(client, name, None)
        if not callable(fn):
            continue
        try:
            resp = fn(**kwargs)
        except TypeError:
            # 方法签名不匹配，跳过
            continue
        except Exception as exc:
            print(f"[CLAIM] 调用 client.{name} 失败：{exc}")
            continue
        positions = _normalize_positions(resp)
        if positions:
            print(f"[CLAIM] 通过 client.{name}() 获取到 {len(positions)} 条仓位数据。")
            return positions
    # 若客户端未提供接口，则尝试 HTTP 获取
    return _http_fetch_positions(client)


def _attempt_claim_via_client(
    client,
    market_id: str,
    token_id: Optional[str],
) -> Tuple[bool, Optional[float]]:
    claim_fn = getattr(client, "claim_positions", None)
    if not callable(claim_fn):
        return False, None
    kwargs = {"market": market_id}
    if token_id:
        kwargs["token_ids"] = [token_id]
    try:
        print(f"[CLAIM] 尝试调用 client.claim_positions({kwargs})…")
        resp = claim_fn(**kwargs)
    except TypeError:
        # 可能参数命名不一致，尝试常见变体
        try:
            kwargs_alt = {"market": market_id}
            if token_id:
                kwargs_alt["token_ids"] = token_id
            print(f"[CLAIM] 改用位置参数调用 client.claim_positions({kwargs_alt})…")
            resp = claim_fn(**kwargs_alt)
        except Exception as exc:
            print(f"[CLAIM] 调用 client.claim_positions 失败：{exc}")
            return False, None
    except Exception as exc:
        print(f"[CLAIM] 调用 client.claim_positions 失败：{exc}")
        return False, None

    print(f"[CLAIM] client.claim_positions 响应：{resp}")
    return _parse_claim_response(resp)


def main() -> None:
    print("[INIT] 准备检查账户可 claim 仓位…")
    client = get_client()

    positions = _fetch_positions(client)
    if not positions:
        print("[CLAIM] 未获取到任何仓位数据。")
        return

    claimable_positions = [pos for pos in positions if _is_claimable(pos)]
    if not claimable_positions:
        print("[CLAIM] 没有发现可 claim 的仓位。")
        return

    print(f"[CLAIM] 共检测到 {len(claimable_positions)} 条可 claim 仓位。")
    total_claimed = 0.0
    results: List[str] = []

    for idx, pos in enumerate(claimable_positions, start=1):
        market_id = _extract_market_id(pos)
        token_id = _extract_token_id(pos)
        amount_hint = _extract_claim_amount(pos)
        outcome = _pick_first(pos, "outcome", "side", "position_side", "token_side")
        print("-" * 60)
        print(
            f"[CLAIM] ({idx}/{len(claimable_positions)}) 市场={market_id} token={token_id} outcome={outcome} "
            f"可claim金额≈{amount_hint}"
        )
        if not market_id:
            print("[CLAIM] 缺少 market_id，跳过。")
            continue

        success, claimed_amt = _attempt_claim_via_client(client, market_id, token_id)
        if not success:
            success, claimed_amt = _http_claim(client, market_id, token_id)

        if success:
            claimed_amt = claimed_amt if claimed_amt is not None else amount_hint or 0.0
            if claimed_amt:
                total_claimed += claimed_amt
            msg = (
                f"市场 {market_id} claim 成功，token={token_id}，到账金额≈{claimed_amt if claimed_amt is not None else '未知'}"
            )
            print(f"[CLAIM] {msg}")
            results.append(msg)
        else:
            msg = f"市场 {market_id} claim 失败，token={token_id}"
            print(f"[CLAIM] {msg}")
            results.append(msg)

    print("=" * 60)
    print("[SUMMARY] 处理结果：")
    for line in results:
        print(f"  - {line}")
    print(f"[SUMMARY] 累计 claim 金额≈{total_claimed:.6f}")


if __name__ == "__main__":
    main()
