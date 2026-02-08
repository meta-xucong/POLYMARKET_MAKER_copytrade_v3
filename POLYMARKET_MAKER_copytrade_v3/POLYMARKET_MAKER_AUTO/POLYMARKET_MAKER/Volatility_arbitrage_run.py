# Volatility_arbitrage_run.py
# -*- coding: utf-8 -*-
"""
运行入口（循环策略版）：
- 事件页 /event/<slug>：列出子问题并选择（与老版一致）。
- 新增：
  1) 交互输入：买入份数（留空按 $1 反推）、跌幅窗口/阈值、盈利百分比、可选买入触发价；
  2) 基于 `VolArbStrategy` 的循环状态机：跌幅触发买入 → 成交确认 → 盈利达标卖出；
  3) 成交回调推进状态机，可重复执行买卖循环；
  4) 支持 stop 指令和市场关闭检测，安全退出。
"""
from __future__ import annotations
import sys
import os  # ✅ 修复：必须在使用os.fdopen之前import os

# ✅ P0修复：设置行缓冲输出，确保日志立即写入文件
# 问题：默认的全缓冲模式导致日志滞留在缓冲区，造成"卡死"假象
# 解决：强制行缓冲，每条print立即flush到日志文件
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
elif hasattr(sys.stdout, 'buffer'):
    # Python 3.7+ 兼容方案
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)
    sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', buffering=1)
import time
import threading
import re
import hmac
import hashlib
import json
import inspect
from queue import Queue, Empty
from collections import deque
from typing import Dict, Any, Tuple, List, Optional, Deque
import math
from decimal import Decimal, ROUND_UP, ROUND_DOWN
import requests
from pathlib import Path
from datetime import datetime, timezone, timedelta, date, time as dtime
from json import JSONDecodeError
try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except Exception:  # pragma: no cover - 兼容无 zoneinfo 的环境
    ZoneInfo = None  # type: ignore
    class ZoneInfoNotFoundError(Exception):
        pass
from Volatility_arbitrage_strategy import (
    StrategyConfig,
    VolArbStrategy,
    ActionType,
    Action,
)
from maker_execution import (
    maker_buy_follow_bid,
    maker_sell_follow_ask_with_floor_wait,
    _fetch_best_price,
    OrderbookNotFoundError,
    PriceNoneStreakError,
    set_price_none_exit_threshold,
)

# ========== 错误日志记录函数 ==========
def _log_error(error_type: str, error_data: Dict[str, Any]) -> None:
    """
    记录错误到独立的错误日志文件。

    :param error_type: 错误类型标识（如 STARTUP_TIMEOUT, WS_ERROR 等）
    :param error_data: 错误相关数据（字典格式）
    """
    try:
        # 确定错误日志文件路径
        script_dir = Path(__file__).parent.parent
        log_dir = script_dir / "logs" / "autorun"
        log_dir.mkdir(parents=True, exist_ok=True)
        error_log_path = log_dir / "error_log.txt"

        # 构建日志条目
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        log_entry = {
            "timestamp": timestamp,
            "error_type": error_type,
            "data": error_data
        }

        # 追加写入日志文件
        with open(error_log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")

    except Exception as e:
        # 错误日志记录失败时，仅打印到控制台，不中断程序
        print(f"[ERROR_LOG] 写入错误日志失败: {e}")


# ========== 退出token记录函数 ==========
def _record_exit_token(token_id: str, exit_reason: str, exit_data: Optional[Dict[str, Any]] = None) -> None:
    """
    记录因各种原因退出maker交易的token。

    用于 Slot Refill (回填) 功能：记录退出的token及其状态，以便后续判断是否可以重新加入maker队列。

    :param token_id: token ID
    :param exit_reason: 退出原因（如 STARTUP_TIMEOUT, MARKET_CLOSED, SIGNAL_TIMEOUT 等）
    :param exit_data: 退出相关数据（可选），应包含以下字段便于回填判断：
        - has_position: bool - 是否有持仓
        - position_size: float - 持仓数量
        - entry_price: float - 买入均价
        - last_bid: float - 最后bid价格
        - last_ask: float - 最后ask价格
    """
    # 不可回填的退出原因
    NON_REFILLABLE_REASONS = {
        "MARKET_CLOSED",
        "USER_STOPPED",
        "DEADLINE_REACHED",
    }

    try:
        # 确定退出记录文件路径
        script_dir = Path(__file__).parent.parent
        data_dir = script_dir / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        exit_record_path = data_dir / "exit_tokens.json"

        # 构建记录条目（增强版：添加 exit_ts 和 refillable 字段）
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        record = {
            "timestamp": timestamp,
            "exit_ts": time.time(),  # 精确时间戳，用于冷却时间计算
            "token_id": token_id,
            "exit_reason": exit_reason,
            "exit_data": exit_data or {},
            "refillable": exit_reason not in NON_REFILLABLE_REASONS,  # 是否可回填
        }

        # 读取现有记录（如果存在）
        existing_records = []
        if exit_record_path.exists():
            try:
                with open(exit_record_path, "r", encoding="utf-8") as f:
                    existing_records = json.load(f)
                if not isinstance(existing_records, list):
                    existing_records = []
            except (json.JSONDecodeError, OSError):
                existing_records = []

        # 添加新记录
        existing_records.append(record)

        # 只保留最近1000条记录（避免文件过大）
        if len(existing_records) > 1000:
            existing_records = existing_records[-1000:]

        # 原子写入
        tmp_path = exit_record_path.with_suffix('.tmp')
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(existing_records, f, ensure_ascii=False, indent=2)
        tmp_path.replace(exit_record_path)

        refill_hint = "可回填" if record["refillable"] else "不可回填"
        print(f"[EXIT_RECORD] 已记录退出token: {token_id[:20]}... 原因: {exit_reason} ({refill_hint})")

    except Exception as e:
        # 记录失败时，仅打印到控制台，不中断程序
        print(f"[EXIT_RECORD] 写入退出记录失败: {e}")

# ========== 1) Client：优先 ws 版，回退 rest 版 ==========
def _get_client():
    try:
        from Volatility_arbitrage_main_ws import get_client  # 优先
        return get_client()
    except Exception as e1:
        try:
            from Volatility_arbitrage_main_rest import get_client  # 退回
            return get_client()
        except Exception as e2:
            print("[ERR] 无法导入 get_client：", e1, "|", e2)
            _log_error("IMPORT_ERROR", {
                "module": "get_client",
                "error_ws": str(e1),
                "error_rest": str(e2),
                "message": "无法导入 get_client 模块"
            })
            sys.exit(1)

# ========== 2) 行情订阅（未动） ==========
try:
    from Volatility_arbitrage_main_ws import ws_watch_by_ids
except Exception as e:
    print("[ERR] 无法从 Volatility_arbitrage_main_ws 导入 ws_watch_by_ids：", e)
    _log_error("IMPORT_ERROR", {
        "module": "ws_watch_by_ids",
        "error": str(e),
        "message": "无法导入 ws_watch_by_ids 模块"
    })
    sys.exit(1)

CLOB_API_HOST = "https://clob.polymarket.com"
GAMMA_ROOT = os.getenv("POLY_GAMMA_ROOT", "https://gamma-api.polymarket.com")
DATA_API_ROOT = os.getenv("POLY_DATA_API_ROOT", "https://data-api.polymarket.com")
API_MIN_ORDER_SIZE = 5.0
# P0修复：将过期阈值从5秒放宽到60秒
# 原因：5秒太严格，导致从主循环到买入流程之间的正常延迟就会让快照过期
# 60秒容忍短时抖动与API延迟，降低噪声，同时保持陈旧数据检测能力
ORDERBOOK_STALE_AFTER_SEC = 60.0
# WS+REST 最终价格连续 None 的硬编码退出阈值（0=禁用）
PRICE_NONE_EXIT_COUNT = 100
POSITION_SYNC_INTERVAL = 60.0
POST_BUY_POSITION_CHECK_DELAY = 60.0
POST_BUY_POSITION_CHECK_ATTEMPTS = 5
POST_BUY_POSITION_CHECK_INTERVAL = 7.0
POST_BUY_POSITION_CHECK_ROUND_COOLDOWN = 60.0
POST_BUY_POSITION_CONFIRM_MAX_ROUNDS = 10  # 最大重试轮数，防止无限循环
POST_BUY_POSITION_MATCH_REL_TOL = 1e-4
POST_BUY_POSITION_MATCH_ABS_TOL = 1e-6


_REQUEST_RATE_LIMIT_SEC = 1.0
_last_request_ts = 0.0
_request_lock = threading.Lock()

DEFAULT_RUN_CONFIG_PATH = os.getenv(
    "POLY_RUN_CONFIG_PATH",
    os.path.join(os.path.dirname(__file__), "config", "run_params.json"),
)

DEFAULT_DEADLINE_FALLBACK = {
    "time": "12:59",
    "timezone": "America/New_York",
    "fallback_offset": -240,
}

def _enforce_request_rate_limit() -> None:
    global _last_request_ts
    with _request_lock:
        now = time.monotonic()
        elapsed = now - _last_request_ts
        remaining = _REQUEST_RATE_LIMIT_SEC - elapsed
        if remaining > 0:
            time.sleep(remaining)
        _last_request_ts = time.monotonic()


def _safe_load_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = json.load(f)
            if isinstance(content, dict):
                return content
            print(f"[WARN] 配置文件 {path} 顶层应为对象，已忽略。")
            return {}
    except FileNotFoundError:
        print(f"[WARN] 未找到配置文件 {path}，将使用内置默认值。")
        return {}
    except JSONDecodeError as exc:
        print(f"[ERR] 读取配置 {path} 失败：{exc}")
        sys.exit(1)


def _coerce_float(val: Any) -> Optional[float]:
    try:
        if val is None:
            return None
        return float(val)
    except (TypeError, ValueError):
        return None


def _normalize_ratio(val: Any, default: float) -> float:
    parsed = _coerce_float(val)
    if parsed is None:
        return default
    if parsed > 1:
        parsed = parsed / 100.0
    if parsed < 0:
        print(f"[WARN] 百分比 {val} 不能为负，已回退到默认值 {default}。")
        return default
    return parsed


def _load_run_config(path: Optional[str]) -> Dict[str, Any]:
    candidate = path or DEFAULT_RUN_CONFIG_PATH
    if not os.path.isabs(candidate):
        candidate = os.path.abspath(candidate)
    print(f"[CONFIG] 使用配置文件: {candidate}")
    return _safe_load_json(candidate)

def _strategy_accepts_total_position(strategy: VolArbStrategy) -> bool:
    """Return True when ``strategy.on_buy_filled`` can consume ``total_position``."""

    handler = getattr(strategy, "on_buy_filled", None)
    if handler is None or not callable(handler):
        return False

    try:
        signature = inspect.signature(handler)
    except (TypeError, ValueError):
        return False

    for param in signature.parameters.values():
        if param.kind == inspect.Parameter.VAR_KEYWORD:
            return True
    return "total_position" in signature.parameters

def _extract_market_slug(s: str) -> str:
    m = re.search(r"/market/([^/?#]+)", s)
    if m:
        return m.group(1)
    s = s.strip()
    if s and ("/" not in s) and ("?" not in s) and ("&" not in s):
        return s
    return ""


_TEXT_TIMEZONE_REGEXES = [
    (re.compile(r"\b(?:u\.s\.|us)?\s*eastern(?:\s+(?:standard|daylight))?\s+time\b", re.I), "America/New_York"),
    (re.compile(r"\b(?:et|est|edt)\b", re.I), "America/New_York"),
    (re.compile(r"\b(?:u\.s\.|us)?\s*central(?:\s+(?:standard|daylight))?\s+time\b", re.I), "America/Chicago"),
    (re.compile(r"\b(?:ct|cst|cdt)\b", re.I), "America/Chicago"),
    (re.compile(r"\b(?:u\.s\.|us)?\s*pacific(?:\s+(?:standard|daylight))?\s+time\b", re.I), "America/Los_Angeles"),
    (re.compile(r"\b(?:pt|pst|pdt)\b", re.I), "America/Los_Angeles"),
    (re.compile(r"\b(?:mountain|mt|mst|mdt)\s+time\b", re.I), "America/Denver"),
]

_JSON_LIKE_PREFIX_RE = re.compile(r"^\s*[\[{]")

_TIME_COMPONENT_RE = re.compile(r"(\d{1,2}):(\d{2})(?::(\d{2}))?")


def _describe_timezone_hint(hint: Any) -> str:
    if hint is None:
        return ""
    if isinstance(hint, dict) and "offset_minutes" in hint:
        try:
            minutes = float(hint["offset_minutes"])
        except (TypeError, ValueError):
            return str(hint)
        sign = "+" if minutes >= 0 else "-"
        mins = abs(int(minutes))
        hours, remain = divmod(mins, 60)
        return f"UTC{sign}{hours:02d}:{remain:02d}"
    return str(hint)


def _timezone_from_hint(hint: Any) -> Optional[timezone]:
    if hint is None:
        return None
    if isinstance(hint, dict) and "offset_minutes" in hint:
        try:
            minutes = float(hint["offset_minutes"])
        except (TypeError, ValueError):
            return None
        return timezone(timedelta(minutes=minutes))
    if isinstance(hint, (int, float)):
        return timezone(timedelta(minutes=float(hint)))

    text = str(hint).strip()
    if not text:
        return None

    lowered = text.lower()
    keyword_map = {
        "et": "America/New_York",
        "est": "America/New_York",
        "edt": "America/New_York",
        "eastern": "America/New_York",
        "ct": "America/Chicago",
        "cst": "America/Chicago",
        "cdt": "America/Chicago",
        "pt": "America/Los_Angeles",
        "pst": "America/Los_Angeles",
        "pdt": "America/Los_Angeles",
    }
    canonical = keyword_map.get(lowered)
    if ZoneInfo and canonical:
        try:
            return ZoneInfo(canonical)
        except ZoneInfoNotFoundError:
            pass
    if canonical and not ZoneInfo:
        # zoneinfo 不可用时退化为标准时区（不考虑夏令时）
        offsets = {
            "America/New_York": -300,
            "America/Chicago": -360,
            "America/Los_Angeles": -480,
        }
        minutes = offsets.get(canonical)
        if minutes is not None:
            return timezone(timedelta(minutes=minutes))

    # UTC±HH:MM 或 ±HH:MM
    m = re.match(r"^(?:utc)?\s*([+-])\s*(\d{1,2})(?::?(\d{2}))?$", lowered)
    if m:
        sign = 1 if m.group(1) == "+" else -1
        hours = int(m.group(2))
        mins = int(m.group(3) or 0)
        total = sign * (hours * 60 + mins)
        return timezone(timedelta(minutes=total))

    # 纯数字：视为分钟
    if lowered.replace(".", "", 1).lstrip("+-").isdigit():
        try:
            value = float(lowered)
        except ValueError:
            return None
        # 数值 <= 24 视为小时，否则视为分钟
        minutes = value * 60 if abs(value) <= 24 else value
        return timezone(timedelta(minutes=minutes))

    if ZoneInfo:
        try:
            return ZoneInfo(text)
        except ZoneInfoNotFoundError:
            return None
    return None


def _timezone_hint_from_text_block(block: Any) -> Optional[str]:
    """Parse free-text fields (e.g. rules) to infer well-known timezones."""

    if block is None:
        return None
    if isinstance(block, str):
        lowered = block.lower()
        for regex, zone in _TEXT_TIMEZONE_REGEXES:
            if regex.search(lowered):
                return zone
        return None
    if isinstance(block, dict):
        for value in block.values():
            hint = _timezone_hint_from_text_block(value)
            if hint:
                return hint
        return None
    if isinstance(block, (list, tuple)):
        for item in block:
            hint = _timezone_hint_from_text_block(item)
            if hint:
                return hint
    return None


def _parse_json_like_string(source: Any) -> Optional[Any]:
    if not isinstance(source, str):
        return None
    if not _JSON_LIKE_PREFIX_RE.match(source):
        return None
    try:
        return json.loads(source)
    except (ValueError, JSONDecodeError, TypeError):
        return None


def _infer_timezone_hint(obj: Any) -> Optional[Any]:
    direct_keys = (
        "eventTimezone",
        "event_timezone",
        "timezone",
        "timeZone",
        "marketTimezone",
        "timezoneName",
        "timezone_name",
        "timezoneShort",
        "timezone_short",
    )
    offset_keys = (
        "timezoneOffsetMinutes",
        "timezone_offset_minutes",
        "timezoneOffset",
        "timezone_offset",
        "eventTimezoneOffsetMinutes",
    )
    text_keys = (
        "rules",
        "resolutionRules",
        "resolutionCriteria",
        "resolutionDescription",
        "resolutionSources",
        "resolutionSource",
        "description",
        "details",
        "notes",
        "info",
        "longDescription",
        "shortDescription",
        "question",
        "title",
        "subtitle",
        "body",
        "text",
    )
    nested_keys = (
        "event",
        "parentMarket",
        "eventInfo",
        "collection",
        "extraInfo",
        "metadata",
        "meta",
    )

    seen: set[int] = set()

    def _scan(value: Any) -> Optional[Any]:
        if isinstance(value, dict):
            oid = id(value)
            if oid in seen:
                return None
            seen.add(oid)
            for key in direct_keys:
                val = value.get(key)
                if isinstance(val, str) and val.strip():
                    return val.strip()
            for key in offset_keys:
                val = value.get(key)
                if isinstance(val, (int, float)):
                    return {"offset_minutes": float(val)}
            for key in text_keys:
                if key in value:
                    hint = _timezone_hint_from_text_block(value.get(key))
                    if hint:
                        return hint
            for key in nested_keys:
                if key in value:
                    hint = _scan(value.get(key))
                    if hint:
                        return hint
            for val in value.values():
                hint = _scan(val)
                if hint:
                    return hint
            return None
        if isinstance(value, (list, tuple)):
            for item in value:
                hint = _scan(item)
                if hint:
                    return hint
            return None
        if isinstance(value, str):
            parsed = _parse_json_like_string(value)
            if parsed is not None:
                hint = _scan(parsed)
                if hint:
                    return hint
            return _timezone_hint_from_text_block(value)
        return None

    return _scan(obj) if isinstance(obj, (dict, list, tuple)) else None


def _parse_timestamp(val: Any, timezone_hint: Optional[Any] = None) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        ts = float(val)
        if ts > 1e12:
            ts = ts / 1000.0
        return ts
    if isinstance(val, str):
        raw = val.strip()
        if not raw:
            return None
        try:
            ts = float(raw)
            if ts > 1e12:
                ts = ts / 1000.0
            return ts
        except ValueError:
            pass
        iso = raw.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(iso)
            tzinfo = dt.tzinfo
            if tzinfo is None:
                tzinfo = _timezone_from_hint(timezone_hint) or timezone.utc
                dt = dt.replace(tzinfo=tzinfo)
            return dt.astimezone(timezone.utc).timestamp()
        except ValueError:
            pass
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d",
        ):
            try:
                dt = datetime.strptime(raw, fmt)
                tzinfo = _timezone_from_hint(timezone_hint) or timezone.utc
                dt = dt.replace(tzinfo=tzinfo)
                return dt.astimezone(timezone.utc).timestamp()
            except ValueError:
                continue
    return None


def _value_has_meaningful_time_component(value: Any) -> bool:
    if isinstance(value, (int, float)):
        return True
    if not isinstance(value, str):
        return False
    match = _TIME_COMPONENT_RE.search(value)
    if not match:
        return False
    try:
        hour = int(match.group(1) or 0)
        minute = int(match.group(2) or 0)
        second = int(match.group(3) or 0)
    except (TypeError, ValueError):
        return False
    return not (hour == 0 and minute == 0 and second == 0)


def _get_zoneinfo_or_fallback(name: str, fallback_offset_minutes: int) -> timezone:
    if ZoneInfo:
        try:
            return ZoneInfo(name)  # type: ignore[arg-type]
        except ZoneInfoNotFoundError:
            pass
    return timezone(timedelta(minutes=fallback_offset_minutes))


def _parse_time_of_day_spec(spec: Dict[str, Any]) -> Tuple[int, int, int]:
    hour = 0
    minute = 0
    second = 0

    time_field = spec.get("time")
    if isinstance(time_field, str):
        m = re.match(r"\s*(\d{1,2})[:\.]?(\d{2})?(?:[:\.]?(\d{2}))?", time_field)
        if m:
            hour = int(m.group(1) or 0)
            minute = int(m.group(2) or 0)
            second = int(m.group(3) or 0)

    hour = int(spec.get("hour", hour) or hour)
    minute = int(spec.get("minute", minute) or minute)
    second = int(spec.get("second", second) or second)

    hour = max(0, min(23, hour))
    minute = max(0, min(59, minute))
    second = max(0, min(59, second))
    return hour, minute, second


def _apply_manual_deadline_override_meta(
    meta: Optional[Dict[str, Any]], override_ts: Optional[float]
) -> Dict[str, Any]:
    base_meta: Dict[str, Any] = dict(meta or {})
    if not override_ts:
        return base_meta
    if base_meta.get("resolved_ts"):
        return base_meta
    base_meta["end_ts"] = override_ts
    base_meta["end_ts_precise"] = True
    return base_meta


def _should_offer_common_deadline_options(meta: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(meta, dict):
        return False
    if meta.get("resolved_ts"):
        return False
    end_ts = meta.get("end_ts")
    if not isinstance(end_ts, (int, float)):
        return False
    return not bool(meta.get("end_ts_precise"))


def _common_deadline_override(
    base_date_utc: date, choice: Any, tz_hint: Optional[Any]
) -> Tuple[Optional[float], bool]:
    options = {
        "1": {"label": "12:00 PM ET", "hour": 12, "minute": 0, "tz": "America/New_York", "fallback": -240},
        "2": {"label": "23:59 ET", "hour": 23, "minute": 59, "tz": "America/New_York", "fallback": -240},
        "3": {"label": "00:00 UTC", "hour": 0, "minute": 0, "tz": "UTC", "fallback": 0},
        "4": {"label": "不设定结束时间点", "no_deadline": True},
    }
    key = str(choice)
    if key not in options:
        return None, False
    spec = options[key]
    if spec.get("no_deadline"):
        return None, True
    tz_name = tz_hint or spec.get("tz") or "UTC"
    tzinfo = _get_zoneinfo_or_fallback(tz_name, spec.get("fallback", 0))
    dt_target = datetime.combine(
        base_date_utc,
        dtime(hour=spec.get("hour", 0), minute=spec.get("minute", 0)),
        tzinfo=tzinfo,
    )
    return dt_target.astimezone(timezone.utc).timestamp(), False


def _prompt_common_deadline_override(
    base_date_utc: date,
    *,
    allow_skip: bool = False,
    intro_text: Optional[str] = None,
) -> Tuple[Optional[float], bool]:
    if intro_text:
        print(intro_text)
    else:
        print(
            "[WARN] 自动获取的截止时间缺少明确的时区/时刻信息，",
            "将基于事件标注日期",
            f" {base_date_utc.isoformat()} 进行人工选择。",
        )
        print(
            "请选择常用结束时间点（默认 1）：\\n",
            "  [1] 12:00 PM ET（金融 / 市场预测类）\\n",
            "  [2] 23:59 ET（天气 / 逐日统计类）\\n",
            "  [3] UTC 00:00（跨时区国际事件）\\n",
            "  [4] 不设定结束时间点",
        )
    options = {
        "1": {"label": "12:00 PM ET", "hour": 12, "minute": 0, "tz": "America/New_York", "fallback": -240},
        "2": {"label": "23:59 ET", "hour": 23, "minute": 59, "tz": "America/New_York", "fallback": -240},
        "3": {"label": "00:00 UTC", "hour": 0, "minute": 0, "tz": "UTC", "fallback": 0},
        "4": {"label": "不设定结束时间点", "no_deadline": True},
    }
    if allow_skip:
        print(
            "如需使用以上常用时间点覆盖市场截止时间，请输入编号；"
            "直接回车则沿用自动识别的截止日期。"
        )

    choice = ""
    while choice not in options:
        raw = input().strip()
        if allow_skip and not raw:
            print("[INFO] 已沿用自动识别的截止时间。")
            return None, False
        choice = raw or "1"
        if choice not in options:
            print("[ERR] 请输入 1、2、3 或 4：")
    spec = options[choice]
    if spec.get("no_deadline"):
        print("[INFO] 已选择不设定结束时间点，将跳过截止时间校验与倒计时。")
        return None, True
    tzinfo = (
        timezone.utc
        if spec["tz"] == "UTC"
        else _get_zoneinfo_or_fallback(spec["tz"], spec["fallback"])
    )
    local_dt = datetime.combine(
        base_date_utc,
        dtime(hour=spec["hour"], minute=spec["minute"]),
    ).replace(tzinfo=tzinfo)
    utc_dt = local_dt.astimezone(timezone.utc)
    print(
        "[INFO] 已手动指定该市场监控截止为 "
        f"{local_dt.isoformat()} ({spec['label']})，即 UTC {utc_dt.isoformat()}。",
    )
    return utc_dt.timestamp(), False


def _default_deadline_ts(
    base_date_utc: date,
    default_spec: Optional[Dict[str, Any]],
    tz_hint: Optional[Any],
) -> Optional[float]:
    spec: Dict[str, Any] = dict(DEFAULT_DEADLINE_FALLBACK)
    if isinstance(default_spec, dict):
        spec.update({k: v for k, v in default_spec.items() if v is not None})

    hour, minute, second = _parse_time_of_day_spec(spec)
    tz_name = spec.get("timezone") or tz_hint or DEFAULT_DEADLINE_FALLBACK["timezone"]
    fallback_offset = int(_coerce_float(spec.get("fallback_offset")) or 0)
    tzinfo = (
        timezone.utc
        if str(tz_name).upper() == "UTC"
        else _get_zoneinfo_or_fallback(str(tz_name), fallback_offset)
    )
    dt_target = datetime.combine(
        base_date_utc, dtime(hour=hour, minute=minute, second=second), tzinfo=tzinfo
    )
    return dt_target.astimezone(timezone.utc).timestamp()

def _count_decimal_places(value: Any) -> Optional[int]:
    try:
        dec = Decimal(str(value))
    except Exception:
        return None
    dec = dec.normalize()
    exponent = dec.as_tuple().exponent
    if exponent >= 0:
        return 0
    return abs(int(exponent))


def _infer_market_price_precision_from_raw(raw: Any) -> Optional[int]:
    if not isinstance(raw, dict):
        return None

    def _normalize_candidate(val: Any) -> Optional[int]:
        if val is None:
            return None
        try:
            if isinstance(val, (int, float)) and val > 0 and val < 1:
                dp = _count_decimal_places(val)
                return dp
            cand_int = int(val)
            if cand_int >= 0:
                return cand_int
        except Exception:
            return None
        return None

    primary_keys = (
        "price_precision",
        "pricePrecision",
        "priceDecimals",
        "displayPriceDecimalPrecision",
        "displayPriceDecimals",
        "priceDecimal",
        "priceDecimalPlaces",
    )
    for key in primary_keys:
        candidate = _normalize_candidate(raw.get(key))
        if candidate:
            return candidate

    tick_keys = (
        "priceTick",
        "tickSize",
        "tick",
        "priceIncrement",
        "minimumPriceIncrement",
    )
    for key in tick_keys:
        candidate = _normalize_candidate(raw.get(key))
        if candidate:
            return candidate

    nested_lists = ("outcomes", "tokens", "clobTokens", "clobTokenIds")
    for k in nested_lists:
        seq = raw.get(k)
        if not isinstance(seq, list):
            continue
        for item in seq:
            if not isinstance(item, dict):
                continue
            nested_candidate = _infer_market_price_precision_from_raw(item)
            if nested_candidate:
                return nested_candidate
    return None


def _infer_market_price_precision(meta: Optional[Dict[str, Any]]) -> Optional[int]:
    if not isinstance(meta, dict):
        return None
    if "price_precision" in meta:
        try:
            cand = int(meta["price_precision"])
            if cand >= 0:
                return cand
        except Exception:
            pass
    raw_meta = meta.get("raw") if isinstance(meta, dict) else None
    return _infer_market_price_precision_from_raw(raw_meta)


def _market_meta_from_obj(m: dict, timezone_override: Optional[Any] = None) -> Dict[str, Any]:
    meta: Dict[str, Any] = {}
    if not isinstance(m, dict):
        return meta
    meta["slug"] = m.get("slug") or m.get("marketSlug") or m.get("market_slug")
    meta["market_id"] = (
        m.get("marketId")
        or m.get("id")
        or m.get("market_id")
        or m.get("conditionId")
        or m.get("condition_id")
    )

    tz_hint = timezone_override if timezone_override is not None else _infer_timezone_hint(m)
    if tz_hint:
        meta["timezone_hint"] = tz_hint

    end_keys = (
        "endDate",
        "endTime",
        "closeTime",
        "closeDate",
        "closedTime",
        "expiry",
        "expirationTime",
    )
    for key in end_keys:
        raw_value = m.get(key)
        ts = _parse_timestamp(raw_value, tz_hint)
        if ts:
            meta["end_ts"] = ts
            meta["end_ts_precise"] = _value_has_meaningful_time_component(raw_value)
            break

    resolve_keys = (
        "resolvedTime",
        "resolutionTime",
        "resolveTime",
        "resolvedAt",
        "finalizationTime",
        "finalizedTime",
        "settlementTime",
    )
    for key in resolve_keys:
        ts = _parse_timestamp(m.get(key), tz_hint)
        if ts:
            meta["resolved_ts"] = ts
            break

    if "end_ts" not in meta and "resolved_ts" in meta:
        meta["end_ts"] = meta["resolved_ts"]

    precision = _infer_market_price_precision_from_raw(m)
    if precision is not None:
        meta["price_precision"] = precision

    meta["raw"] = m
    return meta


def _apply_timezone_override_meta(
    meta: Optional[Dict[str, Any]], override_hint: Optional[Any]
) -> Dict[str, Any]:
    """Rebuild meta info with the provided timezone override when possible."""

    base_meta: Dict[str, Any] = dict(meta or {})
    if not override_hint:
        return base_meta

    raw_meta = base_meta.get("raw") if isinstance(base_meta, dict) else None
    if isinstance(raw_meta, dict):
        return _market_meta_from_obj(raw_meta, override_hint)

    base_meta["timezone_hint"] = override_hint
    return base_meta


def _maybe_fetch_market_meta_from_source(source: str) -> Dict[str, Any]:
    slug = _extract_market_slug(source)
    if not slug:
        return {}
    m = _fetch_market_by_slug(slug)
    if m:
        return _market_meta_from_obj(m)
    return {}


def _market_has_ended(meta: Dict[str, Any], now: Optional[float] = None) -> bool:
    if not meta:
        return False
    if now is None:
        now = time.time()
    candidates: List[float] = []
    for key in ("resolved_ts", "end_ts"):
        ts = meta.get(key)
        if isinstance(ts, (int, float)):
            candidates.append(float(ts))
    if not candidates:
        return False
    return now >= min(candidates)


def _extract_position_size(status: Dict[str, Any]) -> float:
    if not isinstance(status, dict):
        return 0.0
    for key in ("position_size", "position", "size"):
        val = status.get(key)
        if val is None:
            continue
        try:
            size = float(val)
            if size > 0:
                return size
        except (TypeError, ValueError):
            continue
    return 0.0


def _merge_remote_position_size(
    current_size: Optional[float],
    remote_size: Optional[float],
    *,
    eps: float = 1e-6,
    dust_floor: Optional[float] = None,
) -> Tuple[Optional[float], bool]:
    """Return normalized remote size and whether it differs from the current state."""

    def _normalize(value: Optional[float], *, apply_dust: bool) -> Optional[float]:
        if value is None:
            return None
        try:
            normalized = float(value)
        except (TypeError, ValueError):
            return None
        floor = eps
        if apply_dust and isinstance(dust_floor, (int, float)):
            floor = max(float(dust_floor), floor)
        if normalized <= floor:
            return None
        return normalized

    current_norm = _normalize(current_size, apply_dust=False)
    remote_norm = _normalize(remote_size, apply_dust=True)

    if current_norm is None and remote_norm is None:
        return None, False
    if current_norm is None and remote_norm is not None:
        return remote_norm, True
    if current_norm is not None and remote_norm is None:
        return None, True
    assert current_norm is not None and remote_norm is not None
    if abs(current_norm - remote_norm) > eps:
        return remote_norm, True
    return remote_norm, False


def _should_attempt_claim(
    meta: Dict[str, Any],
    status: Dict[str, Any],
    closed_by_ws: bool,
) -> bool:
    pos_size = _extract_position_size(status)
    if pos_size <= 0:
        return False
    if closed_by_ws:
        return True
    return _market_has_ended(meta)


def _resolve_client_host(client) -> str:
    env_host = os.getenv("POLY_HOST")
    if isinstance(env_host, str) and env_host.strip():
        return env_host.strip().rstrip("/")

    for attr in ("host", "_host", "base_url", "api_url"):
        val = getattr(client, attr, None)
        if isinstance(val, str) and val.strip():
            host = val.strip().rstrip("/")
            if "gamma-api" in host:
                return host.replace("gamma-api", "clob")
            return host

    return CLOB_API_HOST


def _extract_api_creds(client) -> Optional[Dict[str, str]]:
    def _pair_from_mapping(mp: Dict[str, Any]) -> Optional[Dict[str, str]]:
        if not isinstance(mp, dict):
            return None
        key_keys = ("key", "apiKey", "api_key", "id", "apiId", "api_id")
        secret_keys = ("secret", "apiSecret", "api_secret", "apiSecretKey")
        key_val = next((mp.get(k) for k in key_keys if mp.get(k)), None)
        secret_val = next((mp.get(k) for k in secret_keys if mp.get(k)), None)
        if key_val and secret_val:
            return {"key": str(key_val), "secret": str(secret_val)}
        return None

    def _pair_from_object(obj: Any) -> Optional[Dict[str, str]]:
        if obj is None:
            return None
        # 对部分库返回的命名元组/数据类做兼容
        for attr_key in ("key", "apiKey", "api_key", "id", "apiId", "api_id"):
            key_val = getattr(obj, attr_key, None)
            if key_val:
                break
        else:
            key_val = None
        for attr_secret in ("secret", "apiSecret", "api_secret", "apiSecretKey"):
            secret_val = getattr(obj, attr_secret, None)
            if secret_val:
                break
        else:
            secret_val = None
        if key_val and secret_val:
            return {"key": str(key_val), "secret": str(secret_val)}
        if hasattr(obj, "to_dict"):
            try:
                return _pair_from_mapping(obj.to_dict())
            except Exception:
                return None
        return None

    def _pair_from_sequence(seq: Any) -> Optional[Dict[str, str]]:
        if not isinstance(seq, (list, tuple)) or len(seq) < 2:
            return None
        key_val, secret_val = seq[0], seq[1]
        if key_val and secret_val:
            return {"key": str(key_val), "secret": str(secret_val)}
        return None

    candidates = [
        getattr(client, "api_creds", None),
        getattr(client, "_api_creds", None),
    ]
    getter = getattr(client, "get_api_creds", None)
    if callable(getter):
        try:
            candidates.append(getter())
        except Exception:
            pass
    key = getattr(client, "api_key", None)
    secret = getattr(client, "api_secret", None)
    if key and secret:
        candidates.append({"key": key, "secret": secret})
    # 兼容直接从环境变量注入 API key/secret 的场景
    env_key = os.getenv("POLY_API_KEY")
    env_secret = os.getenv("POLY_API_SECRET")
    if env_key and env_secret:
        candidates.append({"key": env_key, "secret": env_secret})

    for cand in candidates:
        if cand is None:
            continue
        pair = None
        if isinstance(cand, dict):
            pair = _pair_from_mapping(cand)
        elif isinstance(cand, (list, tuple)):
            pair = _pair_from_sequence(cand)
        else:
            pair = _pair_from_object(cand)
        if pair:
            return pair
    return None


def _sign_payload(secret: str, timestamp: str, method: str, path: str, body: str) -> str:
    payload = f"{timestamp}{method.upper()}{path}{body}"
    return hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()


def _claim_via_http(client, market_id: str, token_id: Optional[str]) -> bool:
    creds = _extract_api_creds(client)
    if not creds:
        print("[CLAIM] 当前客户端缺少 API 凭证信息，无法调用 HTTP claim 接口。")
        return False

    host = _resolve_client_host(client)
    path = "/v1/user/clob/positions/claim"
    url = f"{host}{path}"
    payload: Dict[str, Any] = {"market": market_id}
    if token_id:
        payload["tokenIds"] = [token_id]

    body = json.dumps(payload, separators=(",", ":"))
    ts = str(int(time.time() * 1000))
    signature = _sign_payload(creds["secret"], ts, "POST", path, body)
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": creds["key"],
        "X-API-Signature": signature,
        "X-API-Timestamp": ts,
    }

    try:
        _enforce_request_rate_limit()
        resp = requests.post(url, data=body, headers=headers, timeout=10)
    except Exception as exc:
        print(f"[CLAIM] 请求 {url} 时出现异常：{exc}")
        return False

    if resp.status_code == 404:
        print("[CLAIM] 目标 claim 接口返回 404，请确认所使用的 Clob API 版本是否支持自动 claim。")
        return False
    if resp.status_code >= 500:
        print(f"[CLAIM] 服务端 {resp.status_code} 错误：{resp.text}")
        return False
    if resp.status_code in (401, 403):
        print(f"[CLAIM] 接口拒绝访问（{resp.status_code}）：{resp.text}")
        return False

    try:
        data = resp.json()
    except ValueError:
        data = resp.text

    print(f"[CLAIM] HTTP {path} 返回状态 {resp.status_code}，响应：{data}")
    if isinstance(data, dict) and data.get("error"):
        return False
    return resp.ok


def _extract_positions_from_data_api_response(payload: Any) -> Optional[List[dict]]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return data
        return None
    return None


def _normalize_wallet_address(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        try:
            hexed = value.hex()
        except Exception:
            return None
        return hexed if hexed else None
    if isinstance(value, str):
        candidate = value.strip()
        return candidate or None
    if isinstance(value, (list, tuple, set)):
        for item in value:
            candidate = _normalize_wallet_address(item)
            if candidate:
                return candidate
        return None
    if isinstance(value, dict):
        keys = (
            "address",
            "wallet",
            "walletAddress",
            "wallet_address",
            "funder",
            "owner",
            "defaultAddress",
            "default_address",
        )
        for key in keys:
            candidate = _normalize_wallet_address(value.get(key))
            if candidate:
                return candidate
    return None


def _resolve_wallet_address(client) -> Tuple[Optional[str], str]:
    if client is not None:
        direct_attrs = (
            "funder",
            "owner",
            "address",
            "wallet",
            "wallet_address",
            "walletAddress",
            "default_address",
            "defaultAddress",
            "deposit_address",
            "depositAddress",
        )
        for attr in direct_attrs:
            try:
                cand = getattr(client, attr, None)
            except Exception:
                continue
            address = _normalize_wallet_address(cand)
            if address:
                return address, f"client.{attr}"

        try:
            attrs = list(dir(client))
        except Exception:
            attrs = []
        for attr in attrs:
            if "address" not in attr.lower():
                continue
            if attr in direct_attrs:
                continue
            try:
                cand = getattr(client, attr, None)
            except Exception:
                continue
            address = _normalize_wallet_address(cand)
            if address:
                return address, f"client.{attr}"

    env_candidates = (
        "POLY_DATA_ADDRESS",
        "POLY_FUNDER",
        "POLY_WALLET",
        "POLY_ADDRESS",
    )
    for env_name in env_candidates:
        cand = os.getenv(env_name)
        address = _normalize_wallet_address(cand)
        if address:
            return address, f"env:{env_name}"

    return None, "缺少地址，无法从数据接口拉取持仓。"


def _fetch_positions_from_data_api(client) -> Tuple[List[dict], bool, str]:
    address, origin_hint = _resolve_wallet_address(client)

    if not address:
        return [], False, origin_hint

    url = f"{DATA_API_ROOT}/positions"

    limit = 500
    offset = 0
    collected: List[dict] = []
    total_records: Optional[int] = None

    while True:
        params = {
            "user": address,
            "limit": limit,
            "offset": offset,
            "sizeThreshold": 0,
        }
        try:
            _enforce_request_rate_limit()
            resp = requests.get(url, params=params, timeout=10)
        except requests.RequestException as exc:
            return [], False, f"数据接口请求失败：{exc}"

        if resp.status_code == 404:
            return [], False, "数据接口返回 404（请确认使用 Proxy/Deposit 地址查询 user 参数）"

        try:
            resp.raise_for_status()
        except requests.RequestException as exc:
            return [], False, f"数据接口请求失败：{exc}"

        try:
            payload = resp.json()
        except ValueError:
            return [], False, "数据接口响应解析失败"

        positions = _extract_positions_from_data_api_response(payload)
        if positions is None:
            return [], False, "数据接口返回格式异常，缺少 data 字段。"

        collected.extend(positions)
        meta = payload.get("meta") if isinstance(payload, dict) else {}
        if isinstance(meta, dict):
            raw_total = meta.get("total") or meta.get("count")
            try:
                if raw_total is not None:
                    total_records = int(raw_total)
            except (TypeError, ValueError):
                total_records = None

        if not positions or (total_records is not None and len(collected) >= total_records):
            break

        offset += len(positions)

    total = total_records if total_records is not None else len(collected)
    origin_detail = f" via {origin_hint}" if origin_hint else ""
    origin = f"data-api positions(limit={limit}, total={total}, param=user){origin_detail}"
    return collected, True, origin


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        if isinstance(value, str):
            try:
                return float(value.strip())
            except (TypeError, ValueError):
                return None
        return None


def _position_dict_candidates(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    if isinstance(entry, dict):
        candidates.append(entry)
        for key in ("position", "token", "asset", "outcome"):
            nested = entry.get(key)
            if isinstance(nested, dict):
                candidates.append(nested)
    return candidates


def _position_matches_token(entry: Dict[str, Any], token_id: str) -> bool:
    token_str = str(token_id)
    if not token_str:
        return False
    id_keys = (
        "tokenId",
        "token_id",
        "clobTokenId",
        "clob_token_id",
        "assetId",
        "asset_id",
        "outcomeTokenId",
        "outcome_token_id",
        "token",
        "asset",
        "id",
    )
    for cand in _position_dict_candidates(entry):
        for key in id_keys:
            val = cand.get(key)
            if val is None:
                continue
            if str(val) == token_str:
                return True
    return False


def _extract_position_size_from_entry(entry: Dict[str, Any]) -> Optional[float]:
    size_keys = (
        "size",
        "positionSize",
        "position_size",
        "position",
        "quantity",
        "qty",
        "balance",
        "amount",
    )
    for cand in _position_dict_candidates(entry):
        for key in size_keys:
            val = _coerce_float(cand.get(key))
            if val is None:
                continue
            if val >= 0:
                return val
    return None


def _plan_manual_buy_size(
    manual_size: Optional[float],
    owned_size: Optional[float],
    *,
    enforce_target: bool,
    eps: float = 1e-9,
) -> Tuple[Optional[float], bool]:
    """Compute the effective order size for manual buy mode."""

    if manual_size is None:
        return None, False

    try:
        requested = float(manual_size)
    except (TypeError, ValueError):
        return None, False

    if requested <= 0:
        return None, True

    current_owned = 0.0
    if owned_size is not None:
        try:
            current_owned = max(float(owned_size), 0.0)
        except (TypeError, ValueError):
            current_owned = 0.0

    if not enforce_target:
        return requested, False

    remaining = max(requested - current_owned, 0.0)
    if remaining <= eps:
        return None, True
    return remaining, False


def _extract_avg_price_from_entry(entry: Dict[str, Any]) -> Optional[float]:
    avg_keys = (
        "avg_price",
        "avgPrice",
        "average_price",
        "averagePrice",
        "avgExecutionPrice",
        "avg_execution_price",
        "averageExecutionPrice",
        "average_execution_price",
        "entry_price",
        "entryPrice",
        "entryAveragePrice",
        "entry_average_price",
        "execution_price",
        "executionPrice",
    )
    for cand in _position_dict_candidates(entry):
        for key in avg_keys:
            val = _coerce_float(cand.get(key))
            if val is not None and val > 0:
                return val

    notional_keys = (
        "total_cost",
        "totalCost",
        "net_cost",
        "netCost",
        "cost",
        "position_cost",
        "positionCost",
        "purchase_value",
        "purchaseValue",
        "buy_value",
        "buyValue",
    )
    size = _extract_position_size_from_entry(entry)
    if size is None or size <= 0:
        return None
    for cand in _position_dict_candidates(entry):
        for key in notional_keys:
            notional = _coerce_float(cand.get(key))
            if notional is None:
                continue
            if abs(size) < 1e-12:
                continue
            price = notional / size
            if price > 0:
                return price
    return None


def _lookup_position_avg_price(
    client,
    token_id: str,
) -> Tuple[Optional[float], Optional[float], str]:
    if not token_id:
        return None, None, "token_id 缺失"

    retry_times = 5
    retry_interval = 1.0
    last_info: Optional[str] = None

    for attempt in range(retry_times):
        positions, ok, origin = _fetch_positions_from_data_api(client)

        if positions:
            for pos in positions:
                if not isinstance(pos, dict):
                    continue
                if not _position_matches_token(pos, token_id):
                    continue
                avg_price = _extract_avg_price_from_entry(pos)
                pos_size = _extract_position_size_from_entry(pos)
                return avg_price, pos_size, origin

            last_info = f"未在 {origin or 'positions'} 中找到 token {token_id}"
        else:
            if ok:
                last_info = origin if origin else "数据接口返回空列表"
            else:
                last_info = origin if origin else "未知原因"

        if attempt < retry_times - 1:
            time.sleep(retry_interval)

    return None, None, last_info or f"未在 positions 中找到 token {token_id}"


def _fetch_position_snapshot_with_cache(
    *,
    client,
    token_id: str,
    cache: Optional[Tuple[Optional[float], Optional[float], Optional[str]]],
    cache_ts: float,
    log_errors: bool,
    force: bool = False,
    cache_ttl: float = 2.0,
) -> Tuple[Optional[Tuple[Optional[float], Optional[float], Optional[str]]], float]:
    now = time.time()
    if not force and cache is not None and now - cache_ts <= cache_ttl:
        return cache, cache_ts

    try:
        snapshot = _lookup_position_avg_price(client, token_id)
        return snapshot, now
    except Exception as probe_exc:
        if log_errors:
            print(f"[WATCHDOG][SELL] 持仓查询异常：{probe_exc}")
        if cache is not None:
            return cache, cache_ts
        return None, cache_ts


def _attempt_claim(client, meta: Dict[str, Any], token_id: str) -> None:
    market_id = meta.get("market_id") if isinstance(meta, dict) else None
    print(f"[CLAIM] 检测到需处理的未平仓仓位，token_id={token_id}，开始尝试 claim…")
    if not market_id:
        print("[CLAIM] 未找到 market_id，无法自动 claim，请手动处理。")
        return

    claim_fn = getattr(client, "claim_positions", None)
    if callable(claim_fn):
        claim_kwargs = {"market": market_id}
        if token_id:
            claim_kwargs["token_ids"] = [token_id]
        try:
            print(f"[CLAIM] 尝试调用 claim_positions({claim_kwargs})…")
            resp = claim_fn(**claim_kwargs)
            print(f"[CLAIM] 响应: {resp}")
            return
        except TypeError as exc:
            print(f"[CLAIM] claim_positions 参数不匹配: {exc}，改用 HTTP 接口。")
        except Exception as exc:
            print(f"[CLAIM] 调用 claim_positions 失败: {exc}，改用 HTTP 接口。")

    if _claim_via_http(client, market_id, token_id):
        return

    print("[CLAIM] 未找到可用的 claim 方法，请手动处理。")

def _http_json(url: str, params=None) -> Optional[Any]:
    try:
        _enforce_request_rate_limit()
        r = requests.get(url, params=params or {}, timeout=10)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

def _list_markets_under_event(event_slug: str) -> List[dict]:
    if not event_slug:
        return []
    # A) /events?slug=<slug>
    for closed_flag in ("false", "true", None):
        params = {"slug": event_slug}
        if closed_flag is not None:
            params["closed"] = closed_flag
        data = _http_json(f"{GAMMA_ROOT}/events", params=params)
        evs = []
        if isinstance(data, dict) and "data" in data:
            evs = data["data"]
        elif isinstance(data, list):
            evs = data
        if isinstance(evs, list):
            for ev in evs:
                mkts = ev.get("markets") or []
                if mkts:
                    return mkts
        # 若找到事件但 markets 为空，则无需继续尝试其它 closed_flag
        if evs:
            break
    # B) /markets?search=<slug> 精确过滤 eventSlug
    data = _http_json(f"{GAMMA_ROOT}/markets", params={"limit": 200, "search": event_slug})
    mkts = []
    if isinstance(data, dict) and "data" in data:
        mkts = data["data"]
    elif isinstance(data, list):
        mkts = data
    if isinstance(mkts, list):
        return [m for m in mkts if str(m.get("eventSlug") or "") == str(event_slug)]
    return []

def _fetch_market_by_slug(market_slug: str) -> Optional[dict]:
    return _http_json(f"{GAMMA_ROOT}/markets/slug/{market_slug}")

def _fetch_market_by_token_id(token_id: str) -> Optional[dict]:
    if not token_id:
        return None

    def _normalize_clob_tokens(raw_value: Any) -> List[str]:
        if raw_value is None:
            return []
        if isinstance(raw_value, (list, tuple)):
            return [str(item) for item in raw_value if item is not None]
        if isinstance(raw_value, str):
            trimmed = raw_value.strip()
            if trimmed.startswith("[") and trimmed.endswith("]"):
                try:
                    parsed = json.loads(trimmed)
                except JSONDecodeError:
                    parsed = None
                if isinstance(parsed, list):
                    return [str(item) for item in parsed if item is not None]
            return [trimmed]
        return [str(raw_value)]

    def _select_market(markets: List[dict]) -> Optional[dict]:
        if not markets:
            return None
        token_str = str(token_id)
        matches = []
        for market in markets:
            clob_tokens = _normalize_clob_tokens(market.get("clobTokenIds"))
            if token_str in clob_tokens:
                matches.append(market)
        if not matches:
            return None
        for market in matches:
            if market.get("active") is True and market.get("closed") is False:
                return market
        return matches[0]

    for param_name in ("clob_token_ids", "clobTokenIds"):
        params = {param_name: str(token_id), "limit": 50}
        data = _http_json(f"{GAMMA_ROOT}/markets", params=params)
        markets = []
        if isinstance(data, dict) and "data" in data:
            markets = data["data"]
        elif isinstance(data, list):
            markets = data
        if isinstance(markets, list):
            match = _select_market(markets)
            if match is not None:
                return match
    return None

def _pick_market_subquestion(markets: List[dict]) -> dict:
    print("[CHOICE] 该事件下存在多个子问题，请选择其一，或直接粘贴具体子问题URL：")
    for i, m in enumerate(markets):
        title = m.get("title") or m.get("question") or m.get("slug")
        end_ts = m.get("endDate") or m.get("endTime") or ""
        mslug = m.get("slug") or ""
        url = f"https://polymarket.com/market/{mslug}" if mslug else "(no slug)"
        print(f"  [{i}] {title}  (end={end_ts})  -> {url}")
    while True:
        s = input("请输入序号或粘贴URL：").strip()
        if s.startswith(("http://", "https://")):
            return {"__direct_url__": s}
        if s.isdigit():
            idx = int(s)
            if 0 <= idx < len(markets):
                return markets[idx]
        print("请输入有效序号或URL。")

    raise ValueError("子问题未包含 tokenId，且兜底解析失败。")

# ====== 下单执行工具 ======
def _floor(x: float, dp: int) -> float:
    q = Decimal(str(x)).quantize(Decimal("1." + "0"*dp), rounding=ROUND_DOWN)
    return float(q)

def _normalize_sell_pair(price: float, size: float) -> Tuple[float, float]:
    # 价格 4dp；份数 2dp（下单时再 floor 一次，确保不超）
    return _floor(price, 4), _floor(size, 2)

def _place_buy_fak(client, token_id: str, price: float, size: float) -> Dict[str, Any]:
    return execute_auto_buy(client=client, token_id=token_id, price=price, size=size)

def _place_sell_fok(client, token_id: str, price: float, size: float) -> Dict[str, Any]:
    from py_clob_client.clob_types import OrderArgs, OrderType
    from py_clob_client.order_builder.constants import SELL
    eff_p, eff_s = _normalize_sell_pair(price, size)
    order = OrderArgs(token_id=str(token_id), side=SELL, price=float(eff_p), size=float(eff_s))
    signed = client.create_order(order)
    return client.post_order(signed, OrderType.FOK)


def _place_sell_ioc(client, token_id: str, price: float, size: float) -> Dict[str, Any]:
    """
    发出 IOC (Immediate or Cancel) 卖单，允许部分成交。
    注意：py_clob_client 使用 FAK (Fill and Kill) 来模拟 IOC 行为。
    """
    from py_clob_client.clob_types import OrderArgs, OrderType
    from py_clob_client.order_builder.constants import SELL
    eff_p, eff_s = _normalize_sell_pair(price, size)
    order = OrderArgs(token_id=str(token_id), side=SELL, price=float(eff_p), size=float(eff_s))
    signed = client.create_order(order)
    # py_clob_client 使用 FAK 来实现 IOC 语义（部分成交，剩余取消）
    order_type = getattr(OrderType, "FAK", None) or getattr(OrderType, "IOC", OrderType.FOK)
    return client.post_order(signed, order_type)


def _normalize_open_order(order: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(order, dict):
        return None
    order_id = order.get("order_id") or order.get("id") or order.get("orderId")
    token_id = order.get("token_id") or order.get("tokenId") or order.get("asset_id")
    side = order.get("side") or order.get("orderType") or order.get("type")
    price = order.get("price") or order.get("rate")
    size = (
        order.get("size")
        or order.get("remaining")
        or order.get("original_size")
        or order.get("originalSize")
        or order.get("amount")
    )
    if not order_id or not token_id:
        return None
    return {
        "order_id": str(order_id),
        "token_id": str(token_id),
        "side": str(side) if side is not None else "",
        "price": float(price) if price is not None else None,
        "size": float(size) if size is not None else None,
    }


def _fetch_open_orders_norm(client: Any) -> List[Dict[str, Any]]:
    try:
        from py_clob_client.clob_types import OpenOrderParams
        payload = client.get_orders(OpenOrderParams())
    except Exception:
        try:
            payload = client.get_orders()
        except Exception:
            return []
    orders = payload if isinstance(payload, list) else []
    normalized: List[Dict[str, Any]] = []
    for order in orders:
        parsed = _normalize_open_order(order)
        if not parsed:
            continue
        normalized.append(parsed)
    return normalized


def _cancel_order(client: Any, order_id: str) -> None:
    if callable(getattr(client, "cancel", None)):
        client.cancel(order_id=order_id)
        return
    if callable(getattr(client, "cancel_order", None)):
        client.cancel_order(order_id)
        return
    if callable(getattr(client, "cancel_orders", None)):
        client.cancel_orders([order_id])
        return
    private = getattr(client, "private", None)
    if private is not None:
        if callable(getattr(private, "cancel", None)):
            private.cancel(order_id=order_id)
            return
        if callable(getattr(private, "cancel_order", None)):
            private.cancel_order(order_id)
            return
        if callable(getattr(private, "cancel_orders", None)):
            private.cancel_orders([order_id])
            return


def _cancel_open_orders_for_token(client: Any, token_id: str) -> int:
    open_orders = _fetch_open_orders_norm(client)
    canceled = 0
    for order in open_orders:
        if str(order.get("token_id")) != str(token_id):
            continue
        order_id = order.get("order_id")
        if not order_id:
            continue
        try:
            _cancel_order(client, str(order_id))
            canceled += 1
        except Exception:
            continue
    return canceled


def _cancel_open_buy_orders_for_token(client: Any, token_id: str) -> int:
    open_orders = _fetch_open_orders_norm(client)
    canceled = 0
    for order in open_orders:
        if str(order.get("token_id")) != str(token_id):
            continue
        if str(order.get("side", "")).upper() != "BUY":
            continue
        order_id = order.get("order_id")
        if not order_id:
            continue
        try:
            _cancel_order(client, str(order_id))
            canceled += 1
        except Exception:
            continue
    return canceled


# ===== 主流程 =====
def main(run_config: Optional[Dict[str, Any]] = None):
    client = _get_client()
    creds_check = _extract_api_creds(client)
    if not creds_check or not creds_check.get("key") or not creds_check.get("secret"):
        print("[ERR] 无法获取完整 API 凭证，请检查配置后重试。")
        _log_error("API_CREDS_ERROR", {
            "message": "无法获取完整 API 凭证",
            "has_creds": bool(creds_check),
            "has_key": bool(creds_check.get("key") if creds_check else False),
            "has_secret": bool(creds_check.get("secret") if creds_check else False)
        })
        return
    print("[INIT] API 凭证已验证。")
    print("[INIT] ClobClient 就绪。")

    if run_config is None:
        run_cfg: Dict[str, Any] = _load_run_config(None)
    else:
        run_cfg = run_config
    token_id = (
        run_cfg.get("token_id")
        or run_cfg.get("tokenId")
        or run_cfg.get("asset_id")
        or run_cfg.get("assetId")
    )
    if not token_id:
        print("[ERR] 配置未提供 token_id，无法下单。")
        _log_error("CONFIG_ERROR", {
            "message": "配置未提供 token_id",
            "config_keys": list(run_cfg.keys()) if run_cfg else []
        })
        return
    exit_signal_path = run_cfg.get("exit_signal_path") or run_cfg.get("exit_signal_file")
    exit_signal_path = Path(exit_signal_path) if exit_signal_path else None
    source = str(
        run_cfg.get("market_url")
        or run_cfg.get("source")
        or run_cfg.get("url")
        or ""
    ).strip()

    timezone_override_hint: Optional[Any] = run_cfg.get("timezone")
    manual_deadline_override_ts: Optional[float] = _coerce_float(
        run_cfg.get("deadline_override_ts")
    )
    manual_deadline_disabled = bool(run_cfg.get("disable_deadline_checks", False))
    title = str(
        run_cfg.get("topic_name")
        or run_cfg.get("topic_id")
        or run_cfg.get("market_name")
        or token_id
    )
    market_meta = _maybe_fetch_market_meta_from_source(source) if source else {}
    if not market_meta and token_id:
        print("[INFO] 未提供 market_url，尝试通过 token_id 获取市场元数据。")
        token_market = _fetch_market_by_token_id(str(token_id))
        if token_market:
            market_meta = _market_meta_from_obj(token_market)
        else:
            print("[WARN] 未能通过 token_id 获取市场元数据，可能无法识别截止时间或价格精度。")
    raw_meta = market_meta.get("raw") if isinstance(market_meta, dict) else None
    if isinstance(raw_meta, dict):
        title = raw_meta.get("title") or raw_meta.get("question") or title
    market_meta = _apply_timezone_override_meta(market_meta, timezone_override_hint)
    market_meta = _apply_manual_deadline_override_meta(
        market_meta,
        manual_deadline_override_ts,
    )
    print(f"[INFO] 市场/子问题标题: {title}")
    print(f"[INFO] 使用 token_id: {token_id}")

    tz_hint = market_meta.get("timezone_hint") if isinstance(market_meta, dict) else None
    if tz_hint:
        print(
            "[INFO] 市场标注时区: "
            f"{_describe_timezone_hint(tz_hint)}"
        )
    else:
        tz_choice = timezone_override_hint or "America/New_York"
        timezone_override_hint = tz_choice
        market_meta = _apply_timezone_override_meta(market_meta, timezone_override_hint)
        market_meta = _apply_manual_deadline_override_meta(
            market_meta,
            manual_deadline_override_ts,
        )
        tz_hint = market_meta.get("timezone_hint")
        print(
            "[INFO] 未检测到市场时区，已依据配置指定为: "
            f"{_describe_timezone_hint(tz_hint)}"
        )

    def _fmt_ts(ts_val: Optional[float]) -> Optional[str]:
        if ts_val is None:
            return None
        try:
            ts_f = float(ts_val)
        except (TypeError, ValueError):
            return None
        dt = datetime.fromtimestamp(ts_f, tz=timezone.utc)
        return dt.isoformat()

    def _snapshot_ts(snap: Dict[str, Any]) -> float:
        for key in ("ts", "timestamp", "time"):
            ts_val = _coerce_float(snap.get(key))
            if ts_val is not None:
                return ts_val
        return time.time()

    end_ts = market_meta.get("end_ts") if isinstance(market_meta, dict) else None
    resolved_ts = market_meta.get("resolved_ts") if isinstance(market_meta, dict) else None
    if end_ts or resolved_ts:
        end_str = _fmt_ts(end_ts)
        resolve_str = _fmt_ts(resolved_ts)
        if end_str:
            print(f"[INFO] 市场计划截止时间 (UTC): {end_str}")
        if resolve_str and resolve_str != end_str:
            print(f"[INFO] 市场预计结算时间 (UTC): {resolve_str}")

    def _calc_deadline(meta: Dict[str, Any]) -> Optional[float]:
        candidates: List[float] = []
        if isinstance(meta, dict):
            for key in ("end_ts", "resolved_ts"):
                ts_val = meta.get(key)
                if isinstance(ts_val, (int, float)):
                    candidates.append(float(ts_val))
        return min(candidates) if candidates else None

    market_deadline_ts = _calc_deadline(market_meta)
    deadline_policy = run_cfg.get("deadline_policy") if isinstance(run_cfg, dict) else {}
    default_deadline_spec = (
        deadline_policy.get("default_deadline") if isinstance(deadline_policy, dict) else None
    )
    override_choice = None
    if isinstance(deadline_policy, dict):
        override_choice = deadline_policy.get("override_choice")
        try:
            override_choice = int(override_choice) if override_choice is not None else None
        except (TypeError, ValueError):
            override_choice = None
        manual_deadline_disabled = bool(
            deadline_policy.get("disable_deadline", manual_deadline_disabled)
        )
    deadline_tz_hint = None
    if isinstance(deadline_policy, dict):
        deadline_tz_hint = deadline_policy.get("timezone")

    def _apply_default_deadline(reason: str) -> None:
        nonlocal market_meta, market_deadline_ts, manual_deadline_override_ts

        base_ts: Optional[float] = market_deadline_ts
        if base_ts is None and isinstance(market_meta, dict):
            for key in ("end_ts", "resolved_ts"):
                cand = _coerce_float(market_meta.get(key))
                if cand is not None:
                    base_ts = cand
                    break
        base_date = (
            datetime.fromtimestamp(base_ts, tz=timezone.utc).date()
            if base_ts
            else datetime.now(tz=timezone.utc).date()
        )
        tz_pref = deadline_tz_hint or tz_hint or timezone_override_hint
        fallback_ts = _default_deadline_ts(base_date, default_deadline_spec, tz_pref)
        if fallback_ts is None:
            return
        manual_deadline_override_ts = fallback_ts
        market_meta = _apply_manual_deadline_override_meta(market_meta, fallback_ts)
        market_deadline_ts = _calc_deadline(market_meta)
        local_tz = _timezone_from_hint(tz_pref) or timezone.utc
        local_dt = datetime.fromtimestamp(fallback_ts, tz=local_tz)
        print(
            f"[WARN] {reason}，已按默认配置设置截止时间为 {local_dt.isoformat()} ({tz_pref or 'UTC'})。"
        )

    if manual_deadline_disabled:
        market_meta = dict(market_meta or {})
        market_meta.pop("end_ts", None)
        market_meta.pop("resolved_ts", None)
        market_deadline_ts = None
    elif market_deadline_ts and override_choice:
        base_date_utc = datetime.fromtimestamp(
            float(market_deadline_ts), tz=timezone.utc
        ).date()
        override_ts, deadline_disabled = _common_deadline_override(
            base_date_utc,
            override_choice,
            deadline_tz_hint or tz_hint or timezone_override_hint,
        )
        manual_deadline_disabled = manual_deadline_disabled or deadline_disabled
        if override_ts is not None:
            manual_deadline_override_ts = override_ts
            market_meta = _apply_manual_deadline_override_meta(
                market_meta,
                manual_deadline_override_ts,
            )
            market_deadline_ts = _calc_deadline(market_meta)
        if manual_deadline_disabled:
            market_meta = dict(market_meta or {})
            market_meta.pop("end_ts", None)
            market_meta.pop("resolved_ts", None)
            market_deadline_ts = None

    if not manual_deadline_disabled and _should_offer_common_deadline_options(market_meta):
        override_spec = {
            "hour": 23,
            "minute": 59,
            "timezone": "America/New_York",
            "fallback_offset": -240,
        }
        default_deadline_spec = dict(default_deadline_spec or {})
        default_deadline_spec.update(override_spec)
        _apply_default_deadline("自动获取的截止日期缺少具体时刻")
    if not manual_deadline_disabled and not market_deadline_ts:
        print("[WARN] 未能自动获取市场结束时间，将进入无截止日期模式继续运行。")
        manual_deadline_disabled = True
        market_meta = dict(market_meta or {})
        market_meta.pop("end_ts", None)
        market_meta.pop("resolved_ts", None)
        market_deadline_ts = None
    if market_deadline_ts:
        dt_deadline = datetime.fromtimestamp(market_deadline_ts, tz=timezone.utc)
        print(
            "[INFO] 监控目标结束时间 (UTC): "
            f"{dt_deadline.isoformat()}"
        )
    elif manual_deadline_disabled:
        print("[WARN] 未设定结束时间点：跳过截止时间校验和倒计时。")
    else:
        print("[ERR] 未能获取市场结束时间，程序终止。")
        _log_error("MARKET_DEADLINE_ERROR", {
            "message": "未能获取市场结束时间",
            "token_id": token_id,
            "source": source,
            "manual_deadline_disabled": manual_deadline_disabled
        })
        return

    def _calc_profit_floor(meta: Dict[str, Any]) -> Tuple[float, Optional[int]]:
        precision = _infer_market_price_precision(meta)
        floor = 0.003
        if precision == 2:
            floor = 0.01
        elif precision == 3:
            floor = 0.003
        return floor, precision

    profit_floor, market_price_precision = _calc_profit_floor(market_meta)

    def _log_profit_floor(prefix: str = "[INFO]") -> None:
        if market_price_precision is not None:
            print(
                f"{prefix} 检测到市场价格精度为 {market_price_precision} 位小数，"
                f"盈利百分比下限为 {profit_floor * 100:.3f}%。"
            )
        else:
            print(
                f"{prefix} 未能识别市场价格精度，默认按盈利百分比下限 {profit_floor * 100:.3f}% 执行。"
            )

    def _enforce_profit_floor(current: float, *, prefix: str = "[WARN]") -> float:
        if current < profit_floor:
            print(
                f"{prefix} 盈利百分比不能低于 {profit_floor * 100:.3f}%，"
                f"已自动调整为 {profit_floor * 100:.3f}%。"
            )
            return profit_floor
        return current

    _log_profit_floor()

    token_id = str(token_id)
    print(f"[INIT] 目标 token_id: {token_id}")

    manual_order_size: Optional[float] = _coerce_float(run_cfg.get("order_size"))
    manual_size_is_target = bool(
        run_cfg.get("order_size_is_target", manual_order_size is not None)
    )
    if manual_order_size is not None and manual_order_size <= 0:
        print("[ERR] 配置的份数必须大于 0，退出。")
        _log_error("CONFIG_ERROR", {
            "message": "配置的份数必须大于 0",
            "order_size": manual_order_size,
            "token_id": token_id
        })
        return
    if manual_order_size is not None:
        mode_note = "总持仓目标" if manual_size_is_target else "单笔下单量"
        print(
            f"[INIT] 已读取手动份数 {manual_order_size}，模式：{mode_note}。"
        )
    else:
        print("[INIT] 未指定手动份数，将按 $1 反推下单量。")

    sell_mode_raw = str(run_cfg.get("sell_mode") or "aggressive").lower()
    sell_mode = "conservative" if sell_mode_raw == "conservative" else "aggressive"
    print(f"[INIT] 卖出挂单模式：{sell_mode}")

    buy_threshold = _coerce_float(run_cfg.get("buy_price_threshold"))
    max_buy_price = _coerce_float(run_cfg.get("max_buy_price"))
    if max_buy_price is None:
        max_buy_price = 0.98  # 默认买入价格上限 0.98
    drop_window = _coerce_float(run_cfg.get("drop_window_minutes")) or 10.0
    drop_pct = _normalize_ratio(run_cfg.get("drop_pct"), 0.05)
    profit_pct = _normalize_ratio(run_cfg.get("profit_pct"), 0.05)
    profit_pct = _enforce_profit_floor(profit_pct)

    incremental_drop_pct_step = _normalize_ratio(
        run_cfg.get("incremental_drop_pct_step"), 0.0
    )
    enable_incremental_drop_pct = bool(
        run_cfg.get("enable_incremental_drop_pct", incremental_drop_pct_step > 0)
    )
    if not enable_incremental_drop_pct:
        incremental_drop_pct_step = 0.0

    if enable_incremental_drop_pct:
        print(
            f"[INIT] 卖出后递增买入阈值已启用，步长 {incremental_drop_pct_step * 100:.4f}%。"
        )
    else:
        print("[INIT] 未启用递增跌幅阈值，保持固定阈值运行。")

    print(f"[INIT] 买入价格上限: {max_buy_price:.4f}（价格>=此值时不买入）")

    stagnation_window_minutes = _coerce_float(run_cfg.get("stagnation_window_minutes"))
    if stagnation_window_minutes is None:
        stagnation_window_minutes = 120.0
    stagnation_pct = _normalize_ratio(run_cfg.get("stagnation_pct"), 0.0)
    no_event_exit_minutes = _coerce_float(run_cfg.get("no_event_exit_minutes"))
    if no_event_exit_minutes is None:
        no_event_exit_minutes = 10.0
    signal_timeout_minutes = _coerce_float(run_cfg.get("signal_timeout_minutes"))
    if signal_timeout_minutes is None:
        signal_timeout_minutes = 30.0
    price_none_exit_count = max(0, int(PRICE_NONE_EXIT_COUNT))
    set_price_none_exit_threshold(price_none_exit_count)
    if stagnation_window_minutes <= 0:
        print("[INIT] 价格停滞监控已禁用。")
    else:
        print(
            "[INIT] 价格停滞监控窗口 "
            f"{stagnation_window_minutes:.1f} 分钟，阈值 {stagnation_pct * 100:.4f}%。"
        )
    if no_event_exit_minutes <= 0:
        print("[INIT] 启动后无行情自动退出已禁用。")
    else:
        print(
            "[INIT] 启动后无行情自动退出阈值 "
            f"{no_event_exit_minutes:.1f} 分钟。"
        )
    if signal_timeout_minutes <= 0:
        print("[INIT] 交易信号超时退出已禁用。")
    else:
        print(
            "[INIT] 交易信号超时退出阈值 "
            f"{signal_timeout_minutes:.1f} 分钟（长时间无买入/卖出信号将退出）。"
        )
    if price_none_exit_count <= 0:
        print("[INIT] 最终价格None次数退出已禁用。")
    else:
        print(
            "[INIT] 最终价格None次数退出阈值 "
            f"{price_none_exit_count} 次（WS+REST）。"
        )

    sell_inactive_hours = _coerce_float(run_cfg.get("sell_inactive_hours"))
    if sell_inactive_hours is None:
        sell_inactive_hours = 0.0
    sell_inactive_timeout_sec = max(float(sell_inactive_hours or 0.0), 0.0) * 3600.0
    if sell_inactive_timeout_sec <= 0:
        print("[INIT] 卖出挂单空窗释放已禁用。")
    else:
        print(
            "[INIT] 卖出挂单空窗释放阈值 "
            f"{sell_inactive_hours:.1f} 小时。"
        )

    sell_only_start_ts: Optional[float] = None
    countdown_cfg = run_cfg.get("countdown") if isinstance(run_cfg, dict) else {}
    countdown_timezone_hint = (
        countdown_cfg.get("timezone") if isinstance(countdown_cfg, dict) else None
    ) or tz_hint
    if market_deadline_ts and isinstance(countdown_cfg, dict):
        countdown_in: Optional[Any] = countdown_cfg.get("absolute_time") or countdown_cfg.get(
            "timestamp"
        )
        used_minutes = False
        if countdown_in is None:
            countdown_in = countdown_cfg.get("minutes_before_end", 300)
            used_minutes = countdown_in is not None
        if countdown_in is not None:
            parsed_ts: Optional[float] = None
            if used_minutes:
                try:
                    minutes_before = float(countdown_in)
                    parsed_ts = market_deadline_ts - minutes_before * 60.0
                except Exception:
                    parsed_ts = None
            if parsed_ts is None:
                parsed_ts = _parse_timestamp(str(countdown_in), countdown_timezone_hint)
            if not parsed_ts:
                print("[ERR] 倒计时开始时间解析失败，程序终止。")
                _log_error("COUNTDOWN_CONFIG_ERROR", {
                    "message": "倒计时开始时间解析失败",
                    "countdown_input": str(countdown_in),
                    "countdown_timezone": countdown_timezone_hint,
                    "token_id": token_id
                })
                return
            if parsed_ts >= market_deadline_ts:
                print("[ERR] 倒计时开始时间必须早于市场结束时间，程序终止。")
                _log_error("COUNTDOWN_CONFIG_ERROR", {
                    "message": "倒计时开始时间必须早于市场结束时间",
                    "countdown_ts": parsed_ts,
                    "market_deadline_ts": market_deadline_ts,
                    "token_id": token_id
                })
                return
            sell_only_start_ts = parsed_ts
            if used_minutes:
                print(
                    f"[INFO] 倒计时卖出模式将在市场结束前 {countdown_in} 分钟开启。"
                )
            else:
                dt_start = datetime.fromtimestamp(parsed_ts, tz=timezone.utc)
                print(
                    "[INFO] 倒计时卖出模式将在 UTC 时间 "
                    f"{dt_start.isoformat()} 开启。"
                )

    cfg = StrategyConfig(
        token_id=token_id,
        buy_price_threshold=buy_threshold,
        max_buy_price=max_buy_price,
        drop_window_minutes=drop_window,
        drop_pct=drop_pct,
        profit_pct=profit_pct,
        disable_sell_signals=True,
        enable_incremental_drop_pct=enable_incremental_drop_pct,
        incremental_drop_pct_step=incremental_drop_pct_step,
    )
    strategy = VolArbStrategy(cfg)
    strategy_supports_total_position = _strategy_accepts_total_position(strategy)

    latest: Dict[str, Dict[str, Any]] = {}
    action_queue: Queue[Action] = Queue()
    stop_event = threading.Event()
    sell_only_event = threading.Event()
    market_closed_detected = False
    ws_state_lock = threading.Lock()
    ws_state: Dict[str, Any] = {
        "last_event_ts": 0.0,
        "last_state": "init",
        "last_state_ts": time.time(),
        "open": False,
        "last_error": "",
    }
    exit_only = bool(run_cfg.get("exit_only", False))

    # ========== Maker 配置（从 global_config 传递）==========
    maker_poll_sec = float(run_cfg.get("maker_poll_sec", 10.0))
    maker_position_sync_interval = float(run_cfg.get("maker_position_sync_interval", POSITION_SYNC_INTERVAL))
    if maker_poll_sec != 10.0 or maker_position_sync_interval != POSITION_SYNC_INTERVAL:
        print(f"[INIT] Maker 配置: poll_sec={maker_poll_sec}s, position_sync_interval={maker_position_sync_interval}s")

    # ========== Slot Refill (回填) 恢复状态 ==========
    resume_state = run_cfg.get("resume_state")
    if resume_state and isinstance(resume_state, dict):
        has_position = resume_state.get("has_position", False)
        position_size = resume_state.get("position_size")
        entry_price = resume_state.get("entry_price")
        skip_buy = resume_state.get("skip_buy", False)
        refill_retry_count = run_cfg.get("refill_retry_count", 0)

        print(f"\n[REFILL] ========== 回填恢复状态 ==========")
        print(f"[REFILL] token_id: {token_id[:20]}...")
        print(f"[REFILL] 重试次数: {refill_retry_count}")

        if has_position and position_size:
            # 有持仓：同步到策略，跳过买入阶段
            print(f"[REFILL] 检测到持仓恢复状态:")
            print(f"[REFILL]   持仓数量: {position_size}")
            print(f"[REFILL]   买入价格: {entry_price}")
            print(f"[REFILL]   跳过买入: {skip_buy}")
            strategy.sync_position(
                total_position=position_size,
                ref_price=entry_price
            )
            print(f"[REFILL] ✓ 已同步持仓状态到策略，将直接进入卖出等待")
        else:
            # 无持仓：正常启动等待买入
            print(f"[REFILL] 无持仓恢复状态，将正常等待买入信号")

        print(f"[REFILL] ========================================\n")

    def _exit_cleanup_only(reason: str) -> None:
        """
        清仓专用函数：使用 IOC 循环卖出，直到持仓全部清完才退出。
        - IOC (Immediate or Cancel) 允许部分成交，未成交部分自动取消
        - 每次循环重新获取最新 best_bid 价格
        - 设置最大重试次数防止死循环
        """
        print(f"[EXIT] 收到清仓信号: {reason}")

        # 1. 撤销所有挂单
        canceled = _cancel_open_orders_for_token(client, token_id)
        if canceled:
            print(f"[EXIT] 已尝试撤销挂单数量={canceled}")

        # 清仓参数
        DUST_THRESHOLD = 0.5  # 小于此数量视为清仓完成（尘埃量）
        MAX_ITERATIONS = 60   # 最大循环次数
        RETRY_INTERVAL_SEC = 5.0  # 每次循环间隔（秒）
        MIN_PRICE = 0.01      # 最低清仓价格

        iteration = 0
        while iteration < MAX_ITERATIONS:
            iteration += 1

            # 2. 查询当前持仓
            snapshot, _ = _fetch_position_snapshot_with_cache(
                client=client,
                token_id=token_id,
                cache=None,
                cache_ts=0.0,
                log_errors=True,
                force=True,
            )
            remaining_pos = snapshot[1] if snapshot else None

            # 检查是否已清仓完成
            if remaining_pos is None or remaining_pos <= DUST_THRESHOLD:
                print(f"[EXIT] 清仓完成，剩余持仓={remaining_pos or 0:.4f}（低于尘埃阈值 {DUST_THRESHOLD}）")
                break

            print(f"[EXIT] 第 {iteration}/{MAX_ITERATIONS} 次清仓尝试，剩余持仓={remaining_pos:.4f}")

            # 3. 获取最新 best_bid 价格
            exit_price = MIN_PRICE
            try:
                best_bid = _fetch_best_price(client, str(token_id), "bid")
                if best_bid is not None and best_bid.price and best_bid.price > 0:
                    exit_price = float(best_bid.price)
            except OrderbookNotFoundError:
                print(f"[EXIT] orderbook 不存在，使用最低清仓价: {MIN_PRICE}")
            except Exception as exc:
                print(f"[EXIT] 获取 best_bid 失败: {exc}，使用最低清仓价: {MIN_PRICE}")

            # 4. 发出 IOC 卖单（允许部分成交）
            try:
                resp = _place_sell_ioc(
                    client,
                    token_id=token_id,
                    price=exit_price,
                    size=remaining_pos
                )
                print(
                    f"[EXIT] 已发出 IOC 清仓卖单 size={remaining_pos:.4f} price={exit_price:.4f} resp={resp}"
                )
            except Exception as exc:
                print(f"[EXIT] IOC 清仓卖单失败: {exc}")

            # 5. 等待一段时间后再次检查
            time.sleep(RETRY_INTERVAL_SEC)

        # 最终状态检查
        if iteration >= MAX_ITERATIONS:
            final_snapshot, _ = _fetch_position_snapshot_with_cache(
                client=client,
                token_id=token_id,
                cache=None,
                cache_ts=0.0,
                log_errors=True,
                force=True,
            )
            final_pos = final_snapshot[1] if final_snapshot else 0
            print(
                f"[EXIT][WARN] 达到最大清仓次数 {MAX_ITERATIONS}，"
                f"剩余持仓={final_pos:.4f}（可能需要人工处理）"
            )
            if (final_pos or 0.0) > DUST_THRESHOLD:
                raise RuntimeError(
                    f"exit-only cleanup 未完成，remaining={float(final_pos):.6f}"
                )

        strategy.stop("sell signal")
        stop_event.set()

    def _cancel_open_buy_orders_before_exit(reason: str) -> None:
        canceled = _cancel_open_buy_orders_for_token(client, token_id)
        if canceled:
            print(f"[EXIT] {reason} -> 已撤销 BUY 挂单数量={canceled}")

    if exit_only:
        _exit_cleanup_only("exit-only cleanup")
        return

    slug_for_refresh = ""
    if isinstance(market_meta, dict):
        slug_for_refresh = (
            str(market_meta.get("slug") or "")
            or str(market_meta.get("market_slug") or "")
        )
        if not slug_for_refresh:
            raw_meta = market_meta.get("raw") if isinstance(market_meta, dict) else {}
            if isinstance(raw_meta, dict):
                slug_for_refresh = str(raw_meta.get("slug") or "")
    if not slug_for_refresh:
        slug_for_refresh = _extract_market_slug(source)
    unable_to_refresh_logged = False

    def _refresh_market_meta() -> Dict[str, Any]:
        nonlocal market_meta, market_deadline_ts, unable_to_refresh_logged
        nonlocal profit_floor, market_price_precision, profit_pct
        slug = slug_for_refresh
        if not slug:
            if not unable_to_refresh_logged:
                print("[COUNTDOWN] 无市场 slug，无法刷新事件状态，仅依赖本地信息。")
                unable_to_refresh_logged = True
            return market_meta
        m_obj = _fetch_market_by_slug(slug)
        if isinstance(m_obj, dict):
            refreshed = _market_meta_from_obj(m_obj, timezone_override_hint)
            if refreshed:
                if manual_deadline_override_ts and not refreshed.get("resolved_ts"):
                    refreshed = _apply_manual_deadline_override_meta(
                        refreshed,
                        manual_deadline_override_ts,
                    )
                if manual_deadline_disabled:
                    refreshed = dict(refreshed)
                    refreshed.pop("end_ts", None)
                    refreshed.pop("resolved_ts", None)
                    market_deadline_ts = None
                else:
                    new_deadline = _calc_deadline(refreshed)
                    if new_deadline:
                        market_deadline_ts = new_deadline
                new_floor, new_precision = _calc_profit_floor(refreshed)
                meta_changed = (
                    new_precision != market_price_precision
                    or abs(new_floor - profit_floor) > 1e-12
                )
                market_meta = refreshed
                profit_floor = new_floor
                market_price_precision = new_precision
                if meta_changed:
                    _log_profit_floor("[INFO][REFRESH]")
                    adjusted_profit = _enforce_profit_floor(
                        profit_pct, prefix="[ADJUST]"
                    )
                    if adjusted_profit != profit_pct:
                        profit_pct = adjusted_profit
                        cfg.profit_pct = cfg.profit_ratio = profit_pct
                        strategy.cfg.profit_pct = strategy.cfg.profit_ratio = profit_pct
        return market_meta

    def _calc_size_by_1dollar(ask_px: float) -> float:
        if not ask_px or ask_px <= 0:
            return 1.0
        s = 1.0 / ask_px
        return float(Decimal(str(s)).quantize(Decimal("1"), rounding=ROUND_UP))

    def _probe_position_size_for_buy() -> Tuple[Optional[float], Optional[str]]:
        try:
            _avg_px, total_pos, origin_note = _lookup_position_avg_price(client, token_id)
        except Exception as probe_exc:
            print(f"[WATCHDOG][BUY] 下单前持仓查询异常：{probe_exc}")
            return None, None
        origin_display = origin_note or "positions"
        return total_pos, origin_display

    def _extract_ts(raw: Optional[Any]) -> float:
        if raw is None:
            return time.time()
        if isinstance(raw, (int, float)):
            ts = float(raw)
            if ts > 1e12:
                ts = ts / 1000.0
            return ts
        if isinstance(raw, str):
            text = raw.strip()
            if not text:
                return time.time()
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            try:
                parsed = datetime.fromisoformat(text)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed.timestamp()
            except ValueError:
                try:
                    ts = float(text)
                    if ts > 1e12:
                        ts = ts / 1000.0
                    return ts
                except Exception:
                    return time.time()
        return time.time()

    def _is_market_closed(payload: Dict[str, Any]) -> bool:
        status_keys = ["status", "market_status", "marketStatus"]
        for key in status_keys:
            val = payload.get(key)
            if isinstance(val, str) and val.lower() in {"closed", "settled", "resolved", "expired"}:
                return True
        bool_keys = ["is_closed", "market_closed", "closed", "isMarketClosed"]
        for key in bool_keys:
            val = payload.get(key)
            if isinstance(val, bool) and val:
                return True
            if isinstance(val, str) and val.strip().lower() in {"true", "1", "yes"}:
                return True
        return False

    def _event_indicates_market_closed(ev: Dict[str, Any]) -> bool:
        if not isinstance(ev, dict):
            return False

        if _is_market_closed(ev):
            return True

        queue: List[Dict[str, Any]] = []
        for key in ("market", "market_state", "marketState", "marketStatus", "data", "payload"):
            val = ev.get(key)
            if isinstance(val, dict):
                queue.append(val)
            elif isinstance(val, list):
                for item in val:
                    if isinstance(item, dict):
                        queue.append(item)

        while queue:
            item = queue.pop()
            if _is_market_closed(item):
                return True
            for key, val in item.items():
                if isinstance(val, dict):
                    queue.append(val)
                elif isinstance(val, list):
                    for sub in val:
                        if isinstance(sub, dict):
                            queue.append(sub)
        return False

    def _parse_price_change(pc: Dict[str, Any]) -> Tuple[float, float, float]:
        def _to_float(val: Any) -> Optional[float]:
            if val is None:
                return None
            try:
                return float(val)
            except (TypeError, ValueError):
                return None

        price_fields = (
            "last_trade_price",
            "last_price",
            "mark_price",
            "price",
        )

        bid = _to_float(pc.get("best_bid"))
        ask = _to_float(pc.get("best_ask"))

        price_val: Optional[float] = None
        for key in price_fields:
            price_val = _to_float(pc.get(key))
            if price_val is not None:
                break

        if price_val is None:
            if bid is not None and ask is not None:
                price_val = (bid + ask) / 2.0
            elif bid is not None:
                price_val = bid
            elif ask is not None:
                price_val = ask
            else:
                price_val = 0.0

        return (
            bid or 0.0,
            ask or 0.0,
            price_val,
        )

    last_event_processed_ts = 0.0

    def _on_event(ev: Dict[str, Any]):
        nonlocal market_closed_detected, last_event_processed_ts, last_signal_ts
        if stop_event.is_set():
            return
        if not isinstance(ev, dict):
            return
        if _event_indicates_market_closed(ev):
            print("[MARKET] 收到市场关闭事件，准备退出…")
            market_closed_detected = True
            strategy.stop("market closed")
            stop_event.set()
            return

        if ev.get("event_type") == "price_change":
            pcs = ev.get("price_changes", [])
        elif "price_changes" in ev:
            pcs = ev.get("price_changes", [])
        else:
            return
        ts = _extract_ts(ev.get("timestamp") or ev.get("ts") or ev.get("time"))
        with ws_state_lock:
            ws_state["last_event_ts"] = time.time()
        now = time.time()
        if now - last_event_processed_ts < 60.0:
            return
        last_event_processed_ts = now

        for pc in pcs:
            if str(pc.get("asset_id")) != str(token_id):
                continue
            bid, ask, last = _parse_price_change(pc)
            # ✅ P0修复：使用当前时间而非事件时间戳，保持与共享WS模式一致
            latest[token_id] = {"price": last, "best_bid": bid, "best_ask": ask, "ts": time.time()}

            # 添加独立WS的调试日志（与共享WS对应）
            if not hasattr(_on_event, "_last_debug_log"):
                _on_event._last_debug_log = 0
                _on_event._event_count = 0
                print(f"[WS][INDEPENDENT] 开始接收独立WS事件 (token={token_id})")

            _on_event._event_count += 1
            now = time.time()
            if now - _on_event._last_debug_log >= 30:  # 每30秒打印一次
                print(f"[WS][INDEPENDENT] events={_on_event._event_count}, "
                      f"bid={bid}, ask={ask}, price={last}")
                _on_event._last_debug_log = now

            action = strategy.on_tick(best_ask=ask, best_bid=bid, ts=ts)
            if action and action.action in (ActionType.BUY, ActionType.SELL):
                action_queue.put(action)
                last_signal_ts = time.time()  # 更新最后一次信号时间
            if _is_market_closed(pc):
                print("[MARKET] 检测到市场关闭信号，准备退出…")
                market_closed_detected = True
                strategy.stop("market closed")
                stop_event.set()
                break

    def _confirm_market_closed():
        nonlocal market_closed_detected
        attempt = 0
        while not stop_event.is_set():
            refreshed_meta = _refresh_market_meta()
            attempt += 1
            now = time.time()
            if _market_has_ended(refreshed_meta, now):
                print("[MARKET] 已确认市场结束，可进行后续处理。")
                market_closed_detected = True
                strategy.stop("market ended confirmed")
                stop_event.set()
                return
            if attempt == 1:
                print("[MARKET] 倒计时结束但市场尚未标记结束，10 秒后再次检查…")
            else:
                print(
                    f"[MARKET] 第 {attempt} 次检查仍未确认结束，10 秒后再次重试…"
                )
            for _ in range(10):
                if stop_event.is_set():
                    return
                time.sleep(1)

                for _ in range(int(wait)):
                    if stop_event.is_set():
                        return
                    time.sleep(1)

    # 从命令行参数获取共享 WS 缓存路径（不再使用环境变量 POLY_WS_SHARED_CACHE）
    shared_ws_cache_path = os.getenv("_POLY_WS_CACHE_ARG")

    # Fallback: 如果没有通过命令行传递，尝试使用固定路径约定
    if not shared_ws_cache_path:
        # 约定路径：../data/ws_cache.json（相对于当前脚本）
        default_cache_path = Path(__file__).parent.parent / "data" / "ws_cache.json"
        if default_cache_path.exists():
            try:
                # 检查文件是否在最近2分钟内更新过
                cache_age = time.time() - default_cache_path.stat().st_mtime
                if cache_age < 120:
                    shared_ws_cache_path = str(default_cache_path)
                    print(f"[WS] 使用默认共享缓存路径: {shared_ws_cache_path}")
            except OSError:
                pass

    use_shared_ws = bool(shared_ws_cache_path)
    last_shared_ts = 0.0

    def _load_shared_ws_snapshot() -> Optional[Dict[str, Any]]:
        if not shared_ws_cache_path:
            return None
        try:
            with open(shared_ws_cache_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            tokens = payload.get("tokens") if isinstance(payload, dict) else None
            if not isinstance(tokens, dict):
                if not hasattr(_load_shared_ws_snapshot, "_warned_format"):
                    print(f"[WS][SHARED] ✗ 缓存文件格式错误（缺少 tokens 字段）")
                    print(f"[WS][SHARED] 缓存内容类型: {type(payload)}")
                    _load_shared_ws_snapshot._warned_format = True
                return None
            snapshot = tokens.get(str(token_id))
            if snapshot is None:
                if not hasattr(_load_shared_ws_snapshot, "_warned_missing_token"):
                    print(f"[WS][SHARED] ✗ 缓存中未找到 token {token_id} 的数据")
                    print(f"[WS][SHARED] 查询key: '{str(token_id)}' (类型={type(str(token_id)).__name__}, 长度={len(str(token_id))})")
                    print(f"[WS][SHARED] 缓存中的 tokens: {list(tokens.keys())[:5]}...")
                    print(f"[WS][SHARED] 缓存总token数: {len(tokens)}")

                    # 调试：显示缓存key的格式
                    if tokens:
                        first_key = list(tokens.keys())[0]
                        print(f"[WS][SHARED] 缓存key示例: '{first_key}' (类型={type(first_key).__name__}, 长度={len(first_key)})")

                    _load_shared_ws_snapshot._warned_missing_token = True
                    _load_shared_ws_snapshot._first_warned_at = time.time()
                elif time.time() - getattr(_load_shared_ws_snapshot, "_first_warned_at", 0) > 30:
                    # 30秒后重新警告一次
                    print(f"[WS][SHARED] ⚠ 启动30秒后仍未从缓存获取到token {token_id} 的数据")
                    print(f"[WS][SHARED] 当前缓存包含的tokens: {list(tokens.keys())[:10]}")
            return snapshot
        except (OSError, json.JSONDecodeError) as e:
            if not hasattr(_load_shared_ws_snapshot, "_warned_error"):
                print(f"[WS][SHARED] ✗ 读取缓存文件失败: {e}")
                _load_shared_ws_snapshot._warned_error = True
            return None

    def _apply_shared_ws_snapshot() -> None:
        nonlocal last_shared_ts, last_signal_ts

        # 追踪函数调用频率（诊断用）
        if not hasattr(_apply_shared_ws_snapshot, "_total_calls"):
            _apply_shared_ws_snapshot._total_calls = 0
            _apply_shared_ws_snapshot._last_call_log = 0
        _apply_shared_ws_snapshot._total_calls += 1

        # 每100次调用打印一次（约50秒）
        if _apply_shared_ws_snapshot._total_calls % 100 == 1:
            print(f"[WS][SHARED][TRACE] 函数调用次数: {_apply_shared_ws_snapshot._total_calls}")
            sys.stdout.flush()

        snapshot = _load_shared_ws_snapshot()

        # 首次读取失败的警告（首次打印，之后周期性打印）
        if not snapshot:
            if not hasattr(_apply_shared_ws_snapshot, "_warned_missing"):
                if not os.path.exists(shared_ws_cache_path):
                    print(f"[WARN] ⚠ 共享WS缓存文件不存在: {shared_ws_cache_path}")
                else:
                    print(f"[WARN] ⚠ 共享WS缓存中未找到token {token_id}，可能聚合器未订阅此token")
                print(f"[WARN] ⚠ 策略将无法获取价格数据，不会生成交易信号！")
                sys.stdout.flush()  # 立即输出首次警告
                _apply_shared_ws_snapshot._warned_missing = True
                _apply_shared_ws_snapshot._first_missing_at = time.time()
                _apply_shared_ws_snapshot._last_missing_log = time.time()
                _apply_shared_ws_snapshot._missing_count = 0

            _apply_shared_ws_snapshot._missing_count += 1
            elapsed = time.time() - _apply_shared_ws_snapshot._first_missing_at

            # 周期性打印警告（每60秒一次）以便观察问题
            if time.time() - _apply_shared_ws_snapshot._last_missing_log >= 60:
                print(f"[WARN][PERSISTENT] ⚠ 缓存中持续 {elapsed:.0f}秒 无token数据 (尝试次数={_apply_shared_ws_snapshot._missing_count})")
                print(f"[WARN][PERSISTENT] ⚠ 策略无法接收价格更新，不会生成交易信号！")
                print(f"[WARN][PERSISTENT] token_id={token_id}")
                print(f"[WARN][PERSISTENT] cache_path={shared_ws_cache_path}")
                sys.stdout.flush()  # 立即输出警告

                # 尝试读取缓存诊断
                try:
                    if os.path.exists(shared_ws_cache_path):
                        with open(shared_ws_cache_path, "r", encoding="utf-8") as f:
                            payload = json.load(f)
                        tokens = payload.get("tokens", {})
                        cache_token_ids = list(tokens.keys())[:10]
                        print(f"[WARN][PERSISTENT] 缓存中实际有 {len(tokens)} 个token")
                        print(f"[WARN][PERSISTENT] 缓存中的token_id示例: {cache_token_ids}")
                        # 检查格式匹配
                        if str(token_id) not in tokens:
                            print(f"[WARN][PERSISTENT] ✗ str(token_id)='{str(token_id)}' 不在缓存keys中")
                            # 尝试查找相似的key
                            similar = [k for k in cache_token_ids if token_id in k or k in str(token_id)]
                            if similar:
                                print(f"[WARN][PERSISTENT] 💡 发现相似key: {similar}")
                    else:
                        print(f"[WARN][PERSISTENT] 缓存文件不存在")
                except Exception as diag_e:
                    print(f"[WARN][PERSISTENT] 诊断失败: {diag_e}")

                _apply_shared_ws_snapshot._last_missing_log = time.time()

            # ✅ 长时间无缓存数据则退出，释放队列位置
            if elapsed > 1800:  # 30分钟超时
                print(f"[ERROR] 共享WS缓存中持续30分钟无此token数据，聚合器可能未订阅")
                print(f"[ERROR] ⚠ 策略无法获取价格数据，无法生成交易信号！")
                print(f"[EXIT] 释放队列：token {token_id} 无法从聚合器获取数据")
                print(f"[HINT] 请检查：1) 聚合器是否运行 2) 是否订阅了此token")
                sys.stdout.flush()
                _cancel_open_buy_orders_before_exit("AGGREGATOR_NO_DATA")
                _log_error("AGGREGATOR_NO_DATA", {
                    "token_id": token_id,
                    "message": "聚合器缓存中持续无此token数据",
                    "timeout_seconds": 1800
                })
                _record_exit_token(token_id, "AGGREGATOR_NO_DATA", {
                    "duration_seconds": elapsed,
                    "timeout_seconds": 1800,
                    "has_position": False,  # 启动阶段，无持仓
                })
                strategy.stop("aggregator no data")
                stop_event.set()
            return  # ← 关键：没有数据时直接return，不调用strategy.on_tick()！

        # 重置警告标志
        if hasattr(_apply_shared_ws_snapshot, "_warned_missing"):
            elapsed = time.time() - _apply_shared_ws_snapshot._first_missing_at
            print(f"[INFO] ✓ 共享WS缓存文件已就绪（等待了 {elapsed:.1f}秒）")
            print(f"[INFO] ✓ 策略现在可以接收价格数据并生成交易信号")
            sys.stdout.flush()
            delattr(_apply_shared_ws_snapshot, "_warned_missing")

        if _is_market_closed(snapshot):
            print("[MARKET] 收到市场关闭事件，准备退出…")
            _cancel_open_buy_orders_before_exit("MARKET_CLOSED")
            strategy.stop("market closed")
            stop_event.set()
            return

        ts = _extract_ts(snapshot.get("ts"))
        if ts is None:
            ts = time.time()

        # 使用 seq 进行去重（seq是单调递增的序列号，比updated_at更可靠）
        # seq是聚合器为每个token单调递增的计数器，每次该token有新事件就+1
        seq = snapshot.get("seq", 0)
        updated_at = snapshot.get("updated_at", 0.0)

        # P0修复：运行时数据新鲜度检查（30分钟阈值）
        # 如果缓存数据过期超过30分钟，说明市场可能失去流动性，应退出释放队列位置
        if updated_at > 0:
            data_age = time.time() - updated_at
            STALE_DATA_THRESHOLD = 1800.0  # 30分钟

            if data_age > STALE_DATA_THRESHOLD:
                print(f"[STALE_DATA] 缓存数据过期 {data_age/60:.1f} 分钟（阈值 {STALE_DATA_THRESHOLD/60:.0f} 分钟）")
                print(f"[STALE_DATA] 该token市场可能失去流动性，退出交易释放队列位置")

                _cancel_open_buy_orders_before_exit("STALE_DATA_IN_TRADING")
                _log_error("STALE_DATA_IN_TRADING", {
                    "token_id": token_id,
                    "data_age_seconds": data_age,
                    "data_age_minutes": data_age / 60.0,
                    "updated_at": updated_at,
                    "threshold_seconds": STALE_DATA_THRESHOLD,
                    "current_seq": seq,
                    "message": "运行时检测到数据过期，市场可能无流动性"
                })

                # 注意：此时无法获取精确的持仓状态，标记为 "unknown"
                # 回填时将根据策略判断是否重试
                _record_exit_token(token_id, "STALE_DATA_IN_TRADING", {
                    "data_age_minutes": data_age / 60.0,
                    "threshold_minutes": STALE_DATA_THRESHOLD / 60.0,
                    "updated_at": updated_at,
                    "last_seq": seq,
                    "has_position": None,  # 持仓状态未知
                })

                strategy.stop("stale data detected in trading loop")
                stop_event.set()
                return

        # 初始化去重状态和调试计数器
        if not hasattr(_apply_shared_ws_snapshot, "_last_seq"):
            _apply_shared_ws_snapshot._last_seq = 0
            _apply_shared_ws_snapshot._last_updated_at = 0.0
            _apply_shared_ws_snapshot._read_count = 0  # 总读取次数
            _apply_shared_ws_snapshot._skip_count = 0  # 跳过次数
            _apply_shared_ws_snapshot._last_detailed_log = 0  # 详细日志时间戳
            _apply_shared_ws_snapshot._last_tick_ts = 0.0  # 上次调用on_tick的时间

        # 累计读取次数
        _apply_shared_ws_snapshot._read_count += 1

        # 每60秒打印一次详细的调试信息
        now = time.time()
        if now - _apply_shared_ws_snapshot._last_detailed_log >= 60:
            print(f"[WS][SHARED][DEBUG] 读取统计: read={_apply_shared_ws_snapshot._read_count}, "
                  f"skip={_apply_shared_ws_snapshot._skip_count}, "
                  f"当前seq={seq} (last={_apply_shared_ws_snapshot._last_seq}), "
                  f"updated_at={updated_at:.2f} (last={_apply_shared_ws_snapshot._last_updated_at:.2f})")
            _apply_shared_ws_snapshot._last_detailed_log = now

        # 判断是否需要将数据喂给策略：
        # 1. seq增大 → 肯定是新数据，必须喂给策略
        # 2. seq变小 → 聚合器重启导致seq重置，必须喂给策略
        # 3. seq相同但updated_at变大 → 同一事件的数据被修正，必须喂给策略
        # 4. 初始化阶段(latest为空) → 必须至少更新一次latest，即使数据重复
        # 5. seq和updated_at都不变 + 距离上次on_tick超过10秒 → 周期性喂给策略（让横盘数据也能累积）
        # 6. 其他情况 → 跳过策略调用但仍更新latest（确保latest始终有最新快照）
        should_feed_strategy = False
        is_new_data = False
        skip_reason = ""

        # 检查是否在初始化阶段（latest尚未设置）
        is_initializing = not latest.get(token_id)

        if seq > _apply_shared_ws_snapshot._last_seq:
            # 正常的新数据（seq递增）
            is_new_data = True
            should_feed_strategy = True
            # 打印seq增长（每次都打印，便于诊断）
            seq_delta = seq - _apply_shared_ws_snapshot._last_seq
            if seq_delta > 1 or _apply_shared_ws_snapshot._last_seq == 0:
                print(f"[WS][SHARED] ✓ 检测到seq增长: {_apply_shared_ws_snapshot._last_seq} → {seq} (+{seq_delta})")
        elif seq < _apply_shared_ws_snapshot._last_seq:
            # seq变小，说明聚合器重启，重置seq
            print(f"[WS][SHARED] ⚠ 检测到seq重置 ({_apply_shared_ws_snapshot._last_seq} → {seq})，接受新数据")
            is_new_data = True
            should_feed_strategy = True
        elif is_initializing:
            # 初始化阶段，即使seq和updated_at都不变，也要至少更新一次
            should_feed_strategy = True
            print(f"[WS][SHARED] 初始化: 首次读取到数据 (seq={seq})")
        elif seq == _apply_shared_ws_snapshot._last_seq:
            # seq相同，检查updated_at
            if updated_at > _apply_shared_ws_snapshot._last_updated_at:
                # 同一seq但时间戳变大，可能是数据修正
                is_new_data = True
                should_feed_strategy = True
            else:
                # seq和updated_at都不变，检查是否需要周期性喂给策略
                time_since_last_tick = now - _apply_shared_ws_snapshot._last_tick_ts
                if time_since_last_tick >= 5.0:  # 优化为5秒周期，平衡性能和响应速度
                    # 即使数据不变，也周期性喂给策略（让横盘价格也能累积历史）
                    should_feed_strategy = True
                    if time_since_last_tick >= 30.0:  # 超过30秒才打印日志
                        print(f"[WS][SHARED] 市场横盘 {time_since_last_tick:.0f}秒，周期性喂价格给策略 (seq={seq})")
                else:
                    # 距离上次on_tick不到5秒，跳过策略调用
                    _apply_shared_ws_snapshot._skip_count += 1
                    skip_reason = f"seq和updated_at都未变化且距上次tick仅{time_since_last_tick:.1f}秒"
                    # 注意：即使跳过策略调用，仍然更新latest快照（见下文）

        if not should_feed_strategy:
            # 即使不调用策略，也要更新latest快照（确保初始化能完成）
            bid = float(snapshot.get("best_bid") or 0.0)
            ask = float(snapshot.get("best_ask") or 0.0)
            last_px = float(snapshot.get("price") or 0.0)
            # ✅ P0修复：使用当前时间而非事件时间戳，避免市场横盘时快照被误判为过期
            latest[token_id] = {"price": last_px, "best_bid": bid, "best_ask": ask, "ts": time.time()}
            _apply_shared_ws_snapshot._skip_count += 1

            # 调试日志：每100次跳过打印一次
            if not hasattr(_apply_shared_ws_snapshot, "_skip_log_count"):
                _apply_shared_ws_snapshot._skip_log_count = 0
            _apply_shared_ws_snapshot._skip_log_count += 1
            if _apply_shared_ws_snapshot._skip_log_count % 100 == 1:
                print(f"[WS][SHARED][SKIP] 跳过策略调用 (总跳过次数={_apply_shared_ws_snapshot._skip_count}): {skip_reason}")
                print(f"[WS][SHARED][SKIP] 当前状态: seq={seq}, last_seq={_apply_shared_ws_snapshot._last_seq}, bid={bid}, ask={ask}")
            return

        # 更新去重状态（只在真正有新数据时更新）
        if is_new_data:
            _apply_shared_ws_snapshot._last_seq = seq
            _apply_shared_ws_snapshot._last_updated_at = updated_at

        # 无论是否新数据，只要喂给策略就更新last_tick_ts
        _apply_shared_ws_snapshot._last_tick_ts = now
        last_shared_ts = ts  # 保留用于日志

        bid = float(snapshot.get("best_bid") or 0.0)
        ask = float(snapshot.get("best_ask") or 0.0)
        last_px = float(snapshot.get("price") or 0.0)
        # ✅ P0修复：latest使用当前时间，避免市场横盘时快照被误判为过期
        # strategy.on_tick仍使用事件原始时间戳ts，保持策略逻辑不变
        latest[token_id] = {"price": last_px, "best_bid": bid, "best_ask": ask, "ts": time.time()}
        action = strategy.on_tick(best_ask=ask, best_bid=bid, ts=ts)
        if action and action.action in (ActionType.BUY, ActionType.SELL):
            action_queue.put(action)
            last_signal_ts = time.time()  # 更新最后一次信号时间
        with ws_state_lock:
            ws_state["last_event_ts"] = time.time()

        # 定期打印调试信息（每30秒，便于观察seq变化）
        if not hasattr(_apply_shared_ws_snapshot, "_last_debug_log"):
            _apply_shared_ws_snapshot._last_debug_log = 0
            _apply_shared_ws_snapshot._update_count = 0
            print(f"[WS][SHARED] 开始从共享缓存读取数据 (token={token_id})")

        _apply_shared_ws_snapshot._update_count += 1
        now = time.time()
        if now - _apply_shared_ws_snapshot._last_debug_log >= 30:  # 从300秒改为30秒
            elapsed = now - _apply_shared_ws_snapshot._last_debug_log if _apply_shared_ws_snapshot._last_debug_log > 0 else 30
            updates_per_min = (_apply_shared_ws_snapshot._update_count / elapsed) * 60 if elapsed > 0 else 0
            print(f"[WS][SHARED] seq={seq}, updates={_apply_shared_ws_snapshot._update_count} "
                  f"({updates_per_min:.1f}/min), bid={bid}, ask={ask}, price={last_px}")
            _apply_shared_ws_snapshot._last_debug_log = now
            _apply_shared_ws_snapshot._update_count = 0  # 重置计数器

        # 诊断日志：函数即将返回（首次打印）
        if not hasattr(_apply_shared_ws_snapshot, "_return_logged"):
            print(f"[WS][SHARED][TRACE] ✓ 函数执行完成，准备返回 (latest已设置: {bool(latest.get(token_id))})")
            _apply_shared_ws_snapshot._return_logged = True

    def _on_ws_state(state: str, info: Dict[str, Any]):
        ts_now = time.time()
        with ws_state_lock:
            ws_state["last_state"] = state
            ws_state["last_state_ts"] = ts_now
            if state == "open":
                ws_state["open"] = True
                ws_state["last_error"] = ""
            elif state == "error":
                ws_state["last_error"] = str(info.get("error") or "")
            elif state in {"closed", "silence"}:
                ws_state["open"] = False

        if state == "open":
            print("[WS] 连接已建立，等待行情推送…")
        elif state == "silence":
            timeout = info.get("timeout")
            print(f"[WS] 超过 {timeout}s 未收到消息，正在尝试重连…")
        elif state == "error":
            print(f"[WS] 连接异常：{info.get('error')}")
        elif state == "closed":
            status_code = info.get("status_code")
            print(f"[WS] 连接关闭（code={status_code}），等待重连…")

    # 健康检查：即使设置了共享 WS，也要验证文件是否存在
    if use_shared_ws:
        print(f"[WS][SHARED] ✓ 将使用共享 WS 缓存模式")
        print(f"[WS][SHARED] 缓存路径: {shared_ws_cache_path}")
        if not os.path.exists(shared_ws_cache_path):
            print(f"[WS][SHARED] ✗ 缓存文件不存在")
            print(f"[WS][FATAL] 共享WS不可用，禁止切换独立WS，退出")
            _cancel_open_buy_orders_before_exit("SHARED_WS_UNAVAILABLE")
            _record_exit_token(token_id, "SHARED_WS_UNAVAILABLE", {
                "cache_path": shared_ws_cache_path,
                "reason": "missing",
            })
            strategy.stop("shared ws unavailable")
            stop_event.set()
            return
        else:
            # 检查文件是否过期（超过5分钟未更新）
            try:
                file_age = time.time() - os.path.getmtime(shared_ws_cache_path)
                if file_age > 300:
                    print(f"[WS][SHARED] ✗ 缓存文件过期（{file_age:.0f}秒未更新）")
                    print(f"[WS][FATAL] 共享WS缓存过期，禁止切换独立WS，退出")
                    _cancel_open_buy_orders_before_exit("SHARED_WS_UNAVAILABLE")
                    _record_exit_token(token_id, "SHARED_WS_UNAVAILABLE", {
                        "cache_path": shared_ws_cache_path,
                        "reason": "stale",
                        "file_age_seconds": file_age,
                    })
                    strategy.stop("shared ws unavailable")
                    stop_event.set()
                    return
                else:
                    print(f"[WS][SHARED] ✓ 缓存文件新鲜（{file_age:.0f}秒前更新）")
            except OSError:
                print("[WARN] 无法读取共享 WS 缓存文件状态")
                print("[WS][FATAL] 共享WS状态不可读，禁止切换独立WS，退出")
                _cancel_open_buy_orders_before_exit("SHARED_WS_UNAVAILABLE")
                _record_exit_token(token_id, "SHARED_WS_UNAVAILABLE", {
                    "cache_path": shared_ws_cache_path,
                    "reason": "stat_failed",
                })
                strategy.stop("shared ws unavailable")
                stop_event.set()
                return

    if not use_shared_ws:
        print("[WS][FATAL] 独立WS已被禁用，但use_shared_ws=False，退出")
        _cancel_open_buy_orders_before_exit("SHARED_WS_UNAVAILABLE")
        _record_exit_token(token_id, "SHARED_WS_UNAVAILABLE", {
            "cache_path": shared_ws_cache_path,
            "reason": "disabled_fallback",
        })
        strategy.stop("shared ws unavailable")
        stop_event.set()
        return

    print("[RUN] 监听行情中… 输入 stop / exit 可手动停止。")

    start_wait = time.time()
    wait_timeout = 600.0  # 最长等待600秒（10分钟），避免API响应慢导致的误伤
    last_progress_log = start_wait
    progress_log_interval = 30.0  # 等待进度日志打印间隔（秒）
    wait_loop_iteration = 0  # 诊断计数器
    while not latest.get(token_id) and not stop_event.is_set():
        wait_loop_iteration += 1
        now_ts = time.time()
        elapsed_wait = now_ts - start_wait

        # 快速失败检查：如果共享WS模式下缓存数据过期，提前退出
        if use_shared_ws and elapsed_wait > 30:  # 等待30秒后开始检查
            snapshot = _load_shared_ws_snapshot()
            if snapshot:
                updated_at = snapshot.get("updated_at", 0)
                data_age = time.time() - updated_at if updated_at > 0 else 999999

                # 检查市场状态
                is_closed = (
                    snapshot.get("market_closed")
                    or snapshot.get("is_closed")
                    or snapshot.get("closed")
                )

                if is_closed:
                    print(f"[WAIT][CLOSED] 检测到市场已关闭，提前退出")
                    print("[QUEUE] 释放队列：市场已关闭。")
                    _cancel_open_buy_orders_before_exit("MARKET_CLOSED")
                    _log_error("MARKET_CLOSED", {
                        "token_id": token_id,
                        "elapsed_seconds": elapsed_wait,
                        "message": "市场已关闭"
                    })
                    _record_exit_token(token_id, "MARKET_CLOSED", {
                        "elapsed_seconds": elapsed_wait,
                        "detected_at_startup": True,
                        "has_position": False,  # 启动阶段，无持仓
                    })
                    stop_event.set()
                    return

                # 数据过期超过30分钟，认为token可能无流动性
                if data_age > 1800:
                    print(f"[WAIT][STALE] 缓存数据过期{data_age:.0f}秒，该token可能无流动性")
                    print("[QUEUE] 释放队列：token长期无数据更新，提前退出。")
                    _cancel_open_buy_orders_before_exit("STALE_DATA_TIMEOUT")
                    _log_error("STALE_DATA_TIMEOUT", {
                        "token_id": token_id,
                        "elapsed_seconds": elapsed_wait,
                        "data_age_seconds": data_age,
                        "message": "缓存数据长期未更新，token可能无流动性"
                    })
                    _record_exit_token(token_id, "STALE_DATA_TIMEOUT", {
                        "elapsed_seconds": elapsed_wait,
                        "data_age_seconds": data_age,
                        "has_position": False,  # 启动阶段，无持仓
                    })
                    stop_event.set()
                    return

        # 超时检查
        if elapsed_wait > wait_timeout:
            timeout_msg = f"[WAIT][TIMEOUT] 等待{elapsed_wait:.0f}秒后仍未收到行情，退出"
            print(timeout_msg)
            if use_shared_ws:
                cache_info = f"共享WS缓存路径: {shared_ws_cache_path}"
                reason_info = "可能原因：(1)聚合器未订阅该token (2)该token无行情数据"
                print(f"[WAIT][TIMEOUT] {cache_info}")
                print(f"[WAIT][TIMEOUT] {reason_info}")
            queue_info = "[QUEUE] 释放队列：启动后无法获取行情数据，已退出。"
            print(queue_info)

            # 记录到错误日志文件
            _log_error("STARTUP_TIMEOUT", {
                "token_id": token_id,
                "elapsed_seconds": elapsed_wait,
                "use_shared_ws": use_shared_ws,
                "shared_ws_cache_path": shared_ws_cache_path if use_shared_ws else None,
                "message": "启动后等待行情超时"
            })
            _cancel_open_buy_orders_before_exit("STARTUP_TIMEOUT")
            _record_exit_token(token_id, "STARTUP_TIMEOUT", {
                "elapsed_seconds": elapsed_wait,
                "use_shared_ws": use_shared_ws,
                "has_position": False,  # 启动阶段，无持仓
            })

            stop_event.set()
            return

        if now_ts - last_progress_log >= progress_log_interval:
            state_note = ""
            with ws_state_lock:
                last_state = ws_state.get("last_state")
                last_state_ts = ws_state.get("last_state_ts", 0.0)
                last_event_ts = ws_state.get("last_event_ts", 0.0)
                last_error = ws_state.get("last_error")
            state_age = now_ts - last_state_ts if last_state_ts else None
            event_age = now_ts - last_event_ts if last_event_ts else None
            if last_state:
                parts = [f"ws={last_state}"]
                if state_age is not None:
                    parts.append(f"age={state_age:.0f}s")
                if event_age is not None and event_age < 1e9 and last_event_ts > 0:
                    parts.append(f"last_event={event_age:.0f}s")
                if last_error:
                    parts.append(f"err={last_error}")
                state_note = " | " + " ".join(parts)
            mode_tag = "[SHARED]" if use_shared_ws else "[INDEPENDENT]"
            print(f"[WAIT]{mode_tag} 尚未收到行情，继续等待({elapsed_wait:.0f}s / {wait_timeout:.0f}s)…{state_note}")

            # 显示去重统计信息
            if use_shared_ws and hasattr(_apply_shared_ws_snapshot, "_read_count"):
                read_count = _apply_shared_ws_snapshot._read_count
                skip_count = _apply_shared_ws_snapshot._skip_count
                last_seq = _apply_shared_ws_snapshot._last_seq
                print(f"[WAIT][DEBUG] 读取统计: read={read_count}, skip={skip_count}, last_seq={last_seq}, latest_set={'是' if latest.get(token_id) else '否'}")

            # 共享WS模式下，额外打印缓存诊断信息
            if use_shared_ws and elapsed_wait > 15:  # 从30秒改为15秒
                try:
                    snapshot = _load_shared_ws_snapshot()
                    if snapshot is None:
                        with open(shared_ws_cache_path, "r", encoding="utf-8") as f:
                            payload = json.load(f)
                        tokens = payload.get("tokens", {})
                        print(f"[WAIT][DEBUG] 缓存中有 {len(tokens)} 个token: {list(tokens.keys())[:5]}...")
                        if str(token_id) not in tokens:
                            print(f"[WAIT][DEBUG] ✗ 目标token {token_id} 不在缓存中")
                        else:
                            print(f"[WAIT][DEBUG] ✓ 目标token {token_id} 已在缓存中，但可能是旧数据")
                except Exception as e:
                    print(f"[WAIT][DEBUG] 读取缓存诊断信息失败: {e}")

            last_progress_log = now_ts
        if use_shared_ws:
            _apply_shared_ws_snapshot()
            # 诊断日志：检查函数调用后的状态
            if not hasattr(_apply_shared_ws_snapshot, "_post_call_log_count"):
                _apply_shared_ws_snapshot._post_call_log_count = 0
            _apply_shared_ws_snapshot._post_call_log_count += 1
            if _apply_shared_ws_snapshot._post_call_log_count <= 3:  # 只打印前3次
                print(f"[WAIT][TRACE] _apply_shared_ws_snapshot()返回，latest已设置: {bool(latest.get(token_id))}, stop_event: {stop_event.is_set()}")
        time.sleep(0.2)
        # 诊断日志：检查循环状态
        if wait_loop_iteration <= 5:  # 只打印前5次迭代
            print(f"[WAIT][TRACE] Loop iteration {wait_loop_iteration}, latest: {bool(latest.get(token_id))}, stop: {stop_event.is_set()}")

    # 诊断日志：waiting loop结束
    print(f"[WAIT][END] ✓ Waiting loop结束，latest已设置: {bool(latest.get(token_id))}")

    if stop_event.is_set():
        print("[EXIT] 已终止。")
        return

    print("[WAIT][END] ✓ stop_event未设置，继续初始化...")

    print("[INIT][TRACE] 1. 准备启动input_listener线程...")

    def _input_listener():
        while not stop_event.is_set():
            try:
                cmd = input().strip().lower()
            except EOFError:
                break
            if cmd in {"stop", "exit", "quit"}:
                print("[CMD] 收到停止指令，准备退出…")
                strategy.stop("manual stop")
                stop_event.set()
                break

    threading.Thread(target=_input_listener, daemon=True).start()
    print("[INIT][TRACE] 2. input_listener线程已启动")

    def _fmt_price(val: Optional[Any]) -> str:
        try:
            return f"{float(val):.4f}"
        except (TypeError, ValueError):
            return "-"

    def _fmt_pct(val: Optional[Any]) -> str:
        try:
            return f"{float(val) * 100.0:.2f}%"
        except (TypeError, ValueError):
            return "-"

    def _fmt_minutes(seconds: Optional[Any]) -> str:
        try:
            sec = float(seconds)
        except (TypeError, ValueError):
            return "-"
        return f"{sec / 60.0:.1f}m"

    def _snapshot_stale(snap: Dict[str, Any]) -> bool:
        raw_ts = snap.get("ts")
        if raw_ts is None:
            return False
        try:
            ts_val = float(raw_ts)
        except (TypeError, ValueError):
            return False
        if ts_val > 1e12:
            ts_val = ts_val / 1000.0
        return (time.time() - ts_val) > ORDERBOOK_STALE_AFTER_SEC

    def _latest_best_bid() -> Optional[float]:
        snap = latest.get(token_id) or {}
        # P0诊断：添加详细的调试信息
        if not hasattr(_latest_best_bid, "_diag_logged"):
            _latest_best_bid._diag_logged = True
            print(f"[DIAG][BID] latest字典中的token数据: {snap}")
            print(f"[DIAG][BID] ORDERBOOK_STALE_AFTER_SEC = {ORDERBOOK_STALE_AFTER_SEC}")

        # ✅ P0修复：如果快照过期，主动从缓存重新读取（买入/卖出流程阻塞时）
        if _snapshot_stale(snap):
            # P0诊断：显示为什么判定为过期
            ts = snap.get("ts")
            if ts:
                age = time.time() - float(ts)
                print(f"[DIAG][BID] 快照过期: 年龄={age:.1f}s > 阈值={ORDERBOOK_STALE_AFTER_SEC}s")
                print(f"[DIAG][BID] 尝试从缓存刷新数据...")

            # 从共享缓存重新读取最新数据
            if use_shared_ws:
                fresh_snap = _load_shared_ws_snapshot()
                if fresh_snap:
                    # ✅ P0修复：验证缓存数据本身是否新鲜
                    cache_updated_at = fresh_snap.get("updated_at")
                    if cache_updated_at:
                        cache_age = time.time() - float(cache_updated_at)
                        if cache_age > ORDERBOOK_STALE_AFTER_SEC:
                            print(f"[DIAG][BID] ✗ 缓存数据过期: 年龄={cache_age:.1f}s > 阈值={ORDERBOOK_STALE_AFTER_SEC}s")
                            print(f"[DIAG][BID] 缓存数据过期，返回None触发REST API fallback")
                            return None

                    latest[token_id] = {
                        "price": float(fresh_snap.get("price") or 0.0),
                        "best_bid": float(fresh_snap.get("best_bid") or 0.0),
                        "best_ask": float(fresh_snap.get("best_ask") or 0.0),
                        "ts": time.time()
                    }
                    snap = latest[token_id]
                    print(f"[DIAG][BID] ✓ 已刷新快照: bid={snap.get('best_bid')}, ask={snap.get('best_ask')} (缓存数据新鲜)")
                else:
                    print(f"[DIAG][BID] ✗ 无法从缓存刷新数据")
                    return None
            else:
                # 独立WS模式下，过期就返回None（依赖实时事件更新）
                return None
        try:
            value = snap.get("best_bid")
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def _latest_best_ask() -> Optional[float]:
        snap = latest.get(token_id) or {}

        # ✅ P0修复：如果快照过期，主动从缓存重新读取（买入/卖出流程阻塞时）
        if _snapshot_stale(snap):
            # 从共享缓存重新读取最新数据
            if use_shared_ws:
                fresh_snap = _load_shared_ws_snapshot()
                if fresh_snap:
                    # ✅ P0修复：验证缓存数据本身是否新鲜
                    cache_updated_at = fresh_snap.get("updated_at")
                    if cache_updated_at:
                        cache_age = time.time() - float(cache_updated_at)
                        if cache_age > ORDERBOOK_STALE_AFTER_SEC:
                            fallback_ask = fresh_snap.get("best_ask")
                            fallback_bid = fresh_snap.get("best_bid")
                            if fallback_ask or fallback_bid:
                                latest[token_id] = {
                                    "price": float(fresh_snap.get("price") or 0.0),
                                    "best_bid": float(fallback_bid or 0.0),
                                    "best_ask": float(fallback_ask or 0.0),
                                    "ts": time.time(),
                                }
                                snap = latest[token_id]
                                print(
                                    f"[DIAG][ASK] ⚠ 缓存数据过期: 年龄={cache_age:.1f}s > 阈值={ORDERBOOK_STALE_AFTER_SEC}s，"
                                    "使用过期缓存软降级"
                                )
                            else:
                                if not hasattr(_latest_best_ask, "_stale_logged"):
                                    _latest_best_ask._stale_logged = True
                                    print(f"[DIAG][ASK] ✗ 缓存数据过期: 年龄={cache_age:.1f}s > 阈值={ORDERBOOK_STALE_AFTER_SEC}s")
                                    print(f"[DIAG][ASK] 缓存数据过期，返回None触发REST API fallback")
                                return None

                    latest[token_id] = {
                        "price": float(fresh_snap.get("price") or 0.0),
                        "best_bid": float(fresh_snap.get("best_bid") or 0.0),
                        "best_ask": float(fresh_snap.get("best_ask") or 0.0),
                        "ts": time.time()
                    }
                    snap = latest[token_id]
                    if not hasattr(_latest_best_ask, "_refresh_logged"):
                        _latest_best_ask._refresh_logged = True
                        print(f"[DIAG][ASK] ✓ 已刷新快照: bid={snap.get('best_bid')}, ask={snap.get('best_ask')} (缓存数据新鲜)")
                else:
                    return None
            else:
                # 独立WS模式下，过期就返回None（依赖实时事件更新）
                return None

        try:
            value = snap.get("best_ask")
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def _latest_price() -> Optional[float]:
        snap = latest.get(token_id) or {}

        # ✅ P0修复：如果快照过期，主动从缓存重新读取（买入/卖出流程阻塞时）
        if _snapshot_stale(snap):
            # 从共享缓存重新读取最新数据
            if use_shared_ws:
                fresh_snap = _load_shared_ws_snapshot()
                if fresh_snap:
                    # ✅ P0修复：验证缓存数据本身是否新鲜
                    cache_updated_at = fresh_snap.get("updated_at")
                    if cache_updated_at:
                        cache_age = time.time() - float(cache_updated_at)
                        if cache_age > ORDERBOOK_STALE_AFTER_SEC:
                            # 缓存数据过期，返回None触发REST API fallback
                            return None

                    latest[token_id] = {
                        "price": float(fresh_snap.get("price") or 0.0),
                        "best_bid": float(fresh_snap.get("best_bid") or 0.0),
                        "best_ask": float(fresh_snap.get("best_ask") or 0.0),
                        "ts": time.time()
                    }
                    snap = latest[token_id]
                else:
                    return None
            else:
                # 独立WS模式下，过期就返回None（依赖实时事件更新）
                return None

        try:
            value = snap.get("price")
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def _exit_signal_active() -> bool:
        return bool(exit_signal_path and exit_signal_path.exists())

    def _force_exit(reason: str) -> None:
        """
        正在运行的进程收到 exit signal 时的清仓函数。
        使用 IOC 循环卖出，直到持仓全部清完才退出。
        """
        if stop_event.is_set():
            return
        print(f"[EXIT] 收到清仓信号: {reason}")

        # 1. 撤销所有挂单
        canceled = _cancel_open_orders_for_token(client, token_id)
        if canceled:
            print(f"[EXIT] 已尝试撤销挂单数量={canceled}")

        # 清仓参数
        DUST_THRESHOLD = 0.5  # 小于此数量视为清仓完成（尘埃量）
        MAX_ITERATIONS = 60   # 最大循环次数
        RETRY_INTERVAL_SEC = 5.0  # 每次循环间隔（秒）
        MIN_PRICE = 0.01      # 最低清仓价格

        iteration = 0
        while iteration < MAX_ITERATIONS:
            iteration += 1

            # 2. 查询当前持仓
            snapshot, _ = _fetch_position_snapshot_with_cache(
                client=client,
                token_id=token_id,
                cache=None,
                cache_ts=0.0,
                log_errors=True,
                force=True,
            )
            remaining_pos = snapshot[1] if snapshot else None

            # 检查是否已清仓完成
            if remaining_pos is None or remaining_pos <= DUST_THRESHOLD:
                print(f"[EXIT] 清仓完成，剩余持仓={remaining_pos or 0:.4f}（低于尘埃阈值 {DUST_THRESHOLD}）")
                break

            print(f"[EXIT] 第 {iteration}/{MAX_ITERATIONS} 次清仓尝试，剩余持仓={remaining_pos:.4f}")

            # 3. 获取最新价格（优先使用 WebSocket 缓存的价格）
            exit_price = _latest_best_bid()
            if exit_price is None:
                exit_price = _latest_best_ask()
            if exit_price is None:
                exit_price = _latest_price()
            if exit_price is None or exit_price <= 0:
                # 回退到 API 查询
                try:
                    best_bid = _fetch_best_price(client, str(token_id), "bid")
                    if best_bid is not None and best_bid.price and best_bid.price > 0:
                        exit_price = float(best_bid.price)
                    else:
                        exit_price = MIN_PRICE
                except Exception:
                    exit_price = MIN_PRICE

            # 4. 发出 IOC 卖单（允许部分成交）
            try:
                resp = _place_sell_ioc(
                    client,
                    token_id=token_id,
                    price=exit_price,
                    size=remaining_pos
                )
                print(
                    f"[EXIT] 已发出 IOC 清仓卖单 size={remaining_pos:.4f} price={exit_price:.4f} resp={resp}"
                )
            except Exception as exc:
                print(f"[EXIT] IOC 清仓卖单失败: {exc}")

            # 5. 等待一段时间后再次检查
            time.sleep(RETRY_INTERVAL_SEC)

        # 最终状态检查
        if iteration >= MAX_ITERATIONS:
            final_snapshot, _ = _fetch_position_snapshot_with_cache(
                client=client,
                token_id=token_id,
                cache=None,
                cache_ts=0.0,
                log_errors=True,
                force=True,
            )
            final_pos = final_snapshot[1] if final_snapshot else 0
            print(
                f"[EXIT][WARN] 达到最大清仓次数 {MAX_ITERATIONS}，"
                f"剩余持仓={final_pos:.4f}（可能需要人工处理）"
            )

        strategy.stop("sell signal")
        stop_event.set()

    def _awaiting_blocking(awaiting: Any) -> bool:
        if awaiting is None:
            return False
        if awaiting == ActionType.BUY and awaiting_buy_passthrough:
            return False
        return True

    def _has_actionable_position(status_snapshot: Optional[Dict[str, Any]] = None) -> bool:
        status_snapshot = status_snapshot or strategy.status()
        dust_floor = max(API_MIN_ORDER_SIZE or 0.0, 1e-4)
        for candidate in (position_size, _extract_position_size(status_snapshot)):
            try:
                if candidate is not None and float(candidate) > dust_floor:
                    return True
            except (TypeError, ValueError):
                continue
        return False

    print("[INIT][TRACE] 3. 准备初始化变量和策略状态...")

    position_size: Optional[float] = None
    last_order_size: Optional[float] = None
    status_snapshot = strategy.status()
    print("[INIT][TRACE] 4. 策略状态已获取")
    initial_pos = _extract_position_size(status_snapshot)
    if initial_pos > 0:
        position_size = initial_pos
        last_order_size = initial_pos
    max_position_cap: Optional[float] = None
    if manual_order_size is not None and manual_size_is_target:
        try:
            max_position_cap = max(float(manual_order_size), 0.0)
        except (TypeError, ValueError):
            max_position_cap = None
    last_log: Optional[float] = None
    buy_cooldown_until: float = 0.0
    pending_buy: Optional[Action] = None
    short_buy_cooldown = 1.0
    next_position_sync: float = 0.0
    position_sync_block_until: float = 0.0
    awaiting_buy_passthrough: bool = True
    exit_after_sell_only_clear: bool = False
    min_loop_interval = 0.1  # 从1.0秒改为0.1秒，提高主循环频率以匹配聚合器缓存更新频率
    next_loop_after = 0.0
    stagnation_window_seconds = max(float(stagnation_window_minutes), 0.0) * 60.0
    no_event_exit_seconds = max(float(no_event_exit_minutes), 0.0) * 60.0
    signal_timeout_seconds = max(float(signal_timeout_minutes), 0.0) * 60.0
    stagnation_history: Deque[Tuple[float, float]] = deque()
    stagnation_triggered: bool = False
    run_started_at = time.time()
    last_signal_ts = run_started_at  # 跟踪最后一次交易信号的时间

    def _reconcile_empty_long_state(reason: str) -> None:
        nonlocal position_size

        status_snapshot = strategy.status()
        state = status_snapshot.get("state")
        awaiting = status_snapshot.get("awaiting")
        if state != "LONG" or awaiting is not None:
            return

        if _has_actionable_position(status_snapshot):
            return

        latest_bid = _latest_best_bid()
        fallback_px = latest_bid if latest_bid is not None else 0.0
        position_size = None
        print(
            f"[STATE][RECONCILE] {reason} 检测到 LONG 无持仓，强制归零 (fallback_px={fallback_px:.4f})"
        )
        strategy.on_sell_filled(avg_price=fallback_px, remaining=0.0)

    def _maybe_refresh_position_size(reason: str, *, force: bool = False) -> None:
        nonlocal position_size, next_position_sync
        now = time.time()
        if not force and now < next_position_sync:
            return
        if now < position_sync_block_until:
            next_position_sync = max(next_position_sync, position_sync_block_until)
            return
        next_position_sync = now + maker_position_sync_interval
        try:
            avg_px, total_pos, origin_note = _lookup_position_avg_price(client, token_id)
        except Exception as probe_exc:
            print(f"[WATCHDOG][POSITION] {reason} 持仓查询异常：{probe_exc}")
            return

        status_snapshot = strategy.status()
        current_state = status_snapshot.get("state")
        awaiting = status_snapshot.get("awaiting")
        awaiting_is_sell = False
        if awaiting is not None:
            awaiting_val = getattr(awaiting, "value", awaiting)
            awaiting_is_sell = awaiting_val == ActionType.SELL
        has_local_position = _extract_position_size(status_snapshot) > 0
        eps = 1e-6
        dust_floor = max(API_MIN_ORDER_SIZE or 0.0, 1e-4)
        new_size, changed = _merge_remote_position_size(
            position_size, total_pos, dust_floor=dust_floor
        )
        position_size = new_size
        should_sync_state = changed

        if new_size is not None and avg_px is None:
            origin_display = origin_note or "positions"
            print(
                f"[FATAL][POSITION] {reason} -> origin={origin_display} 无法获取持仓均价，停止脚本以避免错误卖出。"
            )
            strategy.stop("missing avg price during position sync")
            stop_event.set()
            return

        if not should_sync_state:
            # 即便仓位未变动，也要校正策略状态：
            #  - 远端有仓位但策略仍为 FLAT；
            #  - 远端无仓位但策略仍为 LONG。
            should_sync_state = (
                (new_size is not None and current_state != "LONG")
                or (new_size is None and (current_state != "FLAT" or has_local_position))
            )

        # 若仍处于卖出待确认状态但已无有效仓位，也需强制刷新为空仓，避免卡在 BUY 前置检查。
        if not should_sync_state and awaiting_is_sell:
            remote_is_dust = total_pos is not None and total_pos < dust_floor - eps
            should_sync_state = new_size is None or remote_is_dust

        if not should_sync_state:
            return

        origin_display = origin_note or "positions"
        avg_display = f"{avg_px:.4f}" if avg_px is not None else "-"
        if new_size is None:
            dust_note = ""
            if total_pos is not None and total_pos < dust_floor - eps:
                dust_note = f" (size={float(total_pos):.4f} < 最小挂单量 {dust_floor:.2f}，视为无持仓)"
            print(
                f"[WATCHDOG][POSITION] {reason} -> origin={origin_display} avg={avg_display} 当前无持仓{dust_note}"
            )
            latest_bid = _latest_best_bid()
            fallback_px = latest_bid if latest_bid is not None else 0.0
            strategy.on_sell_filled(avg_price=fallback_px, remaining=0.0)
            print(
                f"[STATE] 同步策略为空仓 (fallback_px={fallback_px:.4f})"
            )
        else:
            print(
                f"[WATCHDOG][POSITION] {reason} -> origin={origin_display} avg={avg_display} size={new_size:.4f}"
            )
            latest_bid = _latest_best_bid()
            latest_ask = _latest_best_ask()
            fallback_px = avg_px
            if fallback_px is None:
                fallback_px = latest_ask if latest_ask is not None else latest_bid
            if fallback_px is None:
                fallback_px = 0.0
            strategy.on_buy_filled(fallback_px, total_position=new_size, size=0.0)
            print(
                f"[STATE] 同步策略持仓 -> price={fallback_px:.4f} size={new_size:.4f}"
            )
            try:
                if position_size is not None:
                    # 先撤销该 token 的所有已有挂单，释放被锁定的仓位
                    canceled = _cancel_open_orders_for_token(client, token_id)
                    if canceled:
                        print(f"[POSITION][SYNC] 已撤销 {canceled} 个现有挂单以释放锁定仓位")
                    strategy.mark_awaiting(ActionType.SELL)
                    print("[STATE] 同步远端持仓后，标记等待卖出以避免误入买入分支。")
                    _execute_sell(position_size, floor_hint=fallback_px, source="[POSITION][SYNC]")
            except Exception as exc:
                print(f"[WATCHDOG][POSITION] 自动卖出旧仓位失败：{exc}")

    def _activate_sell_only(reason: str) -> None:
        nonlocal exit_after_sell_only_clear
        if sell_only_event.is_set():
            return

        sell_only_event.set()
        strategy.enable_sell_only(reason)
        _maybe_refresh_position_size("[COUNTDOWN] 进入仅卖出模式前同步持仓", force=True)
        has_position = _has_actionable_position()

        print("[COUNTDOWN] 已进入仅卖出模式：倒计时窗口内不再买入。")
        if has_position:
            exit_after_sell_only_clear = True
            print("[COUNTDOWN] 仍有持仓，将继续等待卖出，清仓后停止脚本。")
        else:
            print("[COUNTDOWN] 当前无持仓，倒计时仅卖出模式下直接停止脚本。")
            strategy.stop("countdown sell-only window (flat)")
            stop_event.set()

    def _countdown_monitor():
        if not market_deadline_ts:
            return
        last_display: Optional[int] = None
        sell_only_warn_logged = False
        while not stop_event.is_set():
            now = time.time()
            if sell_only_start_ts and not sell_only_event.is_set():
                until_sell_only = sell_only_start_ts - now
                if until_sell_only <= 0:
                    _activate_sell_only("countdown window")
                elif until_sell_only <= 300 and not sell_only_warn_logged:
                    mins = int(max(until_sell_only, 0) // 60)
                    secs = int(max(until_sell_only, 0) % 60)
                    print(
                        "[COUNTDOWN] 距离仅卖出模式开启还剩 "
                        f"{mins:02d}:{secs:02d}。"
                    )
                    sell_only_warn_logged = True
            remaining = market_deadline_ts - now
            if remaining <= 0:
                if last_display != 0:
                    print("[COUNTDOWN] 距离市场结束还剩 00:00")
                print("[COUNTDOWN] 倒计时结束，开始确认市场状态…")
                _confirm_market_closed()
                return
            if remaining <= 300:
                secs_left = int(remaining)
                if secs_left != last_display:
                    mm = secs_left // 60
                    ss = secs_left % 60
                    print(
                        f"[COUNTDOWN] 距离市场结束还剩 {mm:02d}:{ss:02d}"
                    )
                    last_display = secs_left
                for _ in range(5):
                    if stop_event.is_set():
                        return
                    time.sleep(0.2)
            else:
                wait = min(remaining - 300, 60)
                if wait <= 0:
                    wait = 1
                deadline = time.time() + wait
                while time.time() < deadline:
                    if stop_event.is_set():
                        return
                    time.sleep(0.2)

    # NOTE: sell_only / countdown 初始化已移到 _execute_sell 定义之后，
    # 避免 _activate_sell_only → _maybe_refresh_position_size → _execute_sell
    # 在 _execute_sell 尚未定义时被调用导致 NameError。

    def _execute_sell(
        order_qty: Optional[float],
        *,
        floor_hint: Optional[float],
        source: str,
    ) -> None:
        nonlocal position_size, last_order_size, position_sync_block_until, next_position_sync, next_loop_after

        position_snapshot_cache: Optional[
            Tuple[Optional[float], Optional[float], Optional[str]]
        ] = None
        position_snapshot_ts: float = 0.0

        def _fetch_position_snapshot(*, log_errors: bool, force: bool = False):
            nonlocal position_snapshot_cache, position_snapshot_ts
            snapshot, snapshot_ts = _fetch_position_snapshot_with_cache(
                client=client,
                token_id=token_id,
                cache=position_snapshot_cache,
                cache_ts=position_snapshot_ts,
                log_errors=log_errors,
                force=force,
            )
            position_snapshot_ts = snapshot_ts
            if snapshot is not None:
                position_snapshot_cache = snapshot
            return snapshot

        def _resolve_order_qty() -> Optional[float]:
            candidates = [order_qty, position_size, last_order_size]
            for candidate in candidates:
                if candidate is None:
                    continue
                try:
                    qty = float(candidate)
                except (TypeError, ValueError):
                    continue
                if qty > 0:
                    return qty
            return None

        eff_qty = _resolve_order_qty()
        if eff_qty is None:
            print(f"[WARN] {source} 未能确定有效的卖出数量，跳过此次卖出。")
            strategy.on_reject("invalid sell size")
            return

        floor_price = strategy.sell_trigger_price()
        if floor_price is None:
            floor_price = floor_hint
        elif floor_hint is not None:
            # 地板价不得低于“买入均价 + 盈利阈值”，若上游给出更高提示价则取较高值。
            floor_price = max(floor_price, floor_hint)

        if floor_price is None:
            print(f"[WARN] {source} 无法计算卖出地板价，跳过卖出流程。")
            strategy.on_reject("missing sell trigger")
            return

        def _sell_progress_probe() -> None:
            snapshot = _fetch_position_snapshot(log_errors=True, force=True)
            if snapshot is None:
                return
            avg_px, total_pos, origin_note = snapshot
            origin_display = origin_note or "positions"
            if total_pos is None or total_pos <= 0:
                print(f"[WATCHDOG][SELL] 持仓检查 -> origin={origin_display} 当前无持仓")
                return
            avg_display = f"{avg_px:.4f}" if avg_px is not None else "-"
            print(
                f"[WATCHDOG][SELL] 持仓检查 -> origin={origin_display} avg={avg_display} size={total_pos:.4f}"
            )

        def _position_size_fetcher() -> Optional[float]:
            snapshot = _fetch_position_snapshot(log_errors=False, force=False)
            if snapshot is None:
                return None
            _avg_px, total_pos, _origin = snapshot
            return total_pos

        # 动态SELL超时：根据entry_price计算超时时间
        # entry_price >= 0.95: 1小时, >= 0.90: 2小时, else: 4小时
        dynamic_sell_timeout_sec = sell_inactive_timeout_sec  # 默认使用配置值
        current_status = strategy.status()
        current_entry_price = current_status.get("entry_price")
        if current_entry_price is not None and sell_inactive_timeout_sec > 0:
            if current_entry_price >= 0.95:
                dynamic_sell_timeout_sec = min(sell_inactive_timeout_sec, 1.0 * 3600.0)  # 1小时
            elif current_entry_price >= 0.90:
                dynamic_sell_timeout_sec = min(sell_inactive_timeout_sec, 2.0 * 3600.0)  # 2小时
            else:
                dynamic_sell_timeout_sec = min(sell_inactive_timeout_sec, 4.0 * 3600.0)  # 4小时
            if dynamic_sell_timeout_sec != sell_inactive_timeout_sec:
                print(
                    f"[SELL] 动态超时调整: entry_price={current_entry_price:.4f} -> "
                    f"timeout={dynamic_sell_timeout_sec/3600.0:.1f}h (原配置: {sell_inactive_timeout_sec/3600.0:.1f}h)"
                )

        try:
            sell_resp = maker_sell_follow_ask_with_floor_wait(
                client=client,
                token_id=token_id,
                position_size=eff_qty,
                floor_X=float(floor_price),
                poll_sec=maker_poll_sec,
                min_order_size=API_MIN_ORDER_SIZE,
                best_ask_fn=_latest_best_ask,
                stop_check=stop_event.is_set,
                sell_mode=sell_mode,
                inactive_timeout_sec=dynamic_sell_timeout_sec,
                progress_probe=_sell_progress_probe,
                progress_probe_interval=60.0,
                position_fetcher=_position_size_fetcher,
                position_refresh_interval=30.0,
                price_decimals=market_price_precision,
            )
        except Exception as exc:
            print(f"[ERR] {source} 卖出挂单异常：{exc}")
            strategy.on_reject(str(exc))
            return

        print(f"[TRADE][SELL][MAKER] resp={sell_resp}")
        sell_status = str(sell_resp.get("status") or "").upper()
        if sell_status == "ABANDONED":
            print(
                "[RELEASE] 卖出挂单长期无动作，准备退出。"
            )
            # 防御性撤单：确保不残留卖单锁定仓位（maker_execution 层已尝试撤单，
            # 此处作为第二道防线以防上层撤单失败或遗漏）
            try:
                canceled = _cancel_open_orders_for_token(client, token_id)
                if canceled:
                    print(f"[RELEASE] ABANDONED 退出前撤销 {canceled} 个残留挂单")
            except Exception:
                pass
            # 获取当前持仓和价格信息用于回填
            snap = latest.get(token_id) or {}
            status = strategy.status()
            _cancel_open_buy_orders_before_exit("SELL_ABANDONED")
            _record_exit_token(token_id, "SELL_ABANDONED", {
                "has_position": True,  # 仍有持仓
                "position_size": float(sell_resp.get("remaining") or eff_qty),
                "entry_price": status.get("entry_price"),
                "last_bid": float(snap.get("best_bid") or 0.0),
                "last_ask": float(snap.get("best_ask") or 0.0),
                "sell_floor_price": floor_price,
                "inactive_timeout_hours": dynamic_sell_timeout_sec / 3600.0,
            })
            strategy.stop("sell inactive release")
            stop_event.set()
            return
        sell_filled = float(sell_resp.get("filled") or 0.0)
        sell_avg = sell_resp.get("avg_price")
        eps = 1e-4
        sell_remaining = float(sell_resp.get("remaining") or 0.0)
        dust_threshold = (
            API_MIN_ORDER_SIZE if API_MIN_ORDER_SIZE and API_MIN_ORDER_SIZE > 0 else None
        )
        treat_as_dust = False
        if dust_threshold is not None and sell_remaining > eps:
            if sell_remaining < dust_threshold - 1e-9:
                treat_as_dust = True
        remaining_for_strategy = None if treat_as_dust else sell_remaining
        strategy.on_sell_filled(
            avg_price=sell_avg if sell_filled > 0 else None,
            size=sell_filled if sell_filled > 0 else None,
            remaining=remaining_for_strategy,
        )
        sold_out = remaining_for_strategy is None or sell_remaining <= eps
        if sold_out:
            block_until = time.time() + 180.0
            position_sync_block_until = max(position_sync_block_until, block_until)
            next_position_sync_ts = max(next_position_sync, position_sync_block_until)
            next_position_sync = next_position_sync_ts
        if remaining_for_strategy is None:
            strategy.mark_awaiting(None)
            print(
                "[STATE] 卖出流程已完成或剩余低于最小下单量，切换为等待买入/空闲状态。"
            )
            wait_after_sell_sec = 300.0
            heartbeat_interval = 60.0
            pause_deadline = time.time() + wait_after_sell_sec
            heartbeat_tick = 1
            heartbeat_total = int(wait_after_sell_sec // heartbeat_interval)
            remote_sync_errors = 0
            while True:
                remaining_wait = pause_deadline - time.time()
                if remaining_wait <= 0:
                    break
                sleep_window = min(heartbeat_interval, remaining_wait)
                if stop_event.wait(sleep_window):
                    break
                try:
                    snapshot = _fetch_position_snapshot(log_errors=True, force=True)
                    if snapshot is None:
                        print("[STATE][SYNC] 远端仓位查询结果为空或未返回。")
                    else:
                        avg_px, total_pos, origin = snapshot
                        origin_display = origin or "positions"
                        avg_display = f"{avg_px:.4f}" if avg_px is not None else "-"
                        if total_pos is None:
                            size_note = "None"
                            size_detail = "未知"
                        else:
                            size_note = f"{total_pos:.4f}"
                            size_detail = "空仓" if total_pos <= 0 else "持仓"
                        print(
                            "[STATE][SYNC] 远端仓位 -> "
                            f"origin={origin_display} avg={avg_display} size={size_note} ({size_detail})"
                        )
                except Exception as sync_exc:
                    remote_sync_errors += 1
                    print(f"[STATE][SYNC] 获取远端仓位异常：{sync_exc}")
                print(
                    "[STATE] 卖出完成，等待远端仓位同步中… "
                    f"心跳 {heartbeat_tick}/{heartbeat_total}"
                )
                heartbeat_tick += 1
            if remote_sync_errors >= heartbeat_total:
                print(
                    "[STATE][SYNC] 连续多次获取远端仓位均异常，终止脚本以避免风险。"
                )
                stop_event.set()
                next_loop_after = max(next_loop_after, pause_deadline)
                return
            next_loop_after = max(next_loop_after, pause_deadline)
        if sell_remaining > eps and not treat_as_dust:
            position_size = sell_remaining
            last_order_size = sell_remaining
            display_price = sell_avg if sell_avg is not None else floor_price
            print(
                "[STATE] 卖出部分成交 -> "
                f"price={display_price:.4f} sold={sell_filled:.4f} remaining={sell_remaining:.4f} status={sell_status}"
            )
        else:
            position_size = None
            last_order_size = None
            sold_display = sell_filled if sell_filled > 0 else (eff_qty or 0.0)
            display_price = sell_avg if sell_avg is not None else floor_price
            dust_note = ""
            if treat_as_dust and sell_remaining > eps and dust_threshold is not None:
                dust_note = (
                    f" (剩余 {sell_remaining:.4f} < 最小挂单量 {dust_threshold:.2f}，视为完成)"
                )
            print(
                "[STATE] 卖出成交 -> "
                f"price={display_price:.4f} size={sold_display:.4f} status={sell_status}{dust_note}"
            )

    def _handle_stagnation_exit(change_ratio: float, window_span: float) -> None:
        nonlocal exit_after_sell_only_clear, stagnation_triggered
        if stagnation_triggered:
            return
        stagnation_triggered = True
        print(
            "[STAGNANT] 价格在 "
            f"{_fmt_minutes(window_span)} 内波动 {_fmt_pct(change_ratio)} "
            f"≤ 阈值 {_fmt_pct(stagnation_pct)}，触发退出。"
        )
        _maybe_refresh_position_size("[STAGNANT][SYNC]", force=True)
        if _has_actionable_position():
            sell_only_event.set()
            strategy.enable_sell_only("price stagnation")
            exit_after_sell_only_clear = True
            floor_hint = _latest_best_bid()
            if floor_hint is None:
                floor_hint = _latest_best_ask()
            if floor_hint is None:
                floor_hint = _latest_price()
            _execute_sell(position_size, floor_hint=floor_hint, source="[STAGNANT]")
        else:
            strategy.stop("price stagnation")
            print("[QUEUE] 释放队列：价格停滞且无持仓，已退出。")
            snap = latest.get(token_id) or {}
            _cancel_open_buy_orders_before_exit("STAGNATION_NO_POSITION")
            _record_exit_token(token_id, "STAGNATION_NO_POSITION", {
                "has_position": False,
                "change_ratio": change_ratio,
                "window_span_minutes": window_span / 60.0,
                "stagnation_threshold": stagnation_pct,
                "last_bid": float(snap.get("best_bid") or 0.0),
                "last_ask": float(snap.get("best_ask") or 0.0),
            })
            stop_event.set()

    def _handle_no_feed_exit(idle_seconds: float) -> None:
        nonlocal exit_after_sell_only_clear, stagnation_triggered
        if stagnation_triggered:
            return
        stagnation_triggered = True
        print(
            "[STAGNANT] "
            f"{_fmt_minutes(idle_seconds)} 未收到行情更新，触发退出。"
        )
        _maybe_refresh_position_size("[STAGNANT][NO-FEED][SYNC]", force=True)
        if _has_actionable_position():
            sell_only_event.set()
            strategy.enable_sell_only("price stagnation (no feed)")
            exit_after_sell_only_clear = True
            floor_hint = _latest_best_bid()
            if floor_hint is None:
                floor_hint = _latest_best_ask()
            if floor_hint is None:
                floor_hint = _latest_price()
            _execute_sell(position_size, floor_hint=floor_hint, source="[STAGNANT][NO-FEED]")
        else:
            strategy.stop("price stagnation (no feed)")
            print("[QUEUE] 释放队列：长时间无行情且无持仓，已退出。")
            snap = latest.get(token_id) or {}
            _cancel_open_buy_orders_before_exit("NO_FEED_NO_POSITION")
            _record_exit_token(token_id, "NO_FEED_NO_POSITION", {
                "has_position": False,
                "idle_minutes": idle_seconds / 60.0,
                "last_bid": float(snap.get("best_bid") or 0.0),
                "last_ask": float(snap.get("best_ask") or 0.0),
            })
            stop_event.set()

    # --- 所有内部函数已定义，安全执行 sell_only / countdown 初始化 ---
    print(f"[INIT][TRACE] 5. 检查sell_only和countdown (sell_only_start_ts={sell_only_start_ts}, market_deadline_ts={market_deadline_ts})")

    if sell_only_start_ts and time.time() >= sell_only_start_ts:
        print("[INIT][TRACE] 6. 调用_activate_sell_only()...")
        _activate_sell_only("countdown window")

    if market_deadline_ts:
        print("[INIT][TRACE] 7. 启动countdown_monitor线程...")
        threading.Thread(target=_countdown_monitor, daemon=True).start()

    print("[INIT][TRACE] 8. 所有初始化完成，准备进入主循环...")

    # 主循环诊断变量（用于追踪循环是否正常执行）
    loop_iteration_count = 0
    last_loop_diagnostic_log = time.time()

    print(f"[MAIN_LOOP] 🚀 进入主循环 (use_shared_ws={use_shared_ws})")
    sys.stdout.flush()  # 立即刷新确保日志输出

    try:
        while not stop_event.is_set():
            now = time.time()
            if now < next_loop_after:
                wait = next_loop_after - now
                if wait > 0 and stop_event.wait(wait):
                    break
            now = time.time()
            loop_started = now
            loop_iteration_count += 1

            # 每60秒打印一次主循环运行状态
            if now - last_loop_diagnostic_log >= 60:
                print(f"[MAIN_LOOP] ✓ 主循环运行中 (iterations={loop_iteration_count}, use_shared_ws={use_shared_ws})")
                sys.stdout.flush()
                last_loop_diagnostic_log = now

            try:
                if use_shared_ws:
                    _apply_shared_ws_snapshot()
                if _exit_signal_active():
                    _force_exit("sell signal file detected")
                    break
                if pending_buy is not None and now >= buy_cooldown_until:
                    if sell_only_event.is_set():
                        print("[COUNTDOWN] 仍在仅卖出模式内，丢弃待执行的买入信号。")
                        strategy.on_reject("sell-only window active")
                        pending_buy = None
                    else:
                        status = strategy.status()
                        state = status.get("state")
                        awaiting = status.get("awaiting")
                        # 使用本地与策略两侧的持仓快照，避免残留仓位时误买
                        dust_floor = max(API_MIN_ORDER_SIZE or 0.0, 1e-4)
                        strat_pos = status.get("position_size")
                        has_position = False
                        for pos in (position_size, strat_pos):
                            if pos is not None and pos > dust_floor:
                                has_position = True
                                break

                        awaiting_blocking = _awaiting_blocking(awaiting)
                        if state != "FLAT" or awaiting_blocking or has_position:
                            reason = (
                                "cooldown ended but still in position"
                                if has_position
                                else "cooldown ended but state not FLAT"
                            )
                            print(
                                "[COOLDOWN] 冷却结束但仍有持仓/非空等待状态，丢弃待执行的买入信号。"
                            )
                            strategy.on_reject(reason)
                            pending_buy = None
                        else:
                            print("[COOLDOWN] 冷却结束，重新尝试买入…")
                            action_queue.put(pending_buy)
                            pending_buy = None

                if now >= next_position_sync:
                    _maybe_refresh_position_size("[LOOP]")

                _reconcile_empty_long_state("[LOOP]")

                if no_event_exit_seconds > 0 and not stagnation_triggered:
                    with ws_state_lock:
                        last_event_ts = float(ws_state.get("last_event_ts") or 0.0)
                    if last_event_ts <= 0:
                        idle_seconds = now - run_started_at
                        if idle_seconds >= no_event_exit_seconds:
                            _handle_no_feed_exit(idle_seconds)
                            if stop_event.is_set():
                                break
                            continue

                if stagnation_window_seconds > 0 and not stagnation_triggered:
                    with ws_state_lock:
                        last_event_ts = float(ws_state.get("last_event_ts") or 0.0)
                    if last_event_ts > 0:
                        idle_seconds = now - last_event_ts
                        if idle_seconds >= stagnation_window_seconds:
                            _handle_no_feed_exit(idle_seconds)
                            if stop_event.is_set():
                                break
                            continue
                    snap = latest.get(token_id) or {}
                    last_px = float(snap.get("price") or 0.0)
                    if last_px > 0:
                        snap_ts = _snapshot_ts(snap)
                        stagnation_history.append((snap_ts, last_px))
                        while (
                            stagnation_history
                            and snap_ts - stagnation_history[0][0]
                            > stagnation_window_seconds
                        ):
                            stagnation_history.popleft()
                        if len(stagnation_history) >= 2:
                            window_span = snap_ts - stagnation_history[0][0]
                            if window_span >= stagnation_window_seconds:
                                prices = [p for _, p in stagnation_history]
                                high = max(prices)
                                low = min(prices)
                                if high > 0:
                                    change_ratio = (high - low) / high
                                    if change_ratio <= stagnation_pct:
                                        _handle_stagnation_exit(change_ratio, window_span)
                                        if stop_event.is_set():
                                            break
                                        continue

                # 交易信号超时检查：长时间无买入/卖出信号则退出
                if signal_timeout_seconds > 0 and not stagnation_triggered:
                    signal_idle_seconds = now - last_signal_ts
                    if signal_idle_seconds >= signal_timeout_seconds:
                        print(
                            f"[SIGNAL_TIMEOUT] {signal_idle_seconds / 60.0:.1f} 分钟内无交易信号，释放队列退出"
                        )
                        print(f"[SIGNAL_TIMEOUT] 最后信号时间: {time.strftime('%H:%M:%S', time.localtime(last_signal_ts))}")
                        status = strategy.status()
                        has_position = _has_actionable_position(status)
                        position_size_for_exit = None
                        if has_position:
                            position_size_for_exit = (
                                status.get("position_size")
                                or status.get("size")
                                or position_size
                            )
                        strategy.stop("signal timeout (force exit)")
                        print("[QUEUE] 释放队列：长时间无交易信号，已退出。")
                        # 获取最后的价格数据用于回填判断
                        snap = latest.get(token_id) or {}
                        _cancel_open_buy_orders_before_exit("SIGNAL_TIMEOUT")
                        _record_exit_token(token_id, "SIGNAL_TIMEOUT", {
                            "signal_idle_minutes": signal_idle_seconds / 60.0,
                            "timeout_threshold_minutes": signal_timeout_seconds / 60.0,
                            "last_signal_time": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_signal_ts)),
                            "has_position": has_position,
                            "position_size": position_size_for_exit,
                            "last_bid": float(snap.get("best_bid") or 0.0),
                            "last_ask": float(snap.get("best_ask") or 0.0),
                        })
                        stop_event.set()
                        if stop_event.is_set():
                            break
                        continue

                if sell_only_event.is_set() and exit_after_sell_only_clear:
                    status = strategy.status()
                    awaiting = status.get("awaiting")
                    awaiting_val = getattr(awaiting, "value", awaiting)
                    awaiting_is_sell = awaiting_val == ActionType.SELL
                    if not _has_actionable_position(status) and not awaiting_is_sell:
                        print("[COUNTDOWN] 倒计时仅卖出模式下已清仓，脚本将退出。")
                        if stagnation_triggered:
                            print("[QUEUE] 释放队列：停滞清仓完成，已退出。")
                        strategy.stop("countdown sell-only cleared position")
                        stop_event.set()
                        break

                # ✅ 增加：主循环心跳检测，避免卡死时无输出
                if not hasattr(loop_started, '__name__'):  # 确保loop_started是时间戳而不是函数
                    loop_heartbeat_msg = f"[HEARTBEAT] 主循环运行中... (use_shared_ws={use_shared_ws})"
                    if last_log is None:
                        print(loop_heartbeat_msg + " (首次心跳)")
                        sys.stdout.flush()  # 立即刷新首次心跳日志

                if last_log is None or now - last_log >= 30.0:  # 从60秒改为30秒，更频繁的心跳日志
                    snap = latest.get(token_id) or {}
                    bid = float(snap.get("best_bid") or 0.0)
                    ask = float(snap.get("best_ask") or 0.0)
                    last_px = float(snap.get("price") or 0.0)
                    mid_px = (bid + ask) / 2 if (bid > 0 and ask > 0) else 0.0  # 策略使用的中间价
                    st = strategy.status()
                    awaiting = st.get("awaiting")
                    awaiting_s = awaiting.value if hasattr(awaiting, "value") else awaiting
                    entry_price = st.get("entry_price")

                    # ✅ P0修复：明确显示无数据状态，避免用户困惑
                    if not snap:
                        print(f"[WARN] ⚠⚠⚠ latest中无token {token_id} 数据，策略未接收价格更新！")
                        print(f"[WARN] 原因：共享WebSocket缓存中没有此token数据")
                        print(f"[WARN] 结果：不会生成交易信号，不会下单")
                        sys.stdout.flush()

                    # 如果bid/ask/price都是0，说明没有数据
                    no_data_marker = " [⚠无数据⚠]" if (bid == 0.0 and ask == 0.0 and last_px == 0.0) else ""
                    print(
                        f"[PX] bid={bid:.4f} ask={ask:.4f} mid={mid_px:.4f} last={last_px:.4f}{no_data_marker} | "
                        f"state={st.get('state')} awaiting={awaiting_s} entry={entry_price}"
                    )
                    sys.stdout.flush()  # 立即刷新心跳日志

                    extra_lines: List[str] = []

                    drop_stats = st.get("drop_stats") or {}
                    config_snapshot = st.get("config") or {}
                    history_len = st.get("price_history_len")
                    history_display = history_len if history_len is not None else "-"
                    window_seconds = drop_stats.get("window_seconds")
                    extra_lines.append(
                        "    时间窗口: "
                        f"{_fmt_minutes(window_seconds)} | 采样点数: {history_display}"
                    )

                    drop_line = (
                        "    窗口跌幅: 当前 "
                        f"{_fmt_pct(drop_stats.get('current_drop_ratio'))} / 最大 "
                        f"{_fmt_pct(drop_stats.get('max_drop_ratio'))} / 阈值 "
                        f"{_fmt_pct(config_snapshot.get('drop_pct'))}"
                    )
                    extra_lines.append(drop_line)

                    price_line = (
                        "    窗口价格(mid): 高 "
                        f"{_fmt_price(drop_stats.get('window_high'))} / 低 "
                        f"{_fmt_price(drop_stats.get('window_low'))}"
                    )
                    extra_lines.append(price_line)

                    # 显示最后信号时间（如果启用了信号超时）
                    if signal_timeout_seconds > 0:
                        signal_idle_seconds = now - last_signal_ts
                        signal_idle_minutes = signal_idle_seconds / 60.0
                        timeout_minutes = signal_timeout_seconds / 60.0
                        extra_lines.append(
                            f"    信号超时: {signal_idle_minutes:.1f}分钟无信号 / 阈值 {timeout_minutes:.0f}分钟"
                        )

                    if st.get("sell_only"):
                        extra_lines.append("    状态：倒计时仅卖出模式（禁止买入）")
                    for line in extra_lines:
                        print(line)
                    last_log = now

                # ✅ P0修复：action处理必须在每次循环中执行，不能在心跳日志条件块内
                # 原BUG：action_queue.get()在条件块内导致只有每30秒才处理一次交易信号
                # 修复：将action处理移到条件块外，确保每次循环（~0.5秒）都检查队列
                try:
                    action = action_queue.get(timeout=0.5)
                except Empty:
                    action = None

                if stop_event.is_set():
                    break

                if action is None:
                    continue

                # 诊断日志：记录收到的action
                print(f"[ACTION] 🔔 收到交易信号: type={action.action}, target_price={getattr(action, 'target_price', 'N/A')}")

                snap = latest.get(token_id) or {}
                bid = float(snap.get("best_bid") or 0.0)
                ask = float(snap.get("best_ask") or 0.0)
    
                if (
                    not manual_deadline_disabled
                    and not market_closed_detected
                    and market_meta
                    and _market_has_ended(market_meta, now)
                ):
                    print("[MARKET] 达到市场截止时间，准备退出…")
                    market_closed_detected = True
                    strategy.stop("market ended")
                    stop_event.set()
                    continue
    
                if action.action == ActionType.SELL:
                    floor_override = action.target_price
                    _execute_sell(position_size, floor_hint=floor_override, source="[SIGNAL]")
                    continue
    
                if action.action != ActionType.BUY:
                    print(f"[WARN] 收到未预期的动作 {action.action}，已忽略。")
                    continue
    
                if sell_only_event.is_set():
                    print("[COUNTDOWN] 当前处于倒计时仅卖出模式，忽略买入信号。")
                    strategy.on_reject("sell-only window active")
                    continue
    
                status = strategy.status()
                dust_floor = max(API_MIN_ORDER_SIZE or 0.0, 1e-4)
                current_state = status.get("state")
                awaiting = status.get("awaiting")
                strat_pos = status.get("position_size")
                raw_position: Optional[float] = None
                actionable_position: Optional[float] = None
                treat_as_dust: bool = False
                for pos in (position_size, strat_pos):
                    try:
                        val = float(pos)
                    except (TypeError, ValueError):
                        continue
                    if val <= 0:
                        continue
                    raw_position = max(raw_position or 0.0, val)
                if raw_position is not None:
                    if raw_position > dust_floor:
                        actionable_position = raw_position
                    else:
                        treat_as_dust = True
    
                if actionable_position is not None:
                    print(
                        f"[BUY][BLOCK] 检测到可卖出仓位 {actionable_position:.4f}，先清仓后再尝试买入。"
                    )
                    pending_buy = action
                    buy_cooldown_until = time.time() + short_buy_cooldown
                    # 计算清仓地板价：优先使用策略的入场价格，否则查询持仓均价
                    block_floor_hint: Optional[float] = None
                    if strategy.sell_trigger_price() is None:
                        # 策略没有入场价格，需要从 data-api 查询持仓均价
                        try:
                            block_avg_px, _, block_origin = _lookup_position_avg_price(client, token_id)
                            if block_avg_px is not None and block_avg_px > 0:
                                # 使用均价计算地板价：均价 * (1 + profit_pct)
                                block_floor_hint = block_avg_px * (1.0 + profit_pct)
                                print(
                                    f"[BUY][BLOCK] 从 {block_origin} 获取均价 {block_avg_px:.4f}，"
                                    f"计算地板价 {block_floor_hint:.4f}"
                                )
                                # 同步入场价格到策略，避免后续重复查询
                                strategy.sync_long_state(ref_price=block_avg_px)
                            else:
                                # 查不到均价，使用当前 bid 作为保本地板价
                                if bid is not None and bid > 0:
                                    block_floor_hint = bid
                                    print(
                                        f"[BUY][BLOCK] 无法获取持仓均价，使用当前 bid={bid:.4f} 作为保本地板价"
                                    )
                        except Exception as block_exc:
                            print(f"[BUY][BLOCK] 查询持仓均价异常：{block_exc}")
                            if bid is not None and bid > 0:
                                block_floor_hint = bid
                                print(f"[BUY][BLOCK] 使用当前 bid={bid:.4f} 作为 fallback 地板价")
                    _execute_sell(actionable_position, floor_hint=block_floor_hint, source="[BUY][BLOCK]")
                    continue
    
                if treat_as_dust:
                    fallback_px = bid if bid > 0 else ask
                    strategy.on_sell_filled(avg_price=fallback_px or 0.0, remaining=0.0)
                    position_size = None
                    last_order_size = None
                    print(
                        f"[BUY][DUST] 检测到尘埃仓位 {raw_position:.4f} < 最小挂单量 {dust_floor:.2f}，忽略并继续买入。"
                    )
                    status = strategy.status()
                    current_state = status.get("state")
                    awaiting = status.get("awaiting")
    
                awaiting_blocking = _awaiting_blocking(awaiting)
                if current_state != "FLAT" or awaiting_blocking:
                    _maybe_refresh_position_size("[BUY][STATE-SYNC]", force=True)
                    status = strategy.status()
                    current_state = status.get("state")
                    awaiting = status.get("awaiting")
                    awaiting_blocking = _awaiting_blocking(awaiting)
    
                    # 强制兜底：
                    # 1) 若策略仍认为持仓但本地/策略均无可用仓位，直接同步为空仓；
                    # 2) 若存在遗留的 BUY 待确认，自动解除阻塞。
                    combined_pos = _extract_position_size(status)
                    local_pos_candidates = [position_size, strat_pos]
                    for pos in local_pos_candidates:
                        try:
                            numeric = float(pos) if pos is not None else 0.0
                        except (TypeError, ValueError):
                            continue
                        if numeric > combined_pos:
                            combined_pos = numeric
    
                    if current_state != "FLAT" and (combined_pos is None or combined_pos <= dust_floor):
                        fallback_px = bid if bid > 0 else ask
                        strategy.on_sell_filled(avg_price=fallback_px or 0.0, remaining=0.0)
                        position_size = None
                        last_order_size = None
                        status = strategy.status()
                        current_state = status.get("state")
                        awaiting = status.get("awaiting")
                        awaiting_blocking = _awaiting_blocking(awaiting)
    
                    if awaiting_blocking:
                        awaiting_val = getattr(awaiting, "value", awaiting)
                        if awaiting_val == ActionType.BUY:
                            strategy.on_reject("auto-clear stale awaiting BUY")
                            status = strategy.status()
                            current_state = status.get("state")
                            awaiting = status.get("awaiting")
                            awaiting_blocking = _awaiting_blocking(awaiting)
    
                    if current_state != "FLAT" or awaiting_blocking:
                        print(
                            "[BUY][SKIP] 当前状态非 FLAT 或仍有持仓/待确认订单，丢弃买入信号。"
                        )
                        strategy.on_reject("state not flat or position exists")
                        continue
                now_for_buy = time.time()
                if now_for_buy < buy_cooldown_until:
                    remaining = buy_cooldown_until - now_for_buy
                    print(
                        f"[COOLDOWN] 买入冷却中，剩余 {remaining:.1f}s 再尝试买入。"
                    )
                    pending_buy = action
                    continue
    
                if max_position_cap is not None:
                    _maybe_refresh_position_size("[BUY][PRE]", force=True)
    
                ref_price = action.ref_price or ask or float(snap.get("price") or 0.0)
                if manual_order_size is not None:
                    probed_size, origin_display = _probe_position_size_for_buy()
                    current_position = probed_size
                    if current_position is None:
                        current_position = position_size
                    else:
                        position_size = current_position
                    owned = max(float(current_position or 0.0), 0.0)
                    planned_size, skip_manual = _plan_manual_buy_size(
                        manual_order_size,
                        owned,
                        enforce_target=manual_size_is_target,
                    )
                    if skip_manual:
                        origin_note = origin_display or "positions"
                        print(
                            f"[SKIP][BUY] 当前仓位({origin_note}) {owned:.4f} 已满足手动目标 {manual_order_size:.4f}，跳过本次买入。"
                        )
                        if max_position_cap is not None:
                            _maybe_refresh_position_size("[BUY][CAP-SYNC]", force=True)
                        strategy.on_reject("manual target already satisfied")
                        continue
                    if planned_size is None or planned_size <= 0:
                        print("[WARN] 手动份数解析异常，跳过本次买入。")
                        strategy.on_reject("invalid manual size")
                        continue
                    order_size = planned_size
                    if max_position_cap is not None:
                        remaining_cap = max(max_position_cap - owned, 0.0)
                        if remaining_cap <= 0:
                            origin_note = origin_display or "positions"
                            print(
                                f"[SKIP][BUY] 当前仓位({origin_note}) {owned:.4f} 已达到封顶 {max_position_cap:.4f}，跳过本次买入。"
                            )
                            _maybe_refresh_position_size("[BUY][CAP-SYNC]", force=True)
                            strategy.on_reject("position cap reached")
                            continue
                        if order_size > remaining_cap:
                            print(
                                f"[HINT][BUY] 按封顶 {max_position_cap:.4f} 调整下单量 {order_size:.4f} -> {remaining_cap:.4f}"
                            )
                            order_size = remaining_cap
                    origin_hint = f"({origin_display})" if origin_display else ""
                    if manual_size_is_target:
                        print(
                            f"[HINT][BUY] 目标 {manual_order_size:.4f} - 当前仓位{origin_hint} {owned:.4f} -> 本次下单 {order_size:.4f}"
                        )
                    else:
                        if owned > 0:
                            print(
                                f"[HINT][BUY] 当前仓位{origin_hint} {owned:.4f}，但按手动份数 {order_size:.4f} 下单。"
                            )
                        else:
                            print(f"[HINT][BUY] 手动份数 -> 本次下单 {order_size:.4f}")
                else:
                    order_size = _calc_size_by_1dollar(ref_price)
                    print(f"[HINT] 未指定份数，按 $1 反推 -> size={order_size}")
    
                def _buy_progress_probe() -> None:
                    try:
                        avg_px, total_pos, origin_note = _lookup_position_avg_price(client, token_id)
                    except Exception as probe_exc:
                        print(f"[WATCHDOG][BUY] 持仓查询异常：{probe_exc}")
                        return
                    origin_display = origin_note or "positions"
                    if total_pos is None or total_pos <= 0:
                        print(f"[WATCHDOG][BUY] 持仓检查 -> origin={origin_display} 当前无持仓")
                        return
                    avg_display = f"{avg_px:.4f}" if avg_px is not None else "-"
                    print(
                        f"[WATCHDOG][BUY] 持仓检查 -> origin={origin_display} avg={avg_display} size={total_pos:.4f}"
                    )
    
                baseline_position = float(position_size or 0.0)
    
                def _buy_fill_delta_probe() -> Optional[float]:
                    try:
                        _, latest_pos, _ = _lookup_position_avg_price(client, token_id)
                    except Exception as probe_exc:
                        print(f"[WATCHDOG][BUY] 持仓校对异常：{probe_exc}")
                        return None
                    if latest_pos is None:
                        return None
                    return max(float(latest_pos) - baseline_position, 0.0)
    
                if awaiting_buy_passthrough:
                    awaiting_buy_passthrough = False
                try:
                    buy_resp = maker_buy_follow_bid(
                        client=client,
                        token_id=token_id,
                        target_size=order_size,
                        max_buy_price=max_buy_price,
                        poll_sec=maker_poll_sec,
                        min_quote_amt=1.0,
                        min_order_size=API_MIN_ORDER_SIZE,
                        best_bid_fn=_latest_best_bid,
                        stop_check=stop_event.is_set,
                        external_fill_probe=_buy_fill_delta_probe,
                        progress_probe=_buy_progress_probe,
                        progress_probe_interval=60.0,
                    )
                except Exception as exc:
                    print(f"[ERR] 买入下单异常：{exc}")
                    strategy.on_reject(str(exc))
                    buy_cooldown_until = time.time() + short_buy_cooldown
                    continue
                print(f"[TRADE][BUY][MAKER] resp={buy_resp}")
                buy_status = str(buy_resp.get("status") or "").upper()
                filled_amt = float(buy_resp.get("filled") or 0.0)
                avg_price = buy_resp.get("avg_price")
                if filled_amt > 0:
                    fallback_price = float(avg_price if avg_price is not None else ref_price)
                    prior_position = float(position_size or 0.0)
                    expected_total_position = prior_position + filled_amt
                    actual_avg_price: Optional[float] = None
                    actual_total_position: Optional[float] = None
                    origin_note = ""
                    if POST_BUY_POSITION_CHECK_DELAY > 0:
                        print(
                            f"[INFO] 买入成交，延迟 {POST_BUY_POSITION_CHECK_DELAY:.0f}s 再查询持仓均价，等待 data-api 刷新…"
                        )
                        time.sleep(POST_BUY_POSITION_CHECK_DELAY)
                    print(
                        f"[INFO] 开始持仓均价多轮确认（目标 size≈{expected_total_position:.4f}，"
                        f"最多 {POST_BUY_POSITION_CHECK_ATTEMPTS} 轮，每轮间隔 {POST_BUY_POSITION_CHECK_INTERVAL:.0f}s）"
                    )

                    consecutive_hits = 0
                    last_avg: Optional[float] = None
                    confirmed_avg: Optional[float] = None
                    confirmed_total: Optional[float] = None
                    success_samples: List[Tuple[float, float]] = []
                    round_index = 0
                    used_fallback = False
                    while confirmed_avg is None or confirmed_total is None:
                        round_index += 1
                        # 检查是否达到最大重试轮数
                        if round_index > POST_BUY_POSITION_CONFIRM_MAX_ROUNDS:
                            origin_display = origin_note or "positions"
                            print(
                                f"[WARN] 持仓均价确认已达最大轮数 {POST_BUY_POSITION_CONFIRM_MAX_ROUNDS}，"
                                f"使用下单均价 {fallback_price:.4f} 作为 fallback。 origin={origin_display}"
                            )
                            confirmed_avg = fallback_price
                            confirmed_total = expected_total_position
                            used_fallback = True
                            break
                        round_success_start = len(success_samples)
                        for attempt in range(POST_BUY_POSITION_CHECK_ATTEMPTS):
                            try:
                                actual_avg_price, actual_total_position, origin_note = _lookup_position_avg_price(
                                    client, token_id
                                )
                            except Exception as exc:
                                print(
                                    f"[WARN] 持仓均价查询异常：{exc}，沿用下单均价 {fallback_price:.4f}。"
                                )
                                actual_avg_price = None
                                actual_total_position = None
                                break

                            if (
                                actual_avg_price is not None
                                and actual_total_position is not None
                            ):
                                success_samples.append((actual_avg_price, actual_total_position))
                                origin_display = origin_note or "positions"
                                print(
                                    f"[TRACE] 持仓均价查询结果（第{attempt + 1}次/本轮）"
                                    f" origin={origin_display} avg={actual_avg_price:.6f} size={actual_total_position:.6f}"
                                )
                                if (
                                    last_avg is not None
                                    and math.isclose(
                                        actual_avg_price,
                                        last_avg,
                                        rel_tol=POST_BUY_POSITION_MATCH_REL_TOL,
                                        abs_tol=POST_BUY_POSITION_MATCH_ABS_TOL,
                                    )
                                ):
                                    consecutive_hits += 1
                                else:
                                    consecutive_hits = 1
                                    last_avg = actual_avg_price

                                if consecutive_hits >= 3:
                                    confirmed_avg = actual_avg_price
                                    confirmed_total = actual_total_position
                                    break
                            else:
                                consecutive_hits = 0
                                last_avg = None

                            if attempt < POST_BUY_POSITION_CHECK_ATTEMPTS - 1:
                                time.sleep(POST_BUY_POSITION_CHECK_INTERVAL)

                        if confirmed_avg is not None and confirmed_total is not None:
                            break

                        if len(success_samples) == round_success_start:
                            origin_display = origin_note or "positions"
                            print(
                                "[WARN] 持仓均价多轮确认未能获取任何有效结果，暂停 60s 后重试整个查询流程。 "
                                f"origin={origin_display}"
                            )
                            consecutive_hits = 0
                            last_avg = None
                            time.sleep(60)
                            continue

                        tolerance_kwargs = {
                            "rel_tol": POST_BUY_POSITION_MATCH_REL_TOL,
                            "abs_tol": POST_BUY_POSITION_MATCH_ABS_TOL,
                        }
                        for candidate_avg, candidate_total in success_samples:
                            if candidate_total is None:
                                continue

                            match_count = 0
                            for avg_val, total_val in success_samples:
                                if (
                                    math.isclose(candidate_avg, avg_val, **tolerance_kwargs)
                                ):
                                    match_count += 1
                            if match_count >= 3:
                                confirmed_avg = candidate_avg
                                confirmed_total = candidate_total
                                break

                            if confirmed_avg is None or confirmed_total is None:
                                origin_display = origin_note or "positions"
                                print(
                                    "[WARN] 持仓均价未满足三次一致确认，继续进入下一轮查询。 "
                                    f"已完成 {round_index * POST_BUY_POSITION_CHECK_ATTEMPTS} 次尝试，origin={origin_display}"
                                )
                                consecutive_hits = 0
                                last_avg = None
                                time.sleep(POST_BUY_POSITION_CHECK_ROUND_COOLDOWN)

                    if stop_event.is_set():
                        return

                    fill_px = confirmed_avg
                    position_size = confirmed_total
                    last_order_size = filled_amt
                    display_size = (
                        confirmed_total if confirmed_total is not None else position_size
                    )
                    origin_display = origin_note or "positions"
                    print(
                        f"[STATE] 持仓均价确认 -> origin={origin_display} avg={fill_px:.4f} size={display_size:.4f}"
                    )
                    buy_filled_kwargs = {
                        "avg_price": fill_px,
                        "size": filled_amt,
                    }
                    if strategy_supports_total_position:
                        buy_filled_kwargs["total_position"] = position_size
                    strategy.on_buy_filled(**buy_filled_kwargs)
                    print(
                        f"[STATE] 买入成交 -> status={buy_status or 'N/A'} price={fill_px:.4f} size={position_size:.4f}"
                    )
                else:
                    reason_text = str(buy_resp)
                    print(f"[WARN] 买入未成交(status={buy_status or 'N/A'})：{reason_text}")
                    strategy.on_reject(reason_text)
                buy_cooldown_until = time.time() + short_buy_cooldown
    
                if filled_amt <= 0:
                    continue
    
                _execute_sell(position_size, floor_hint=strategy.sell_trigger_price(), source="[POST-BUY]")

            finally:
                next_loop_after = loop_started + min_loop_interval

    except KeyboardInterrupt:
        print("[CMD] 捕获到 Ctrl+C，准备退出…")
        strategy.stop("keyboard interrupt")
        stop_event.set()

    except OrderbookNotFoundError as not_found_exc:
        print(f"[ERROR] orderbook连续404，退出token: {not_found_exc}")
        _cancel_open_buy_orders_before_exit("ORDERBOOK_NOT_FOUND")
        _record_exit_token(token_id, "ORDERBOOK_NOT_FOUND", {
            "token_id": token_id,
            "message": str(not_found_exc),
            "consecutive_404": 5,
            "has_position": None,
        })
        _log_error("ORDERBOOK_NOT_FOUND", {
            "token_id": token_id,
            "message": str(not_found_exc),
            "consecutive_404": 5,
        })
        strategy.stop("orderbook not found")
        stop_event.set()

    except PriceNoneStreakError as none_exc:
        print(f"[ERROR] 价格连续None达到阈值，退出token: {none_exc}")
        _cancel_open_buy_orders_before_exit("PRICE_NONE_STREAK")
        _record_exit_token(token_id, "PRICE_NONE_STREAK", {
            "token_id": token_id,
            "side": getattr(none_exc, "side", "unknown"),
            "streak": int(getattr(none_exc, "streak", 0) or 0),
            "threshold": price_none_exit_count,
            "has_position": None,
        })
        _log_error("PRICE_NONE_STREAK", {
            "token_id": token_id,
            "message": str(none_exc),
            "side": getattr(none_exc, "side", "unknown"),
            "streak": int(getattr(none_exc, "streak", 0) or 0),
            "threshold": price_none_exit_count,
        })
        strategy.stop("price none streak")
        stop_event.set()

    except Exception as main_loop_exc:
        # ✅ P0修复：捕获主循环中的所有异常，确保能诊断问题
        print(f"[ERROR] 主循环异常退出: {type(main_loop_exc).__name__}: {main_loop_exc}")
        print(f"[ERROR] 异常堆栈: {traceback.format_exc()}")
        sys.stdout.flush()
        strategy.stop(f"main loop exception: {type(main_loop_exc).__name__}")
        stop_event.set()
        # 记录异常到日志文件
        _log_error("MAIN_LOOP_EXCEPTION", {
            "exception_type": type(main_loop_exc).__name__,
            "exception_message": str(main_loop_exc),
            "traceback": traceback.format_exc(),
            "token_id": token_id,
            "use_shared_ws": use_shared_ws
        })

    finally:
        stop_event.set()
        final_status = strategy.status()
        print(f"[EXIT] 最终状态: {final_status}")
        try:
            if _should_attempt_claim(market_meta, final_status, market_closed_detected):
                _attempt_claim(client, market_meta, token_id)
            else:
                print("[CLAIM] 未检测到需要 claim 的仓位，脚本结束。")
        except Exception as claim_exc:
            print(f"[CLAIM] 自动 claim 过程出现异常: {claim_exc}")


if __name__ == "__main__":
    # 解析命令行参数：
    # python Volatility_arbitrage_run.py <config_path> [--shared-ws-cache=<path>]
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else None

    # 解析共享 WS 缓存路径参数（替代环境变量）
    shared_ws_cache_arg = None
    for arg in sys.argv[2:]:
        if arg.startswith("--shared-ws-cache="):
            shared_ws_cache_arg = arg.split("=", 1)[1]
            break

    # 存储到全局变量供后续使用
    if shared_ws_cache_arg:
        os.environ["_POLY_WS_CACHE_ARG"] = shared_ws_cache_arg

    cfg = _load_run_config(cfg_path)
    main(cfg)
