"""
poly_maker_autorun
-------------------

基础骨架：配置加载、主循环、命令/交互入口。
当前版本通过 copytrade 产出的 token 文件驱动话题调度。
"""
from __future__ import annotations

import argparse
import copy
import fcntl
import json
import math
import os
import random
import queue
import requests
import select
import signal
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

# =====================
# 配置与常量
# =====================
PROJECT_ROOT = Path(__file__).resolve().parent
MAKER_ROOT = PROJECT_ROOT / "POLYMARKET_MAKER"
if str(MAKER_ROOT) not in sys.path:
    sys.path.insert(0, str(MAKER_ROOT))

DEFAULT_GLOBAL_CONFIG = {
    "copytrade_poll_sec": 30.0,
    "sell_position_poll_interval_sec": 7200.0,
    "command_poll_sec": 5.0,
    "max_concurrent_tasks": 10,
    "max_exit_cleanup_tasks": 3,  # 清仓任务独立槽位，不受 max_concurrent_tasks 限制
    "log_dir": str(PROJECT_ROOT / "logs" / "autorun"),
    "data_dir": str(PROJECT_ROOT / "data"),
    "handled_topics_path": str(PROJECT_ROOT / "data" / "handled_topics.json"),
    "copytrade_tokens_path": str(
        PROJECT_ROOT.parent / "copytrade" / "tokens_from_copytrade.json"
    ),
    "copytrade_sell_signals_path": str(
        PROJECT_ROOT.parent / "copytrade" / "copytrade_sell_signals.json"
    ),
    "process_start_retries": 1,
    "process_retry_delay_sec": 2.0,
    "process_graceful_timeout_sec": 5.0,
    "process_stagger_max_sec": 3.0,
    "topic_start_cooldown_sec": 5.0,
    "log_excerpt_interval_sec": 15.0,
    "runtime_status_path": str(PROJECT_ROOT / "data" / "autorun_status.json"),
    "ws_debug_raw": False,
    # Slot refill (回填) 配置
    "enable_slot_refill": True,
    "refill_cooldown_minutes": 30.0,
    "max_refill_retries": 3,
    "refill_check_interval_sec": 60.0,
    # Pending 软淘汰（避免无数据 token 长期卡在 pending）
    "enable_pending_soft_eviction": True,
    "pending_soft_eviction_minutes": 12.0,
    "pending_soft_eviction_check_interval_sec": 300.0,
    # Shared WS 等待配置
    "shared_ws_max_pending_wait_sec": 45.0,
    "shared_ws_wait_poll_sec": 0.5,
    "shared_ws_wait_failures_before_pause": 2,
    "shared_ws_wait_pause_minutes": 1.0,
    "shared_ws_wait_escalation_window_sec": 240.0,
    "shared_ws_wait_escalation_min_failures": 2,
}

# Shared WS 等待防抖参数（写死，避免依赖外部 JSON）
SHARED_WS_WAIT_ESCALATION_WINDOW_SEC = 240.0
SHARED_WS_WAIT_ESCALATION_MIN_FAILURES = 2
ORDER_SIZE_DECIMALS = 4  # Polymarket 下单数量精度（按买单精度取整）
DATA_API_ROOT = os.getenv("POLY_DATA_API_ROOT", "https://data-api.polymarket.com")
POSITION_CHECK_CACHE_TTL_SEC = 300.0
POSITION_CHECK_NEGATIVE_CACHE_TTL_SEC = 10.0
POSITION_CLEANUP_DUST_THRESHOLD = 0.5
EXIT_CLEANUP_MAX_RETRIES = 3
DATA_API_RATE_LIMIT_SEC = 1.0
_data_api_last_request_ts = 0.0
_data_api_request_lock = threading.Lock()


class _TeeStream:
    def __init__(self, primary: Any, secondary: Any) -> None:
        self._primary = primary
        self._secondary = secondary
        self._lock = threading.Lock()

    def write(self, data: str) -> int:
        with self._lock:
            written = self._primary.write(data)
            self._secondary.write(data)
            self._secondary.flush()
            return written

    def flush(self) -> None:
        with self._lock:
            self._primary.flush()
            self._secondary.flush()

    def isatty(self) -> bool:
        return bool(getattr(self._primary, "isatty", lambda: False)())

    def fileno(self) -> int:
        return int(getattr(self._primary, "fileno", lambda: -1)())


def _setup_main_log(log_dir: Path) -> Optional[Path]:
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        filename = time.strftime("autorun_main_%Y%m%d_%H%M%S.log", time.localtime())
        log_path = log_dir / filename
        log_file = open(log_path, "a", encoding="utf-8", buffering=1)
        sys.stdout = _TeeStream(sys.stdout, log_file)
        sys.stderr = _TeeStream(sys.stderr, log_file)
        return log_path
    except Exception as exc:
        print(f"[WARN] 无法创建主程序日志文件: {exc}")
        return None


# ========== 错误日志记录函数 ==========
def _log_error(error_type: str, error_data: Dict[str, Any]) -> None:
    """
    记录错误到独立的错误日志文件。

    :param error_type: 错误类型标识（如 WS_AGGREGATOR_ERROR, TASK_START_ERROR 等）
    :param error_data: 错误相关数据（字典格式）
    """
    try:
        # 确定错误日志文件路径
        log_dir = PROJECT_ROOT / "logs" / "autorun"
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


def _atomic_json_write(path: Path, data: Any) -> None:
    """原子写入 JSON 文件：先写临时文件，再 rename，避免与外部进程竞态导致数据丢失。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        dir=str(path.parent), suffix=".tmp", prefix=".copytrade_"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.rename(tmp_path, str(path))
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _resolve_position_address_from_env() -> tuple[Optional[str], str]:
    env_candidates = (
        "POLY_DATA_ADDRESS",
        "POLY_FUNDER",
        "POLY_WALLET",
        "POLY_ADDRESS",
    )
    for env_name in env_candidates:
        cand = os.getenv(env_name)
        if cand and str(cand).strip():
            return str(cand).strip(), f"env:{env_name}"
    return None, "缺少地址，无法从数据接口拉取持仓。"


def _position_matches_token(entry: Dict[str, Any], token_id: str) -> bool:
    token_keys = ("token_id", "tokenId", "token", "asset", "asset_id")
    for key in token_keys:
        val = entry.get(key)
        if val and str(val) == token_id:
            return True
    return False


def _extract_position_token_id(entry: Dict[str, Any]) -> Optional[str]:
    token_keys = ("token_id", "tokenId", "token", "asset", "asset_id")
    for key in token_keys:
        val = entry.get(key)
        if val is None:
            continue
        token_id = str(val).strip()
        if token_id:
            return token_id
    return None


def _extract_position_size(entry: Dict[str, Any]) -> Optional[float]:
    size_keys = (
        "size",
        "position_size",
        "quantity",
        "balance",
        "shares",
        "amount",
    )
    for key in size_keys:
        val = entry.get(key)
        if val is None or isinstance(val, bool):
            continue
        try:
            return float(val)
        except (TypeError, ValueError):
            continue
    return None


def _fetch_position_size_from_data_api(
    address: str,
    token_id: str,
) -> tuple[Optional[float], str]:
    if not address:
        return None, "缺少地址，无法查询持仓。"
    url = f"{DATA_API_ROOT}/positions"
    limit = 500
    offset = 0
    while True:
        params = {
            "user": address,
            "limit": limit,
            "offset": offset,
            "sizeThreshold": 0,
        }
        try:
            with _data_api_request_lock:
                global _data_api_last_request_ts
                now = time.time()
                wait_sec = DATA_API_RATE_LIMIT_SEC - (now - _data_api_last_request_ts)
                if wait_sec > 0:
                    time.sleep(wait_sec)
                _data_api_last_request_ts = time.time()
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code == 404:
                return None, "数据接口返回 404（请确认使用 Proxy/Deposit 地址查询 user 参数）"
            resp.raise_for_status()
            payload = resp.json()
        except requests.RequestException as exc:
            return None, f"数据接口请求失败：{exc}"
        except ValueError:
            return None, "数据接口响应解析失败"

        if isinstance(payload, list):
            positions = payload
        elif isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, list):
                positions = data
            elif isinstance(data, dict) and isinstance(data.get("positions"), list):
                positions = data.get("positions")
            else:
                positions = payload.get("positions")
        else:
            positions = None
        if positions is None:
            return None, "数据接口返回格式异常，positions 未找到。"
        if not isinstance(positions, list):
            return None, "数据接口返回格式异常，positions 非列表。"

        for pos in positions:
            if not isinstance(pos, dict):
                continue
            if not _position_matches_token(pos, token_id):
                continue
            pos_size = _extract_position_size(pos)
            return pos_size, "ok"

        if not positions:
            break

        offset += len(positions)
    return None, "未找到持仓记录"


def _fetch_position_snapshot_map_from_data_api(
    address: str,
) -> tuple[Dict[str, float], str]:
    if not address:
        return {}, "缺少地址，无法查询持仓。"
    url = f"{DATA_API_ROOT}/positions"
    limit = 500
    offset = 0
    snapshot: Dict[str, float] = {}
    while True:
        params = {
            "user": address,
            "limit": limit,
            "offset": offset,
            "sizeThreshold": 0,
        }
        try:
            with _data_api_request_lock:
                global _data_api_last_request_ts
                now = time.time()
                wait_sec = DATA_API_RATE_LIMIT_SEC - (now - _data_api_last_request_ts)
                if wait_sec > 0:
                    time.sleep(wait_sec)
                _data_api_last_request_ts = time.time()
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code == 404:
                return {}, "数据接口返回 404（请确认使用 Proxy/Deposit 地址查询 user 参数）"
            resp.raise_for_status()
            payload = resp.json()
        except requests.RequestException as exc:
            return {}, f"数据接口请求失败：{exc}"
        except ValueError:
            return {}, "数据接口响应解析失败"

        if isinstance(payload, list):
            positions = payload
        elif isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, list):
                positions = data
            elif isinstance(data, dict) and isinstance(data.get("positions"), list):
                positions = data.get("positions")
            else:
                positions = payload.get("positions")
        else:
            positions = None
        if positions is None:
            return {}, "数据接口返回格式异常，positions 未找到。"
        if not isinstance(positions, list):
            return {}, "数据接口返回格式异常，positions 非列表。"

        for pos in positions:
            if not isinstance(pos, dict):
                continue
            token_id = _extract_position_token_id(pos)
            if not token_id:
                continue
            pos_size = float(_extract_position_size(pos) or 0.0)
            snapshot[token_id] = pos_size

        if not positions:
            break
        offset += len(positions)
    return snapshot, "ok"


def _topic_id_from_entry(entry: Any) -> str:
    """从 copytrade token 条目中提取 token_id。"""

    if isinstance(entry, str):
        return entry.strip()
    if isinstance(entry, dict):
        token_id = entry.get("token_id") or entry.get("tokenId")
        if isinstance(token_id, str) and token_id.strip():
            return token_id.strip()
        return ""
    return str(entry).strip()


def _safe_topic_filename(topic_id: str) -> str:
    return topic_id.replace("/", "_").replace("\\", "_")


def _coerce_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        if isinstance(value, str):
            raw = value.replace(",", "").strip()
            if not raw:
                return None
            return float(raw)
    except Exception:
        return None
    return None


def _env_flag(name: str) -> bool:
    raw = os.getenv(name, "").strip().lower()
    return raw in {"1", "true", "yes", "on", "y", "debug"}


def _ceil_to_precision(value: float, decimals: int) -> float:
    factor = 10 ** decimals
    return math.ceil(value * factor - 1e-12) / factor


def _scale_order_size_by_volume(
    base_size: float,
    total_volume: float,
    *,
    base_volume: Optional[float] = None,
    growth_factor: float = 0.5,
    decimals: int = ORDER_SIZE_DECIMALS,
) -> float:
    """根据市场成交量对基础下单份数进行递增（边际递减）。"""

    if base_size <= 0 or total_volume <= 0:
        return base_size

    effective_base_volume = _coerce_float(base_volume) or total_volume
    if effective_base_volume <= 0:
        return base_size

    effective_growth = max(growth_factor, 0.0)
    vol_ratio = max(total_volume / effective_base_volume, 1.0)
    # 使用对数增长控制放大：
    #   - base_volume 附近仅有轻微提升；
    #   - 成交量每提升 10 倍仅线性增加 growth_factor，边际效用递减。
    weight = 1.0 + effective_growth * math.log10(vol_ratio)
    weighted_size = base_size * weight
    return _ceil_to_precision(weighted_size, decimals)


def _load_json_file(path: Path) -> Dict[str, Any]:
    """读取 JSON 配置，不存在则返回空 dict。"""
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError as exc:  # pragma: no cover - 粗略校验
            raise RuntimeError(f"无法解析 JSON 配置: {path}: {exc}") from exc


def _dump_json_file(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def read_handled_topics(path: Path) -> set[str]:
    """读取历史已处理话题集合，空文件或字段缺失则返回空集合。"""

    data = _load_json_file(path)
    topics = data.get("topics") or data.get("handled_topics")
    if topics is None:
        return set()
    if not isinstance(topics, list):  # pragma: no cover - 容错
        print(f"[WARN] handled_topics 文件格式异常，已忽略: {path}")
        return set()
    return {str(t) for t in topics}


def write_handled_topics(path: Path, topics: set[str]) -> None:
    """写入最新的已处理话题集合。"""

    payload = {
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "total": len(topics),
        "topics": sorted(topics),
    }
    _dump_json_file(path, payload)


def compute_new_topics(latest: List[Any], handled: set[str]) -> List[str]:
    """从最新筛选结果中筛出尚未处理的话题列表。"""

    result: List[str] = []
    for entry in latest:
        topic_id = _topic_id_from_entry(entry)
        if topic_id and topic_id not in handled:
            result.append(topic_id)
    return result


@dataclass
class GlobalConfig:
    copytrade_poll_sec: float = DEFAULT_GLOBAL_CONFIG["copytrade_poll_sec"]
    sell_position_poll_interval_sec: float = DEFAULT_GLOBAL_CONFIG[
        "sell_position_poll_interval_sec"
    ]
    command_poll_sec: float = DEFAULT_GLOBAL_CONFIG["command_poll_sec"]
    max_concurrent_tasks: int = DEFAULT_GLOBAL_CONFIG["max_concurrent_tasks"]
    max_exit_cleanup_tasks: int = DEFAULT_GLOBAL_CONFIG["max_exit_cleanup_tasks"]
    log_dir: Path = field(default_factory=lambda: Path(DEFAULT_GLOBAL_CONFIG["log_dir"]))
    data_dir: Path = field(default_factory=lambda: Path(DEFAULT_GLOBAL_CONFIG["data_dir"]))
    handled_topics_path: Path = field(
        default_factory=lambda: Path(DEFAULT_GLOBAL_CONFIG["handled_topics_path"])
    )
    copytrade_tokens_path: Path = field(
        default_factory=lambda: Path(DEFAULT_GLOBAL_CONFIG["copytrade_tokens_path"])
    )
    copytrade_sell_signals_path: Path = field(
        default_factory=lambda: Path(DEFAULT_GLOBAL_CONFIG["copytrade_sell_signals_path"])
    )
    process_start_retries: int = DEFAULT_GLOBAL_CONFIG["process_start_retries"]
    process_retry_delay_sec: float = DEFAULT_GLOBAL_CONFIG["process_retry_delay_sec"]
    process_graceful_timeout_sec: float = DEFAULT_GLOBAL_CONFIG[
        "process_graceful_timeout_sec"
    ]
    process_stagger_max_sec: float = DEFAULT_GLOBAL_CONFIG["process_stagger_max_sec"]
    topic_start_cooldown_sec: float = DEFAULT_GLOBAL_CONFIG["topic_start_cooldown_sec"]
    log_excerpt_interval_sec: float = DEFAULT_GLOBAL_CONFIG["log_excerpt_interval_sec"]
    runtime_status_path: Path = field(
        default_factory=lambda: Path(DEFAULT_GLOBAL_CONFIG["runtime_status_path"])
    )
    ws_debug_raw: bool = bool(DEFAULT_GLOBAL_CONFIG["ws_debug_raw"])
    # Shared WS 等待配置
    shared_ws_max_pending_wait_sec: float = DEFAULT_GLOBAL_CONFIG[
        "shared_ws_max_pending_wait_sec"
    ]
    shared_ws_wait_poll_sec: float = DEFAULT_GLOBAL_CONFIG["shared_ws_wait_poll_sec"]
    shared_ws_wait_failures_before_pause: int = DEFAULT_GLOBAL_CONFIG[
        "shared_ws_wait_failures_before_pause"
    ]
    shared_ws_wait_pause_minutes: float = DEFAULT_GLOBAL_CONFIG[
        "shared_ws_wait_pause_minutes"
    ]
    shared_ws_wait_escalation_window_sec: float = DEFAULT_GLOBAL_CONFIG[
        "shared_ws_wait_escalation_window_sec"
    ]
    shared_ws_wait_escalation_min_failures: int = DEFAULT_GLOBAL_CONFIG[
        "shared_ws_wait_escalation_min_failures"
    ]
    # Slot refill (回填) 配置
    enable_slot_refill: bool = bool(DEFAULT_GLOBAL_CONFIG["enable_slot_refill"])
    refill_cooldown_minutes: float = DEFAULT_GLOBAL_CONFIG["refill_cooldown_minutes"]
    max_refill_retries: int = DEFAULT_GLOBAL_CONFIG["max_refill_retries"]
    refill_check_interval_sec: float = DEFAULT_GLOBAL_CONFIG["refill_check_interval_sec"]
    enable_pending_soft_eviction: bool = DEFAULT_GLOBAL_CONFIG[
        "enable_pending_soft_eviction"
    ]
    pending_soft_eviction_minutes: float = DEFAULT_GLOBAL_CONFIG[
        "pending_soft_eviction_minutes"
    ]
    pending_soft_eviction_check_interval_sec: float = DEFAULT_GLOBAL_CONFIG[
        "pending_soft_eviction_check_interval_sec"
    ]
    # Maker 子进程配置
    maker_poll_sec: float = 10.0  # 挂单轮询间隔（秒）
    maker_position_sync_interval: float = 60.0  # 仓位同步间隔（秒）

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GlobalConfig":
        data = data or {}
        scheduler = data.get("scheduler") or {}
        paths = data.get("paths") or {}
        debug = data.get("debug") or {}
        maker = data.get("maker") or {}
        flat_overrides = {k: v for k, v in data.items() if k not in {"scheduler", "paths", "maker"}}
        merged = {**DEFAULT_GLOBAL_CONFIG, **flat_overrides}

        log_dir = Path(
            paths.get("log_directory")
            or merged.get("log_dir", DEFAULT_GLOBAL_CONFIG["log_dir"])
        )
        data_dir = Path(
            paths.get("data_directory")
            or merged.get("data_dir", DEFAULT_GLOBAL_CONFIG["data_dir"])
        )

        handled_topics_path = Path(
            merged.get("handled_topics_path")
            or paths.get("handled_topics_file")
            or data_dir / "handled_topics.json"
        )
        copytrade_tokens_path = Path(
            merged.get("copytrade_tokens_path")
            or paths.get("copytrade_tokens_file")
            or PROJECT_ROOT.parent / "copytrade" / "tokens_from_copytrade.json"
        )
        copytrade_sell_signals_path = Path(
            merged.get("copytrade_sell_signals_path")
            or paths.get("copytrade_sell_signals_file")
            or PROJECT_ROOT.parent / "copytrade" / "copytrade_sell_signals.json"
        )
        runtime_status_path = Path(
            merged.get("runtime_status_path")
            or paths.get("run_state_file")
            or data_dir / "autorun_status.json"
        )

        return cls(
            copytrade_poll_sec=float(
                scheduler.get("copytrade_poll_seconds")
                or merged.get(
                    "copytrade_poll_sec", DEFAULT_GLOBAL_CONFIG["copytrade_poll_sec"]
                )
            ),
            sell_position_poll_interval_sec=float(
                scheduler.get("sell_position_poll_interval_sec")
                or merged.get(
                    "sell_position_poll_interval_sec",
                    DEFAULT_GLOBAL_CONFIG["sell_position_poll_interval_sec"],
                )
            ),
            command_poll_sec=float(
                scheduler.get("command_poll_seconds")
                or scheduler.get("poll_interval_seconds")
                or merged.get("command_poll_sec", DEFAULT_GLOBAL_CONFIG["command_poll_sec"])
            ),
            max_concurrent_tasks=int(
                scheduler.get(
                    "max_concurrent_tasks", DEFAULT_GLOBAL_CONFIG["max_concurrent_tasks"]
                )
            ),
            max_exit_cleanup_tasks=int(
                scheduler.get(
                    "max_exit_cleanup_tasks", DEFAULT_GLOBAL_CONFIG["max_exit_cleanup_tasks"]
                )
            ),
            log_dir=log_dir,
            data_dir=data_dir,
            handled_topics_path=handled_topics_path,
            copytrade_tokens_path=copytrade_tokens_path,
            copytrade_sell_signals_path=copytrade_sell_signals_path,
            process_start_retries=int(
                merged.get("process_start_retries", cls.process_start_retries)
            ),
            process_retry_delay_sec=float(
                merged.get("process_retry_delay_sec", cls.process_retry_delay_sec)
            ),
            process_graceful_timeout_sec=float(
                merged.get(
                    "process_graceful_timeout_sec", cls.process_graceful_timeout_sec
                )
            ),
            process_stagger_max_sec=float(
                merged.get("process_stagger_max_sec", cls.process_stagger_max_sec)
            ),
            topic_start_cooldown_sec=float(
                merged.get("topic_start_cooldown_sec", cls.topic_start_cooldown_sec)
            ),
            log_excerpt_interval_sec=float(
                merged.get("log_excerpt_interval_sec", cls.log_excerpt_interval_sec)
            ),
            runtime_status_path=runtime_status_path,
            ws_debug_raw=bool(
                debug.get("ws_debug_raw")
                or debug.get("ws_raw")
                or merged.get("ws_debug_raw", cls.ws_debug_raw)
            ),
            shared_ws_max_pending_wait_sec=float(
                merged.get(
                    "shared_ws_max_pending_wait_sec",
                    merged.get(
                        "shared_ws_wait_timeout_sec",
                        cls.shared_ws_max_pending_wait_sec,
                    ),
                )
            ),
            shared_ws_wait_poll_sec=float(
                merged.get(
                    "shared_ws_wait_poll_sec",
                    cls.shared_ws_wait_poll_sec,
                )
            ),
            shared_ws_wait_failures_before_pause=int(
                merged.get(
                    "shared_ws_wait_failures_before_pause",
                    cls.shared_ws_wait_failures_before_pause,
                )
            ),
            shared_ws_wait_pause_minutes=float(
                merged.get(
                    "shared_ws_wait_pause_minutes",
                    cls.shared_ws_wait_pause_minutes,
                )
            ),
            shared_ws_wait_escalation_window_sec=float(
                merged.get(
                    "shared_ws_wait_escalation_window_sec",
                    cls.shared_ws_wait_escalation_window_sec,
                )
            ),
            shared_ws_wait_escalation_min_failures=int(
                merged.get(
                    "shared_ws_wait_escalation_min_failures",
                    cls.shared_ws_wait_escalation_min_failures,
                )
            ),
            # Slot refill (回填) 配置
            enable_slot_refill=bool(
                scheduler.get("enable_slot_refill", merged.get("enable_slot_refill", True))
            ),
            refill_cooldown_minutes=float(
                scheduler.get("refill_cooldown_minutes", merged.get("refill_cooldown_minutes", 30.0))
            ),
            max_refill_retries=int(
                scheduler.get("max_refill_retries", merged.get("max_refill_retries", 3))
            ),
            refill_check_interval_sec=float(
                scheduler.get("refill_check_interval_sec", merged.get("refill_check_interval_sec", 60.0))
            ),
            enable_pending_soft_eviction=bool(
                scheduler.get(
                    "enable_pending_soft_eviction",
                    merged.get("enable_pending_soft_eviction", True),
                )
            ),
            pending_soft_eviction_minutes=float(
                scheduler.get(
                    "pending_soft_eviction_minutes",
                    merged.get("pending_soft_eviction_minutes", 60.0),
                )
            ),
            pending_soft_eviction_check_interval_sec=float(
                scheduler.get(
                    "pending_soft_eviction_check_interval_sec",
                    merged.get("pending_soft_eviction_check_interval_sec", 300.0),
                )
            ),
            # Maker 子进程配置
            maker_poll_sec=float(
                maker.get("poll_sec", merged.get("maker_poll_sec", 10.0))
            ),
            maker_position_sync_interval=float(
                maker.get("position_sync_interval", merged.get("maker_position_sync_interval", 60.0))
            ),
        )

    def ensure_dirs(self) -> None:
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)


@dataclass
class TopicTask:
    topic_id: str
    status: str = "pending"
    start_time: float = field(default_factory=time.time)
    last_heartbeat: Optional[float] = None
    notes: List[str] = field(default_factory=list)
    process: Optional[subprocess.Popen] = None
    log_path: Optional[Path] = None
    config_path: Optional[Path] = None
    log_excerpt: str = ""
    restart_attempts: int = 0
    no_restart: bool = False
    end_reason: Optional[str] = None
    last_log_excerpt_ts: float = 0.0

    def heartbeat(self, message: str) -> None:
        self.last_heartbeat = time.time()
        self.notes.append(message)

    def is_running(self) -> bool:
        return bool(self.process) and (self.process.poll() is None)


class AutoRunManager:
    def __init__(
        self,
        global_config: GlobalConfig,
        strategy_defaults: Dict[str, Any],
        run_params_template: Dict[str, Any],
    ):
        self.config = global_config
        self.strategy_defaults = strategy_defaults
        self.run_params_template = run_params_template or {}
        self.stop_event = threading.Event()
        self.command_queue: "queue.Queue[str]" = queue.Queue()
        self.tasks: Dict[str, TopicTask] = {}
        self.latest_topics: List[Dict[str, Any]] = []
        self.topic_details: Dict[str, Dict[str, Any]] = {}
        self.handled_topics: set[str] = set()
        self.pending_topics: List[str] = []
        self.pending_exit_topics: List[str] = []
        self._next_topics_refresh: float = 0.0
        self._next_status_dump: float = 0.0
        self._next_topic_start_at: float = 0.0
        self.status_path = self.config.runtime_status_path
        self._ws_cache_path = self.config.data_dir / "ws_cache.json"
        self._ws_cache_lock = threading.Lock()
        self._ws_cache: Dict[str, Dict[str, Any]] = {}
        self._ws_cache_dirty = False
        self._ws_cache_last_flush = 0.0
        self._ws_thread: Optional[threading.Thread] = None
        self._ws_thread_stop: Optional[threading.Event] = None
        self._ws_token_ids: List[str] = []
        self._ws_aggregator_thread: Optional[threading.Thread] = None
        self._ws_debug_raw = _env_flag("POLY_WS_DEBUG_RAW") or self.config.ws_debug_raw
        self._refill_debug = _env_flag("POLY_REFILL_DEBUG")
        # 增量订阅客户端（替代完全重启WS的方式）
        self._ws_client: Optional[Any] = None  # WSAggregatorClient 实例
        # Slot refill (回填) 相关
        self._exit_tokens_path = self.config.data_dir / "exit_tokens.json"
        self._refill_retry_counts: Dict[str, int] = {}  # token_id -> 已重试次数
        self._next_refill_check: float = 0.0
        self._refilled_tokens: set[str] = set()  # 已回填的token（避免重复回填）
        # Shared WS 等待保护
        self._shared_ws_wait_failures: Dict[str, int] = {}
        self._shared_ws_paused_until: Dict[str, float] = {}
        self._shared_ws_wait_timeout_events: Dict[str, List[float]] = {}
        self._pending_first_seen: Dict[str, float] = {}
        self._shared_ws_pending_since: Dict[str, float] = {}
        self._next_pending_eviction: float = 0.0
        # 已完成 exit-only cleanup 的 token（避免重复触发清仓）
        self._completed_exit_cleanup_tokens: set[str] = set()
        self._handled_sell_signals: set[str] = set()
        self._position_snapshot_cache: Dict[str, Dict[str, Any]] = {}
        self._exit_cleanup_retry_counts: Dict[str, int] = {}
        self._position_address: Optional[str] = None
        self._position_address_origin: Optional[str] = None
        self._position_address_warned: bool = False
        self._sell_bootstrap_done: bool = False
        self._next_sell_full_recheck_at: float = 0.0
        self._sell_position_snapshot: Dict[str, float] = {}
        self._sell_position_snapshot_info: str = ""
        # WS 健康分层清理状态
        self._ws_recent_recovery_ts: Dict[str, float] = {}
        self._ws_prev_stale_state: Dict[str, bool] = {}
        # 日志清理相关（每天清理7天前的日志）
        self._next_log_cleanup: float = 0.0
        self._log_cleanup_interval_sec: float = 3600.0  # 每小时检查一次
        self._log_retention_days: int = 7  # 保留7天
        self._last_cleanup_date: Optional[str] = None  # 记录上次清理日期，避免同一天重复清理

    # ========== 核心循环 ==========
    def run_loop(self) -> None:
        self.config.ensure_dirs()
        self._load_handled_topics()
        self._restore_runtime_status()
        print(f"[INIT] autorun start | copytrade_poll={self.config.copytrade_poll_sec}s")
        self._start_ws_aggregator()
        try:
            while not self.stop_event.is_set():
                try:
                    now = time.time()
                    self._process_commands()
                    self._poll_tasks()
                    self._schedule_pending_exit_cleanup()
                    self._schedule_pending_topics()
                    self._purge_inactive_tasks()
                    if now >= self._next_topics_refresh:
                        self._refresh_topics()
                        # 清理 MARKET_CLOSED 的 token（从 copytrade 文件中移除）
                        self._cleanup_closed_market_tokens()
                        self._next_topics_refresh = now + self.config.copytrade_poll_sec
                    # Slot回填检查
                    if self.config.enable_slot_refill and now >= self._next_refill_check:
                        self._schedule_refill()
                        self._next_refill_check = now + self.config.refill_check_interval_sec
                    if (
                        self.config.enable_pending_soft_eviction
                        and now >= self._next_pending_eviction
                    ):
                        self._evict_stale_pending_topics()
                        self._next_pending_eviction = now + float(
                            self.config.pending_soft_eviction_check_interval_sec
                        )
                    if now >= self._next_status_dump:
                        self._print_status()
                        self._dump_runtime_status()
                        self._next_status_dump = now + max(
                            5.0, self.config.command_poll_sec
                        )
                    # 日志清理检查（每天清理一次7天前的日志）
                    if now >= self._next_log_cleanup:
                        self._cleanup_old_logs()
                        self._next_log_cleanup = now + self._log_cleanup_interval_sec
                    time.sleep(self.config.command_poll_sec)
                except Exception as exc:  # pragma: no cover - 防御性保护
                    print(f"[ERROR] 主循环异常已捕获，将继续运行: {exc}")
                    traceback.print_exc()
                    _log_error("MAIN_LOOP_ERROR", {
                        "message": "主循环异常",
                        "error": str(exc),
                        "traceback": traceback.format_exc()
                    })
                    time.sleep(max(1.0, self.config.command_poll_sec))
        finally:
            self._stop_ws_aggregator()
            self._cleanup_all_tasks()
            self._dump_runtime_status()
            print("[DONE] autorun stopped")

    def _start_ws_aggregator(self) -> None:
        if self._ws_aggregator_thread and self._ws_aggregator_thread.is_alive():
            return
        self._ws_aggregator_thread = threading.Thread(
            target=self._ws_aggregator_loop,
            daemon=True,
        )
        self._ws_aggregator_thread.start()

    def _stop_ws_aggregator(self) -> None:
        self._stop_ws_subscription()
        if self._ws_aggregator_thread and self._ws_aggregator_thread.is_alive():
            self._ws_aggregator_thread.join(timeout=3)

    def _desired_ws_token_ids(self) -> List[str]:
        """获取需要订阅的token列表（包括运行中的和待启动的）"""
        token_ids = []

        # 1. 运行中的任务
        for topic_id, task in self.tasks.items():
            if task.is_running():
                token_ids.append(topic_id)

        # 2. 待启动的pending tokens（提前订阅，避免启动后等待）
        for topic_id in self.pending_topics:
            if topic_id not in token_ids:
                token_ids.append(topic_id)

        return sorted({tid for tid in token_ids if tid})

    def _ws_aggregator_loop(self) -> None:
        last_health_check = 0.0
        last_event_count = 0  # 跟踪上次检查时的事件数
        _consecutive_errors = 0

        while not self.stop_event.is_set():
            try:
                desired = self._desired_ws_token_ids()
                if desired != self._ws_token_ids:
                    self._restart_ws_subscription(desired)
                self._flush_ws_cache_if_needed()

                # 定期健康检查（每10秒，加快故障检测和恢复）
                now = time.time()
                if now - last_health_check >= 10.0:
                    current_count = getattr(self, '_ws_event_count', 0)

                    # 检查数据流是否停滞
                    if current_count == last_event_count and self._ws_token_ids:
                        print(f"[WARN] WS 聚合器10秒内未收到任何新事件（订阅了 {len(self._ws_token_ids)} 个token）")
                    elif current_count > last_event_count:
                        # 数据流正常，每小时打印一次统计（避免刷屏）
                        if not hasattr(self, '_last_flow_log'):
                            self._last_flow_log = 0.0
                        if now - self._last_flow_log >= 3600.0:
                            print(f"[WS][FLOW] 数据流正常，10秒内收到 {current_count - last_event_count} 个事件")
                            self._last_flow_log = now

                    last_event_count = current_count
                    self._health_check()
                    last_health_check = now

                _consecutive_errors = 0  # 本轮正常，重置连续错误计数
            except Exception as exc:
                _consecutive_errors += 1
                backoff = min(30.0, 1.0 * (2 ** min(_consecutive_errors, 5)))
                print(
                    f"[ERROR] WS 聚合器线程异常（连续第{_consecutive_errors}次），"
                    f"{backoff:.0f}秒后重试: {exc}"
                )
                traceback.print_exc()
                _log_error("WS_AGGREGATOR_LOOP_ERROR", {
                    "message": "WS聚合器线程异常",
                    "error": str(exc),
                    "consecutive_errors": _consecutive_errors,
                    "traceback": traceback.format_exc(),
                })
                time.sleep(backoff)
                continue

            time.sleep(0.1)  # 从1.0秒改为0.1秒，提高缓存写入频率

    def _update_ws_subscription(self, token_ids: List[str]) -> None:
        """
        使用增量订阅更新WS订阅列表（避免完全重启WS连接）

        优化点：
        - 计算新增/移除的token差异
        - 通过增量消息添加/移除订阅，而不是重启整个WS连接
        - 保持数据流连续性，消除时序竞态问题
        """
        old_ids = set(self._ws_token_ids)
        new_ids = set(token_ids)
        added = new_ids - old_ids
        removed = old_ids - new_ids

        # 无变化时跳过
        if not added and not removed:
            return

        if added:
            print(f"[WS][AGGREGATOR] ✓ 新增订阅 {len(added)} 个token:")
            for tid in list(added)[:5]:
                print(f"    {tid[:8]}...{tid[-8:]}")
            if len(added) > 5:
                print(f"    ... (还有 {len(added) - 5} 个)")
        if removed:
            print(f"[WS][AGGREGATOR] ✗ 移除订阅 {len(removed)} 个token:")
            for tid in list(removed)[:5]:
                print(f"    {tid[:8]}...{tid[-8:]}")
            if len(removed) > 5:
                print(f"    ... (还有 {len(removed) - 5} 个)")

        # 调试：首次打印完整订阅列表
        if not hasattr(self, '_subscription_list_printed'):
            self._subscription_list_printed = True
            print(f"[WS][AGGREGATOR][DEBUG] 完整订阅列表 ({len(token_ids)} 个):")
            for idx, tid in enumerate(token_ids, 1):
                print(f"    [{idx}] {tid[:8]}...{tid[-8:]}")
            print()

        # 更新本地记录
        self._ws_token_ids = token_ids

        # 使用增量订阅客户端
        if self._ws_client is not None:
            # 先取消订阅移除的token
            if removed:
                self._ws_client.unsubscribe(list(removed))
            # 再订阅新增的token
            if added:
                self._ws_client.subscribe(list(added))
        else:
            # 客户端未初始化，需要启动
            if token_ids:
                self._start_ws_subscription(token_ids)
            else:
                print("[WS][AGGREGATOR] 无token需要订阅")

    # 保留旧方法名作为别名，保持兼容性
    def _restart_ws_subscription(self, token_ids: List[str]) -> None:
        """兼容性别名，内部调用 _update_ws_subscription"""
        self._update_ws_subscription(token_ids)

    def _start_ws_subscription(self, token_ids: List[str]) -> None:
        """
        启动WS订阅（使用增量订阅客户端）

        如果客户端已存在且连接正常，只添加新的订阅；
        否则创建新的客户端实例。
        """
        # 验证 WS 模块导入
        try:
            from Volatility_arbitrage_main_ws import WSAggregatorClient
        except Exception as exc:
            print(f"[ERROR] 无法导入 WS 模块: {exc}")
            print("[ERROR] WS 聚合器启动失败，子进程将使用独立 WS 连接")
            _log_error("WS_AGGREGATOR_IMPORT_ERROR", {
                "message": "无法导入 WS 模块",
                "error": str(exc),
                "tokens_count": len(token_ids)
            })
            return

        # 验证 websocket-client 依赖
        try:
            import websocket
        except ImportError:
            print("[ERROR] 缺少依赖 websocket-client")
            print("[ERROR] 请运行: pip install websocket-client")
            print("[ERROR] WS 聚合器启动失败，子进程将使用独立 WS 连接")
            _log_error("WS_AGGREGATOR_DEPENDENCY_ERROR", {
                "message": "缺少依赖 websocket-client",
                "tokens_count": len(token_ids)
            })
            return

        # 如果客户端已存在且运行正常，只添加订阅
        if self._ws_client is not None and self._ws_client.is_connected():
            self._ws_client.subscribe(token_ids)
            print(f"[WS][AGGREGATOR] 增量订阅 {len(token_ids)} 个token（连接保持）")
            return

        # 创建新的增量订阅客户端
        print(f"[WS][AGGREGATOR] 初始化增量订阅客户端...")
        self._ws_client = WSAggregatorClient(
            on_event=self._on_ws_event,
            on_state=self._on_ws_state,
            auth=self._load_ws_auth(),
            verbose=self._ws_debug_raw,
            label="autorun-aggregator",
        )
        self._ws_client.start()

        # 订阅初始token列表
        if token_ids:
            self._ws_client.subscribe(token_ids)

        print(f"[WS][AGGREGATOR] 聚合订阅启动，tokens={len(token_ids)}")
        print(f"[WS][AGGREGATOR] 缓存文件: {self._ws_cache_path}")
        print(f"[WS][AGGREGATOR] ✓ 使用增量订阅模式（避免WS重启导致的数据中断）")

        # 验证客户端是否成功启动
        time.sleep(2)
        if not self._ws_client.is_connected():
            print("[WS][AGGREGATOR] ⚠ WS连接尚未建立，可能正在重试...")
            # 不立即报错，让客户端自动重连
        else:
            stats = self._ws_client.get_stats()
            print(f"[WS][AGGREGATOR] ✓ WS连接正常 (已订阅: {stats.get('subscribed_tokens', 0)} 个token)")

    def _load_ws_auth(self) -> Optional[Dict[str, str]]:
        api_key = os.getenv("POLY_API_KEY")
        api_secret = os.getenv("POLY_API_SECRET")
        api_passphrase = os.getenv("POLY_API_PASSPHRASE") or os.getenv("POLY_API_PASS_PHRASE")
        if api_key and api_secret and api_passphrase:
            return {
                "apiKey": api_key,
                "secret": api_secret,
                "passphrase": api_passphrase,
            }
        return None

    def _on_ws_state(self, state: str, info: Dict[str, Any]) -> None:
        if state != "open":
            return
        connect_count = int(info.get("connect_count", 0) or 0)
        if connect_count <= 1:
            return
        self._refresh_ws_cache_after_reconnect(connect_count)

    def _refresh_ws_cache_after_reconnect(self, connect_count: int) -> None:
        now = time.time()
        refreshed = 0
        with self._ws_cache_lock:
            for token_id in self._ws_token_ids:
                entry = self._ws_cache.get(token_id)
                if not entry:
                    continue
                entry["updated_at"] = now
                refreshed += 1
            if refreshed:
                self._ws_cache_dirty = True
        if refreshed:
            self._flush_ws_cache_if_needed()
            print(
                f"[WS][AGGREGATOR] 重连后缓存回填 {refreshed} 个token "
                f"(connect={connect_count})"
            )

    def _stop_ws_subscription(self) -> None:
        """停止WS订阅"""
        # 停止增量订阅客户端
        if self._ws_client is not None:
            self._ws_client.stop()
            self._ws_client = None

        # 兼容旧的线程方式（如果有）
        if self._ws_thread_stop is not None:
            self._ws_thread_stop.set()
        if self._ws_thread and self._ws_thread.is_alive():
            self._ws_thread.join(timeout=3)
        self._ws_thread = None
        self._ws_thread_stop = None

    def _update_token_timestamp_from_trade(self, ev: Dict[str, Any]) -> None:
        """
        处理 last_trade_price 事件，仅更新时间戳以避免假僵尸token。
        这类事件表明市场有交易活动，即使价格未显著变化。
        """
        # 获取asset_id（可能在不同字段）
        asset_id = ev.get("asset_id") or ev.get("token_id") or ev.get("tokenId")
        if not asset_id:
            return

        token_id = str(asset_id)

        # 仅更新缓存中的时间戳，保留其他字段
        with self._ws_cache_lock:
            if token_id in self._ws_cache:
                # token已存在，仅更新时间戳
                self._ws_cache[token_id]["updated_at"] = time.time()
                self._ws_cache_dirty = True

                # 可选：更新last_trade_price字段
                trade_price = _coerce_float(ev.get("price") or ev.get("last_trade_price"))
                if trade_price is not None and trade_price > 0:
                    self._ws_cache[token_id]["price"] = trade_price

    def _on_ws_event(self, ev: Dict[str, Any]) -> None:
        # 统计事件接收和过滤情况
        if not hasattr(self, '_ws_event_count'):
            self._ws_event_count = 0
            self._ws_filtered_count = 0
            self._ws_filtered_types: Dict[str, int] = {}
            self._ws_last_stats_log = 0.0

        self._ws_event_count += 1

        if self._ws_debug_raw:
            try:
                print(f"[WS][RAW] {json.dumps(ev, ensure_ascii=False)}")
            except Exception:
                print(f"[WS][RAW] {ev}")

        if not isinstance(ev, dict):
            self._ws_filtered_count += 1
            return

        event_type = ev.get("event_type")

        # 处理 price_change 事件（完整更新）
        if event_type == "price_change":
            pcs = ev.get("price_changes", [])
        elif "price_changes" in ev:
            pcs = ev.get("price_changes", [])
        # ✅ 新增：处理 book 和 tick 事件（订单簿更新）
        elif event_type in ("book", "tick"):
            # 尝试从事件中提取价格信息并转换为 price_changes 格式
            asset_id = ev.get("asset_id") or ev.get("token_id")
            if asset_id:
                bid = _coerce_float(ev.get("best_bid") or ev.get("bid"))
                ask = _coerce_float(ev.get("best_ask") or ev.get("ask"))

                # 如果有有效的bid/ask，就构造price_change格式
                if bid or ask:
                    # ✅ 使用mid=(bid+ask)/2作为最可靠的当前价格
                    mid = (bid + ask) / 2.0 if bid and ask else (bid or ask)
                    # ✅ 只使用last_trade_price，不使用price字段（含义不明确）
                    last = _coerce_float(ev.get("last_trade_price"))
                    pcs = [{
                        "asset_id": asset_id,
                        "best_bid": bid,
                        "best_ask": ask,
                        "last_trade_price": last or mid
                    }]
                    # ✅ 调试日志：确认book/tick事件被处理
                    if not hasattr(self, '_book_tick_log_count'):
                        self._book_tick_log_count = 0
                        self._book_tick_last_log = 0.0
                    self._book_tick_log_count += 1
                    now = time.time()
                    if now - self._book_tick_last_log >= 60:
                        print(f"[WS][AGGREGATOR] ✅ 处理book/tick事件: {self._book_tick_log_count} 次/分钟")
                        self._book_tick_last_log = now
                        self._book_tick_log_count = 0
                else:
                    pcs = []
            else:
                pcs = []
        # 处理 last_trade_price 事件（仅更新时间戳，避免假僵尸）
        elif event_type == "last_trade_price":
            self._update_token_timestamp_from_trade(ev)
            return
        else:
            # 记录被过滤的事件类型
            self._ws_filtered_count += 1
            evt_type = event_type or "unknown"
            self._ws_filtered_types[evt_type] = self._ws_filtered_types.get(evt_type, 0) + 1

            # 每60秒打印一次统计
            now = time.time()
            if now - self._ws_last_stats_log >= 60.0:
                print(
                    f"[WS][STATS] 总事件: {self._ws_event_count}, "
                    f"已处理: {self._ws_event_count - self._ws_filtered_count}, "
                    f"已过滤: {self._ws_filtered_count}"
                )
                if self._ws_filtered_types:
                    print(f"[WS][STATS] 过滤事件类型: {self._ws_filtered_types}")
                self._ws_last_stats_log = now
            return
        ts = ev.get("timestamp") or ev.get("ts") or ev.get("time")
        # 确保时间戳总是有效的
        if ts is None:
            ts = time.time()

        status_keys = (
            "status",
            "market_status",
            "marketStatus",
            "is_closed",
            "market_closed",
            "closed",
            "isMarketClosed",
        )
        event_status = {k: ev.get(k) for k in status_keys if k in ev}
        for pc in pcs:
            token_id = str(pc.get("asset_id") or "")
            if not token_id:
                continue

            # ✅ P0修复：只缓存订阅列表中的token
            # Polymarket的market订阅会返回整个市场（YES+NO），需要过滤
            if token_id not in self._ws_token_ids:
                # 静默跳过未订阅的token（可能是配对token）
                if not hasattr(self, '_ws_unsubscribed_tokens'):
                    self._ws_unsubscribed_tokens = set()
                    self._ws_unsubscribed_log_ts = 0.0
                    self._ws_filter_detail_logged = False

                # 只在第一次遇到时记录
                if token_id not in self._ws_unsubscribed_tokens:
                    self._ws_unsubscribed_tokens.add(token_id)

                    # 第一次过滤时，打印详细信息（调试用）
                    if not self._ws_filter_detail_logged:
                        print(f"[WS][FILTER][DEBUG] 发现未订阅的token: {token_id[:8]}...{token_id[-8:]}")
                        print(f"[WS][FILTER][DEBUG] 当前订阅列表 ({len(self._ws_token_ids)} 个):")
                        for sub_tid in list(self._ws_token_ids)[:5]:
                            print(f"    {sub_tid[:8]}...{sub_tid[-8:]}")
                        if len(self._ws_token_ids) > 5:
                            print(f"    ... (还有 {len(self._ws_token_ids) - 5} 个)")
                        self._ws_filter_detail_logged = True

                    # 每5分钟打印一次汇总（避免刷屏）
                    now = time.time()
                    if now - self._ws_unsubscribed_log_ts >= 300:
                        print(f"[WS][FILTER] 过滤未订阅的token（可能是配对token）: {len(self._ws_unsubscribed_tokens)} 个")
                        if len(self._ws_unsubscribed_tokens) <= 5:
                            for utid in self._ws_unsubscribed_tokens:
                                print(f"  - {utid[:8]}...{utid[-8:]}")
                        self._ws_unsubscribed_log_ts = now
                continue

            bid = _coerce_float(pc.get("best_bid")) or 0.0
            ask = _coerce_float(pc.get("best_ask")) or 0.0

            # ✅ 计算mid价格（最可靠的当前市场价格）
            mid = (bid + ask) / 2.0 if bid and ask else (bid or ask or 0.0)

            # ✅ 只使用last_trade_price作为真实成交价，不使用price字段（price含义不明确）
            # price字段可能是历史价格或订单簿深度价格，不适合用作当前价格
            last = _coerce_float(pc.get("last_trade_price"))

            # 如果没有last_trade_price，直接使用mid
            if last is None or last == 0.0:
                last = mid

            # 获取当前序列号并递增（用于去重）
            with self._ws_cache_lock:
                old_data = self._ws_cache.get(token_id, {})
                seq = old_data.get("seq", 0) + 1

            payload = {
                "price": last,
                "best_bid": bid,
                "best_ask": ask,
                "ts": ts,
                "updated_at": time.time(),
                "event_type": ev.get("event_type"),
                "seq": seq,  # 单调递增的序列号
            }
            for key in status_keys:
                val = pc.get(key)
                if val is None:
                    val = event_status.get(key)
                if val is not None:
                    payload[key] = val
            with self._ws_cache_lock:
                self._ws_cache[token_id] = payload
                self._ws_cache_dirty = True

                # 定期打印缓存更新统计（每分钟）
                if not hasattr(self, '_cache_update_log_ts'):
                    self._cache_update_log_ts = 0
                    self._cache_update_count = 0
                self._cache_update_count += 1
                now = time.time()
                if now - self._cache_update_log_ts >= 60:
                    print(f"[WS][AGGREGATOR] 缓存更新统计: {self._cache_update_count} 次/分钟, "
                          f"tokens={len(self._ws_cache)}, last_seq={seq}")
                    self._cache_update_log_ts = now
                    self._cache_update_count = 0

    def _flush_ws_cache_if_needed(self) -> None:
        now = time.time()
        heartbeat_interval = 30.0
        # ✅ 从1.0秒改为0.1秒：提高缓存写入频率，让子进程能更快看到seq更新
        if not self._ws_cache_dirty and now - self._ws_cache_last_flush < heartbeat_interval:
            return
        with self._ws_cache_lock:
            if not self._ws_cache_dirty and now - self._ws_cache_last_flush < heartbeat_interval:
                return
            if (
                not self._ws_cache_dirty
                and self._ws_cache
                and now - self._ws_cache_last_flush >= heartbeat_interval
            ):
                self._ws_cache_dirty = True
            # ✅ 使用深拷贝避免多线程并发修改导致 "dictionary changed size during iteration" 错误
            data = {
                "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "tokens": copy.deepcopy(self._ws_cache),
            }
            self._ws_cache_dirty = False
            self._ws_cache_last_flush = now
        try:
            self._ws_cache_path.parent.mkdir(parents=True, exist_ok=True)

            lock_path = self._ws_cache_path.with_suffix('.lock')
            with lock_path.open("w", encoding="utf-8") as lock_file:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                # 使用原子写入：先写临时文件，再重命名
                tmp_path = self._ws_cache_path.with_suffix('.tmp')
                with tmp_path.open("w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)

                # 原子操作：重命名（在 Unix 系统上是原子的）
                tmp_path.replace(self._ws_cache_path)
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

        except OSError as exc:
            print(f"[ERROR] 写入 WS 聚合缓存失败: {exc}")
            _log_error("WS_CACHE_WRITE_ERROR", {
                "message": "写入 WS 聚合缓存失败",
                "error": str(exc),
                "cache_path": str(self._ws_cache_path),
                "tokens_count": len(self._ws_cache)
            })
            # 清理临时文件
            try:
                tmp_path = self._ws_cache_path.with_suffix('.tmp')
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass

    def _health_check(self) -> None:
        """
        WS 聚合器健康检查（增强版）
        - 分级阈值：正常(10min) / 警告(30min) / 清理(60min)
        - 市场状态检测：自动清理已关闭市场
        - 日志优化：减少刷屏频率
        """
        # 周期性打印缓存状态（每5分钟）
        if not hasattr(self, '_last_cache_status_log'):
            self._last_cache_status_log = 0.0

        now = time.time()
        if now - self._last_cache_status_log >= 300:  # 5分钟
            with self._ws_cache_lock:
                if self._ws_cache:
                    print(f"\n[WS][CACHE_STATUS] 缓存中的token状态 ({len(self._ws_cache)} 个):")
                    for idx, (tid, data) in enumerate(list(self._ws_cache.items())[:10], 1):
                        seq = data.get("seq", 0)
                        bid = data.get("best_bid", 0)
                        ask = data.get("best_ask", 0)
                        updated = data.get("updated_at", 0)
                        age = now - updated if updated > 0 else 0
                        tid_short = f"{tid[:8]}...{tid[-8:]}"
                        print(f"  [{idx}] {tid_short}: seq={seq}, bid={bid}, ask={ask}, age={age:.0f}s")
                    if len(self._ws_cache) > 10:
                        print(f"  ... (还有 {len(self._ws_cache) - 10} 个token)")
                    print()
            self._last_cache_status_log = now

        # 检查 WS 客户端是否运行（优先检查增量订阅客户端）
        if self._ws_token_ids:
            ws_healthy = False
            if self._ws_client is not None:
                ws_healthy = self._ws_client.is_connected()
                # 打印客户端统计信息（每5分钟）
                if now - self._last_cache_status_log < 1:  # 刚打印完缓存状态
                    stats = self._ws_client.get_stats()
                    print(f"[WS][CLIENT] 连接状态: {'✓' if ws_healthy else '✗'}, "
                          f"已订阅: {stats.get('subscribed_tokens', 0)}, "
                          f"待订阅: {stats.get('pending_subscribe', 0)}, "
                          f"连接次数: {stats.get('connect_count', 0)}")
            elif self._ws_thread and self._ws_thread.is_alive():
                ws_healthy = True

            if not ws_healthy:
                print("[WARN] WS 聚合器连接异常，尝试恢复...")
                self._restart_ws_subscription(self._ws_token_ids)

        # 检查缓存数据
        with self._ws_cache_lock:
            token_count = len(self._ws_cache)
            subscribed_count = len(self._ws_token_ids)

            if subscribed_count > 0 and token_count == 0:
                print(f"[WARN] WS 聚合器订阅了 {subscribed_count} 个token，但缓存为空")
                print("[WARN] 可能尚未接收到数据，或连接异常")

            # 分级阈值检查（秒）
            THRESHOLD_WARNING = 1800  # 30分钟 - 警告
            THRESHOLD_CLEANUP_DEFAULT = 1800  # 默认30分钟清理
            THRESHOLD_CLEANUP_EXTENDED = 3600  # 白名单续命后60分钟清理
            RECOVERY_GRACE_WINDOW = 1800  # 最近30分钟内有恢复信号可续命

            now = time.time()
            warning_tokens = []  # 30分钟未更新
            cleanup_tokens = []  # 达到清理阈值（默认30分钟，恢复白名单可至60分钟）或市场已关闭
            closed_market_tokens = []  # 市场已关闭

            for token_id, data in list(self._ws_cache.items()):
                updated_at = data.get("updated_at", 0)
                age = now - updated_at

                # 检查市场状态
                is_closed = (
                    data.get("market_closed")
                    or data.get("is_closed")
                    or data.get("closed")
                )

                was_stale = self._ws_prev_stale_state.get(token_id, False)
                is_stale_now = age > THRESHOLD_WARNING
                if was_stale and not is_stale_now:
                    self._ws_recent_recovery_ts[token_id] = now
                self._ws_prev_stale_state[token_id] = is_stale_now

                recent_recovery_ts = self._ws_recent_recovery_ts.get(token_id, 0.0)
                has_recent_recovery = recent_recovery_ts > 0 and (now - recent_recovery_ts) <= RECOVERY_GRACE_WINDOW
                cleanup_threshold = THRESHOLD_CLEANUP_EXTENDED if has_recent_recovery else THRESHOLD_CLEANUP_DEFAULT

                if is_closed:
                    closed_market_tokens.append((token_id, age))
                    cleanup_tokens.append((token_id, age, "市场已关闭"))
                elif age > cleanup_threshold:
                    if has_recent_recovery:
                        cleanup_tokens.append((token_id, age, f"{age/60:.0f}分钟无更新（恢复白名单已续命至60分钟）"))
                    else:
                        cleanup_tokens.append((token_id, age, f"{age/60:.0f}分钟无更新"))
                elif age > THRESHOLD_WARNING:
                    warning_tokens.append((token_id, age))

            # 清理过期/关闭的token
            if cleanup_tokens:
                for token_id, age, reason in cleanup_tokens:
                    del self._ws_cache[token_id]
                    self._ws_prev_stale_state.pop(token_id, None)
                    self._ws_recent_recovery_ts.pop(token_id, None)
                    # 记录到日志
                    _log_error("TOKEN_CLEANUP", {
                        "token_id": token_id,
                        "age_seconds": age,
                        "reason": reason,
                        "message": "从缓存中清理token"
                    })
                print(f"[CLEANUP] 清理 {len(cleanup_tokens)} 个过期/关闭的token:")
                for token_id, age, reason in cleanup_tokens[:3]:
                    print(f"  - {token_id[:20]}...: {reason}")
                self._ws_cache_dirty = True

            # 警告日志（降低频率：每60秒才打印一次）
            if not hasattr(self, '_last_warning_log'):
                self._last_warning_log = 0

            if warning_tokens and (now - self._last_warning_log >= 60):
                print(f"[HEALTH] {len(warning_tokens)} 个token数据超过30分钟未更新（默认30分钟清理，最近恢复信号可续命至60分钟）")
                # 只显示前2个
                for token_id, age in warning_tokens[:2]:
                    print(f"  - {token_id[:20]}...: {age/60:.0f}分钟前")
                self._last_warning_log = now

        # 检查文件状态
        if self._ws_cache_path.exists():
            try:
                stat = self._ws_cache_path.stat()
                age = time.time() - stat.st_mtime
                if age > 120:  # 2分钟没更新
                    # 降低日志频率
                    if not hasattr(self, '_last_file_warn'):
                        self._last_file_warn = 0
                    if time.time() - self._last_file_warn >= 60:
                        print(f"[WARN] ws_cache.json 文件过期，最后修改: {age:.0f}秒前")
                        self._last_file_warn = time.time()
            except OSError:
                pass
        else:
            if self._ws_token_ids:
                print(f"[WARN] ws_cache.json 文件不存在: {self._ws_cache_path}")

    def _poll_tasks(self) -> None:
        for task in list(self.tasks.values()):
            proc = task.process
            if not proc:
                continue
            rc = proc.poll()
            if rc is None:
                task.status = "running"
                task.last_heartbeat = time.time()
                self._update_log_excerpt(task)
                if self._log_indicates_market_end(task):
                    task.status = "ended"
                    task.no_restart = True
                    task.end_reason = "market closed"
                    task.heartbeat("market end detected from log")
                    print(
                        f"[AUTO] topic={task.topic_id} 日志显示市场已结束，自动结束该话题。"
                    )
                    self._terminate_task(task, reason="market closed (auto)")
                    # 从 copytrade 文件中移除该 token，避免重启后再次加入
                    self._remove_token_from_copytrade_files(task.topic_id)
                continue
            self._handle_process_exit(task, rc)

        self._purge_inactive_tasks()

    def _handle_process_exit(self, task: TopicTask, rc: int) -> None:
        task.process = None
        if task.status not in {"stopped", "exited", "error", "ended"}:
            task.status = "exited" if rc == 0 else "error"
        task.heartbeat(f"process finished rc={rc}")
        self._update_log_excerpt(task)

        # 如果是因 sell signal 退出（包括运行中收到信号和 exit-only cleanup），
        # 仅在子进程退出码为 0 时标记“清仓完成”；
        # 非 0 视作异常，自动补排一次 exit-only cleanup，避免漏清仓。
        if task.end_reason in ("sell signal", "sell signal cleanup"):
            if rc == 0:
                self._completed_exit_cleanup_tokens.add(task.topic_id)
                self._exit_cleanup_retry_counts.pop(task.topic_id, None)
            else:
                self._completed_exit_cleanup_tokens.discard(task.topic_id)
                retry_count = self._exit_cleanup_retry_counts.get(task.topic_id, 0) + 1
                self._exit_cleanup_retry_counts[task.topic_id] = retry_count
                if retry_count <= EXIT_CLEANUP_MAX_RETRIES:
                    if (
                        task.topic_id not in self.pending_exit_topics
                        and task.topic_id not in self.pending_topics
                    ):
                        self.pending_exit_topics.append(task.topic_id)
                    print(
                        "[COPYTRADE][WARN] sell 清仓进程异常退出，已补排 exit-only cleanup: "
                        f"token_id={task.topic_id} rc={rc} "
                        f"retry={retry_count}/{EXIT_CLEANUP_MAX_RETRIES}"
                    )
                else:
                    # 达到最大重试次数，标记为"已完成"阻止 _apply_sell_signals 再次触发，
                    # 避免通过 handled_topics 移除 → 重新检测 → 再重试的无限循环。
                    self._completed_exit_cleanup_tokens.add(task.topic_id)
                    print(
                        "[COPYTRADE][ERROR] sell 清仓连续失败，已停止自动重试避免循环卡死: "
                        f"token_id={task.topic_id} rc={rc} "
                        f"retry={retry_count}/{EXIT_CLEANUP_MAX_RETRIES}"
                    )
            # 从 handled_topics 移除，允许后续 copytrade 再次发出买入信号时重新交易
            self._remove_from_handled_topics(task.topic_id)

        if task.no_restart:
            return

        if rc != 0:
            max_retries = max(0, int(self.config.process_start_retries))
            if task.restart_attempts < max_retries:
                running = sum(1 for t in self.tasks.values() if t.is_running())
                if running >= max(1, int(self.config.max_concurrent_tasks)):
                    self._enqueue_pending_topic(task.topic_id)
                    task.status = "pending"
                    task.heartbeat("restart deferred due to max concurrency")
                    return
                task.restart_attempts += 1
                task.status = "restarting"
                task.heartbeat(
                    f"restart attempt {task.restart_attempts}/{max_retries} after rc={rc}"
                )
                time.sleep(self.config.process_retry_delay_sec)
                if self._start_topic_process(task.topic_id):
                    return
                if task.restart_attempts < max_retries:
                    self._enqueue_pending_topic(task.topic_id)
            task.status = "error"

    def _update_log_excerpt(self, task: TopicTask, max_bytes: int = 2000) -> None:
        now = time.time()
        interval = max(0.0, float(self.config.log_excerpt_interval_sec))
        if interval and now - task.last_log_excerpt_ts < interval:
            return

        if not task.log_path or not task.log_path.exists():
            task.log_excerpt = ""
            return
        try:
            with task.log_path.open("rb") as f:
                f.seek(0, 2)
                size = f.tell()
                f.seek(max(0, size - max_bytes))
                data = f.read().decode("utf-8", errors="ignore")
            lines = data.strip().splitlines()
            task.log_excerpt = "\n".join(lines[-5:])
            task.last_log_excerpt_ts = now
        except OSError as exc:  # pragma: no cover - 文件访问异常
            task.log_excerpt = f"<log read error: {exc}>"

    def _log_indicates_market_end(self, task: TopicTask) -> bool:
        excerpt = (task.log_excerpt or "").lower()
        if not excerpt:
            return False
        patterns = (
            "[market] 已确认市场结束",
            "[market] 市场结束",
            "[market] 达到市场截止时间",
            "[market] 收到市场关闭事件",
            "[exit] 最终状态",
        )
        return any(p.lower() in excerpt for p in patterns)

    def _check_market_closed_before_start(self, topic_id: str) -> bool:
        """
        在启动子进程前检查市场状态。

        返回 True 表示市场已关闭（应跳过），False 表示可以继续启动。
        只有在 WS 缓存中明确标记为 closed 时才返回 True，避免误判。
        """
        cache_data = self._ws_cache.get(topic_id)
        if not cache_data:
            # 缓存中无数据，不能确定市场状态，允许继续（由子进程判断）
            return False

        # 检查市场关闭标记
        is_closed = (
            cache_data.get("market_closed")
            or cache_data.get("is_closed")
            or cache_data.get("closed")
        )

        if is_closed:
            print(
                f"[SCHEDULE] {topic_id[:8]}... 市场已关闭（缓存标记），"
                f"跳过启动并清理"
            )
            # 记录到 exit_tokens，供后续清理
            self._append_exit_token_record(
                topic_id,
                "MARKET_CLOSED",
                exit_data={"detected_at": "schedule_before_start"},
                refillable=False,
            )
            # 从 copytrade 文件中移除
            self._remove_token_from_copytrade_files(topic_id)
            # 从 pending 相关状态中清理
            self._remove_pending_topic(topic_id)
            # 清理可能残留的 task 对象（从恢复阶段创建的）
            self.tasks.pop(topic_id, None)
            return True

        return False

    def _schedule_pending_topics(self) -> None:
        running = sum(1 for t in self.tasks.values() if t.is_running())
        checks_remaining = len(self.pending_topics)
        while (
            self.pending_topics
            and running < max(1, int(self.config.max_concurrent_tasks))
        ):
            if checks_remaining <= 0:
                break
            now = time.time()
            if now < self._next_topic_start_at:
                break
            topic_id = self.pending_topics.pop(0)
            # 启动前检查市场状态，若已关闭则跳过（不重新入队）
            if self._check_market_closed_before_start(topic_id):
                checks_remaining -= 1
                continue
            if topic_id in self.tasks and self.tasks[topic_id].is_running():
                checks_remaining -= 1
                continue

            paused_until = self._shared_ws_paused_until.get(topic_id)
            if paused_until and now < paused_until:
                self._enqueue_pending_topic(topic_id)
                checks_remaining -= 1
                continue

            with self._ws_cache_lock:
                cached = self._ws_cache.get(topic_id)
            if not cached:
                pending_since = self._shared_ws_pending_since.setdefault(topic_id, now)
                waited = now - pending_since
                max_wait = max(1.0, float(self.config.shared_ws_max_pending_wait_sec))
                if waited >= max_wait:
                    failures = self._shared_ws_wait_failures.get(topic_id, 0) + 1
                    self._shared_ws_wait_failures[topic_id] = failures
                    self._shared_ws_pending_since[topic_id] = now
                    max_failures = max(
                        1, int(self.config.shared_ws_wait_failures_before_pause)
                    )
                    print(
                        f"[WS][WAIT] topic={topic_id[:8]}... 等待WS缓存"
                        f" {max_wait:.0f}s超时 ({failures}/{max_failures})"
                    )

                    escalation_window = max(
                        max_wait,
                        float(
                            getattr(
                                self.config,
                                "shared_ws_wait_escalation_window_sec",
                                SHARED_WS_WAIT_ESCALATION_WINDOW_SEC,
                            )
                        ),
                    )
                    min_escalation_failures = max(
                        1,
                        int(
                            getattr(
                                self.config,
                                "shared_ws_wait_escalation_min_failures",
                                SHARED_WS_WAIT_ESCALATION_MIN_FAILURES,
                            )
                        ),
                    )
                    timeout_events = self._shared_ws_wait_timeout_events.get(topic_id, [])
                    timeout_events = [
                        ts for ts in timeout_events if (now - ts) <= escalation_window
                    ]
                    timeout_events.append(now)
                    self._shared_ws_wait_timeout_events[topic_id] = timeout_events

                    should_pause = (
                        failures >= max_failures
                        and len(timeout_events) >= min_escalation_failures
                    )
                    if should_pause:
                        pause_seconds = max(
                            60.0, float(self.config.shared_ws_wait_pause_minutes) * 60.0
                        )
                        self._shared_ws_paused_until[topic_id] = now + pause_seconds
                        self._shared_ws_wait_failures[topic_id] = 0
                        self._shared_ws_wait_timeout_events[topic_id] = []
                        print(
                            f"[WS][PAUSE] topic={topic_id[:8]}... 暂停 {pause_seconds:.0f}s"
                        )
                self._enqueue_pending_topic(topic_id)
                checks_remaining -= 1
                continue
            if topic_id in self._shared_ws_pending_since:
                waited = now - self._shared_ws_pending_since.pop(topic_id)
                self._shared_ws_wait_failures[topic_id] = 0
                self._shared_ws_paused_until.pop(topic_id, None)
                self._shared_ws_wait_timeout_events[topic_id] = []
                print(
                    f"[WS][READY] topic={topic_id[:8]}... 缓存就绪"
                    f" (等待了 {waited:.1f}s)"
                )

            try:
                started = self._start_topic_process(topic_id)
            except Exception as exc:  # pragma: no cover - 防御性保护
                print(f"[ERROR] 调度话题 {topic_id} 时异常: {exc}")
                traceback.print_exc()
                _log_error("TASK_SCHEDULE_ERROR", {
                    "message": "调度话题时异常",
                    "topic_id": topic_id,
                    "error": str(exc),
                    "traceback": traceback.format_exc()
                })
                started = False
            if not started:
                # 启动失败时重新入队，避免话题被遗忘
                self._enqueue_pending_topic(topic_id)
            elif started:
                self._shared_ws_pending_since.pop(topic_id, None)
                self._next_topic_start_at = now + max(
                    0.0, float(self.config.topic_start_cooldown_sec)
                )
            running = sum(1 for t in self.tasks.values() if t.is_running())
            checks_remaining -= 1

    def _schedule_pending_exit_cleanup(self) -> None:
        # 清仓任务使用独立的槽位限制，不受 max_concurrent_tasks 约束
        # 这确保了当普通任务槽位已满时，清仓任务仍能及时执行
        exit_running = sum(
            1 for t in self.tasks.values()
            if t.is_running() and t.end_reason == "sell signal cleanup"
        )
        max_exit_slots = max(1, int(self.config.max_exit_cleanup_tasks))
        deferred: List[str] = []
        while self.pending_exit_topics and exit_running < max_exit_slots:
            token_id = self.pending_exit_topics.pop(0)
            if token_id in self.tasks and self.tasks[token_id].is_running():
                # maker 进程仍在运行，暂时跳过但保留在队列中等下次调度
                deferred.append(token_id)
                continue
            self._start_exit_cleanup(token_id)
            exit_running += 1
        # 将被跳过的 token 放回队列尾部，避免丢失
        if deferred:
            self.pending_exit_topics.extend(deferred)

    def _enqueue_pending_topic(self, topic_id: str) -> None:
        if not topic_id:
            return
        if topic_id not in self.pending_topics:
            self.pending_topics.append(topic_id)
        self._pending_first_seen.setdefault(topic_id, time.time())

    def _remove_pending_topic(self, topic_id: str) -> None:
        if topic_id in self.pending_topics:
            try:
                self.pending_topics.remove(topic_id)
            except ValueError:
                pass
        self._pending_first_seen.pop(topic_id, None)
        self._shared_ws_wait_failures.pop(topic_id, None)
        self._shared_ws_paused_until.pop(topic_id, None)
        self._shared_ws_wait_timeout_events.pop(topic_id, None)
        self._shared_ws_pending_since.pop(topic_id, None)

    def _append_exit_token_record(
        self,
        token_id: str,
        exit_reason: str,
        *,
        exit_data: Optional[Dict[str, Any]] = None,
        refillable: bool = False,
    ) -> None:
        if not token_id:
            return
        records = self._load_exit_tokens()
        if not isinstance(records, list):
            records = []
        records.append(
            {
                "token_id": token_id,
                "exit_ts": time.time(),
                "exit_reason": exit_reason,
                "exit_data": exit_data or {},
                "refillable": refillable,
            }
        )
        try:
            self._exit_tokens_path.parent.mkdir(parents=True, exist_ok=True)
            with self._exit_tokens_path.open("w", encoding="utf-8") as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
        except OSError as exc:  # pragma: no cover - 文件系统异常
            print(f"[WARN] 写入 exit_tokens.json 失败: {exc}")

    def _evict_stale_pending_topics(self) -> None:
        if not self.config.enable_pending_soft_eviction:
            return
        if not self.pending_topics:
            return

        now = time.time()
        cutoff_sec = max(0.0, float(self.config.pending_soft_eviction_minutes)) * 60.0
        if cutoff_sec <= 0:
            return

        evicted: List[str] = []
        for topic_id in list(self.pending_topics):
            first_seen = self._pending_first_seen.get(topic_id, now)
            if now - first_seen < cutoff_sec:
                continue
            with self._ws_cache_lock:
                cached = self._ws_cache.get(topic_id)
            if cached:
                updated_at = cached.get("updated_at", 0)
                if updated_at and (now - float(updated_at)) < cutoff_sec:
                    continue
                cache_age = now - float(updated_at) if updated_at else None
            else:
                cache_age = None

            exit_data = {
                "pending_age_sec": round(now - first_seen, 1),
                "cache_age_sec": round(cache_age, 1) if cache_age is not None else None,
            }
            self._remove_pending_topic(topic_id)
            # NO_DATA_TIMEOUT 允许回填，但在 _filter_refillable_tokens 中有更严格的次数限制
            self._append_exit_token_record(
                topic_id,
                "NO_DATA_TIMEOUT",
                exit_data=exit_data,
                refillable=True,
            )
            evicted.append(topic_id)

        if evicted:
            print(
                f"[PENDING] 软淘汰 {len(evicted)} 个token（无缓存/长期未更新）"
            )

    def _get_order_base_volume(self) -> Optional[float]:
        return None

    def _build_run_config(self, topic_id: str) -> Dict[str, Any]:
        base_template_raw = json.loads(json.dumps(self.run_params_template or {}))
        base_template = {k: v for k, v in base_template_raw.items() if v is not None}

        base_raw = self.strategy_defaults.get("default", {}) or {}
        base = {k: v for k, v in base_raw.items() if v is not None}

        topic_overrides_raw = (self.strategy_defaults.get("topics") or {}).get(
            topic_id, {}
        )
        topic_overrides = {
            k: v for k, v in topic_overrides_raw.items() if v is not None
        }

        merged = {**base_template, **base, **topic_overrides}

        topic_info = self.topic_details.get(topic_id, {})
        slug = topic_info.get("slug")
        if slug:
            merged["market_url"] = f"https://polymarket.com/market/{slug}"
        merged["topic_id"] = topic_id

        if topic_info.get("title"):
            merged["topic_name"] = topic_info.get("title")
        if topic_info.get("token_id"):
            merged["token_id"] = topic_info.get("token_id")
        if not merged.get("token_id"):
            merged["token_id"] = topic_id
        merged["exit_signal_path"] = str(self._exit_signal_path(topic_id))
        if topic_info.get("yes_token"):
            merged["yes_token"] = topic_info.get("yes_token")
        if topic_info.get("no_token"):
            merged["no_token"] = topic_info.get("no_token")
        if topic_info.get("end_time"):
            merged["end_time"] = topic_info.get("end_time")

        base_order_size = _coerce_float(merged.get("order_size"))
        total_volume = _coerce_float(topic_info.get("total_volume"))
        volume_growth_factor = _coerce_float(merged.get("volume_growth_factor"))
        if base_order_size is not None and total_volume is not None:
            scaled_size = _scale_order_size_by_volume(
                base_order_size,
                total_volume,
                base_volume=self._get_order_base_volume(),
                growth_factor=volume_growth_factor
                if volume_growth_factor is not None and volume_growth_factor > 0
                else 0.5,
            )
            merged["order_size"] = scaled_size

        # Slot refill (回填) 恢复状态
        resume_state = topic_info.get("resume_state")
        if resume_state:
            merged["resume_state"] = resume_state
            refill_retry_count = topic_info.get("refill_retry_count", 0)
            merged["refill_retry_count"] = refill_retry_count

        # Maker 子进程配置（从 global_config 传递）
        merged["maker_poll_sec"] = self.config.maker_poll_sec
        merged["maker_position_sync_interval"] = self.config.maker_position_sync_interval

        return merged

    def _start_topic_process(self, topic_id: str) -> bool:
        config_data = self._build_run_config(topic_id)
        cfg_path = self.config.data_dir / f"run_params_{_safe_topic_filename(topic_id)}.json"
        _dump_json_file(cfg_path, config_data)

        log_path = self.config.log_dir / f"autorun_{_safe_topic_filename(topic_id)}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            log_file = log_path.open("a", encoding="utf-8")
        except OSError as exc:  # pragma: no cover - 文件系统异常
            print(f"[ERROR] 无法创建日志文件 {log_path}: {exc}")
            return False

        max_stagger = max(0.0, float(self.config.process_stagger_max_sec))
        if max_stagger > 0:
            delay = random.uniform(0, max_stagger)
            if delay > 0:
                print(
                    f"[SCHEDULE] topic={topic_id} 启动前随机延迟 {delay:.2f}s 以错峰运行"
                )
                time.sleep(delay)

        # 构建命令行参数（不再使用环境变量）
        cmd = [
            sys.executable,
            str(MAKER_ROOT / "Volatility_arbitrage_run.py"),
            str(cfg_path),
        ]

        # 基于缓存新鲜度判断是否使用共享 WS 模式
        # 启用调试输出（首次子进程启动时）
        if not hasattr(self, '_first_child_started'):
            self._debug_shared_ws_check = True
            self._first_child_started = True

        with self._ws_cache_lock:
            cached = self._ws_cache.get(topic_id)
        if not cached:
            print(
                f"[WS][WAIT] topic={topic_id[:8]}... 缓存未就绪，启动跳过（等待调度重试）"
            )
            return False

        should_use_shared = True

        # 禁用调试输出（避免刷屏）
        if hasattr(self, '_debug_shared_ws_check'):
            delattr(self, '_debug_shared_ws_check')

        if should_use_shared:
            # 通过命令行参数传递共享缓存路径
            cmd.append(f"--shared-ws-cache={self._ws_cache_path}")
            print(f"[WS][CHILD] topic={topic_id[:8]}... → 共享 WS 模式 ✓")

        proc: Optional[subprocess.Popen] = None
        attempts = max(1, int(self.config.process_start_retries))
        env = os.environ.copy()
        for attempt in range(1, attempts + 1):
            try:
                proc = subprocess.Popen(
                    cmd,
                    stdin=subprocess.DEVNULL,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                    env=env,
                )
                log_file.close()
                break
            except Exception as exc:  # pragma: no cover - 子进程异常
                print(
                    f"[ERROR] 启动 topic={topic_id} 失败（尝试 {attempt}/{attempts}）: {exc}"
                )
                if attempt >= attempts:
                    log_file.close()
                    return False
                time.sleep(self.config.process_retry_delay_sec)

        if not proc:
            print(f"[ERROR] topic={topic_id} 启动失败：proc is None")
            return False

        # 启动后给子进程一个极短的引导窗口，避免“刚启动即退出”被误判为运行中。
        bootstrap_wait = min(1.0, max(0.0, float(self.config.command_poll_sec)))
        if bootstrap_wait > 0:
            time.sleep(bootstrap_wait)

        early_rc = proc.poll()
        if early_rc is not None:
            early_tail = ""
            try:
                if log_path.exists():
                    with log_path.open("rb") as lf:
                        lf.seek(0, os.SEEK_END)
                        size = lf.tell()
                        lf.seek(max(0, size - 1500), os.SEEK_SET)
                        early_tail = lf.read().decode("utf-8", errors="ignore").strip()
            except Exception:
                early_tail = ""
            tail_text = (early_tail.splitlines() or ["-"])[-1].strip() if early_tail else "-"
            print(
                f"[ERROR] topic={topic_id} 启动后立即退出 rc={early_rc} tail={tail_text}，将重试"
            )
            return False

        task = self.tasks.get(topic_id) or TopicTask(topic_id=topic_id)
        task.process = proc
        task.config_path = cfg_path
        task.log_path = log_path
        task.status = "running"
        task.heartbeat("started")
        self.tasks[topic_id] = task
        self._update_handled_topics([topic_id])
        # 启动成功后，从回填标记中移除（允许后续再次回填）
        self._refilled_tokens.discard(topic_id)
        # 从 sell 清仓状态中移除，确保新一轮交易的 sell 信号能被正常处理
        self._completed_exit_cleanup_tokens.discard(topic_id)
        self._handled_sell_signals.discard(topic_id)
        self._exit_cleanup_retry_counts.pop(topic_id, None)
        # 检查是否是回填启动。
        # 注意：无持仓回填时 resume_state 可能为 None，不能仅靠 resume_state 判断。
        detail = self.topic_details.get(topic_id) or {}
        is_refill_start = bool(
            detail.get("refill_exit_reason")
            or detail.get("refill_retry_count", 0)
            or detail.get("resume_state") is not None
        )
        # 只有非回填启动（新交易周期）时才重置回填重试计数；
        # 回填启动时保留计数，确保 PRICE_NONE_STREAK/NO_DATA_TIMEOUT 的重试限制生效。
        if not is_refill_start:
            self._refill_retry_counts.pop(topic_id, None)
        # 清理 topic_details 中的 resume_state（已被使用）
        if topic_id in self.topic_details:
            self.topic_details[topic_id].pop("resume_state", None)
            self.topic_details[topic_id].pop("refill_retry_count", None)
        print(f"[START] topic={topic_id} pid={proc.pid} log={log_path}")
        return True

    def _start_exit_cleanup(self, token_id: str) -> None:
        task = self.tasks.get(token_id)
        if task and task.is_running():
            return
        if token_id in self.pending_topics:
            self._remove_pending_topic(token_id)
        if token_id in self.pending_exit_topics:
            try:
                self.pending_exit_topics.remove(token_id)
            except ValueError:
                pass

        config_data = self._build_run_config(token_id)
        config_data["exit_only"] = True
        config_data["token_id"] = config_data.get("token_id") or token_id
        cfg_path = self.config.data_dir / f"run_params_{_safe_topic_filename(token_id)}.json"
        _dump_json_file(cfg_path, config_data)

        log_path = self.config.log_dir / f"autorun_exit_{_safe_topic_filename(token_id)}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            log_file = log_path.open("a", encoding="utf-8")
        except OSError as exc:  # pragma: no cover - 文件系统异常
            print(f"[ERROR] 无法创建清仓日志文件 {log_path}: {exc}")
            return

        # 构建命令行参数（不再使用环境变量）
        cmd = [
            sys.executable,
            str(MAKER_ROOT / "Volatility_arbitrage_run.py"),
            str(cfg_path),
        ]

        # 清仓进程固定使用共享缓存路径（不启用独立 WS 模式）
        cmd.append(f"--shared-ws-cache={self._ws_cache_path}")
        print(f"[WS] 清仓进程将使用共享 WS 模式")

        env = os.environ.copy()
        try:
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                env=env,
            )
        except Exception as exc:  # pragma: no cover - 子进程异常
            log_file.close()
            print(f"[ERROR] 启动清仓进程失败 token={token_id}: {exc}")
            return
        log_file.close()

        task = task or TopicTask(topic_id=token_id)
        task.process = proc
        task.config_path = cfg_path
        task.log_path = log_path
        task.status = "running"
        task.no_restart = True
        task.end_reason = "sell signal cleanup"
        task.heartbeat("sell signal cleanup started")
        self.tasks[token_id] = task
        self._update_handled_topics([token_id])
        print(f"[EXIT-CLEAN] token={token_id} pid={proc.pid} log={log_path}")

    # ========== 历史记录 ==========
    def _load_handled_topics(self) -> None:
        self.handled_topics = read_handled_topics(self.config.handled_topics_path)
        if self.handled_topics:
            preview = ", ".join(sorted(self.handled_topics)[:5])
            print(
                f"[INIT] 已加载历史话题 {len(self.handled_topics)} 个 preview={preview}"
            )
        else:
            print("[INIT] 尚无历史处理话题记录")

    def _update_handled_topics(self, new_topics: List[str]) -> None:
        if not new_topics:
            return
        self.handled_topics.update(new_topics)
        write_handled_topics(self.config.handled_topics_path, self.handled_topics)

    def _remove_from_handled_topics(self, token_id: str) -> None:
        """
        从 handled_topics 中移除指定 token，允许后续重新交易。

        用于：当 token 完成一个完整的交易周期（买入→卖出清仓）后，
        从 handled_topics 中移除，这样 copytrade 再次发出买入信号时可以重新触发交易。
        """
        if not token_id:
            return
        if token_id in self.handled_topics:
            self.handled_topics.discard(token_id)
            write_handled_topics(self.config.handled_topics_path, self.handled_topics)
            print(f"[HANDLED] 已从 handled_topics 移除 token={token_id[:16]}...（允许后续重新交易）")

    # ========== 命令处理 ==========
    def enqueue_command(self, command: str) -> None:
        self.command_queue.put(command)

    def _process_commands(self) -> None:
        while True:
            try:
                cmd = self.command_queue.get_nowait()
            except queue.Empty:
                break
            print(f"[CMD] processing: {cmd}")
            self._handle_command(cmd.strip())

    def _handle_command(self, cmd: str) -> None:
        if not cmd:
            print("[CMD] 忽略空命令（可能未正确捕获输入或输入仅为空白）")
            return
        if cmd in {"quit", "exit"}:
            print("[CHOICE] exit requested")
            self.stop_event.set()
            return
        if cmd == "list":
            self._print_status()
            return
        if cmd.startswith("stop "):
            _, topic_id = cmd.split(" ", 1)
            self._stop_topic(topic_id.strip())
            return
        if cmd == "refresh":
            self._refresh_topics()
            return
        print(f"[WARN] 未识别命令: {cmd}")

    def _print_status(self) -> None:
        if not self.tasks:
            print("[RUN] 当前无运行中的话题")
            return
        running_tasks = self._ordered_running_tasks()
        if not running_tasks:
            print("[RUN] 当前无运行中的话题")
            return

        for idx, task in enumerate(running_tasks, 1):
            self._print_single_task(task, idx)

    def _print_single_task(self, task: TopicTask, index: Optional[int] = None) -> None:
        hb = task.last_heartbeat
        hb_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(hb)) if hb else "-"
        pid_text = str(task.process.pid) if task.process else "-"
        log_name = task.log_path.name if task.log_path else "-"
        log_hint = (task.log_excerpt.splitlines() or ["-"])[-1].strip()

        prefix = f"[RUN {index}]" if index is not None else "[RUN]"
        print(
            f"{prefix} topic={task.topic_id} status={task.status} "
            f"start={time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(task.start_time))} "
            f"pid={pid_text} hb={hb_text} notes={len(task.notes)} "
            f"log={log_name} last_line={log_hint or '-'}"
        )

    def _ordered_running_tasks(self) -> List[TopicTask]:
        return sorted(
            [task for task in self.tasks.values() if task.is_running()],
            key=lambda t: (t.start_time, t.topic_id),
        )

    def _stop_topic(self, topic_or_index: str) -> None:
        topic_id = self._resolve_topic_identifier(topic_or_index)
        if not topic_id:
            return
        task = self.tasks.get(topic_id)
        if not task:
            print(f"[WARN] topic {topic_id} 不在运行列表中")
            return
        task.no_restart = True
        task.end_reason = "stopped by user"
        # 标记为已处理，避免后续 refresh 把同一话题再次入队
        if topic_id not in self.handled_topics:
            self.handled_topics.add(topic_id)
            write_handled_topics(self.config.handled_topics_path, self.handled_topics)
        if topic_id in self.pending_topics:
            self._remove_pending_topic(topic_id)
        self._terminate_task(task, reason="stopped by user")
        self._purge_inactive_tasks()
        print(f"[CHOICE] stop topic={topic_id}")

    def _resolve_topic_identifier(self, text: str) -> Optional[str]:
        text = text.strip()
        if not text:
            print("[WARN] stop 命令缺少参数")
            return None
        if text.isdigit():
            index = int(text)
            running_tasks = self._ordered_running_tasks()
            if 1 <= index <= len(running_tasks):
                return running_tasks[index - 1].topic_id
            print(
                f"[WARN] 无效的序号 {index}，当前运行中的任务数为 {len(running_tasks)}"
            )
            return None
        return text

    def _terminate_task(self, task: TopicTask, reason: str) -> None:
        proc = task.process
        if proc and proc.poll() is None:
            try:
                proc.terminate()
            except Exception as exc:  # pragma: no cover - 终止异常
                print(f"[WARN] 无法终止 topic {task.topic_id}: {exc}")
            try:
                proc.wait(timeout=self.config.process_graceful_timeout_sec)
            except subprocess.TimeoutExpired:
                try:
                    proc.kill()
                    proc.wait(timeout=1.0)
                except Exception as exc:  # pragma: no cover - kill 失败
                    print(f"[WARN] 无法强杀 topic {task.topic_id}: {exc}")
        if task.status not in {"error", "ended"}:
            task.status = "stopped"
        task.heartbeat(reason)

    def _purge_inactive_tasks(self) -> None:
        """移除已停止/结束且不再需要展示的任务。

        注意：如果 topic 仍在 pending_topics 或 pending_exit_topics 中等待重新调度，
        则保留 task 对象（避免丢失待重启的 token）。
        """

        removable: List[str] = []
        for topic_id, task in list(self.tasks.items()):
            if task.is_running():
                continue
            if task.status in {"stopped", "ended", "exited", "error"} or task.no_restart:
                # 如果 token 仍在等待队列中，不要 purge（否则会丢失重试机会）
                if topic_id in self.pending_topics or topic_id in self.pending_exit_topics:
                    continue
                removable.append(topic_id)

        if not removable:
            return

        for topic_id in removable:
            self.tasks.pop(topic_id, None)

    # ========== 市场关闭时自动清理 token ==========
    def _remove_token_from_copytrade_files(self, token_id: str) -> None:
        """
        从 copytrade JSON 文件中移除指定的 token，避免重启后再次加入队列。

        会从以下文件中移除：
        - tokens_from_copytrade.json
        - copytrade_sell_signals.json
        """
        if not token_id:
            return

        # 1. 从 tokens_from_copytrade.json 移除
        tokens_path = self.config.copytrade_tokens_path
        if tokens_path.exists():
            try:
                with tokens_path.open("r", encoding="utf-8") as f:
                    data = json.load(f)

                if isinstance(data, dict) and "tokens" in data:
                    original_count = len(data["tokens"])
                    data["tokens"] = [
                        t for t in data["tokens"]
                        if _topic_id_from_entry(t) != token_id
                    ]
                    removed_count = original_count - len(data["tokens"])

                    if removed_count > 0:
                        data["updated_at"] = time.strftime(
                            "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
                        )
                        _atomic_json_write(tokens_path, data)
                        print(
                            f"[CLEANUP] 已从 tokens_from_copytrade.json 移除 token={token_id[:20]}..."
                        )
            except (json.JSONDecodeError, OSError) as exc:
                print(f"[CLEANUP] 更新 tokens_from_copytrade.json 失败: {exc}")

        # 2. 从 copytrade_sell_signals.json 移除
        signals_path = self.config.copytrade_sell_signals_path
        if signals_path.exists():
            try:
                with signals_path.open("r", encoding="utf-8") as f:
                    data = json.load(f)

                if isinstance(data, dict) and "sell_tokens" in data:
                    original_count = len(data["sell_tokens"])
                    data["sell_tokens"] = [
                        t for t in data["sell_tokens"]
                        if _topic_id_from_entry(t) != token_id
                    ]
                    removed_count = original_count - len(data["sell_tokens"])

                    if removed_count > 0:
                        data["updated_at"] = time.strftime(
                            "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
                        )
                        _atomic_json_write(signals_path, data)
                        print(
                            f"[CLEANUP] 已从 copytrade_sell_signals.json 移除 token={token_id[:20]}..."
                        )
            except (json.JSONDecodeError, OSError) as exc:
                print(f"[CLEANUP] 更新 copytrade_sell_signals.json 失败: {exc}")

        # 3. 从内存队列中移除
        if token_id in self.pending_topics:
            self._remove_pending_topic(token_id)
            print(f"[CLEANUP] 已从 pending_topics 移除 token={token_id[:20]}...")

        if token_id in self.pending_exit_topics:
            try:
                self.pending_exit_topics.remove(token_id)
                print(f"[CLEANUP] 已从 pending_exit_topics 移除 token={token_id[:20]}...")
            except ValueError:
                pass

        # 4. 从 topic_details 中移除
        if token_id in self.topic_details:
            self.topic_details.pop(token_id, None)

        # 5. 从 latest_topics 中移除（latest_topics 是 List[Dict]，需按 token_id 字段过滤）
        original_len = len(self.latest_topics)
        self.latest_topics = [
            entry for entry in self.latest_topics
            if _topic_id_from_entry(entry) != token_id
        ]
        if len(self.latest_topics) < original_len:
            print(f"[CLEANUP] 已从 latest_topics 内存中移除 token={token_id[:20]}...")

    # ========== Slot Refill (回填) 逻辑 ==========
    def _load_exit_tokens(self) -> List[Dict[str, Any]]:
        """
        加载退出token记录文件。

        Returns:
            退出token记录列表
        """
        if not self._exit_tokens_path.exists():
            if self._refill_debug:
                print(f"[REFILL][DEBUG] exit_tokens.json 不存在: {self._exit_tokens_path}")
            return []
        try:
            with self._exit_tokens_path.open("r", encoding="utf-8") as f:
                records = json.load(f)
            if not isinstance(records, list):
                return []
            if self._refill_debug:
                print(f"[REFILL][DEBUG] 读取 exit_tokens 记录数: {len(records)}")
            return records
        except (json.JSONDecodeError, OSError) as exc:
            print(f"[REFILL] 读取退出token记录失败: {exc}")
            return []

    def _cleanup_closed_market_tokens(self) -> None:
        """
        清理 exit_tokens.json 中因 MARKET_CLOSED 退出的 token。
        从 copytrade JSON 文件中移除这些 token，避免重启后再次加入队列。
        """
        exit_records = self._load_exit_tokens()
        if not exit_records:
            return

        cleaned_tokens: List[str] = []
        latest_topic_ids = {
            _topic_id_from_entry(item)
            for item in self.latest_topics
            if _topic_id_from_entry(item)
        }
        for record in exit_records:
            token_id = record.get("token_id")
            exit_reason = record.get("exit_reason", "")

            if not token_id:
                continue

            # 只处理 MARKET_CLOSED 退出原因
            if exit_reason != "MARKET_CLOSED":
                continue

            # 检查是否仍在 copytrade 文件中（避免重复清理）
            if token_id not in latest_topic_ids:
                continue

            # 从 copytrade 文件中移除
            self._remove_token_from_copytrade_files(token_id)
            cleaned_tokens.append(token_id)

        if cleaned_tokens:
            print(
                f"[CLEANUP] 已清理 {len(cleaned_tokens)} 个市场关闭的 token"
            )

    def _should_refill_slots(self) -> bool:
        """
        判断是否需要回填maker slot。

        条件：
        1. 回填功能已启用
        2. 当前运行的任务数 < max_concurrent_tasks

        Returns:
            True 表示需要回填
        """
        if not self.config.enable_slot_refill:
            if self._refill_debug:
                print("[REFILL][DEBUG] 回填已关闭 enable_slot_refill=False")
            return False

        running = sum(1 for t in self.tasks.values() if t.is_running())
        max_slots = max(1, int(self.config.max_concurrent_tasks))
        pending = len(self.pending_topics)
        has_capacity = running < max_slots
        if self._refill_debug:
            print(
                "[REFILL][DEBUG] slot检查: "
                f"running={running} pending={pending} max={max_slots} "
                f"has_capacity={has_capacity}"
            )
        return has_capacity

    def _filter_refillable_tokens(
        self, exit_records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        筛选可回填的token。

        筛选条件：
        1. 退出原因可重试（非 MARKET_CLOSED）
        2. 超过冷却时间
        3. 未超过最大重试次数
        4. 不在当前运行或pending列表中
        5. 未被标记为不可回填

        Args:
            exit_records: 退出token记录列表

        Returns:
            可回填的token记录列表（按优先级排序）
        """
        # 不可重试的退出原因
        NON_RETRYABLE_REASONS = {
            "MARKET_CLOSED",
            "USER_STOPPED",
            "DEADLINE_REACHED",
        }

        # 达到重试上限后永久不再回填的退出原因（防止噪声循环）
        PERMANENT_AFTER_MAX_RETRIES_REASONS = {
            "PRICE_NONE_STREAK",
        }

        now = time.time()
        cooldown_seconds = self.config.refill_cooldown_minutes * 60.0
        max_retries = self.config.max_refill_retries

        refillable: List[Dict[str, Any]] = []
        seen_tokens: set[str] = set()  # 避免重复
        skip_stats: Dict[str, int] = {}

        # 按退出时间倒序处理（最新的优先，避免处理过期记录）
        sorted_records = sorted(
            exit_records,
            key=lambda r: r.get("exit_ts", 0),
            reverse=True,
        )

        for record in sorted_records:
            token_id = record.get("token_id")
            if not token_id:
                skip_stats["missing_token_id"] = skip_stats.get("missing_token_id", 0) + 1
                continue

            # 去重：同一个token只取最新记录
            if token_id in seen_tokens:
                skip_stats["duplicate_token"] = skip_stats.get("duplicate_token", 0) + 1
                continue
            seen_tokens.add(token_id)

            exit_reason = record.get("exit_reason", "")
            exit_ts = record.get("exit_ts", 0)
            refillable_flag = record.get("refillable", True)

            # 检查是否可重试
            if exit_reason in NON_RETRYABLE_REASONS:
                skip_stats["non_retryable"] = skip_stats.get("non_retryable", 0) + 1
                continue

            # 检查是否标记为不可回填
            if not refillable_flag:
                skip_stats["not_refillable"] = skip_stats.get("not_refillable", 0) + 1
                continue

            # 检查冷却时间（exit_ts 到现在的时间间隔）
            if exit_ts > 0 and (now - exit_ts) < cooldown_seconds:
                skip_stats["cooldown"] = skip_stats.get("cooldown", 0) + 1
                continue

            # 检查重试次数（按退出原因分级）
            # - NO_DATA_TIMEOUT: 最多1次，避免低活跃 token 反复回填
            # - SHARED_WS_UNAVAILABLE: 视为基础设施瞬态故障，不设置硬上限
            retry_count = self._refill_retry_counts.get(token_id, 0)
            effective_max_retries = max_retries
            if exit_reason == "NO_DATA_TIMEOUT":
                effective_max_retries = 1
            elif exit_reason == "SHARED_WS_UNAVAILABLE":
                effective_max_retries = 10**9
            if retry_count >= effective_max_retries:
                skip_stats["max_retries"] = skip_stats.get("max_retries", 0) + 1
                if exit_reason in PERMANENT_AFTER_MAX_RETRIES_REASONS:
                    skip_stats["permanent_block"] = skip_stats.get("permanent_block", 0) + 1
                continue

            # 检查是否已在运行或pending
            if token_id in self.tasks and self.tasks[token_id].is_running():
                skip_stats["already_running"] = skip_stats.get("already_running", 0) + 1
                continue
            if token_id in self.pending_topics:
                skip_stats["already_pending"] = skip_stats.get("already_pending", 0) + 1
                continue
            if token_id in self.pending_exit_topics:
                skip_stats["pending_exit"] = skip_stats.get("pending_exit", 0) + 1
                continue

            # 检查是否最近已被回填过（避免短时间内重复回填）
            if token_id in self._refilled_tokens:
                skip_stats["recent_refilled"] = skip_stats.get("recent_refilled", 0) + 1
                continue

            refillable.append(record)

        # 排序优先级：
        # 1. 有持仓的优先（需要尽快卖出）
        # 2. 退出时间早的优先（等待时间长）
        def _sort_key(r: Dict[str, Any]) -> tuple:
            exit_data = r.get("exit_data", {}) or {}
            has_position = exit_data.get("has_position", False)
            exit_ts = r.get("exit_ts", 0)
            # has_position=True 排前面（0 < 1）
            # exit_ts 小的排前面（早退出的优先）
            return (0 if has_position else 1, exit_ts)

        refillable.sort(key=_sort_key)
        if self._refill_debug:
            print(
                "[REFILL][DEBUG] 过滤结果: "
                f"exit_records={len(exit_records)} refillable={len(refillable)} "
                f"cooldown_sec={int(cooldown_seconds)} max_retries={max_retries}"
            )
            if skip_stats:
                print(f"[REFILL][DEBUG] 跳过统计: {skip_stats}")

        return refillable

    def _schedule_refill(self) -> None:
        """
        执行回填调度：从退出记录中选取可回填的token重新加入pending队列。
        """
        if not self._should_refill_slots():
            if self._refill_debug:
                running = sum(1 for t in self.tasks.values() if t.is_running())
                print(
                    "[REFILL][DEBUG] slot不足，跳过回填: "
                    f"running={running} pending={len(self.pending_topics)} "
                    f"max={self.config.max_concurrent_tasks}"
                )
            return

        exit_records = self._load_exit_tokens()
        if not exit_records:
            print("[REFILL] 无退出记录，跳过回填")
            return

        refillable = self._filter_refillable_tokens(exit_records)
        if not refillable:
            running = sum(1 for t in self.tasks.values() if t.is_running())
            print(
                "[REFILL] 无可回填token，记录="
                f"{len(exit_records)} 运行={running} pending={len(self.pending_topics)}"
            )
            return

        # 计算可用slot数（仅用于日志展示）
        running = sum(1 for t in self.tasks.values() if t.is_running())
        available_slots = max(0, self.config.max_concurrent_tasks - running)

        # 回填加入 pending 不受 slot 限制，由 running 控制实际子进程数量
        to_refill = refillable

        print(f"\n[REFILL] ========== Slot回填检查 ==========")
        print(f"[REFILL] 当前运行: {running}/{self.config.max_concurrent_tasks}")
        print(f"[REFILL] 可用slot: {available_slots}")
        print(f"[REFILL] 可回填token: {len(refillable)} 个")

        for record in to_refill:
            token_id = record.get("token_id")
            exit_reason = record.get("exit_reason", "UNKNOWN")
            exit_data = record.get("exit_data", {}) or {}
            has_position = exit_data.get("has_position", False)

            # 增加重试计数
            self._refill_retry_counts[token_id] = self._refill_retry_counts.get(token_id, 0) + 1
            retry_count = self._refill_retry_counts[token_id]

            # 标记为已回填
            self._refilled_tokens.add(token_id)

            # 构建恢复配置
            resume_state = None
            if has_position:
                resume_state = {
                    "has_position": True,
                    "position_size": exit_data.get("position_size"),
                    "entry_price": exit_data.get("entry_price"),
                    "skip_buy": True,  # 跳过买入阶段
                }

            # 保存恢复状态到 topic_details（供 _build_run_config 使用）
            if token_id not in self.topic_details:
                self.topic_details[token_id] = {}
            self.topic_details[token_id]["resume_state"] = resume_state
            self.topic_details[token_id]["refill_retry_count"] = retry_count
            self.topic_details[token_id]["refill_exit_reason"] = exit_reason

            # 加入pending队列
            self._enqueue_pending_topic(token_id)

            state_hint = "有持仓→等待卖出" if has_position else "无持仓→等待买入"
            print(
                f"[REFILL] + 回填 token={token_id[:20]}... "
                f"原因={exit_reason} 状态={state_hint} "
                f"重试={retry_count}/{self.config.max_refill_retries}"
            )

        print(f"[REFILL] 已添加 {len(to_refill)} 个token到pending队列")
        print(f"[REFILL] ========================================\n")

    def _refresh_topics(self) -> None:
        try:
            self.latest_topics = self._load_copytrade_tokens()
            # 保留已有的 resume_state（回填恢复状态），只更新 copytrade 数据
            old_resume_states: Dict[str, Any] = {}
            for tid, detail in self.topic_details.items():
                if detail.get("resume_state") or detail.get("refill_exit_reason"):
                    old_resume_states[tid] = {
                        "resume_state": detail.get("resume_state"),
                        "refill_retry_count": detail.get("refill_retry_count", 0),
                        "refill_exit_reason": detail.get("refill_exit_reason"),
                    }
            self.topic_details = {}
            for item in self.latest_topics:
                topic_id = _topic_id_from_entry(item)
                if not topic_id:
                    continue
                detail = dict(item)
                detail.setdefault("topic_id", topic_id)
                self.topic_details[topic_id] = detail
            # 恢复之前保存的 resume_state（回填恢复状态）
            for tid, saved in old_resume_states.items():
                if tid not in self.topic_details:
                    self.topic_details[tid] = {}
                self.topic_details[tid]["resume_state"] = saved.get("resume_state")
                self.topic_details[tid]["refill_retry_count"] = saved.get("refill_retry_count", 0)
                if saved.get("refill_exit_reason"):
                    self.topic_details[tid]["refill_exit_reason"] = saved["refill_exit_reason"]

            sell_signals = self._load_copytrade_sell_signals()
            self._apply_sell_signals(sell_signals)
            new_topics = [
                topic_id
                for topic_id in compute_new_topics(self.latest_topics, self.handled_topics)
                if topic_id not in sell_signals
            ]
            if new_topics:
                preview = ", ".join(new_topics[:5])
                print(
                    f"[INCR] 新话题 {len(new_topics)} 个，将更新历史记录 preview={preview}"
                )
                added_topics: List[str] = []
                for topic_id in new_topics:
                    if topic_id in self.pending_topics:
                        continue
                    if topic_id in self.tasks and self.tasks[topic_id].is_running():
                        continue
                    self._enqueue_pending_topic(topic_id)
                    added_topics.append(topic_id)
                # 立即标记为已处理，防止下次轮询时重复检测到同一 token
                if added_topics:
                    self._update_handled_topics(added_topics)
            else:
                print("[INCR] 无新增话题")
        except Exception as exc:  # pragma: no cover - 网络/外部依赖
            print(f"[ERROR] 读取 copytrade token 失败：{exc}")
            self.latest_topics = []

    def _load_copytrade_tokens(self) -> List[Dict[str, Any]]:
        path = self.config.copytrade_tokens_path
        if not path.exists():
            print(f"[WARN] copytrade token 文件不存在：{path}")
            return []
        payload = _load_json_file(path)
        raw_tokens = payload.get("tokens")
        if not isinstance(raw_tokens, list):
            print(f"[WARN] copytrade token 文件格式异常：{path}")
            return []
        topics: List[Dict[str, Any]] = []
        for item in raw_tokens:
            if not isinstance(item, dict):
                continue
            token_id = item.get("token_id") or item.get("tokenId")
            if not token_id:
                continue
            market_slug = item.get("market_slug") or item.get("slug")
            topics.append(
                {
                    "topic_id": str(token_id),
                    "token_id": str(token_id),
                    "slug": market_slug,
                    "last_seen": item.get("last_seen"),
                }
            )
        print(f"[COPYTRADE] 已读取 token {len(topics)} 条 | {path}")
        return topics

    def _load_copytrade_sell_signals(self) -> set[str]:
        path = self.config.copytrade_sell_signals_path
        if not path.exists():
            return set()
        payload = _load_json_file(path)
        raw_tokens = payload.get("sell_tokens")
        if not isinstance(raw_tokens, list):
            print(f"[WARN] copytrade sell_signal 文件格式异常：{path}")
            return set()
        signals: set[str] = set()
        skipped = 0
        for item in raw_tokens:
            if not isinstance(item, dict):
                continue
            token_id = item.get("token_id") or item.get("tokenId")
            if not token_id:
                continue
            if not item.get("introduced_by_buy", False):
                skipped += 1
                continue
            signals.add(str(token_id))
        if signals:
            preview = ", ".join(list(signals)[:5])
            print(f"[COPYTRADE] 已读取 sell 信号 {len(signals)} 条 preview={preview}")
        if skipped:
            print(f"[COPYTRADE] 已跳过未引入的 sell 信号 {skipped} 条")
        return signals

    def _exit_signal_path(self, token_id: str) -> Path:
        safe_id = _safe_topic_filename(token_id)
        return self.config.data_dir / f"exit_signal_{safe_id}.json"

    def _issue_exit_signal(self, token_id: str) -> None:
        path = self._exit_signal_path(token_id)
        payload = {
            "token_id": token_id,
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        _dump_json_file(path, payload)

    def _has_account_position(self, token_id: str) -> bool:
        if not token_id:
            return False
        now = time.time()
        cached = self._position_snapshot_cache.get(token_id)
        if cached:
            cached_has_position = bool(cached.get("has_position", False))
            ttl = (
                POSITION_CHECK_CACHE_TTL_SEC
                if cached_has_position
                else POSITION_CHECK_NEGATIVE_CACHE_TTL_SEC
            )
            if now - cached.get("ts", 0.0) <= ttl:
                return cached_has_position

        if not self._position_address:
            address, origin = _resolve_position_address_from_env()
            self._position_address = address
            self._position_address_origin = origin
            if not address and not self._position_address_warned:
                self._position_address_warned = True
                print(f"[COPYTRADE][WARN] {origin}")

        if not self._position_address:
            self._position_snapshot_cache[token_id] = {
                "ts": now,
                "has_position": False,
            }
            return False

        pos_size, info = _fetch_position_size_from_data_api(
            self._position_address,
            token_id,
        )
        normalized_pos_size = float(pos_size or 0.0)
        has_position = normalized_pos_size > POSITION_CLEANUP_DUST_THRESHOLD
        if info != "ok" and not has_position:
            print(f"[COPYTRADE][INFO] 持仓检查失败 token={token_id} info={info}")
        self._position_snapshot_cache[token_id] = {
            "ts": now,
            "has_position": has_position,
            "pos_size": normalized_pos_size,
        }
        return has_position

    def _apply_sell_signals(self, sell_signals: set[str]) -> None:
        if not sell_signals:
            return
        # 仅保留当前仍在 sell 文件中的处理记录，
        # 当上游移除并再次写入同一 token 时，允许新一轮 sell 信号重新触发。
        self._handled_sell_signals.intersection_update(sell_signals)

        now = time.time()
        full_recheck_due = (not self._sell_bootstrap_done) or (
            now >= self._next_sell_full_recheck_at
        )

        if full_recheck_due:
            self._run_full_sell_signal_recheck(sell_signals)
            self._sell_bootstrap_done = True
            self._next_sell_full_recheck_at = now + max(
                300.0,
                float(self.config.sell_position_poll_interval_sec),
            )
            return

        new_signals = sell_signals - self._handled_sell_signals
        if not new_signals:
            return

        for token_id in new_signals:
            task = self.tasks.get(token_id)
            has_running_task = bool(task and task.is_running())
            has_history = token_id in self.handled_topics

            if token_id in self._completed_exit_cleanup_tokens:
                self._handled_sell_signals.add(token_id)
                continue
            if task and not has_running_task and task.end_reason == "sell signal cleanup":
                self._completed_exit_cleanup_tokens.add(token_id)
                self._handled_sell_signals.add(token_id)
                continue

            if not has_running_task and not has_history:
                pos_size = float(self._sell_position_snapshot.get(token_id, 0.0) or 0.0)
                has_position = pos_size > POSITION_CLEANUP_DUST_THRESHOLD
                if not has_position:
                    info = self._sell_position_snapshot_info
                    if info == "ok":
                        info = "未找到持仓记录"
                    print(f"[COPYTRADE][INFO] 持仓检查失败 token={token_id} info={info}")
                    print(
                        "[COPYTRADE] 忽略 sell 信号，未进入 maker 队列: "
                        f"token_id={token_id}"
                    )
                    # 新增 SELL 信号仅做一次校验，避免同一 token 高频重复查询。
                    self._handled_sell_signals.add(token_id)
                    continue
                print(
                    "[COPYTRADE] SELL 信号触发持仓清仓: "
                    f"token_id={token_id}"
                )
            self._trigger_sell_exit(token_id, task)

    def _run_full_sell_signal_recheck(self, sell_signals: set[str]) -> None:
        """启动首轮 + 周期性兜底：全量检查全部 sell token。"""
        if not sell_signals:
            return
        snapshot, info = self._refresh_sell_position_snapshot()
        # 缓存快照供增量路径使用（新增 sell 信号无需再次拉取）
        self._sell_position_snapshot = snapshot
        self._sell_position_snapshot_info = info
        print(
            "[COPYTRADE][INFO] SELL 全量复检: "
            f"signals={len(sell_signals)} positions={len(snapshot)} info={info}"
        )
        for token_id in sell_signals:
            task = self.tasks.get(token_id)
            has_running_task = bool(task and task.is_running())
            has_history = token_id in self.handled_topics

            if token_id in self._completed_exit_cleanup_tokens:
                self._handled_sell_signals.add(token_id)
                continue
            if task and not has_running_task and task.end_reason == "sell signal cleanup":
                self._completed_exit_cleanup_tokens.add(token_id)
                self._handled_sell_signals.add(token_id)
                continue

            if not has_running_task and not has_history:
                pos_size = float(snapshot.get(token_id, 0.0) or 0.0)
                has_position = pos_size > POSITION_CLEANUP_DUST_THRESHOLD
                if not has_position:
                    reason = "未找到持仓记录" if info == "ok" else info
                    print(f"[COPYTRADE][INFO] 持仓检查失败 token={token_id} info={reason}")
                    print(
                        "[COPYTRADE] 忽略 sell 信号，未进入 maker 队列: "
                        f"token_id={token_id}"
                    )
                    # 全量复检也打 handled 标记，避免在下次普通轮询时再次重复检查。
                    self._handled_sell_signals.add(token_id)
                    continue
                print(
                    "[COPYTRADE] SELL 信号触发持仓清仓: "
                    f"token_id={token_id}"
                )
            self._trigger_sell_exit(token_id, task)

    def _trigger_sell_exit(self, token_id: str, task: Optional[TopicTask]) -> None:
        if token_id in self.pending_topics:
            self._remove_pending_topic(token_id)
        if task and task.is_running():
            task.no_restart = True
            task.end_reason = "sell signal"
            task.heartbeat("sell signal received")
        self._issue_exit_signal(token_id)
        if not (task and task.is_running()):
            if (
                token_id not in self.pending_exit_topics
                and token_id not in self.pending_topics
            ):
                self.pending_exit_topics.append(token_id)
        self._handled_sell_signals.add(token_id)

    def _refresh_sell_position_snapshot(self) -> tuple[Dict[str, float], str]:
        if not self._position_address:
            address, origin = _resolve_position_address_from_env()
            self._position_address = address
            self._position_address_origin = origin
            if not address and not self._position_address_warned:
                self._position_address_warned = True
                print(f"[COPYTRADE][WARN] {origin}")

        if not self._position_address:
            return {}, "缺少地址，无法查询持仓。"

        snapshot, info = _fetch_position_snapshot_map_from_data_api(self._position_address)
        if info == "ok":
            print(
                "[COPYTRADE][INFO] SELL 持仓快照已刷新: "
                f"positions={len(snapshot)}"
            )
        else:
            print(f"[COPYTRADE][INFO] SELL 持仓快照刷新失败 info={info}")
        return snapshot, info

    def _cleanup_old_logs(self) -> None:
        """清理7天前的日志文件，每天只执行一次"""
        today = time.strftime("%Y-%m-%d")
        if self._last_cleanup_date == today:
            # 今天已经清理过，跳过
            return

        # 计算截止时间（7天前）
        cutoff_ts = time.time() - (self._log_retention_days * 24 * 3600)

        # 要清理的目录列表
        cleanup_dirs = [
            self.config.data_dir,
            self.config.log_dir,
        ]

        total_deleted = 0
        total_size_freed = 0

        for dir_path in cleanup_dirs:
            if not dir_path.exists():
                continue
            try:
                deleted, size_freed = self._cleanup_directory(dir_path, cutoff_ts)
                total_deleted += deleted
                total_size_freed += size_freed
            except Exception as exc:
                print(f"[LOG_CLEANUP][WARN] 清理目录 {dir_path} 时出错: {exc}")

        self._last_cleanup_date = today

        if total_deleted > 0:
            size_mb = total_size_freed / (1024 * 1024)
            print(f"[LOG_CLEANUP] 清理完成: 删除 {total_deleted} 个文件，释放 {size_mb:.2f} MB")
        else:
            print(f"[LOG_CLEANUP] 检查完成，无需清理（保留 {self._log_retention_days} 天内的文件）")

    def _cleanup_directory(self, dir_path: Path, cutoff_ts: float) -> tuple:
        """递归清理指定目录中的旧文件，返回 (删除文件数, 释放字节数)"""
        deleted_count = 0
        size_freed = 0

        # 保护列表：这些文件/目录不应被删除
        protected_names = {
            "handled_topics.json",
            "autorun_status.json",
            "exit_tokens.json",
            "ws_cache.json",
            ".gitkeep",
        }

        try:
            for item in dir_path.rglob("*"):
                if not item.is_file():
                    continue

                # 跳过受保护的文件
                if item.name in protected_names:
                    continue

                # 只清理日志文件和临时文件
                # 清理的文件类型：.log, .log.*, .json (非保护), .tmp
                suffix_lower = item.suffix.lower()
                name_lower = item.name.lower()

                # 判断是否为可清理的文件类型
                is_log_file = (
                    suffix_lower == ".log"
                    or ".log." in name_lower  # 如 xxx.log.1, xxx.log.2024-01-01
                    or suffix_lower == ".tmp"
                )

                # 对于 data 目录下的 JSON 文件，只清理非保护的
                is_old_json = (
                    suffix_lower == ".json"
                    and item.name not in protected_names
                    and "data" in str(item.parent)
                )

                if not (is_log_file or is_old_json):
                    continue

                try:
                    mtime = item.stat().st_mtime
                    if mtime < cutoff_ts:
                        file_size = item.stat().st_size
                        item.unlink()
                        deleted_count += 1
                        size_freed += file_size
                except OSError:
                    # 文件可能正在被使用，跳过
                    pass

        except Exception as exc:
            print(f"[LOG_CLEANUP][WARN] 遍历目录 {dir_path} 时出错: {exc}")

        # 清理空目录
        try:
            for item in sorted(dir_path.rglob("*"), reverse=True):
                if item.is_dir():
                    try:
                        # 尝试删除空目录
                        if not any(item.iterdir()):
                            item.rmdir()
                    except OSError:
                        pass
        except Exception:
            pass

        return deleted_count, size_freed

    def _cleanup_all_tasks(self) -> None:
        for task in list(self.tasks.values()):
            if task.is_running():
                print(f"[CLEAN] 停止 topic={task.topic_id} ...")
                self._terminate_task(task, reason="cleanup")
        # 写回 handled_topics，确保最新状态落盘
        write_handled_topics(self.config.handled_topics_path, self.handled_topics)

    def _restore_runtime_status(self) -> None:
        """尝试从上次运行的状态文件恢复待处理队列等信息。"""

        if not self.status_path.exists():
            return
        try:
            payload = _load_json_file(self.status_path)
            handled_topics = payload.get("handled_topics") or []
            pending_topics = payload.get("pending_topics") or []
            tasks_snapshot = payload.get("tasks") or {}
        except Exception as exc:  # pragma: no cover - 容错
            print(f"[WARN] 无法读取运行状态文件，已忽略: {exc}")
            return

        if handled_topics:
            self.handled_topics.update(str(t) for t in handled_topics)

        # ===== 构建黑名单：确定已死亡的 token =====
        # 只过滤 MARKET_CLOSED 的 token，避免误删正常 token
        dead_tokens: set = set()
        for record in self._load_exit_tokens():
            if record.get("exit_reason") == "MARKET_CLOSED":
                tid = record.get("token_id")
                if tid:
                    dead_tokens.add(str(tid))

        restored_topics: List[str] = []
        skipped_dead: List[str] = []

        for topic_id in pending_topics:
            topic_id = str(topic_id)
            # 跳过确定已死亡的 token（市场已关闭）
            if topic_id in dead_tokens:
                skipped_dead.append(topic_id)
                continue
            # 只检查是否已在 pending 队列中，不检查 handled_topics
            # 因为保存的 pending_topics 代表"还未完成的任务"，即使在 handled 中也应恢复
            if topic_id in self.pending_topics:
                continue
            restored_topics.append(topic_id)
            self._enqueue_pending_topic(topic_id)

        for topic_id, info in tasks_snapshot.items():
            topic_id = str(topic_id)
            # 跳过确定已死亡的 token（市场已关闭）
            if topic_id in dead_tokens:
                if topic_id not in skipped_dead:
                    skipped_dead.append(topic_id)
                continue
            # 不检查 handled_topics，因为保存的 tasks 代表"上次运行中的任务"
            # 即使在 handled 中也应恢复，以确保任务不丢失
            if topic_id not in self.pending_topics:
                restored_topics.append(topic_id)
                self._enqueue_pending_topic(topic_id)

            task = TopicTask(topic_id=topic_id)
            task.status = "pending"
            task.notes.append("restored from runtime_status")
            config_path = info.get("config_path")
            log_path = info.get("log_path")
            if config_path:
                task.config_path = Path(config_path)
            if log_path:
                task.log_path = Path(log_path)
            self.tasks[topic_id] = task

        if skipped_dead:
            preview = ", ".join(t[:8] + "..." for t in skipped_dead[:5])
            print(f"[RESTORE] 跳过 {len(skipped_dead)} 个已关闭市场的 token: {preview}")

        if restored_topics:
            preview = ", ".join(restored_topics[:5])
            print(f"[RESTORE] 已从运行状态恢复 {len(restored_topics)} 个话题：{preview}")

        pending_exit_topics = payload.get("pending_exit_topics") or []
        for topic_id in pending_exit_topics:
            topic_id = str(topic_id)
            if topic_id in self.pending_exit_topics:
                continue
            self.pending_exit_topics.append(topic_id)

        handled_sell_signals = payload.get("handled_sell_signals") or []
        self._handled_sell_signals = {
            str(token_id) for token_id in handled_sell_signals if str(token_id).strip()
        }

        completed_exit_cleanup_tokens = payload.get("completed_exit_cleanup_tokens") or []
        self._completed_exit_cleanup_tokens = {
            str(token_id)
            for token_id in completed_exit_cleanup_tokens
            if str(token_id).strip()
        }

        exit_cleanup_retry_counts = payload.get("exit_cleanup_retry_counts") or {}
        for token_id, count in exit_cleanup_retry_counts.items():
            token_id = str(token_id)
            try:
                self._exit_cleanup_retry_counts[token_id] = int(count)
            except (TypeError, ValueError):
                pass

    def _dump_runtime_status(self) -> None:
        # GC: 清理 _completed_exit_cleanup_tokens 中不再活跃的条目
        # 仅保留仍在 sell 信号文件、pending 队列或 tasks 中的 token
        active_tokens = (
            set(self.pending_topics)
            | set(self.pending_exit_topics)
            | set(self.tasks.keys())
            | self._handled_sell_signals
        )
        stale = self._completed_exit_cleanup_tokens - active_tokens
        if stale:
            self._completed_exit_cleanup_tokens -= stale
            for tk in stale:
                self._exit_cleanup_retry_counts.pop(tk, None)
        payload = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "handled_topics_total": len(self.handled_topics),
            "handled_topics": sorted(self.handled_topics),
            "pending_topics": list(self.pending_topics),
            "pending_exit_topics": list(self.pending_exit_topics),
            "handled_sell_signals": sorted(self._handled_sell_signals),
            "completed_exit_cleanup_tokens": sorted(
                self._completed_exit_cleanup_tokens
            ),
            "exit_cleanup_retry_counts": dict(self._exit_cleanup_retry_counts),
            "tasks": {},
        }
        for topic_id, task in self.tasks.items():
            payload["tasks"][topic_id] = {
                "status": task.status,
                "pid": task.process.pid if task.process else None,
                "last_heartbeat": task.last_heartbeat,
                "notes": task.notes,
                "log_path": str(task.log_path) if task.log_path else None,
                "config_path": str(task.config_path) if task.config_path else None,
            }
        _dump_json_file(self.status_path, payload)
        print(f"[STATE] 已写入运行状态到 {self.status_path}")

    # ========== 入口方法 ==========
    def command_loop(self) -> None:
        try:
            prompt_shown = False
            while not self.stop_event.is_set():
                try:
                    if not prompt_shown:
                        # 主动刷新提示符，避免被后台日志刷屏覆盖
                        print("poly> ", end="", flush=True)
                        prompt_shown = True

                    ready, _, _ = select.select(
                        [sys.stdin], [], [], self.config.command_poll_sec
                    )
                    if not ready:
                        continue

                    line = sys.stdin.readline()
                    if line == "":
                        cmd = "exit"
                    else:
                        cmd = line.rstrip("\n")
                    prompt_shown = False
                except EOFError:
                    cmd = "exit"
                except Exception as exc:  # pragma: no cover - 保护交互循环不被意外异常终止
                    print(f"[ERROR] command loop input failed: {exc}")
                    traceback.print_exc()
                    time.sleep(self.config.command_poll_sec)
                    continue
                # 立刻反馈收到的命令，避免在日志刷屏时用户误以为命令未被捕获
                if cmd:
                    print(f"[CMD] received: {cmd}")
                else:
                    # 空行依旧入队，后续会在 _handle_command 里被忽略
                    print("[CMD] received: <empty>")
                self.enqueue_command(cmd)
                # 轻微休眠，防止输入为空或重复换行时产生过多提示刷屏
                time.sleep(self.config.command_poll_sec)
        except KeyboardInterrupt:
            print("\n[WARN] Ctrl+C detected, stopping...")
            self.stop_event.set()
        except Exception as exc:  # pragma: no cover - 防御性保护
            print(f"[ERROR] command loop crashed: {exc}")
            traceback.print_exc()


# =====================
# CLI 入口
# =====================

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Polymarket maker autorun")
    parser.add_argument(
        "--global-config",
        type=Path,
        default=MAKER_ROOT / "config" / "global_config.json",
        help="全局调度配置 JSON 路径",
    )
    parser.add_argument(
        "--strategy-config",
        type=Path,
        default=MAKER_ROOT / "config" / "strategy_defaults.json",
        help="策略参数模板 JSON 路径",
    )
    parser.add_argument(
        "--run-config-template",
        type=Path,
        default=MAKER_ROOT / "config" / "run_params.json",
        help="运行参数模板 JSON 路径（传递给 Volatility_arbitrage_run.py）",
    )
    parser.add_argument(
        "--no-repl",
        action="store_true",
        help="禁用交互式命令循环，仅按配置运行",
    )
    parser.add_argument(
        "--command",
        action="append",
        help="启动后自动执行的命令（可多次提供），例如 list 或 stop <topic_id>",
    )
    return parser.parse_args(argv)


def load_configs(
    args: argparse.Namespace,
) -> tuple[GlobalConfig, Dict[str, Any], Dict[str, Any]]:
    global_conf_raw = _load_json_file(args.global_config)
    strategy_conf_raw = _load_json_file(args.strategy_config)
    run_params_template = _load_json_file(args.run_config_template)
    return (
        GlobalConfig.from_dict(global_conf_raw),
        strategy_conf_raw,
        run_params_template,
    )


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    global_conf, strategy_conf, run_params_template = load_configs(args)
    log_path = _setup_main_log(global_conf.log_dir)
    print("=" * 60)
    print("[INIT] Polymarket Maker AutoRun - 聚合器启动")
    print("[VERSION] 支持book/tick事件处理 (2026-01-21)")
    if log_path:
        print(f"[INIT] 主程序日志: {log_path}")
    print("=" * 60)

    manager = AutoRunManager(global_conf, strategy_conf, run_params_template)

    def _handle_sigterm(signum: int, frame: Any) -> None:  # pragma: no cover - 信号处理不可测
        print(f"\n[WARN] signal {signum} received, exiting...")
        manager.stop_event.set()

    signal.signal(signal.SIGTERM, _handle_sigterm)

    worker = threading.Thread(target=manager.run_loop, daemon=True)
    worker.start()

    if args.command:
        for cmd in args.command:
            manager.enqueue_command(cmd)

    if args.no_repl or args.command:
        try:
            while worker.is_alive():
                time.sleep(global_conf.command_poll_sec)
        except KeyboardInterrupt:
            print("\n[WARN] Ctrl+C detected, stopping...")
            manager.stop_event.set()
    else:
        manager.command_loop()

    worker.join()


if __name__ == "__main__":
    main()
