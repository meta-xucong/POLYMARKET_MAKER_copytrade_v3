# Volatility_arbitrage_main_ws.py
# -*- coding: utf-8 -*-
"""
最小 WS 连接器（只负责连接与订阅，不做格式化/节流/查询展示）。
外部可传入 on_event 回调来处理每条事件。支持 verbose 开关（默认关闭，不输出）。

用法：
  from Volatility_arbitrage_main_ws import ws_watch_by_ids
  ws_watch_by_ids([YES_id, NO_id], label="...", on_event=handler, verbose=False)

聚合器用法（支持增量订阅）：
  from Volatility_arbitrage_main_ws import WSAggregatorClient
  client = WSAggregatorClient(on_event=handler, verbose=False)
  client.start()
  client.subscribe(["token1", "token2"])  # 增量添加
  client.unsubscribe(["token1"])  # 增量移除
  client.stop()

依赖：pip install websocket-client
"""
from __future__ import annotations

import json, time, threading, ssl
from typing import Callable, List, Optional, Any, Dict, Set

try:
    import websocket  # websocket-client
except Exception:
    raise RuntimeError("缺少依赖，请先安装： pip install websocket-client")

WS_BASE = "wss://ws-subscriptions-clob.polymarket.com"
CHANNEL = "market"

_REST_RATE_LIMIT_SEC = 1.0
_last_rest_call_ts = 0.0


def _enforce_rest_rate_limit() -> None:
    global _last_rest_call_ts
    now = time.monotonic()
    elapsed = now - _last_rest_call_ts
    remaining = _REST_RATE_LIMIT_SEC - elapsed
    if remaining > 0:
        time.sleep(remaining)
    _last_rest_call_ts = time.monotonic()

def _now() -> str:
    from datetime import datetime
    return datetime.now().strftime("%H:%M:%S")


class WSAggregatorClient:
    """
    支持增量订阅的 WebSocket 聚合客户端。

    特性：
    - 保持单一WS连接，通过增量消息添加/移除订阅
    - 线程安全的 subscribe() / unsubscribe() 方法
    - 自动重连机制
    - 避免完全重启WS连接带来的数据中断

    增量订阅协议（Polymarket CLOB WS API）：
    - 订阅: {"operation": "subscribe", "assets_ids": [...]}
    - 取消订阅: {"operation": "unsubscribe", "assets_ids": [...]}
    """

    def __init__(
        self,
        on_event: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_state: Optional[Callable[[str, Dict[str, Any]], None]] = None,
        auth: Optional[Dict[str, str]] = None,
        verbose: bool = False,
        label: str = "aggregator",
    ):
        self._on_event = on_event
        self._on_state = on_state
        self._auth = auth or None
        self._verbose = verbose
        self._label = label

        # 线程安全锁
        self._lock = threading.Lock()

        # 订阅状态
        self._subscribed_ids: Set[str] = set()  # 已确认订阅的token
        self._pending_subscribe: Set[str] = set()  # 待订阅
        self._pending_unsubscribe: Set[str] = set()  # 待取消订阅

        # WS连接状态
        self._ws: Optional[websocket.WebSocketApp] = None
        self._ws_connected = False
        self._ws_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        # 辅助线程
        self._flush_thread: Optional[threading.Thread] = None
        self._ping_thread: Optional[threading.Thread] = None
        self._silence_thread: Optional[threading.Thread] = None

        # 配置
        self._reconnect_delay = 1
        self._max_reconnect_delay = 60
        self._silence_timeout = 600
        self._flush_interval = 0.1  # 100ms 批量刷新待处理订阅

        # 统计
        self._last_event_ts = 0.0
        self._connect_count = 0
        self._subscribe_count = 0
        self._unsubscribe_count = 0

    def start(self) -> None:
        """启动WS连接线程"""
        if self._ws_thread and self._ws_thread.is_alive():
            return

        self._stop_event.clear()
        self._ws_thread = threading.Thread(target=self._run_loop, daemon=True)
        self._ws_thread.start()

        if self._verbose:
            print(f"[{_now()}][WS][AGGREGATOR] 客户端已启动")

    def stop(self) -> None:
        """停止WS连接"""
        self._stop_event.set()

        # 关闭WS连接
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass

        # 等待线程结束
        if self._ws_thread and self._ws_thread.is_alive():
            self._ws_thread.join(timeout=3)

        if self._verbose:
            print(f"[{_now()}][WS][AGGREGATOR] 客户端已停止")

    def subscribe(self, token_ids: List[str]) -> int:
        """
        增量订阅token（线程安全）

        Args:
            token_ids: 要订阅的token ID列表

        Returns:
            实际新增的订阅数量
        """
        if not token_ids:
            return 0

        added = 0
        with self._lock:
            for tid in token_ids:
                tid = str(tid).strip()
                if not tid:
                    continue
                # 如果已订阅，跳过
                if tid in self._subscribed_ids:
                    continue
                # 如果在待取消列表，移除（取消取消订阅）
                if tid in self._pending_unsubscribe:
                    self._pending_unsubscribe.discard(tid)
                # 添加到待订阅列表
                if tid not in self._pending_subscribe:
                    self._pending_subscribe.add(tid)
                    added += 1

        if added > 0 and self._verbose:
            print(f"[{_now()}][WS][AGGREGATOR] 待订阅队列 +{added} (总待订阅: {len(self._pending_subscribe)})")

        return added

    def unsubscribe(self, token_ids: List[str]) -> int:
        """
        增量取消订阅token（线程安全）

        Args:
            token_ids: 要取消订阅的token ID列表

        Returns:
            实际取消的订阅数量
        """
        if not token_ids:
            return 0

        removed = 0
        with self._lock:
            for tid in token_ids:
                tid = str(tid).strip()
                if not tid:
                    continue
                # 如果只在待订阅列表，直接移除
                if tid in self._pending_subscribe:
                    self._pending_subscribe.discard(tid)
                    removed += 1
                    continue
                # 如果已订阅，添加到待取消列表
                if tid in self._subscribed_ids:
                    if tid not in self._pending_unsubscribe:
                        self._pending_unsubscribe.add(tid)
                        removed += 1

        if removed > 0 and self._verbose:
            print(f"[{_now()}][WS][AGGREGATOR] 待取消队列 +{removed} (总待取消: {len(self._pending_unsubscribe)})")

        return removed

    def get_subscribed_ids(self) -> List[str]:
        """获取当前已订阅的token列表（线程安全）"""
        with self._lock:
            return list(self._subscribed_ids)

    def get_pending_count(self) -> Dict[str, int]:
        """获取待处理的订阅/取消订阅数量"""
        with self._lock:
            return {
                "pending_subscribe": len(self._pending_subscribe),
                "pending_unsubscribe": len(self._pending_unsubscribe),
                "subscribed": len(self._subscribed_ids),
            }

    def is_connected(self) -> bool:
        """检查WS是否已连接"""
        return self._ws_connected and self._ws_thread and self._ws_thread.is_alive()

    def _run_loop(self) -> None:
        """WS连接主循环（带自动重连）"""
        headers = [
            "Origin: https://polymarket.com",
            "User-Agent: Mozilla/5.0",
        ]

        while not self._stop_event.is_set():
            self._ws_connected = False
            self._connect_count += 1

            # 创建WS连接
            self._ws = websocket.WebSocketApp(
                WS_BASE + "/ws/" + CHANNEL,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
                header=headers,
            )

            try:
                self._ws.run_forever(
                    sslopt={"cert_reqs": ssl.CERT_REQUIRED},
                    ping_interval=25,
                    ping_timeout=10,
                )
            except Exception as exc:
                if self._verbose:
                    print(f"[{_now()}][WS][AGGREGATOR][EXCEPTION] {exc}")
                self._notify_state("error", {"error": str(exc)})

            self._ws_connected = False

            if self._stop_event.is_set():
                break

            # 重连延迟
            if self._verbose:
                print(f"[{_now()}][WS][AGGREGATOR] 连接断开，{self._reconnect_delay}s 后重试...")

            # 分段sleep，便于快速响应stop
            for _ in range(int(self._reconnect_delay * 10)):
                if self._stop_event.is_set():
                    break
                time.sleep(0.1)

            self._reconnect_delay = min(self._reconnect_delay * 2, self._max_reconnect_delay)

    def _on_open(self, ws) -> None:
        """WS连接建立回调"""
        self._ws_connected = True
        self._reconnect_delay = 1
        self._last_event_ts = time.monotonic()

        if self._verbose:
            print(f"[{_now()}][WS][AGGREGATOR][OPEN] 连接已建立 (第{self._connect_count}次)")

        # 发送初始握手消息（空订阅列表）
        # Polymarket要求先发送type消息建立channel
        initial_msg = {"type": CHANNEL, "assets_ids": []}
        if self._auth:
            initial_msg["auth"] = self._auth
        try:
            ws.send(json.dumps(initial_msg))
        except Exception as e:
            if self._verbose:
                print(f"[{_now()}][WS][AGGREGATOR][ERROR] 发送握手消息失败: {e}")

        # 将已订阅的token重新加入待订阅队列（重连后需要重新订阅）
        with self._lock:
            if self._subscribed_ids:
                resubscribe_count = len(self._subscribed_ids)
                self._pending_subscribe.update(self._subscribed_ids)
                self._subscribed_ids.clear()
                if self._verbose:
                    print(f"[{_now()}][WS][AGGREGATOR] 重连后重新订阅 {resubscribe_count} 个token")

        # 启动辅助线程
        self._start_helper_threads(ws)

        self._notify_state("open", {"connect_count": self._connect_count})

    def _start_helper_threads(self, ws) -> None:
        """启动辅助线程（flush、ping、silence guard）"""
        # 批量刷新线程
        def flush_loop():
            while self._ws_connected and not self._stop_event.is_set():
                try:
                    self._flush_pending_subscriptions(ws)
                except Exception as e:
                    if self._verbose:
                        print(f"[{_now()}][WS][AGGREGATOR][FLUSH_ERROR] {e}")
                time.sleep(self._flush_interval)

        self._flush_thread = threading.Thread(target=flush_loop, daemon=True)
        self._flush_thread.start()

        # PING心跳线程
        def ping_loop():
            while self._ws_connected and not self._stop_event.is_set():
                try:
                    ws.send("PING")
                except Exception:
                    break
                time.sleep(10)

        self._ping_thread = threading.Thread(target=ping_loop, daemon=True)
        self._ping_thread.start()

        # 静默检测线程
        def silence_guard():
            while self._ws_connected and not self._stop_event.is_set():
                time.sleep(5)
                if self._stop_event.is_set() or not self._ws_connected:
                    break
                if time.monotonic() - self._last_event_ts >= self._silence_timeout:
                    if self._verbose:
                        print(f"[{_now()}][WS][AGGREGATOR][SILENCE] {self._silence_timeout}s 无消息，主动重连")
                    self._notify_state("silence", {"timeout": self._silence_timeout})
                    try:
                        ws.close()
                    except Exception:
                        pass
                    break

        self._silence_thread = threading.Thread(target=silence_guard, daemon=True)
        self._silence_thread.start()

    def _flush_pending_subscriptions(self, ws) -> None:
        """批量刷新待处理的订阅/取消订阅请求"""
        # 先处理取消订阅
        unsubscribe_batch: List[str] = []
        with self._lock:
            if self._pending_unsubscribe:
                unsubscribe_batch = list(self._pending_unsubscribe)
                self._pending_unsubscribe.clear()

        if unsubscribe_batch:
            msg = {"operation": "unsubscribe", "assets_ids": unsubscribe_batch}
            if self._auth:
                msg["auth"] = self._auth
            try:
                ws.send(json.dumps(msg))
                self._unsubscribe_count += len(unsubscribe_batch)
                with self._lock:
                    for tid in unsubscribe_batch:
                        self._subscribed_ids.discard(tid)
                if self._verbose:
                    print(f"[{_now()}][WS][AGGREGATOR] ✗ 取消订阅 {len(unsubscribe_batch)} 个token")
            except Exception as e:
                # 发送失败，放回队列
                with self._lock:
                    self._pending_unsubscribe.update(unsubscribe_batch)
                if self._verbose:
                    print(f"[{_now()}][WS][AGGREGATOR][ERROR] 取消订阅失败: {e}")

        # 再处理订阅
        subscribe_batch: List[str] = []
        with self._lock:
            if self._pending_subscribe:
                subscribe_batch = list(self._pending_subscribe)
                self._pending_subscribe.clear()

        if subscribe_batch:
            msg = {"operation": "subscribe", "assets_ids": subscribe_batch}
            if self._auth:
                msg["auth"] = self._auth
            try:
                ws.send(json.dumps(msg))
                self._subscribe_count += len(subscribe_batch)
                with self._lock:
                    self._subscribed_ids.update(subscribe_batch)
                if self._verbose:
                    print(f"[{_now()}][WS][AGGREGATOR] ✓ 订阅 {len(subscribe_batch)} 个token (总订阅: {len(self._subscribed_ids)})")
            except Exception as e:
                # 发送失败，放回队列
                with self._lock:
                    self._pending_subscribe.update(subscribe_batch)
                if self._verbose:
                    print(f"[{_now()}][WS][AGGREGATOR][ERROR] 订阅失败: {e}")

    def _on_message(self, ws, message: str) -> None:
        """WS消息回调"""
        # 忽略非JSON（如PONG）
        try:
            data = json.loads(message)
        except Exception:
            return

        self._last_event_ts = time.monotonic()

        if self._on_event is None:
            if self._verbose:
                print(f"[{_now()}][WS][AGGREGATOR][EVENT] {data}")
            return

        # 逐条回调
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    try:
                        self._on_event(item)
                    except Exception:
                        pass
        elif isinstance(data, dict):
            try:
                self._on_event(data)
            except Exception:
                pass

    def _on_error(self, ws, error) -> None:
        """WS错误回调"""
        if self._verbose:
            print(f"[{_now()}][WS][AGGREGATOR][ERROR] {error}")
        self._notify_state("error", {"error": str(error)})

    def _on_close(self, ws, status_code, msg) -> None:
        """WS关闭回调"""
        self._ws_connected = False
        if self._verbose:
            print(f"[{_now()}][WS][AGGREGATOR][CLOSED] {status_code} {msg}")
        self._notify_state("closed", {"status_code": status_code, "message": msg})

    def _notify_state(self, state: str, info: Optional[Dict[str, Any]] = None) -> None:
        """通知连接状态变化"""
        if self._on_state is None:
            return
        payload = info or {}
        payload["label"] = self._label
        try:
            self._on_state(state, payload)
        except Exception:
            pass

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self._lock:
            return {
                "connected": self._ws_connected,
                "connect_count": self._connect_count,
                "subscribe_count": self._subscribe_count,
                "unsubscribe_count": self._unsubscribe_count,
                "subscribed_tokens": len(self._subscribed_ids),
                "pending_subscribe": len(self._pending_subscribe),
                "pending_unsubscribe": len(self._pending_unsubscribe),
            }

def ws_watch_by_ids(
    asset_ids: List[str],
    label: str = "",
    on_event: Optional[Callable[[Dict[str, Any]], None]] = None,
    *,
    on_state: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    verbose: bool = False,
    stop_event: Optional[threading.Event] = None,
):
    """
    只负责：连接 → 订阅 → 将 WS 事件回调给 on_event（逐条 dict）。
    - asset_ids: 订阅的 token_ids（字符串）
    - label: 可选，仅用于启动打印（不参与逻辑）
    - on_event: 回调函数，参数是一条事件（dict）。若服务端下发 list，将按条回调。
    - on_state: 连接状态通知，state in {open, closed, error, silence}
    - verbose: 默认 False。为 True 时打印 OPEN/SUB/ERROR/CLOSED 及无回调时的事件。
    """
    ids = [str(x) for x in asset_ids if x]
    if not ids:
        raise ValueError("asset_ids 为空")

    if verbose and label:
        print(f"[INIT] 订阅: {label}")
    if verbose:
        for i, tid in enumerate(ids):
            print(f"  - token_id[{i}] = {tid}")

    stop_event = stop_event or threading.Event()

    reconnect_delay = 1
    max_reconnect_delay = 60
    silence_timeout = 600  # 秒，超过则主动重连以避免卡死

    headers = [
        "Origin: https://polymarket.com",
        "User-Agent: Mozilla/5.0",
    ]

    while not stop_event.is_set():
        ping_stop = {"v": False}
        silence_guard_stop = {"v": False}

        def _notify(state: str, info: Optional[Dict[str, Any]] = None) -> None:
            if on_state is None:
                return
            payload = info or {}
            try:
                on_state(state, payload)
            except Exception:
                pass

        last_event_ts = time.monotonic()

        def on_open(ws):
            nonlocal reconnect_delay, last_event_ts
            if verbose:
                print(f"[{_now()}][WS][OPEN] -> {WS_BASE+'/ws/'+CHANNEL}")
            payload = {"type": CHANNEL, "assets_ids": ids}
            ws.send(json.dumps(payload))
            reconnect_delay = 1
            last_event_ts = time.monotonic()
            _notify("open", {"label": label, "asset_ids": ids})

            # 文本心跳 PING（与底层 ping 帧并行存在）
            def _ping():
                while not ping_stop["v"] and not stop_event.is_set():
                    try:
                        ws.send("PING")
                        time.sleep(10)
                    except Exception:
                        break

            threading.Thread(target=_ping, daemon=True).start()

            def _silence_guard():
                while not silence_guard_stop["v"] and not stop_event.is_set():
                    time.sleep(5)
                    if stop_event.is_set() or silence_guard_stop["v"]:
                        break
                    if time.monotonic() - last_event_ts < silence_timeout:
                        continue
                    if verbose:
                        print(
                            f"[{_now()}][WS][SILENCE] {label or ids} {silence_timeout}s 无消息，主动重连。"
                        )
                    _notify(
                        "silence",
                        {"label": label, "asset_ids": ids, "timeout": silence_timeout},
                    )
                    try:
                        ws.close()
                    except Exception:
                        pass
                    break

            threading.Thread(target=_silence_guard, daemon=True).start()

        def on_message(ws, message):
            nonlocal last_event_ts
            # 忽略非 JSON 文本（如 PONG）
            try:
                data = json.loads(message)
            except Exception:
                return

            last_event_ts = time.monotonic()

            # 无回调：仅在 verbose=True 时打印，否则静默
            if on_event is None:
                if verbose:
                    print(f"[{_now()}][WS][EVENT] {data}")
                return

            # 逐条回调
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        try:
                            on_event(item)
                        except Exception:
                            pass
            elif isinstance(data, dict):
                try:
                    on_event(data)
                except Exception:
                    pass

        def on_error(ws, error):
            if verbose:
                print(f"[{_now()}][WS][ERROR] {error}")
            _notify("error", {"label": label, "error": str(error)})

        def on_close(ws, status_code, msg):
            ping_stop["v"] = True
            silence_guard_stop["v"] = True
            if verbose:
                print(f"[{_now()}][WS][CLOSED] {status_code} {msg}")
            _notify(
                "closed",
                {"label": label, "status_code": status_code, "message": msg},
            )

        wsa = websocket.WebSocketApp(
            WS_BASE + "/ws/" + CHANNEL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            header=headers,
        )

        try:
            wsa.run_forever(
                sslopt={"cert_reqs": ssl.CERT_REQUIRED},
                ping_interval=25,
                ping_timeout=10,
            )
        except Exception as exc:
            ping_stop["v"] = True
            silence_guard_stop["v"] = True
            if verbose:
                print(f"[{_now()}][WS][EXCEPTION] {exc}")
            _notify("error", {"label": label, "error": str(exc)})
        finally:
            ping_stop["v"] = True
            silence_guard_stop["v"] = True

        if stop_event.is_set():
            break

        if verbose:
            print(f"[{_now()}][WS] 连接结束，{reconnect_delay}s 后重试…")
        time.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)

# --- 仅供独立运行调试 ---
def _parse_cli(argv: List[str]) -> Optional[str]:
    for i, a in enumerate(argv):
        if a == "--source" and i + 1 < len(argv):
            return argv[i + 1].strip()
        if a.startswith("--source="):
            return a.split("=", 1)[1].strip()
    return None

def _resolve_ids_via_rest(source: str):
    import urllib.parse, requests, json
    GAMMA_API = "https://gamma-api.polymarket.com/markets"

    def _is_url(s: str) -> bool:
        return s.startswith("http://") or s.startswith("https://")

    def _extract_market_slug(url: str):
        p = urllib.parse.urlparse(url)
        parts = [x for x in p.path.split("/") if x]
        if len(parts) >= 2 and parts[0] == "event":
            return parts[-1]
        if len(parts) >= 2 and parts[0] == "market":
            return parts[1]
        return None

    if _is_url(source):
        slug = _extract_market_slug(source)
        if not slug:
            raise ValueError("无法从 URL 解析出 market slug")
        _enforce_rest_rate_limit()
        r = requests.get(GAMMA_API, params={"limit": 1, "slug": slug}, timeout=10)
        r.raise_for_status()
        arr = r.json()
        if not (isinstance(arr, list) and arr):
            raise ValueError("gamma-api 未找到该市场")
        m = arr[0]
        title = m.get("question") or slug
        token_ids_raw = m.get("clobTokenIds", "[]")
        token_ids = json.loads(token_ids_raw) if isinstance(token_ids_raw, str) else (token_ids_raw or [])
        return [x for x in token_ids if x], title

    if "," in source:
        a, b = [x.strip() for x in source.split(",", 1)]
        title = "manual-token-ids"
        return [x for x in (a, b) if x], title

    raise ValueError("未识别的输入。")

if __name__ == "__main__":
    import sys
    src = _parse_cli(sys.argv[1:])
    if not src:
        print('请输入 Polymarket 市场 URL：')
        src = input().strip()
        if not src:
            raise SystemExit(1)
    ids, label = _resolve_ids_via_rest(src)

    # 独立运行调试：开启 verbose 以便观察
    def _dbg(ev): print(ev)
    ws_watch_by_ids(ids, label=label, on_event=_dbg, verbose=True)
