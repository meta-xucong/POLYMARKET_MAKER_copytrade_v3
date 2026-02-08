#!/usr/bin/env python3
"""
WS 聚合器调试工具：记录所有原始 WS 事件
用于诊断为什么 seq 不递增
"""
import sys
import json
import time
from pathlib import Path

# 添加 MAKER 路径
MAKER_ROOT = Path(__file__).resolve().parent / "POLYMARKET_MAKER"
sys.path.insert(0, str(MAKER_ROOT))

from Volatility_arbitrage_main_ws import ws_watch_by_ids

# 测试用的 token_ids（从你的运行状态中提取）
TEST_TOKENS = [
    "92338023949892178944669766466918011858071833335063600591564160751176113496073",
    "108199626497821046618572524993737909727470476156115808563328756112796709819504",
]

event_count = 0
event_types = {}
last_update = time.time()

def on_event(ev):
    global event_count, last_update
    event_count += 1
    last_update = time.time()

    # 统计事件类型
    evt_type = "unknown"
    if isinstance(ev, dict):
        evt_type = ev.get("event_type", "no_event_type")
        if "price_changes" in ev:
            evt_type += "+price_changes"

    event_types[evt_type] = event_types.get(evt_type, 0) + 1

    # 打印详细事件内容（前10条）
    if event_count <= 10:
        print(f"\n=== Event #{event_count} ===")
        print(f"Type: {evt_type}")
        print(f"Full content: {json.dumps(ev, indent=2)}")

    # 每100条打印一次统计
    if event_count % 100 == 0:
        print(f"\n[STATS] Total events: {event_count}")
        print(f"[STATS] Event types: {event_types}")

def on_state(state, info):
    print(f"[STATE] {state}: {info}")

def monitor():
    """监控线程：定期报告状态"""
    import threading

    def _monitor():
        global event_count, last_update
        last_count = 0
        while True:
            time.sleep(30)
            now = time.time()
            idle = now - last_update
            rate = (event_count - last_count) / 30.0

            print(f"\n[MONITOR] Count: {event_count} (+{event_count-last_count} in 30s, {rate:.2f}/s)")
            print(f"[MONITOR] Idle: {idle:.1f}s since last event")
            print(f"[MONITOR] Event types: {event_types}")

            last_count = event_count

    t = threading.Thread(target=_monitor, daemon=True)
    t.start()

if __name__ == "__main__":
    print(f"[INIT] 启动 WS 调试监控...")
    print(f"[INIT] 订阅 {len(TEST_TOKENS)} 个 tokens")

    monitor()

    try:
        ws_watch_by_ids(
            TEST_TOKENS,
            label="debug",
            on_event=on_event,
            on_state=on_state,
            verbose=True,  # 启用详细日志
        )
    except KeyboardInterrupt:
        print(f"\n\n[FINAL] Total events received: {event_count}")
        print(f"[FINAL] Event type distribution: {event_types}")
