#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
实时监控WebSocket缓存更新情况
每秒读取一次缓存文件，显示每个token的seq变化
"""

import json
import time
from pathlib import Path
from collections import defaultdict

SCRIPT_DIR = Path(__file__).parent
WS_CACHE_FILE = SCRIPT_DIR / "data" / "ws_cache.json"

def load_cache():
    """读取缓存文件"""
    if not WS_CACHE_FILE.exists():
        return None
    try:
        with open(WS_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ 读取失败: {e}")
        return None

def monitor(duration=60):
    """
    监控缓存更新
    duration: 监控时长（秒）
    """
    print("=" * 100)
    print("WebSocket 缓存实时监控")
    print("=" * 100)
    print(f"监控时长: {duration}秒 | 采样频率: 每秒1次")
    print()

    last_seqs = {}  # token_id -> last_seq
    update_counts = defaultdict(int)  # token_id -> 更新次数
    start_time = time.time()
    iteration = 0

    while time.time() - start_time < duration:
        iteration += 1
        cache = load_cache()

        if not cache:
            print(f"[{iteration:03d}] ❌ 缓存文件不存在或无法读取")
            time.sleep(1)
            continue

        tokens = cache.get("tokens", {})
        if not tokens:
            print(f"[{iteration:03d}] ⚠️  缓存为空")
            time.sleep(1)
            continue

        # 检查每个token的seq变化
        updates_this_round = []
        for token_id, data in tokens.items():
            seq = data.get("seq", 0)
            last_seq = last_seqs.get(token_id, 0)

            if seq > last_seq:
                update_counts[token_id] += 1
                updates_this_round.append({
                    "token_id": token_id,
                    "old_seq": last_seq,
                    "new_seq": seq,
                    "delta": seq - last_seq,
                    "bid": data.get("best_bid"),
                    "ask": data.get("best_ask"),
                })
                last_seqs[token_id] = seq
            elif token_id not in last_seqs:
                # 首次出现
                last_seqs[token_id] = seq

        # 打印本轮更新
        if updates_this_round:
            print(f"\n[{iteration:03d}] 🔄 发现更新 ({len(updates_this_round)} 个token):")
            for upd in updates_this_round:
                tid_short = upd["token_id"][:8] + "..." + upd["token_id"][-8:]
                print(f"  {tid_short}: seq {upd['old_seq']} → {upd['new_seq']} "
                      f"(+{upd['delta']}) | bid={upd['bid']}, ask={upd['ask']}")
        else:
            # 每10秒打印一次静默状态
            if iteration % 10 == 0:
                print(f"[{iteration:03d}] ⏸️  无更新 | tokens={len(tokens)}")

        time.sleep(1)

    # 总结
    print("\n" + "=" * 100)
    print("监控总结")
    print("=" * 100)

    if not last_seqs:
        print("❌ 没有检测到任何token")
        return

    print(f"总共监控了 {len(last_seqs)} 个token，时长 {duration} 秒\n")

    # 按更新次数排序
    sorted_tokens = sorted(update_counts.items(), key=lambda x: x[1], reverse=True)

    print("更新频率统计:")
    print(f"{'Token ID (前8...后8)':<25} {'更新次数':<10} {'平均间隔':<15} {'最终seq':<10}")
    print("-" * 70)

    for token_id, count in sorted_tokens:
        tid_short = token_id[:8] + "..." + token_id[-8:]
        avg_interval = duration / count if count > 0 else float('inf')
        final_seq = last_seqs.get(token_id, 0)
        print(f"{tid_short:<25} {count:<10} {avg_interval:.1f}秒{'':<10} {final_seq:<10}")

    # 显示无更新的token
    no_update_tokens = [tid for tid in last_seqs if tid not in update_counts or update_counts[tid] == 0]
    if no_update_tokens:
        print(f"\n⚠️  无更新的token ({len(no_update_tokens)} 个):")
        for tid in no_update_tokens:
            tid_short = tid[:8] + "..." + tid[-8:]
            print(f"  {tid_short} (seq={last_seqs[tid]})")

    print("\n" + "=" * 100)

if __name__ == "__main__":
    import sys
    duration = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    try:
        monitor(duration)
    except KeyboardInterrupt:
        print("\n\n⏹️  用户中断监控")
