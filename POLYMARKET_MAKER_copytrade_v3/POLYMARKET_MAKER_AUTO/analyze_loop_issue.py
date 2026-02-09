#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分析主循环问题的脚本
检查为什么child process的main loop可能不执行或只执行一次
"""

import re
from pathlib import Path
from collections import Counter

SCRIPT_DIR = Path(__file__).parent

def analyze_child_logs():
    """分析child process日志，查找主循环执行模式"""
    print("=" * 80)
    print("Child Process 主循环分析")
    print("=" * 80)

    log_dir = SCRIPT_DIR / "logs" / "autorun"
    if not log_dir.exists():
        print(f"❌ 日志目录不存在: {log_dir}")
        return

    # 找到所有child process日志
    log_files = list(log_dir.glob("autorun_*.log"))
    if not log_files:
        print("❌ 未找到任何child process日志文件")
        return

    print(f"✅ 找到 {len(log_files)} 个日志文件\n")

    for log_file in sorted(log_files, key=lambda x: x.stat().st_mtime, reverse=True)[:5]:
        print(f"\n{'='*80}")
        print(f"分析文件: {log_file.name}")
        print(f"{'='*80}")

        try:
            with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
        except Exception as e:
            print(f"❌ 读取失败: {e}")
            continue

        lines = content.splitlines()
        total_lines = len(lines)
        print(f"总行数: {total_lines}")

        # 分析关键事件
        ws_shared_calls = []  # [WS][SHARED] seq=X 调用
        ws_debug_calls = []   # [WS][SHARED][DEBUG] 调用
        loop_events = []      # [LOOP] 事件
        exit_events = []      # [EXIT] 事件
        error_events = []     # [ERROR]/[FATAL] 事件
        wait_events = []      # [WAIT] 事件

        for idx, line in enumerate(lines, 1):
            if "[WS][SHARED] seq=" in line:
                # 提取seq值
                match = re.search(r'seq=(\d+)', line)
                if match:
                    seq = int(match.group(1))
                    ws_shared_calls.append((idx, seq, line.strip()))

            if "[WS][SHARED][DEBUG]" in line:
                ws_debug_calls.append((idx, line.strip()))

            if "[LOOP]" in line:
                loop_events.append((idx, line.strip()))

            if "[EXIT]" in line or "[QUEUE]" in line:
                exit_events.append((idx, line.strip()))

            if "[ERROR]" in line or "[FATAL]" in line:
                error_events.append((idx, line.strip()))

            if "[WAIT]" in line:
                wait_events.append((idx, line.strip()))

        # 打印统计
        print(f"\n事件统计:")
        print(f"  [WS][SHARED] seq=X 调用: {len(ws_shared_calls)} 次")
        print(f"  [WS][SHARED][DEBUG] 调用: {len(ws_debug_calls)} 次")
        print(f"  [LOOP] 事件: {len(loop_events)} 次")
        print(f"  [WAIT] 事件: {len(wait_events)} 次")
        print(f"  [EXIT] 事件: {len(exit_events)} 次")
        print(f"  [ERROR/FATAL] 事件: {len(error_events)} 次")

        # 显示seq变化趋势
        if ws_shared_calls:
            print(f"\n[WS][SHARED] seq 变化:")
            for idx, seq, line in ws_shared_calls[:10]:  # 只显示前10次
                print(f"  行{idx}: seq={seq}")
            if len(ws_shared_calls) > 10:
                print(f"  ... (还有{len(ws_shared_calls)-10}次调用)")

            # 检查seq是否增长
            seqs = [seq for _, seq, _ in ws_shared_calls]
            if len(set(seqs)) == 1:
                print(f"  ⚠️  seq始终为 {seqs[0]}，从未更新！")
            else:
                print(f"  ✅ seq有变化: {min(seqs)} → {max(seqs)}")
        else:
            print(f"\n⚠️  未找到 [WS][SHARED] seq=X 调用")

        # 显示DEBUG调用
        if ws_debug_calls:
            print(f"\n[WS][SHARED][DEBUG] 详情:")
            for idx, line in ws_debug_calls[:3]:
                print(f"  行{idx}: {line}")

        # 显示EXIT事件
        if exit_events:
            print(f"\n[EXIT] 事件 (最后5个):")
            for idx, line in exit_events[-5:]:
                print(f"  行{idx}: {line}")

        # 显示ERROR事件
        if error_events:
            print(f"\n[ERROR/FATAL] 事件:")
            for idx, line in error_events[:5]:
                print(f"  行{idx}: {line}")

        # 显示最后20行
        print(f"\n最后20行日志:")
        print("-" * 80)
        for line in lines[-20:]:
            print(f"  {line}")
        print("-" * 80)

        # 分析结论
        print(f"\n🔍 诊断结论:")

        if len(ws_shared_calls) == 0:
            print("  ❌ 从未成功调用 _apply_shared_ws_snapshot()")
            print("     可能原因: waiting loop超时退出，或缓存文件读取失败")
        elif len(ws_shared_calls) == 1:
            print("  ⚠️  只调用了1次 _apply_shared_ws_snapshot()")
            print("     可能原因:")
            print("       1. waiting loop调用1次后退出")
            print("       2. main loop未启动或立即退出")
            print("       3. main loop启动但未再次调用该函数")
        else:
            print(f"  ✅ 多次调用 _apply_shared_ws_snapshot() ({len(ws_shared_calls)}次)")
            if len(set(seqs)) == 1:
                print("     ⚠️  但seq从未更新，可能是:")
                print("        1. 缓存文件未更新")
                print("        2. dedup逻辑阻止了更新")

        if exit_events:
            print(f"  ℹ️  进程正常退出 (有EXIT日志)")
        elif total_lines > 0:
            print(f"  ⚠️  日志突然中断 (无EXIT日志)，可能:")
            print("     1. 进程被kill")
            print("     2. 进程hang住")
            print("     3. 进程进入无限循环但不打印日志")

if __name__ == "__main__":
    try:
        analyze_child_logs()
    except KeyboardInterrupt:
        print("\n\n⏹️  分析中断")
