#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
诊断token_id格式匹配问题
检查ws_cache.json中的token_id格式与实际查询的格式是否一致
"""

import json
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent

def diagnose_token_format():
    """诊断token_id格式问题"""
    print("=" * 80)
    print("Token ID 格式匹配诊断")
    print("=" * 80)

    # 1. 读取ws_cache.json
    cache_path = SCRIPT_DIR / "ws_cache.json"
    if not cache_path.exists():
        print(f"❌ 缓存文件不存在: {cache_path}")
        return

    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as e:
        print(f"❌ 读取缓存文件失败: {e}")
        return

    tokens = payload.get("tokens", {})
    if not tokens:
        print("❌ 缓存中没有tokens数据")
        return

    print(f"✅ 缓存中有 {len(tokens)} 个token\n")

    # 2. 读取tokens_from_copytrade.json获取期望的token_id列表
    copytrade_path = SCRIPT_DIR / "tokens_from_copytrade.json"
    expected_tokens = []

    if copytrade_path.exists():
        try:
            with open(copytrade_path, "r", encoding="utf-8") as f:
                copytrade_data = json.load(f)
                expected_tokens = copytrade_data.get("token_ids", [])
            print(f"✅ 期望订阅 {len(expected_tokens)} 个token\n")
        except Exception as e:
            print(f"⚠️  读取copytrade配置失败: {e}\n")

    # 3. 分析缓存中的token_id格式
    print("📊 缓存中的token_id格式分析:")
    print("-" * 80)

    cache_tokens = list(tokens.keys())
    for i, token_id in enumerate(cache_tokens[:10], 1):  # 只显示前10个
        token_data = tokens[token_id]
        seq = token_data.get("seq", "N/A")
        price = token_data.get("price", "N/A")

        print(f"\n[Token {i}]")
        print(f"  ID: {token_id}")
        print(f"  类型: {type(token_id)}")
        print(f"  长度: {len(token_id)}")
        print(f"  Seq: {seq}")
        print(f"  Price: {price}")

        # 检查是否是数字字符串
        if token_id.isdigit():
            print(f"  ✓ 格式: 纯数字字符串")
        else:
            print(f"  ✗ 格式: 包含非数字字符")

    if len(cache_tokens) > 10:
        print(f"\n... (还有 {len(cache_tokens)-10} 个token)")

    # 4. 检查期望token是否在缓存中
    if expected_tokens:
        print(f"\n{'='*80}")
        print("🔍 格式匹配检查:")
        print("-" * 80)

        for i, expected_id in enumerate(expected_tokens[:5], 1):  # 只检查前5个
            # 尝试多种格式
            formats_to_try = [
                str(expected_id),
                str(int(expected_id)) if str(expected_id).isdigit() else expected_id,
                expected_id.strip() if isinstance(expected_id, str) else str(expected_id),
            ]

            print(f"\n[期望Token {i}]")
            print(f"  原始值: {expected_id}")
            print(f"  原始类型: {type(expected_id)}")

            found = False
            for fmt in formats_to_try:
                if fmt in tokens:
                    print(f"  ✅ 找到匹配: 格式='{fmt}', 类型={type(fmt)}")
                    found = True
                    break

            if not found:
                print(f"  ❌ 未找到匹配")
                print(f"  尝试过的格式:")
                for fmt in formats_to_try:
                    print(f"    - '{fmt}' (类型={type(fmt).__name__})")

                # 模糊匹配：检查是否有相似的key
                similar_keys = [k for k in cache_tokens if expected_id in k or k in str(expected_id)]
                if similar_keys:
                    print(f"  💡 发现相似的key:")
                    for key in similar_keys[:3]:
                        print(f"    - '{key}'")

    # 5. 显示建议
    print(f"\n{'='*80}")
    print("💡 诊断建议:")
    print("-" * 80)

    # 检查token_id格式一致性
    all_numeric = all(k.isdigit() for k in cache_tokens)
    if all_numeric:
        print("  ✓ 所有缓存token_id都是纯数字字符串格式")
    else:
        print("  ⚠️  缓存中的token_id格式不一致")

    # 检查长度
    lengths = set(len(k) for k in cache_tokens)
    if len(lengths) == 1:
        print(f"  ✓ 所有token_id长度一致: {list(lengths)[0]} 字符")
    else:
        print(f"  ⚠️  token_id长度不一致: {lengths}")

    print("\n  建议:")
    print("  1. 确保child process查询时使用 str(token_id)")
    print("  2. 检查token_id是否被意外截断或修改")
    print("  3. 在_load_shared_ws_snapshot()中添加详细的调试日志")

if __name__ == "__main__":
    try:
        diagnose_token_format()
    except KeyboardInterrupt:
        print("\n\n⏹️  诊断中断")
