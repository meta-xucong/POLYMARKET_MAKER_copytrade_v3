#!/bin/bash

# 获取脚本所在目录（V2文件夹）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "======================================================"
echo "聚合器诊断脚本"
echo "======================================================"
echo ""
echo "工作目录: $SCRIPT_DIR"
echo ""

# 1. 检查代码版本
echo "【1】检查代码库中的版本..."
cd POLYMARKET_MAKER_AUTO

if grep -q 'elif event_type in ("book", "tick")' poly_maker_autorun.py; then
    echo "✅ 代码库包含book/tick事件处理"
else
    echo "❌ 代码库不包含book/tick事件处理"
fi

if grep -q '处理book/tick事件' poly_maker_autorun.py; then
    echo "✅ 代码库包含book/tick调试日志"
else
    echo "❌ 代码库不包含book/tick调试日志"
fi

if grep -q 'VERSION.*支持book/tick事件处理' poly_maker_autorun.py; then
    echo "✅ 代码库包含版本标识"
else
    echo "❌ 代码库不包含版本标识"
fi

if grep -q 'min_loop_interval = 0.1' POLYMARKET_MAKER/Volatility_arbitrage_run.py; then
    echo "✅ 代码库包含子进程循环优化 (0.1秒)"
else
    echo "❌ 代码库子进程循环仍为旧值 (1.0秒)"
fi

echo ""

# 2. 检查运行中的进程
echo "【2】检查运行中的聚合器进程..."
AGGREGATOR_PID=$(ps aux | grep -E 'poly_maker_autorun\.py|python.*autorun' | grep -v grep | head -1 | awk '{print $2}')

if [ -z "$AGGREGATOR_PID" ]; then
    echo "❌ 未找到运行中的聚合器进程"
else
    echo "✅ 找到聚合器进程 PID: $AGGREGATOR_PID"
    echo "   启动时间: $(ps -p $AGGREGATOR_PID -o lstart=)"
    echo "   运行时长: $(ps -p $AGGREGATOR_PID -o etime=)"
fi

echo ""

# 3. 检查最新日志
echo "【3】检查最新运行日志..."

# 查找最新的日志文件（在POLYMARKET_MAKER_AUTO目录下）
LATEST_LOG=$(find . -name "*.log" -type f -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | cut -d' ' -f2-)

if [ -z "$LATEST_LOG" ]; then
    echo "⚠️  未找到日志文件"
else
    echo "最新日志: $LATEST_LOG"
    echo ""

    # 检查是否有版本标识
    if tail -1000 "$LATEST_LOG" 2>/dev/null | grep -q "VERSION.*支持book/tick"; then
        echo "✅ 日志中找到版本标识 (运行的是新版本)"
    else
        echo "❌ 日志中未找到版本标识 (可能运行的是旧版本)"
    fi

    # 检查是否有book/tick处理日志
    BOOK_TICK_COUNT=$(tail -1000 "$LATEST_LOG" 2>/dev/null | grep -c "处理book/tick事件" || echo "0")
    if [ "$BOOK_TICK_COUNT" -gt 0 ]; then
        echo "✅ 日志中找到book/tick事件处理 ($BOOK_TICK_COUNT 条)"
        tail -5 "$LATEST_LOG" 2>/dev/null | grep "处理book/tick事件"
    else
        echo "❌ 日志中未找到book/tick事件处理"
    fi

    # 检查最近的update频率
    echo ""
    echo "最近的update频率统计:"
    tail -100 "$LATEST_LOG" 2>/dev/null | grep -E "updates=.*\(" | tail -5
fi

echo ""
echo "======================================================"
echo "诊断建议:"
echo "======================================================"

if [ -z "$AGGREGATOR_PID" ]; then
    echo "⚠️  聚合器进程未运行，需要启动"
elif ! tail -1000 "$LATEST_LOG" 2>/dev/null | grep -q "VERSION.*支持book/tick"; then
    echo "⚠️  聚合器进程正在运行旧版本代码"
    echo ""
    echo "建议操作:"
    echo "  1. 停止旧进程: kill $AGGREGATOR_PID"
    echo "  2. 等待进程退出: sleep 5"
    echo "  3. 启动新版本: cd $SCRIPT_DIR/POLYMARKET_MAKER_AUTO"
    echo "  4. 运行: python3 poly_maker_autorun.py [your-args]"
    echo ""
    echo "  或使用 restart_aggregator.sh 脚本自动重启"
else
    echo "✅ 聚合器正在运行最新版本"
    echo ""
    echo "如果update频率仍然很低 (2.0/min):"
    echo "  - 检查WebSocket连接是否正常"
    echo "  - 检查是否有网络问题或API限制"
    echo "  - 查看完整日志确认是否有错误信息"
    echo "  - 确认子进程也已重启（子进程需要新代码才能提高频率）"
fi

echo ""
