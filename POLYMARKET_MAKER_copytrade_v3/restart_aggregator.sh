#!/bin/bash

# 获取脚本所在目录（V2文件夹）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "======================================================"
echo "聚合器重启脚本"
echo "======================================================"
echo ""
echo "工作目录: $SCRIPT_DIR"
echo ""

cd POLYMARKET_MAKER_AUTO

# 1. 查找并停止旧进程
echo "【1】查找运行中的聚合器进程..."
AGGREGATOR_PID=$(ps aux | grep -E 'poly_maker_autorun\.py|python.*autorun' | grep -v grep | head -1 | awk '{print $2}')

if [ -z "$AGGREGATOR_PID" ]; then
    echo "ℹ️  未找到运行中的聚合器进程"
else
    echo "找到进程 PID: $AGGREGATOR_PID"
    echo "停止进程..."
    kill $AGGREGATOR_PID

    # 等待进程退出
    for i in {1..10}; do
        if ! ps -p $AGGREGATOR_PID > /dev/null 2>&1; then
            echo "✅ 进程已停止"
            break
        fi
        echo "等待进程退出... ($i/10)"
        sleep 1
    done

    # 如果还没退出，强制杀死
    if ps -p $AGGREGATOR_PID > /dev/null 2>&1; then
        echo "⚠️  进程未正常退出，使用 SIGKILL..."
        kill -9 $AGGREGATOR_PID
        sleep 1
    fi
fi

echo ""

# 2. 验证代码版本
echo "【2】验证代码版本..."
VERSION_CHECK=0

if grep -q 'VERSION.*支持book/tick事件处理' poly_maker_autorun.py; then
    echo "✅ 代码包含版本标识（聚合器）"
    VERSION_CHECK=$((VERSION_CHECK + 1))
else
    echo "❌ 代码不包含版本标识，请确认代码已更新"
fi

if grep -q 'min_loop_interval = 0.1' POLYMARKET_MAKER/Volatility_arbitrage_run.py; then
    echo "✅ 代码包含子进程循环优化 (0.1秒)"
    VERSION_CHECK=$((VERSION_CHECK + 1))
else
    echo "❌ 子进程循环仍为旧值 (1.0秒)"
fi

if [ $VERSION_CHECK -lt 2 ]; then
    echo ""
    echo "❌ 代码版本检查未通过，请先更新代码"
    exit 1
fi

echo ""

# 3. 提示启动命令
echo "【3】准备启动新版本..."
echo ""
echo "请使用以下命令启动聚合器:"
echo ""
echo "  cd $SCRIPT_DIR/POLYMARKET_MAKER_AUTO"
echo "  python3 poly_maker_autorun.py [your-args] > aggregator.log 2>&1 &"
echo ""
echo "或者，如果你有特定的启动脚本，请使用你的启动脚本"
echo ""
echo "启动后，使用以下命令验证版本:"
echo "  tail -50 aggregator.log | grep VERSION"
echo ""
echo "应该看到: [VERSION] 支持book/tick事件处理 (2026-01-21)"
echo ""
echo "注意: 需要重启所有子进程才能应用min_loop_interval优化！"
echo ""
echo "======================================================"
