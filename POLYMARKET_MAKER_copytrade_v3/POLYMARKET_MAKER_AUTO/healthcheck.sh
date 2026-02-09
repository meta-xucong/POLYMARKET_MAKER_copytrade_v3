#!/bin/bash
# Polymaker Autorun 健康检查脚本
# 用法：crontab -e 添加：
# */5 * * * * /path/to/healthcheck.sh

AUTORUN_DIR="/home/trader/polymarket_api/POLYMARKET_MAKER_copytrade_v2/POLYMARKET_MAKER_AUTO"
WS_CACHE="$AUTORUN_DIR/data/ws_cache.json"
LOG_FILE="$AUTORUN_DIR/healthcheck.log"
PYTHON_BIN="/root/.pyenv/versions/poly312/bin/python"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# 检查 autorun 是否运行
if ! pgrep -f "poly_maker_autorun.py" > /dev/null; then
    log "ERROR: autorun 未运行，尝试重启..."
    cd "$AUTORUN_DIR" || exit 1
    nohup $PYTHON_BIN poly_maker_autorun.py > "autorun_$(date +%Y%m%d_%H%M%S).log" 2>&1 &
    log "INFO: autorun 已重启，PID=$!"
    exit 0
fi

# 检查 ws_cache.json 是否更新
if [ -f "$WS_CACHE" ]; then
    CACHE_AGE=$(( $(date +%s) - $(stat -c %Y "$WS_CACHE") ))
    if [ $CACHE_AGE -gt 120 ]; then
        log "WARN: ws_cache.json 已 ${CACHE_AGE}秒 未更新，可能异常"
    fi
else
    log "ERROR: ws_cache.json 不存在"
fi

# 检查子进程数量
CHILD_COUNT=$(pgrep -f "Volatility_arbitrage_run.py" | wc -l)
log "INFO: 当前运行子进程: $CHILD_COUNT 个"

# 检查内存使用
MEM_PERCENT=$(ps aux | grep poly_maker_autorun | grep -v grep | awk '{print $4}')
log "INFO: autorun 内存使用: ${MEM_PERCENT}%"
