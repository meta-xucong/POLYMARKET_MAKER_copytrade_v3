
# Volatility_arbitrage_strategy.py
# 极简策略（扩展版）：
#   - 在窗口内跟踪价格高点，若当前价相对高点下跌超过 drop_pct，触发 BUY；
#   - 可选 buy_price_threshold 作为额外保底条件；
#   - 持仓后以 profit_pct（默认 5%）的涨幅目标触发 SELL；
#   - 仅产出信号，不负责 size / 精度 / 下单执行。需上游成交回调推进状态。

from __future__ import annotations
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
import time
from typing import Optional, Dict, Any, Deque, Tuple


class ActionType(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"   # 保留类型以便状态查询时使用


@dataclass
class StrategyConfig:
    token_id: str
    buy_price_threshold: Optional[float] = None        # 触发买入的目标价格（可选）
    max_buy_price: Optional[float] = None              # 买入价格上限，超过此价格不买入（可选）
    profit_ratio: float = 0.05                        # 兼容旧字段，默认 5%

    # 新增参数：基于窗口的跌幅/涨幅监控
    drop_window_minutes: float = 10.0
    drop_pct: float = 0.05
    profit_pct: Optional[float] = 0.05
    max_history_points: int = 600

    # 卖出后动态抬升跌幅阈值（默认关闭，如需开启请设定步长）
    enable_incremental_drop_pct: bool = False
    incremental_drop_pct_step: float = 0.001
    incremental_drop_pct_cap: float = 0.20

    # 轻量防抖：同一方向的“待确认”状态下不重复发信号
    disable_duplicate_signal: bool = True

    # maker 模式下可禁用 SELL 信号，由上游自行处理退出
    disable_sell_signals: bool = False

    # 可选价域守门（避免极端边界价误触）
    min_price: Optional[float] = 0.0
    max_price: Optional[float] = 1.0

    # 交易所最小市场单规模（用于识别“尘埃仓位”并视为已清空）
    min_market_order_size: Optional[float] = None


@dataclass
class Action:
    action: ActionType
    token_id: str
    reason: str
    ref_price: float                 # 触发时参考的行情价：BUY 用 best_bid，SELL 用 best_bid
    target_price: Optional[float] = None  # SELL 时为 entry * (1 + profit_pct)
    extra: Dict[str, Any] = field(default_factory=dict)


class VolArbStrategy:
    """
    极简策略状态机（单 token）——严格“确认后换态”版：
      - FLAT → 当 best_bid <= buy_price_threshold 时，发出 BUY；
      - LONG → 当 best_bid >= entry_price * (1 + profit_ratio) 时，发出 SELL。

    注：
      * 本策略不处理 size/精度/下单，只产生信号，由上游执行。
      * 发出 BUY/SELL 信号后进入“待确认”状态，必须由上游在成交后调用
        on_buy_filled / on_sell_filled 才会推进状态机；on_reject() 解除待确认。
    """

    def __init__(self, config: StrategyConfig):
        self.cfg = config
        # profit_pct 与旧字段 profit_ratio 对齐
        if self.cfg.profit_pct is None:
            self.cfg.profit_pct = self.cfg.profit_ratio
        else:
            self.cfg.profit_ratio = self.cfg.profit_pct

        self._state: str = "FLAT"  # or "LONG"
        self._entry_price: Optional[float] = None
        self._awaiting: Optional[ActionType] = None  # BUY/SELL
        self._last_signal: Optional[ActionType] = None
        self._position_size: Optional[float] = None

        # 价格历史缓存：[(timestamp, price)]
        self._price_history: Deque[Tuple[float, float]] = deque()
        self._history_window_seconds: float = self.cfg.drop_window_minutes * 60.0

        # 跌幅统计
        self._window_high_price: Optional[float] = None
        self._window_low_price: Optional[float] = None
        self._max_drop_ratio: Optional[float] = None
        self._current_drop_ratio: Optional[float] = None

        # 最近行情记录
        self._last_tick_ts: Optional[float] = None
        self._last_best_ask: Optional[float] = None
        self._last_best_bid: Optional[float] = None

        # 状态字段
        self._last_buy_price: Optional[float] = None
        self._last_sell_price: Optional[float] = None
        self._manual_stop: bool = False
        self._manual_stop_reason: Optional[str] = None
        self._last_reject_reason: Optional[str] = None
        self._sell_only: bool = False
        self._sell_only_reason: Optional[str] = None

        # 记录跌幅阈值的初始值（用于动态递增的下限）
        self._initial_drop_pct: float = max(self.cfg.drop_pct, 0.0)

        # 最小可交易规模（用于卖出回调时识别尘埃仓位）
        self._min_market_order_size: Optional[float] = self._normalize_min_market_order_size(
            getattr(self.cfg, "min_market_order_size", None)
        )

    # ------------------------ 上游主调用：每笔行情快照 ------------------------
    def on_tick(
        self,
        best_ask: float,
        best_bid: float,
        ts: Optional[float] = None,
    ) -> Optional[Action]:
        """
        上游每次行情推送调用。返回 Action（BUY/SELL）或 None（无动作）。
        """
        if ts is None:
            ts = time.time()

        # 价域守门（如不需要可在 cfg 设置为 None）
        if self.cfg.min_price is not None and (best_ask < self.cfg.min_price or best_bid < self.cfg.min_price):
            return None
        if self.cfg.max_price is not None and (best_ask > self.cfg.max_price or best_bid > self.cfg.max_price):
            return None

        self._last_tick_ts = ts
        self._last_best_ask = best_ask
        self._last_best_bid = best_bid

        price_for_drop = self._prepare_price_history(ts, (best_bid + best_ask) / 2)

        if self._manual_stop:
            return None

        if self._sell_only and self._state == "FLAT":
            return None

        if self._state == "FLAT":
            return self._maybe_buy(price_for_drop, best_bid, ts)

        elif self._state == "LONG":
            return self._maybe_sell(best_bid, ts)

        return None

    # ------------------------ 买入/卖出触发判定 ------------------------
    def _maybe_buy(self, drop_price: float, best_bid: float, ts: Optional[float]) -> Optional[Action]:
        if self._awaiting == ActionType.BUY and self.cfg.disable_duplicate_signal:
            return None  # 等待上游确认，不重复发 BUY

        drop_trigger = False
        drop_ratio: Optional[float] = None
        window_high: Optional[float] = self._window_high_price

        if len(self._price_history) > 1 and window_high is not None and window_high > 0:
            drop_ratio = (window_high - drop_price) / window_high
            drop_trigger = drop_ratio >= self.cfg.drop_pct

        threshold_trigger = (
            self.cfg.buy_price_threshold is not None
            and best_bid <= self.cfg.buy_price_threshold
        )

        if not drop_trigger and not threshold_trigger:
            return None

        # 买入价格上限检查：如果当前价格超过上限，不买入
        if self.cfg.max_buy_price is not None and best_bid >= self.cfg.max_buy_price:
            return None  # 价格过高，不买入

        reasons = []
        extra = {
            "history_points": len(self._price_history),
            "drop_window_minutes": self.cfg.drop_window_minutes,
            "drop_triggered": drop_trigger,
            "threshold_triggered": threshold_trigger,
        }


        if drop_trigger and drop_ratio is not None and window_high is not None:
            reasons.append(
                f"drop({drop_ratio:.4f}) ≥ threshold({self.cfg.drop_pct:.4f}) from high({window_high:.5f})"
            )
            extra.update(
                {
                    "drop_ratio": drop_ratio,
                    "window_high": window_high,
                    "drop_price": drop_price,
                }
            )

        if threshold_trigger and self.cfg.buy_price_threshold is not None:
            reasons.append(
                f"best_bid({best_bid:.5f}) ≤ buy_threshold({self.cfg.buy_price_threshold:.5f})"
            )

        act = Action(
            action=ActionType.BUY,
            token_id=self.cfg.token_id,
            reason="; ".join(reasons) or "drop trigger",
            ref_price=best_bid,
            extra=extra,
        )
        self._last_signal = ActionType.BUY
        self._awaiting = ActionType.BUY  # 必须等待上游 on_buy_filled() 确认
        return act

    def _maybe_sell(self, best_bid: float, ts: Optional[float]) -> Optional[Action]:
        if getattr(self.cfg, "disable_sell_signals", False):
            return None

        if self._entry_price is None:
            return None  # 防守式检查

        if self._awaiting == ActionType.SELL and self.cfg.disable_duplicate_signal:
            return None  # 等待上游确认，不重复发 SELL

        profit_pct = self.cfg.profit_pct if self.cfg.profit_pct is not None else self.cfg.profit_ratio
        target = self._entry_price * (1.0 + profit_pct)
        gain_ratio: Optional[float] = None
        if self._entry_price > 0:
            gain_ratio = (best_bid - self._entry_price) / self._entry_price

        if best_bid >= target:
            reason = (
                f"best_bid({best_bid:.5f}) ≥ target({target:.5f}) = entry({self._entry_price:.5f}) * (1+{profit_pct:.4f})"
            )
            extra = {
                "gain_ratio": gain_ratio,
                "profit_pct": profit_pct,
            }
            act = Action(
                action=ActionType.SELL,
                token_id=self.cfg.token_id,
                reason=reason,
                ref_price=best_bid,
                target_price=target,
                extra=extra,
            )
            self._last_signal = ActionType.SELL
            self._awaiting = ActionType.SELL  # 必须等待上游 on_sell_filled() 确认
            return act
        return None

    def _prepare_price_history(self, ts: float, price: float) -> float:
        self._price_history.append((ts, price))
        self._trim_history(ts)
        return price

    def _trim_history(self, ts: float) -> None:
        window = self._history_window_seconds
        while self._price_history and ts - self._price_history[0][0] > window:
            self._price_history.popleft()
        while self._price_history and len(self._price_history) > self.cfg.max_history_points:
            self._price_history.popleft()
        if self._price_history:
            self._update_drop_metrics()
        else:
            self._reset_drop_metrics()

    def _reset_drop_metrics(self) -> None:
        self._window_high_price = None
        self._window_low_price = None
        self._max_drop_ratio = None
        self._current_drop_ratio = None

    def _update_drop_metrics(self) -> None:
        if not self._price_history:
            self._reset_drop_metrics()
            return

        # ✅ 修复：使用滚动最大值法，确保"跌幅"真的是从高点跌到低点，而不是涨幅
        # 遍历价格历史，维护滚动最高价，只计算从历史高点到之后的低点的跌幅
        running_max: Optional[float] = None
        max_drop = 0.0
        window_high: Optional[float] = None
        window_low: Optional[float] = None

        for _, px in self._price_history:
            # 更新窗口最高价和最低价（用于显示）
            if window_high is None or px > window_high:
                window_high = px
            if window_low is None or px < window_low:
                window_low = px

            # 更新滚动最高价
            if running_max is None or px > running_max:
                running_max = px

            # 计算从历史最高点到当前价格的跌幅
            if running_max is not None and running_max > 0:
                drop = (running_max - px) / running_max
                if drop > max_drop:
                    max_drop = drop

        if window_high is None:
            self._reset_drop_metrics()
            return

        # 当前跌幅：从窗口最高价到当前价格
        current_price = self._price_history[-1][1]
        if window_high > 0 and current_price is not None:
            current_drop = (window_high - current_price) / window_high
        else:
            current_drop = 0.0

        self._window_high_price = window_high
        self._window_low_price = window_low
        self._max_drop_ratio = max_drop
        self._current_drop_ratio = current_drop

    # ------------------------ 上游回调：成交/被拒 ------------------------
    def on_buy_filled(
        self,
        avg_price: float,
        size: Optional[float] = None,
        *,
        total_position: Optional[float] = None,
    ) -> None:
        """上游在实际买入成交后回调。

        :param avg_price: 本次成交的平均买入价。
        :param size: 上游回报的成交份数。缺省视为“新增仓位”。
        :param total_position: 上游若能提供买入后的总持仓，优先使用该值。
        """
        prior_size = 0.0
        if self._position_size is not None:
            try:
                prior_size = max(float(self._position_size), 0.0)
            except (TypeError, ValueError):
                prior_size = 0.0

        def _safe_non_negative(value: Optional[float]) -> Optional[float]:
            if value is None:
                return None
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                return None
            return numeric if numeric > 0 else (0.0 if numeric >= 0 else None)

        added_size: float = 0.0
        new_total: Optional[float] = None

        explicit_total = _safe_non_negative(total_position)
        if explicit_total is not None:
            new_total = explicit_total
            added_size = max(new_total - prior_size, 0.0)
        else:
            filled_amt = _safe_non_negative(size)
            if filled_amt is not None:
                added_size = filled_amt
                if prior_size > 0:
                    new_total = prior_size + filled_amt
                else:
                    new_total = filled_amt

        if new_total is not None and new_total > 0:
            if prior_size > 0 and added_size > 0 and self._entry_price is not None:
                total_for_weight = prior_size + added_size if explicit_total is None else new_total
                if total_for_weight <= 0:
                    total_for_weight = new_total
                self._entry_price = (
                    float(self._entry_price) * prior_size + avg_price * added_size
                ) / max(total_for_weight, 1e-12)
            elif prior_size <= 0:
                self._entry_price = avg_price
            else:
                # 无新增仓位（或旧成本缺失），沿用已有成本
                self._entry_price = (
                    self._entry_price if self._entry_price is not None else avg_price
                )
            self._position_size = new_total if new_total > 0 else None
        else:
            # 回退逻辑：若无法解析新仓位，则至少记录最新价格
            self._entry_price = avg_price
            if size is not None:
                filled_amt = _safe_non_negative(size)
                if filled_amt is not None:
                    self._position_size = filled_amt

        self._last_buy_price = avg_price
        self._state = "LONG"
        self._awaiting = None
        self._last_reject_reason = None

    def on_sell_filled(
        self,
        avg_price: Optional[float] = None,
        *,
        size: Optional[float] = None,
        remaining: Optional[float] = None,
    ) -> None:
        """上游在实际卖出成交后回调。

        :param avg_price: 最近一次卖出的平均价格（若有成交）。
        :param size: 本次卖出的实际数量（可选，便于计算剩余仓位）。
        :param remaining: 当前剩余未卖出的仓位（可选，优先使用）。
        """

        eps = 1e-4

        remaining_size: Optional[float] = None
        if remaining is not None:
            try:
                remaining_size = max(float(remaining), 0.0)
            except (TypeError, ValueError):
                remaining_size = None
        elif size is not None and self._position_size is not None:
            try:
                remaining_size = max(self._position_size - float(size), 0.0)
            except (TypeError, ValueError):
                remaining_size = None

        if remaining_size is not None:
            min_order_size = self._min_market_order_size
            dust_floor = eps
            if min_order_size is not None:
                dust_floor = max(min_order_size, dust_floor)
            if remaining_size <= dust_floor:
                remaining_size = None

        if remaining_size is None:
            self._state = "FLAT"
            self._entry_price = None
            self._position_size = None
            self._awaiting = None
        else:
            self._position_size = remaining_size
            self._state = "LONG"
            self._awaiting = ActionType.SELL

        if remaining_size is None and self._awaiting is not None:
            # 清理非 SELL 的等待状态，确保重新触发买入
            self._awaiting = None

        if remaining_size is None:
            self._price_history.clear()
            self._reset_drop_metrics()

        if avg_price is not None:
            self._last_sell_price = avg_price
        elif self._state == "FLAT":
            # 如果当前 tick 有最新 best_bid 则优先使用
            self._last_sell_price = self._last_best_bid

        if self._state == "FLAT":
            self._maybe_increment_drop_pct()

        self._last_reject_reason = None

    def on_reject(self, reason: Optional[str] = None) -> None:
        """上游在下单失败/被拒绝时回调，解除“待确认”以便重新发信号。"""
        self._awaiting = None
        self._last_reject_reason = reason

    def sync_position(
        self, total_position: Optional[float], *, ref_price: Optional[float] = None
    ) -> None:
        """显式以链上仓位为准同步本地状态。

        * total_position > dust_floor 时：视为持仓，切换为 SELL 阻塞，避免重复买入。
        * total_position <= dust_floor 时：视为清仓，解除等待并允许重新买入。
        """

        eps = 1e-4

        numeric_pos: Optional[float] = None
        if total_position is not None:
            try:
                numeric_pos = max(float(total_position), 0.0)
            except (TypeError, ValueError):
                numeric_pos = None

        dust_floor = eps
        if self._min_market_order_size is not None:
            dust_floor = max(self._min_market_order_size, dust_floor)

        if numeric_pos is None or numeric_pos <= dust_floor:
            # 链上视为已清仓：放行后续买入
            self._state = "FLAT"
            self._position_size = None
            self._awaiting = None
            self._entry_price = None
            self._last_reject_reason = None
            self._maybe_increment_drop_pct()
            return

        # 链上存在可交易持仓：保持 SELL 阻塞，防止重复买入
        self._position_size = numeric_pos
        self._state = "LONG"
        self._awaiting = ActionType.SELL
        if ref_price is not None:
            self._entry_price = ref_price
        elif self._entry_price is None:
            if self._last_buy_price is not None:
                self._entry_price = self._last_buy_price
        self._last_reject_reason = None

    def mark_awaiting(self, action: Optional[ActionType]) -> None:
        """显式设置等待状态（用于外部状态同步时标记 SELL 等流程）。"""

        self._awaiting = action

    def stop(self, reason: Optional[str] = None) -> None:
        """手动暂停策略或在市场关闭时调用。"""
        self._manual_stop = True
        self._manual_stop_reason = reason
        self._awaiting = None

    def resume(self) -> None:
        """恢复策略运行。"""
        self._manual_stop = False
        self._manual_stop_reason = None

    def enable_sell_only(self, reason: Optional[str] = None) -> None:
        """仅允许卖出，不再触发买入信号。"""
        self._sell_only = True
        self._sell_only_reason = reason

    def disable_sell_only(self) -> None:
        """恢复买入能力。"""
        self._sell_only = False
        self._sell_only_reason = None

    # ------------------------ 实用方法 ------------------------
    def update_params(
        self,
        *,
        buy_price_threshold: Optional[float] = None,
        profit_ratio: Optional[float] = None,
        drop_window_minutes: Optional[float] = None,
        drop_pct: Optional[float] = None,
        profit_pct: Optional[float] = None,
        max_history_points: Optional[int] = None,
        enable_incremental_drop_pct: Optional[bool] = None,
        incremental_drop_pct_step: Optional[float] = None,
        incremental_drop_pct_cap: Optional[float] = None,
        min_market_order_size: Optional[float] = None,
    ) -> None:
        if buy_price_threshold is not None:
            self.cfg.buy_price_threshold = buy_price_threshold
        if profit_ratio is not None:
            self.cfg.profit_ratio = profit_ratio
            self.cfg.profit_pct = profit_ratio
        if profit_pct is not None:
            self.cfg.profit_pct = profit_pct
            self.cfg.profit_ratio = profit_pct
        if drop_window_minutes is not None:
            self.cfg.drop_window_minutes = drop_window_minutes
            self._history_window_seconds = drop_window_minutes * 60.0
            if self._last_tick_ts is not None:
                self._trim_history(self._last_tick_ts)
        if drop_pct is not None:
            self.cfg.drop_pct = drop_pct
            self._initial_drop_pct = max(drop_pct, 0.0)
        if max_history_points is not None:
            self.cfg.max_history_points = max(1, int(max_history_points))
            if self._last_tick_ts is not None:
                self._trim_history(self._last_tick_ts)
        if enable_incremental_drop_pct is not None:
            self.cfg.enable_incremental_drop_pct = bool(enable_incremental_drop_pct)
        if incremental_drop_pct_step is not None:
            self.cfg.incremental_drop_pct_step = float(incremental_drop_pct_step)
        if incremental_drop_pct_cap is not None:
            self.cfg.incremental_drop_pct_cap = float(incremental_drop_pct_cap)
        if min_market_order_size is not None:
            self.cfg.min_market_order_size = min_market_order_size
            self._min_market_order_size = self._normalize_min_market_order_size(
                min_market_order_size
            )

    def sell_trigger_price(self) -> Optional[float]:
        if self._entry_price is None:
            return None
        profit_pct = self.cfg.profit_pct if self.cfg.profit_pct is not None else self.cfg.profit_ratio
        return self._entry_price * (1.0 + profit_pct)

    def status(self) -> Dict[str, Any]:
        return {
            "state": self._state,
            "awaiting": self._awaiting,
            "entry_price": self._entry_price,
            "sell_trigger": self.sell_trigger_price(),
            "position_size": self._position_size,
            "last_signal": self._last_signal,
            "last_buy_price": self._last_buy_price,
            "last_sell_price": self._last_sell_price,
            "price_history_len": len(self._price_history),
            "manual_stop": self._manual_stop,
            "manual_stop_reason": self._manual_stop_reason,
            "sell_only": self._sell_only,
            "sell_only_reason": self._sell_only_reason,
            "last_reject_reason": self._last_reject_reason,
            "last_tick": {
                "ts": self._last_tick_ts,
                "best_ask": self._last_best_ask,
                "best_bid": self._last_best_bid,
            },
            "drop_stats": {
                "window_high": self._window_high_price,
                "window_low": self._window_low_price,
                "max_drop_ratio": self._max_drop_ratio,
                "current_drop_ratio": self._current_drop_ratio,
                "window_seconds": self._history_window_seconds,
            },
            "config": {
                "token_id": self.cfg.token_id,
                "buy_price_threshold": self.cfg.buy_price_threshold,
                "profit_ratio": self.cfg.profit_ratio,
                "drop_window_minutes": self.cfg.drop_window_minutes,
                "drop_pct": self.cfg.drop_pct,
                "profit_pct": self.cfg.profit_pct,
                "max_history_points": self.cfg.max_history_points,
                "price_band": (self.cfg.min_price, self.cfg.max_price),
                "disable_duplicate_signal": self.cfg.disable_duplicate_signal,
                "enable_incremental_drop_pct": self.cfg.enable_incremental_drop_pct,
                "incremental_drop_pct_step": self.cfg.incremental_drop_pct_step,
                "incremental_drop_pct_cap": self.cfg.incremental_drop_pct_cap,
                "min_market_order_size": self._min_market_order_size,
            },
        }

    # ------------------------ 内部辅助 ------------------------
    def _maybe_increment_drop_pct(self) -> None:
        if not getattr(self.cfg, "enable_incremental_drop_pct", False):
            return
        step = max(getattr(self.cfg, "incremental_drop_pct_step", 0.0), 0.0)
        if step <= 0:
            return
        current = max(self.cfg.drop_pct, self._initial_drop_pct)
        cap = getattr(self.cfg, "incremental_drop_pct_cap", None)
        if cap is not None:
            cap = max(cap, self._initial_drop_pct)
            current = min(current, cap)
            new_drop = min(current + step, cap)
        else:
            new_drop = current + step
        self.cfg.drop_pct = new_drop

    @staticmethod
    def _normalize_min_market_order_size(value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        return numeric if numeric > 0 else None
