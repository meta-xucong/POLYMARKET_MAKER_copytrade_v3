# JSON 配置字段说明

本文件汇总 `POLYMARKET_MAKER/config/` 目录下各个 JSON 配置的字段含义、推荐取值与格式要求，方便在自动化脚本或单市场策略运行前快速校验参数。

## run_params.json —— 单市场运行参数
用于 `Volatility_arbitrage_run.py` 等单市场脚本。所有百分比字段均支持写成 0~1 之间的小数；若填写大于 1 的数值会被视作百分比并自动除以 100（如填写 5 代表 5%）。【F:POLYMARKET_MAKER/Volatility_arbitrage_run.py†L138-L147】

| 字段 | 作用 | 类型/格式 | 推荐设置 |
| --- | --- | --- | --- |
| `market_url` | 目标市场或子问题的 URL/slug；为空则无法启动。 | 字符串，支持完整链接或 slug | 必填，保持无空格。 |
| `timezone` | 人工指定市场时区，缺失时默认 `America/New_York`。 | IANA 时区名称 | 建议与市场实际时区一致。 |
| `deadline_override_ts` | 人工指定的截止时间戳；设置后覆盖自动识别的截止时间。 | UNIX 秒级或毫秒级时间戳 | 仅当自动解析不可靠时填写。 |
| `disable_deadline_checks` | 是否完全跳过截止时间校验/提示。 | 布尔 | 仅在明确无需截止时间时启用。 |
| `deadline_policy.override_choice` | 选择常用截止时间模板（1=12:00 PM ET，2=23:59 ET，3=00:00 UTC，4=不设定）。 | 整数 1~4 或 `null` | 不填则沿用自动解析；需要人工覆盖时填写编号。 |
| `deadline_policy.disable_deadline` | 是否强制清空截止时间。 | 布尔 | 特殊诊断场景下使用。 |
| `deadline_policy.timezone` | 应用默认截止时间时的时区。 | IANA 时区名称 | 与 `default_deadline` 保持一致。 |
| `deadline_policy.default_deadline.time` | 当需要回退到默认截止时间时使用的“时:分”字符串，可写 `HH:MM` 或 `HH.MM`。 | 字符串（24 小时制） | 例如 `12:59`。 |
| `deadline_policy.default_deadline.timezone` | 默认截止时间对应的时区。 | IANA 时区名称 | 例如 `America/New_York`。 |
| `side` | 下单方向，`YES` 或 `NO`；缺失时会尝试使用 `preferred_side` 或 `highlight_sides[0]`，仍无法确定或值非法时直接报错退出。【F:POLYMARKET_MAKER/Volatility_arbitrage_run.py†L153-L172】【F:POLYMARKET_MAKER/Volatility_arbitrage_run.py†L1941-L1947】 | 字符串（不区分大小写） | 必填；建议与目标 token 一致。 |
| `order_size` | 手动指定份额，配合 `order_size_is_target` 判断含义。 | 正数 | 留空则按 $1 等额推算。 |
| `order_size_is_target` | 为真时将 `order_size` 视为目标总持仓，否则视为单笔下单量。【F:POLYMARKET_MAKER/Volatility_arbitrage_run.py†L1940-L1950】 | 布尔 | 需要限制总敞口时设为 `true`。 |
| `sell_mode` | 卖出挂单策略：`aggressive` 更靠近盘口，`conservative` 稍远。 | 枚举字符串 | `aggressive`/`conservative`。 |
| `buy_price_threshold` | 仅当买入价格低于该阈值（0~1，小数形式）时才下单。 | 浮点数 | 常用 0.1~0.9；留空则按策略默认。 |
| `drop_window_minutes` | 计算下跌幅度的滑动窗口长度（分钟）。 | 浮点数 | 推荐 10~120 之间。 |
| `drop_pct` | 触发买入的相对高点跌幅阈值；>1 会按百分比自动换算。【F:POLYMARKET_MAKER/Volatility_arbitrage_run.py†L1960-L1970】 | 浮点比例 | 例如 `0.05`（5%）。 |
| `profit_pct` | 止盈阈值（收益率）。>1 同样视为百分比输入。【F:POLYMARKET_MAKER/Volatility_arbitrage_run.py†L1960-L1970】 | 浮点比例 | 例如 `0.01`~`0.1`。 |
| `enable_incremental_drop_pct` | 是否在卖出后逐步提高下一次买入的跌幅阈值。 | 布尔 | 与 `incremental_drop_pct_step` 搭配使用。 |
| `incremental_drop_pct_step` | 每次卖出后提升的跌幅阈值步长；0~1 或百分比形式。【F:POLYMARKET_MAKER/Volatility_arbitrage_run.py†L1965-L1979】 | 浮点比例 | 例如 `0.002`（0.2%）。 |
| `stagnation_window_minutes` | 价格停滞检测窗口（分钟）；窗口内价格波动低于阈值时触发退出/清仓。 | 浮点数 | 默认 120；设为 `<=0` 可禁用。 |
| `stagnation_pct` | 价格停滞阈值（波动比例）；>1 会按百分比换算。 | 浮点比例 | 默认 0；例如 `0.001`（0.1%）。 |
| `no_event_exit_minutes` | 启动后 N 分钟内完全无行情时自动退出。 | 浮点数 | 默认 10；设为 `<=0` 可禁用。 |
| `sell_inactive_hours` | 卖出挂单空窗时间（小时），超过该时长未产生卖出动作则释放做市进程但保留挂单。 | 浮点数 | 默认 5；设为 `<=0` 可禁用。 |
| `countdown.minutes_before_end` | 距离市场结束 N 分钟时切换为“仅卖出”模式。 | 浮点数 | 常用 60~360；为空则尝试用绝对时间。 |
| `countdown.absolute_time` | 直接指定倒计时开始的绝对时间，支持时间戳、ISO 字符串（`YYYY-MM-DDTHH:MM:SSZ`）或简单日期/日期时间文本。【F:POLYMARKET_MAKER/Volatility_arbitrage_run.py†L436-L476】【F:POLYMARKET_MAKER/Volatility_arbitrage_run.py†L1981-L2019】 | 数字或字符串 | 优先写 UTC/带时区的 ISO 格式。 |
| `countdown.timezone` | 当 `absolute_time` 只写日期/无时区时，用于推断时区。 | IANA 时区名称 | 与市场时区一致即可。 |

## global_config.json —— 自动化调度与路径
供 `poly_maker_autorun.py` 使用，控制 copytrade token 轮询/调度循环、文件路径与重试策略。【F:poly_maker_autorun.py†L32-L59】【F:poly_maker_autorun.py†L117-L216】

| 字段 | 作用 | 类型/格式 | 推荐设置 |
| --- | --- | --- | --- |
| `scheduler.max_concurrent_jobs` | 同时运行的子进程/任务上限。 | 整数 | 根据机器核数调整，1~4 为宜。 |
| `scheduler.command_poll_seconds` | 主循环轮询/调度间隔秒数。 | 浮点 | 1~10 秒。 |
| `scheduler.copytrade_poll_seconds` | 轮询 copytrade token 文件的间隔秒数。 | 浮点 | 10~60 秒。 |
| `paths.log_directory` | maker 波段子进程日志目录（可相对或绝对路径）。 | 字符串 | 建议写入 `POLYMARKET_MAKER/logs/autorun/`。 |
| `paths.data_directory` | 数据目录（状态快照、退出信号等）。 | 字符串 | 建议与日志目录分开保存。 |
| `paths.run_state_file` | 运行状态快照文件。 | 字符串 | 默认写入 `POLYMARKET_MAKER/data/autorun_status.json`。 |
| `paths.copytrade_tokens_file` | copytrade 产出的 token 文件路径。 | 字符串 | 指向 `copytrade/tokens_from_copytrade.json`。 |
| `paths.copytrade_sell_signals_file` | copytrade 产出的 sell 信号文件路径。 | 字符串 | 指向 `copytrade/copytrade_sell_signals.json`。 |

## strategy_defaults.json —— 策略模板
为不同话题/主题提供默认下单参数与覆盖示例。【F:POLYMARKET_MAKER/config/strategy_defaults.json†L1-L24】

| 字段 | 作用 | 类型/格式 | 推荐设置 |
| --- | --- | --- | --- |
| `default.min_edge` | 最小优势阈值（策略内部估值与盘口差异）。 | 0~1 小数 | 0.01~0.05。 |
| `default.max_position_per_market` | 单市场最大持仓（份额或名义金额）。 | 浮点 | 按风险偏好调整。 |
| `default.order_size` | 默认下单量。 | 浮点 | 20~100 之间常用。 |
| `default.spread_target` | 期望挂单点差目标。 | 0~1 小数 | 0.005~0.02。 |
| `default.refresh_interval_seconds` | 策略刷新/再报价周期。 | 整数（秒） | 5~15。 |
| `default.max_open_orders` | 同时挂单数量上限。 | 整数 | 10~30。 |
| `topics.*` | 针对特定话题 ID/slug 的覆盖：可单独调整 `topic_name`、`min_edge`、`max_position_per_market`、`order_size`、`spread_target`、`refresh_interval_seconds`、`max_open_orders`。 | 按字段类型填写 | 仅覆盖需要调整的字段，其余沿用 `default`。 |
