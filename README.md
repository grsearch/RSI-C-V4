# SOL RSI+量能 Monitor V5-20

Solana 新代币 RSI(7)+EMA99+量能 策略监控机器人。

**1分钟K线 · Birdeye OHLCV 实时刷新 · Helius 链上成交数据 · 空跑/实盘**

---

## 策略逻辑

### 买入条件（同时满足）

| # | 条件 | 说明 |
|---|------|------|
| 1 | RSI(7) < 35 | 当前处于超卖区间 |
| 2 | EMA(99) 斜率 ≥ -1% | 拒绝在 EMA99 下行趋势中买入（防接飞刀，10根×1分钟=10分钟窗口） |
| 3 | K 线数 ≥ 21 | RSI Wilder 收敛门槛 |
| 4 | 过去 5 分钟内 buyVol ≥ 1.2 × sellVol | 链上资金净流入 |
| 5 | 过去 5 分钟内总成交量 ≥ 5 SOL | 流动性门槛 |

> 不等 RSI "上穿" 30，只要 RSI 在超卖区 + 资金净流入 + EMA99 不在下行就立即入场。
> ⚠️ "价格 < EMA(99)" 硬条件已通过 `EMA_PRICE_FILTER_ENABLED=false` 关闭，改由斜率过滤把关趋势方向。

### 卖出条件（优先级从高到低，命中即卖）

| 优先级 | 条件 | 行为 | 默认状态 |
|--------|------|------|----------|
| 1 | RSI(7) > 80 | 恐慌卖出 | ✅ 启用 |
| 2 | RSI(7) 下穿 70 | 全仓卖出 | ✅ 启用 |
| 3 | 涨幅 ≥ +100% | 固定止盈 | ✅ 启用 |
| 4 | 上涨 ≥ +30% 后回撤 -20% | 移动止损 | ✅ 启用 |
| 5 | 跌幅 ≤ -50% | 固定止损 | ✅ V5-26 启用 |
| 6 | 量能萎缩(最近3根均量 < 前4根均量×0.3) | 量能出场 | ✅ V5-26 启用 |
| 7 | 持仓 ≥ 6 小时 | 超时卖出 (TIMEOUT_EXIT) | ✅ 启用 |

> **持仓中** 即使 FDV 跌破 $20K 或 LP 跌破 $10K，也**不强制卖出**，等正常出场条件触发。
> 仅在 **无持仓** 时，FDV/LP 跌破阈值会从监控列表中移除该代币。

### 仓位管理

- 监控期内可多次进出场
- 卖出后 30 分钟冷却期（同币不再交易）
- 每笔买入 **0.2 SOL**

---

## 数据来源

| 数据 | 来源 | 用途 |
|------|------|------|
| K 线 OHLC | **Birdeye OHLCV API**（每30秒刷新，120根K线缓存） | RSI/EMA99 计算 |
| 实时价格 | Birdeye WebSocket（subscribe_price） | stepRSI 实时估算、止损监控 |
| 链上买卖量 | Helius Enhanced WebSocket（transactionSubscribe） | buyVolume / sellVolume 量能过滤 |
| FDV / LP / Symbol | Birdeye token_overview | 入场过滤、自动 symbol 匹配 |
| X (Twitter) 提及 | X API v2 (tweets/counts/recent) | dashboard 显示热度（每2小时刷新） |

---

## 快速开始

### 1. 配置 .env

```bash
cp .env.example .env
```

**必填**:
```
BIRDEYE_API_KEY=你的Key
HELIUS_WSS_URL=wss://atlas-mainnet.helius-rpc.com/?api-key=你的Key
HELIUS_RPC_URL=https://mainnet.helius-rpc.com/?api-key=你的Key
```

**可选**(X mentions 列):
```
X_BEARER_TOKEN=你的X API Token
```

### 2. 空跑模式（推荐先跑一天）

```bash
# .env 中：
DRY_RUN=true
# WALLET_PRIVATE_KEY 可留空

npm install
npm start
```

### 3. 切换实盘

```bash
# .env 修改：
DRY_RUN=false
WALLET_PRIVATE_KEY=你的私钥
JUPITER_API_KEY=你的Jupiter Key

# 重启
npm start
```

### 4. Dashboard

```
http://YOUR_SERVER:3001
```

诊断接口（V5-20）:
```
http://YOUR_SERVER:3001/diag
```

---

## 环境变量（V5-20 默认值）

> ⚠️ 以下默认值已写在源码 `process.env.XXX || 'default'` 中。
> **.env 里不写这些行 = 用默认值**。要改才需要在 .env 里覆盖。

### 核心策略

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `KLINE_INTERVAL_SEC` | `60` | K线宽度（秒，**1分钟**） |
| `RSI_PERIOD` | `7` | RSI 周期 |
| `EMA_PERIOD` | `99` | EMA 长期均线周期 |
| `EMA_PRICE_FILTER_ENABLED` | `false` | **价格 < EMA(99) 硬条件已关闭**，仅靠斜率把关趋势 |
| `EMA_INSUFFICIENT_MODE` | `strict` | K 线不足 99 根时严格不出信号 |
| `EMA_SLOPE_ENABLED` | `true` | 启用 EMA99 斜率过滤（防接飞刀） |
| `EMA_SLOPE_LOOKBACK` | `10` | 斜率回看根数（10×1分钟=10分钟窗口） |
| `EMA_SLOPE_MIN_PCT` | `-1` | 斜率下限（%），≥ -1% 才允许买入 |
| `RSI_BUY_LEVEL` | `35` | 超卖买入阈值 |
| `RSI_SELL_LEVEL` | `70` | RSI 下穿此值卖出 |
| `RSI_PANIC_LEVEL` | `80` | RSI 超过此值立即卖出 |
| `MIN_CANDLES_FOR_SIGNAL` | `21` | RSI 收敛所需最低 K 线数 |
| `SKIP_FIRST_CANDLES` | `3` | 跳过前 N 根 K 线（实际生效值取 max(此值, 21)） |

### 仓位与冷却

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `TRADE_SIZE_SOL` | `0.2` | 每笔买入金额（SOL） |
| `SELL_COOLDOWN_SEC` | `1800` | 卖出后冷却（秒，**30分钟**） |
| `MAX_HOLD_SEC` | `21600` | 最大持仓时间（秒，**6小时**），超时强制卖出 reason=TIMEOUT_EXIT, 0=关闭 |

### 止盈止损

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `TAKE_PROFIT_ENABLED` | `true` | 启用固定止盈 |
| `TAKE_PROFIT_PCT` | `100` | 止盈百分比（**+100%**，不是 +50%） |
| `STOP_LOSS_ENABLED` | `true` | **V5-26 启用**固定止损 |
| `STOP_LOSS_PCT` | `-50` | 止损百分比 |
| `TRAILING_STOP_ENABLED` | `true` | 启用移动止损 |
| `TRAILING_STOP_ACTIVATE` | `30` | 上涨 30% 后激活移动止损 |
| `TRAILING_STOP_PCT` | `-20` | 从峰值回撤 20% 触发卖出 |

### 量能过滤

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `VOL_ENABLED` | `true` | 启用量能买入过滤 |
| `VOL_BUY_MULT` | `1.2` | buyVol ≥ N × sellVol 才买入 |
| `VOL_MIN_TOTAL` | `5` | 最低总成交量(SOL) |
| `VOL_WINDOW_SEC` | `300` | 量能统计窗口（秒） |
| `VOL_DECAY_EXIT_ENABLED` | `true` | **V5-26 启用**量能萎缩出场 |
| `VOL_EXIT_CONSECUTIVE` | `3` | 最近 N 根 K 线连续低量则出场 |
| `VOL_EXIT_LOOKBACK` | `4` | 对比前 N 根的均量 |
| `VOL_EXIT_RATIO` | `0.3` | 最近均量 < 前 N 根均量 × 0.3 视为萎缩(严重才触发) |

### FDV / LP 过滤

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `MIN_FDV_USD` | `15000` | 入场 FDV 过滤 |
| `MIN_LP_USD` | `5000` | 入场 LP 过滤 |
| `FDV_EXIT_USD` | `20000` | 监控中 FDV 退出阈值（**仅无持仓时退出**） |
| `LP_EXIT_USD` | `10000` | 监控中 LP 退出阈值（**仅无持仓时退出**） |

### 监控容量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `MAX_MONITOR_TOKENS` | `95` | 最大监控代币数 |

### Birdeye OHLCV 实时刷新（V5-16）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `OHLCV_REALTIME_ENABLED` | `true` | 启用 OHLCV 实时刷新（替代不准的 ticks 聚合） |
| `OHLCV_REFRESH_SEC` | `30` | 每 N 秒拉一次最新 K 线（V5-32: 1min K 线下用 30s, 让 closed candle 滚得更勤, 配合 orphan-bucket 合并避免量能丢失） |
| `OHLCV_REALTIME_BARS` | `120` | 实时刷新拉的 K 线根数 |

### prevRsi 时效保护（V5-15/V5-17）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `WS_RSI_PREV_STALE_MS` | `3000` | WS 推送 prevRsi 超过 N 毫秒视为过期 |
| `SL_POLL_PREV_STALE_MS` | `3000` | 止损轮询 prevRsi 超过 N 毫秒视为过期 |

### X (Twitter) Mentions（V5-18/V5-19）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `X_BEARER_TOKEN` | - | X Developer API Bearer Token（不填则禁用此列） |
| `X_MENTIONS_INTERVAL_SEC` | `7200` | 刷新间隔（秒，**2小时**） |
| `X_MENTIONS_REQUEST_GAP_SEC` | `15` | 串行请求间隔（秒） |

### 运行模式

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `DRY_RUN` | `true` | 空跑模式（不实际交易） |
| `DRY_RUN_DATA_DIR` | `./data` | 数据持久化目录 |
| `LOG_LEVEL` | `info` | 日志级别 |
| `PORT` | `3001` | HTTP 端口 |

---

## 目录结构

```
sol-rsi/
├── src/
│   ├── index.js          # 主入口
│   ├── monitor.js        # 核心引擎
│   ├── rsi.js            # RSI/EMA/量能/信号评估
│   ├── birdeye.js        # Birdeye API 封装（OHLCV/价格/FDV/LP）
│   ├── heliusWs.js       # Helius Enhanced WS（链上交易订阅）
│   ├── trader.js         # Jupiter Ultra API 实盘交易
│   ├── xMentions.js      # X (Twitter) 提及计数
│   ├── dataStore.js      # 数据持久化
│   ├── reporter.js       # 每日 CSV 报告
│   ├── wsHub.js          # Dashboard WebSocket 广播
│   ├── logger.js         # 日志（winston）
│   └── routes/
│       ├── webhook.js    # POST /webhook/add-token
│       └── dashboard.js  # REST API + /diag 诊断接口
├── public/
│   └── index.html        # 实时 Dashboard 前端
├── data/                  # 持久化数据（自动创建）
│   ├── tokens.json
│   ├── trades.json
│   ├── signals.json
│   └── xMentions.json
├── .env.example
└── package.json
```

---

## 关键操作

### 添加代币

通过 Dashboard 前端，或 POST 接口：

```bash
# 不传 symbol（V5-16 自动从 Birdeye 拉取）
curl -X POST http://localhost:3001/tokens/add \
  -H 'Content-Type: application/json' \
  -d '{"address":"代币mint地址"}'
```

### 检查 OHLCV 实时刷新是否工作（V5-20）

```bash
curl http://localhost:3001/diag | jq .
```

健康指标:
- `env.OHLCV_REALTIME_ENABLED = "true"`
- `callStats.ohlcvSuccess / ohlcvBranch ≈ 100%`（OHLCV 调用成功率）
- `callStats.fallbackBranch < 5%`（fallback 比例低）
- `ohlcvCache.empty < 5`（空缓存极少）

如果 `fallbackBranch` 很高 → Birdeye OHLCV 频繁失败，检查 API key 和套餐。

### 重启不丢监控列表

`data/tokens.json` 自动持久化所有监控代币。重启程序会自动恢复。

---

## 版本历史关键改动

- **V5-9**: 关闭固定止损默认（避免假跌震出）；K 线收敛门槛 21
- **V5-10**: 启用固定止盈 +100%
- **V5-13**: Birdeye getPrice 失败抑制（400/404 静默 5 分钟）
- **V5-14**: 决策追踪 tooltip
- **V5-15**: WS tick prevRsi 时效保护（防虚假下穿）
- **V5-16**: ★ Birdeye OHLCV 实时刷新（K 线源换为官方 API，对齐 GMGN）
- **V5-16**: Symbol 自动匹配；持仓时不退出监控
- **V5-16**: 买入数额 0.2 → 1 SOL
- **V5-17**: OHLCV 最新 K 线 close 用实时价格覆盖（修 RSI 跟不上实时）
- **V5-17**: _slPollPrevRsi 时效保护
- **V5-17**: 删除前端 Age 列
- **V5-18**: X mentions 列
- **V5-19**: X mentions 改串行 + 15s 间隔
- **V5-20**: 加 /diag 诊断接口
- **V5-21**: dashboard 加回 Age 列(代币年龄); 加最大持仓 6h 超时卖出 (TIMEOUT_EXIT)
- **V5-22**: 修 monitor.js 启动日志 "移动止损=关闭 激活线=+undefined%" (rsi.js 顶层补导出 TRAILING_STOP_*)
- **V5-23**: 修前端 K线列被覆盖成 "0笔" bug (V5-21 索引偏移没改干净)
- **V5-24**: dashboard 表格创建 td 数量从 15 改 16, 修整张表空白; 加 birdeye.getCreationInfo 和持久化
- **V5-25**: ★ OHLCV 刷新从 30s 改 300s, 节省 90% Birdeye CU. 实时价由 Birdeye WS SUBSCRIBE_PRICE 提供, 业务行为不变
- **V5-26**: 重新启用固定止损 -20% 和量能萎缩出场; VOL_DECAY 参数 (3根<前4根均×0.3, 严重萎缩才触发)
- **V5-27**: 启动日志加 CU 关键参数行; /diag 加 cuEstimate 字段 + 月度预警
- **V5-28**: /diag 加 wsStream / getPriceStats 字段, 用于诊断 WS 推送命中率
- **V5-29**: ★ 修 Birdeye WS chartType '1s' → '1m' (Birdeye 已不支持 1s). getCachedPrice 过期阈值 10s → 90s 覆盖 1m 推送间隔
- **V5-30**: ★ 真正根因 — Birdeye WS 强制要求 `echo-protocol` subprotocol header, ws 库默认不发, 加 `new WebSocket(URL, 'echo-protocol')` 修复. chartType 回退 '1s' (V5-26 旧服务器一直在用), 缓存 90s 保留
- **V5-31**: ★ K 线宽度 5min → 1min (KLINE_INTERVAL_SEC=60); OHLCV_REFRESH_SEC 对齐 60s; 买入数额回到 0.2 SOL; 硬止损 -20% → -50%; 关闭 "价格 < EMA99" 硬条件 (新增 EMA_PRICE_FILTER_ENABLED=false), 改由斜率过滤把关; EMA_SLOPE_LOOKBACK 5 → 10 (保持 ~10min 窗口), EMA_SLOPE_MIN_PCT 0% → -2% (容忍轻微下行); FDV_EXIT_USD 30K → 20K. 量能窗口 5min 和 5 SOL 门槛保持不变
- **V5-32**: ★ 修 1min K 线下 Buy/Sell 量能全为 0 / 显示 `-` 的 bug. 根因: chain tick 落到"当前未收盘桶"(openTime 不在 closed candles 里)被丢弃, 5min 时影响小, 1min 时窗口窄影响大. 修法: monitor.js 新增 orphan-bucket 合并 — 桶 openTime > 最新 closed candle 的链上量能合并到最近一根上; OHLCV_REFRESH_SEC 60→30 让 closed candle 滚得更勤; /diag 加 volMergeStats 字段供观察
- **V5-33**: ★ EMA_SLOPE_MIN_PCT -2% → -1% (更严, 拒绝几乎任何下行); 买入信号 reason 带上 EMA99 斜率值 (RSI_OVERSOLD(...)+EMA99(slope=X.XXX%)+...); 每日 CSV 报告新增 "买入原因" 列 (在 "盈亏%" 和 "退出原因" 之间)
- **V5-34**: dashboard 实时行情排序规则 — 按 X mentions 数从大到小为主, 同值内按状态序(持仓>冷却>观望). 替换原"按 24h 交易笔数从大到小"的规则
- **V5-35**: 诊断 — 买入瞬间打详细日志 (BUY_DIAG)，包含 closedCount / lastCandleAge / lastClose / realtimePrice / lastClosedRsi / rsiRealtime / avgGain / avgLoss / _lastPriceUsd / _lastPriceAge / recentCloses[10]，用于排查"日志报 RSI=22 但 GMGN 图表 RSI 是 60+"这类买入点位异常问题
- **V5-36**: ★ 修"用过期数据误买"根因 (lolcat 案例: lastCandleAge=307s, _lastPriceUsd=lastClose, RSI=7.93 误触发买入). 三处修复 — (1) rsi.js 加 MAX_STALE_CANDLE_SEC 门槛 (默认 90s), closedCandles 最新一根太旧时拒绝 BUY (SELL 仍工作); (2) monitor.js 删 V5-17 污染逻辑 (`last.close = state._lastPriceUsd` 会污染 RSI 历史序列); (3) monitor.js 加 PRICE_FRESH_MS_FOR_SIGNAL (默认 30s), _lastPriceUsd 过期就不构造 currentCandle, 让上游 fallback 到经过过期检查的 price 变量
- **V5-37**: ★ 鲁棒化 — V5-36 上线后用户服务器 .env 仍是 V5-25 时代 OHLCV_REFRESH_SEC=300, lastCandleAge=331s 全表被 90s 门槛拒. 修法: MAX_STALE_CANDLE_SEC 改为自适应公式 max(KLINE_SEC + OHLCV_REFRESH_SEC + 30, 90), 不显式设置时跟随其他参数走. 启动日志打印实际生效门槛, 便于诊断

---

## ⚠️ 注意事项

- **不要凭记忆修改默认值**。所有默认值在代码里 `process.env.XXX || 'default'` 处定义
- **不要随便关闭 OHLCV_REALTIME_ENABLED**。关掉会回退到不准的 ticks 聚合，RSI 严重失真
- **持仓中不会因 FDV/LP 跌破自动卖出**。仅在 RSI > 80、下穿 70、达到止盈、移动止损触发时才卖
- **README 中所有"默认值"必须以代码为准**。如果改了代码默认值，必须同步更新此文档
