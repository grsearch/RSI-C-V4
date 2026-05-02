'use strict';
// src/monitor.js — 核心监控引擎 V4
//
// V4 改进：
//   1. 去掉监控期限制和最大交易次数限制，代币持续监控直到手动移除
//   2. K线改为1分钟，止损轮询改为1分钟（可配置 SL_POLL_SEC）
//   3. 支持手动添加/删除代币
//   4. 卖出后不退出监控，重置状态等待下一个买入信号
//
// 交易生命周期：
//   addToken → [BUY → SELL → 冷却 → BUY → SELL → ...] → 手动移除 → removeToken

const EventEmitter = require('events');
const { evaluateSignal, buildCandles, filterValidCandles, checkStopLoss,
        calcRSIWithState, stepRSI,
        TRAILING_STOP_ENABLED, TRAILING_STOP_ACTIVATE, TRAILING_STOP_PCT } = require('./rsi');

// RSI 卖出阈值（从 CONFIG 取，与 rsi.js 保持一致）
const { CONFIG: RSI_CONFIG } = require('./rsi');
const _RSI_SELL  = RSI_CONFIG.RSI_SELL;
const _RSI_PANIC = RSI_CONFIG.RSI_PANIC;
const trader    = require('./trader');
const birdeye   = require('./birdeye');
const HIST_BARS = parseInt(process.env.HIST_BARS || '150', 10); // 启动时拉取的历史K线根数
const logger    = require('./logger');
const wsHub     = require('./wsHub');
const dataStore = require('./dataStore');
const heliusWs  = require('./heliusWs');
const xMentions = require('./xMentions');

const FDV_EXIT          = parseFloat(process.env.FDV_EXIT_USD        || '30000');  // ★ V5: 改为3万
const LP_EXIT           = parseFloat(process.env.LP_EXIT_USD         || '10000');  // ★ V5: LP<1万退出
const POLL_SEC          = parseInt(process.env.PRICE_POLL_SEC        || '1',  10);
const KLINE_SEC         = parseInt(process.env.KLINE_INTERVAL_SEC    || '300', 10);
const DRY_RUN           = (process.env.DRY_RUN || 'false') === 'true';
const TRADE_SOL         = parseFloat(process.env.TRADE_SIZE_SOL      || '1');
const SELL_COOLDOWN_SEC = parseInt(process.env.SELL_COOLDOWN_SEC     || '1800', 10); // 默认30分钟
// ★ V5-21: 最大持仓时间 - 防止僵持币长期占用资金
//   持仓超过 N 秒强制卖出, 0 = 关闭. 默认 21600 = 6 小时
const MAX_HOLD_SEC      = parseInt(process.env.MAX_HOLD_SEC          || '21600', 10);
const SL_POLL_SEC       = parseInt(process.env.SL_POLL_SEC           || '60', 10);
const MAX_TOKENS        = parseInt(process.env.MAX_MONITOR_TOKENS    || '95', 10);  // ★ V5: 最大监控数
const OVERVIEW_PATROL_SEC = parseInt(process.env.OVERVIEW_PATROL_SEC || '7200', 10); // ★ V5: FDV/LP巡检间隔(秒)

// ★ 历史K线拉取队列：串行执行 + 退避，避免 Birdeye 429
//   - HIST_FETCH_GAP_MS: 每个请求间隔
//   - HIST_RETRY_MAX:    429/错误时最大重试次数
//   - HIST_RETRY_BASE:   退避基准（指数退避：base × 2^n）
const HIST_FETCH_GAP_MS = parseInt(process.env.HIST_FETCH_GAP_MS || '1200', 10);
const HIST_RETRY_MAX    = parseInt(process.env.HIST_RETRY_MAX    || '5',   10);
const HIST_RETRY_BASE_MS = parseInt(process.env.HIST_RETRY_BASE_MS || '5000', 10);

// ★ V5-16: 实时 OHLCV K 线刷新（每 OHLCV_REFRESH_SEC 秒拉一次最新K线，替代不准的 ticks 聚合）
//   默认开启 (true)。如果出问题想回退旧逻辑，设为 false 即可。
const OHLCV_REALTIME_ENABLED = (process.env.OHLCV_REALTIME_ENABLED || 'true') === 'true';
// 实时刷新只在主轮询里需要的少量 K 线（够算 RSI 和 EMA99 即可）
const OHLCV_REALTIME_BARS = parseInt(process.env.OHLCV_REALTIME_BARS || '120', 10);

// ★ V5-36: _lastPriceUsd 超过此时长就视为过期，不用作 currentCandle.close 构造
//   背景: 低流动性币 BirdeyeWS 可能长时间不推送, _lastPriceUsd 卡死在旧价格
//   现象: 5 分钟前的旧价格被当成"实时价"输入 stepRSI, RSI 算出错值
//   默认 30000ms = 30s. 不必和 PRICE_STALE_MS (60s) 完全相同, 因为信号决策对价格新鲜度比"持仓监控"更敏感
const PRICE_FRESH_MS_FOR_SIGNAL = parseInt(process.env.PRICE_FRESH_MS_FOR_SIGNAL || '30000', 10);

class HistFetchQueue {
  constructor() {
    this._queue = [];     // [{ address, symbol, attempt, resolve }]
    this._running = false;
    this._seen = new Set(); // 去重，同一个 address 已在队列中则跳过
  }
  // 返回 Promise：resolve(candles 或 []) —— 外部拿不到也没关系，结果已经 set 到 state
  enqueue(address, symbol, onDone, attempt = 1) {
    if (this._seen.has(address) && attempt === 1) {
      logger.debug('[HistQueue] %s 已在队列，跳过重复入队', symbol);
      return;
    }
    this._seen.add(address);
    this._queue.push({ address, symbol, attempt, onDone });
    logger.debug('[HistQueue] 入队 %s (attempt=%d, 队列长=%d)', symbol, attempt, this._queue.length);
    this._drain();
  }
  async _drain() {
    if (this._running) return;
    this._running = true;
    while (this._queue.length > 0) {
      const job = this._queue.shift();
      try {
        const candles = await birdeye.getOHLCV(job.address, KLINE_SEC, HIST_BARS);
        const meta = candles?._meta || {};
        const needRetry =
          candles.length === 0 &&
          (meta.error === 'HTTP_429' || meta.error === 'TIMEOUT' || /^HTTP_5\d\d$/.test(meta.error || ''));

        if (needRetry && job.attempt < HIST_RETRY_MAX) {
          const backoff = HIST_RETRY_BASE_MS * Math.pow(2, job.attempt - 1);
          logger.warn('[HistQueue] %s 拉取失败(%s) 第%d次，%ds 后重试',
            job.symbol, meta.error, job.attempt, Math.round(backoff / 1000));
          setTimeout(() => {
            this._seen.delete(job.address);  // 允许重新入队
            this.enqueue(job.address, job.symbol, job.onDone, job.attempt + 1);
          }, backoff);
        } else {
          this._seen.delete(job.address);
          if (job.onDone) job.onDone(candles);
          if (needRetry) {
            logger.error('[HistQueue] %s 重试 %d 次仍失败(%s)，放弃',
              job.symbol, HIST_RETRY_MAX, meta.error);
          }
        }
      } catch (err) {
        logger.warn('[HistQueue] %s 异常: %s', job.symbol, err.message);
        this._seen.delete(job.address);
        if (job.onDone) job.onDone([]);
      }
      // 每次请求间隔
      if (this._queue.length > 0) await new Promise(r => setTimeout(r, HIST_FETCH_GAP_MS));
    }
    this._running = false;
  }
  queueSize() { return this._queue.length; }
}

const histFetchQueue = new HistFetchQueue();

// 全局交易记录
const _allTradeRecords = [];

function _loadPersistedTrades() {
  try {
    const trades = dataStore.loadTrades();
    const cutoff = Date.now() - 24 * 3600 * 1000;
    trades.filter(r => r.buyAt > cutoff).forEach(r => _allTradeRecords.push(r));
    if (_allTradeRecords.length > 0) {
      logger.info('[Monitor] 从磁盘加载了 %d 条交易记录', _allTradeRecords.length);
    }
  } catch (_) {}
}

class TokenMonitor extends EventEmitter {
  constructor() {
    super();
    this._tokens    = new Map();
    this._pollTimer = null;
    this._started   = false;
    // 止损锁：防止同一 token 并发触发多次止损
    this._stopLossLocks = new Set();
    this._slPollTimer = null;  // 独立止损轮询
    this._persistTimer = null; // ★ V5: 定时持久化
  }

  start() {
    if (this._started) return;
    this._started = true;

    dataStore.init();
    _loadPersistedTrades();
    dataStore.startFlush();

    birdeye.priceStream.start();
    heliusWs.start();

    this._scheduleNextPoll();
    this._startStopLossPoller();  // ★ 500ms 独立止损轮询
    logger.info('[Monitor] 启动 | 轮询=%ds K线=%ds 止损轮询=%ds 冷却=%ds DRY_RUN=%s',
      POLL_SEC, KLINE_SEC, SL_POLL_SEC, SELL_COOLDOWN_SEC, DRY_RUN);
    logger.info('[Monitor]   BirdeyeWS=%s  HeliusWS=%s',
      birdeye.priceStream.isConnected() ? '已连接' : '连接中',
      heliusWs.isConnected() ? '已连接' : '连接中');
    logger.info('[Monitor]   移动止损=%s  激活线=+%s%%  回撤线=%s%%',
      TRAILING_STOP_ENABLED ? '开启' : '关闭', TRAILING_STOP_ACTIVATE, TRAILING_STOP_PCT);

    // ★ 加载持久化的代币列表（延迟500ms，等 WS 连接建立）
    setTimeout(() => this._loadPersistedTokens(), 500);

    // ★ V5: 定时持久化状态（每60秒），确保崩溃/重启后不丢失RSI预热和持仓
    this._persistTimer = setInterval(() => this._persistTokens(), 60000);

    // ★ V5: FDV/LP/Age 巡检（每 OVERVIEW_PATROL_SEC 秒一轮，分散请求）
    this._patrolTimer = null;
    this._startOverviewPatrol();
    logger.info('[Monitor]   FDV退出<$%d  LP退出<$%d  最大监控=%d  巡检=%ds',
      FDV_EXIT, LP_EXIT, MAX_TOKENS, OVERVIEW_PATROL_SEC);

    // ★ V5-27: CU 关键参数日志 - 部署后扫一眼就能发现 .env 覆盖问题
    //   读 process.env, 真实反映当前生效的值 (而不是代码 fallback)
    const ohlcvRefresh    = parseInt(process.env.OHLCV_REFRESH_SEC || '300', 10);
    const ohlcvBars       = parseInt(process.env.OHLCV_REALTIME_BARS || '120', 10);
    const ohlcvEnabled    = (process.env.OHLCV_REALTIME_ENABLED || 'true') === 'true';
    const tokenCount      = MAX_TOKENS;
    // 估算每日 CU (按代币数满载, 不含 token_creation_info / token_security 一次性)
    const cuOhlcvDay      = ohlcvEnabled ? Math.round(tokenCount * 86400 / ohlcvRefresh * 40) : 0;
    const cuOverviewDay   = Math.round(tokenCount * 86400 / OVERVIEW_PATROL_SEC * 25);
    const cuMonth         = Math.round((cuOhlcvDay + cuOverviewDay) * 30 / 1e6);
    logger.info('[Monitor] ★ CU关键参数: OHLCV_REFRESH=%ds OHLCV_BARS=%d OHLCV_ENABLED=%s OVERVIEW_PATROL=%ds',
      ohlcvRefresh, ohlcvBars, ohlcvEnabled, OVERVIEW_PATROL_SEC);
    logger.info('[Monitor] ★ 估算月度 CU: ~%dM (OHLCV %.1fM/天 + Overview %.1fK/天) | 配额一般 20M',
      cuMonth, cuOhlcvDay / 1e6, cuOverviewDay / 1e3);
    if (cuMonth > 20) {
      logger.warn('[Monitor] ⚠️  月度 CU 估算 %dM 超过 20M 配额! 建议加大 OHLCV_REFRESH_SEC (当前 %ds)',
        cuMonth, ohlcvRefresh);
    }

    // ★ V5-18: X mentions 24h 计数定时刷新（默认 2 小时一轮）
    xMentions.start(() => Array.from(this._tokens.keys()));
  }

  async _loadPersistedTokens() {
    try {
      const tokens = dataStore.loadTokens();
      if (!tokens || tokens.length === 0) return;
      logger.info('[Monitor] 从磁盘恢复 %d 个监控代币...', tokens.length);
      for (const t of tokens) {
        if (t.address && t.symbol) {
          const added = await this.addToken(t.address, t.symbol, t.meta || {});
          if (!added) continue;

          // ★ V5: 恢复保存的运行状态
          const state = this._tokens.get(t.address);
          if (!state) continue;

          // 恢复 FDV/LP/Age
          if (t.fdv != null) state.fdv = t.fdv;
          if (t.lp != null) state.lp = t.lp;
          if (t.createdAt != null) state.createdAt = t.createdAt;

          // ★ 不恢复 RSI 缓存（_rsiAvgGain 等）— 从 ticks 重新计算
          //   旧缓存的 lastClose 跟当前价格可能差很远，stepRSI 会算出虚高RSI

          // 恢复持仓状态
          if (t.inPosition && t.position) {
            state.inPosition = true;
            state.position   = t.position;
            state.tradeCount = t.tradeCount || 0;
            logger.info('[Monitor] ♻️ 恢复 %s 持仓状态: entry=%.6f SOL=%.4f',
              t.symbol, t.position.entryPriceUsd, t.position.solIn);
          } else {
            state.tradeCount = t.tradeCount || 0;
          }

          // 恢复冷却期
          if (t._sellCooldownUntil && t._sellCooldownUntil > Date.now()) {
            state._sellCooldownUntil = t._sellCooldownUntil;
          }

          // ★ V5: 从磁盘加载历史 ticks 恢复 K 线数据
          try {
            const savedTicks = dataStore.loadTicks(t.address);
            if (savedTicks && savedTicks.length > 0) {
              // 加载最近2小时的 ticks（5分钟K线 × RSI(7) 需要至少9根 = 45分钟，留余量）
              const cutoff = Date.now() - 2 * 60 * 60 * 1000;
              const recentTicks = savedTicks.filter(tk => tk.ts > cutoff);
              if (recentTicks.length > 0) {
                state.ticks = recentTicks;
                logger.info('[Monitor] ♻️ 恢复 %s %d 条历史tick（最近2小时）',
                  t.symbol, recentTicks.length);
              }
            }
          } catch (_) {}

          // ★ 重启后重新拉取历史K线（走队列，串行+退避重试，避免 Birdeye 429）
          histFetchQueue.enqueue(t.address, t.symbol, (histCandles) => {
            const s = this._tokens.get(t.address);
            if (!s) return;
            s._histMeta = histCandles?._meta || { requestedBars: HIST_BARS, returnedItems: 0, closedBars: 0, error: 'EMPTY' };
            if (!histCandles || histCandles.length === 0) return;
            s.historicalCandles = histCandles;
            logger.info('[Monitor] ♻️ %s 历史K线重载: %d 根', t.symbol, histCandles.length);
          });
        }
      }
    } catch (err) {
      logger.error('[Monitor] 加载持久化代币失败: %s', err.message);
    }
  }

  _persistTokens() {
    try {
      const list = Array.from(this._tokens.values()).map(s => ({
        address: s.address,
        symbol:  s.symbol,
        meta:    s.meta || {},
        // ★ V5: 保存运行状态，重启后不丢失
        fdv:            s.fdv,
        lp:             s.lp,
        createdAt:      s.createdAt,
        inPosition:     s.inPosition,
        position:       s.position,
        tradeCount:     s.tradeCount,
        _sellCooldownUntil: s._sellCooldownUntil,
        // ★ 不再保存 RSI 缓存状态（_rsiAvgGain 等）
        //   恢复后由 ticks 重新聚合 K 线重算，避免旧缓存与新 ticks 不匹配导致 RSI 虚高
      }));
      dataStore.saveTokens(list);
    } catch (_) {}
  }

  stop() {
    this._started = false;
    if (this._pollTimer) { clearTimeout(this._pollTimer); this._pollTimer = null; }
    if (this._slPollTimer) { clearInterval(this._slPollTimer); this._slPollTimer = null; }
    if (this._persistTimer) { clearInterval(this._persistTimer); this._persistTimer = null; }
    if (this._patrolTimer) { clearTimeout(this._patrolTimer); this._patrolTimer = null; }
    this._persistTokens();  // ★ V5: 关闭前最后保存一次
    birdeye.priceStream.stop();
    heliusWs.stop();
    dataStore.stopFlush();
  }

  async addToken(address, symbol, meta = {}) {
    if (this._tokens.has(address)) {
      logger.warn('[Monitor] %s 已在监控中，忽略', symbol);
      return false;
    }

    // ★ V5: 最大监控数检查
    if (this._tokens.size >= MAX_TOKENS) {
      const evicted = await this._evictForNewToken();
      if (!evicted) {
        logger.warn('[Monitor] %s 无法添加：监控已满(%d/%d)', symbol, this._tokens.size, MAX_TOKENS);
        return false;
      }
    }

    const now = Date.now();
    const state = {
      address,
      symbol,
      meta,
      fdv               : meta.fdv ?? null,
      lp                : meta.lp  ?? null,
      createdAt         : meta.createdAt ?? null,  // ★ V5: 代币创建时间(ms)
      addedAt           : now,
      ticks             : [],
      historicalCandles : [],  // ★ 启动时从 Birdeye 拉取的历史K线（用于EMA99/RSI预热）
      inPosition        : false,
      position          : null,
      tradeCount        : 0,       // 完成的买卖轮次数
      tradeLogs         : [],
      tradeRecords      : [],
      _prevRsiRealtime  : NaN,
      _prevRsiTs        : 0,
      _lastBuyCandle    : -1,
      _lastSellCandle   : -1,
      _lastPanicSellTs  : 0,       // RSI_PANIC 时间防抖（毫秒时间戳）
      _lastPriceUsd     : null,
      _lastPriceTs      : 0,
      // ★ 实时 RSI 下穿检测缓存（每个 WS tick 更新，不依赖 1s 轮询）
      _rsiAvgGain       : NaN,     // 最新已收盘K线的 avgGain
      _rsiAvgLoss       : NaN,     // 最新已收盘K线的 avgLoss
      _rsiLastClose     : NaN,     // 最新已收盘K线的 close
      _rsiLastCandleTs  : -1,      // 对应的 K 线 openTime（用于检测 K 线是否刷新）
      _rsiPrevTickRsi   : NaN,     // 保留字段（暂未使用）
      _slPollPrevRsi    : NaN,     // 500ms轮询的上一次实时RSI（用于下穿检测）
      _wsTickPrevRsi    : NaN,     // ★ WS tick的上一次实时RSI（用于下穿检测）
      _lastRsiCrossSellTs: 0,      // ★ RSI下穿70的时间防抖（毫秒时间戳）
      // ★ 多次买卖相关
      _sellCooldownUntil: 0,       // 卖出后冷却到期时间戳
      _selling          : false,   // 正在执行卖出中（防并发）
    };

    this._tokens.set(address, state);

    birdeye.priceStream.subscribe(address, (price, ts, ohlcv) => {
      this._onBirdeyePrice(address, price, ts);
    });

    heliusWs.subscribe(address, symbol, (trade) => {
      this._onChainTrade(address, trade);
    });

    // ★ 异步拉取 overview（Age/FDV/LP）+ 历史K线（EMA99/RSI预热）
    (async () => {
      const s = this._tokens.get(address);
      if (!s) return;

      // 1. 拉取 overview
      try {
        const ov = await birdeye.getOverview(address);
        if (ov) {
          if (ov.createdAt) s.createdAt = ov.createdAt;
          if (ov.fdv !== null && Number.isFinite(ov.fdv)) s.fdv = ov.fdv;
          if (ov.liquidity !== null && Number.isFinite(ov.liquidity)) s.lp = ov.liquidity;
          logger.debug('[Monitor] %s overview初始化: fdv=$%s age=%s',
            symbol,
            s.fdv ? Math.round(s.fdv) : '?',
            s.createdAt ? Math.round((Date.now() - s.createdAt) / 3600000) + 'h' : '?');
        }
      } catch (_) {}

      // ★ V5-24: overview 没拿到 createdAt → 走专门接口 token_creation_info
      //   永久缓存到 data/tokenCreation.json, 每个币只拉一次
      if (!s.createdAt) {
        try {
          const ts = await birdeye.getCreationInfo(address);
          if (ts) s.createdAt = ts;
        } catch (_) {}
      }
      // 2. 拉取历史K线（走队列，串行 + 429重试，避免 Birdeye 限流）
      histFetchQueue.enqueue(address, symbol, (histCandles) => {
        const s2 = this._tokens.get(address);
        if (!s2) return;
        s2._histMeta = histCandles?._meta || { requestedBars: HIST_BARS, returnedItems: 0, closedBars: 0, error: 'UNKNOWN' };
        if (histCandles && histCandles.length > 0) {
          s2.historicalCandles = histCandles;
          logger.info('[Monitor] %s 历史K线预热: %d 根 (EMA99/RSI立即可用)',
            symbol, histCandles.length);
        } else {
          logger.warn('[Monitor] %s 历史K线预热失败: %s', symbol, s2._histMeta.error);
        }
      });
    })();

    logger.info("[Monitor] ➕ 开始监控 %s (%s) | DRY_RUN=%s",
      symbol, address, DRY_RUN);
    this._broadcastTokenList();
    this._persistTokens();  // ★ 保存到磁盘
    return true;
  }

  async removeToken(address, reason = 'manual') {
    const state = this._tokens.get(address);
    if (!state) return;

    logger.info('[Monitor] ➖ 移除 %s，原因: %s (共完成%d笔交易)', state.symbol, reason, state.tradeCount);

    // 到期/手动移除时如仍持仓，强制卖出
    if (state.inPosition && !state._selling) {
      logger.info('[Monitor] 📤 持仓中，先执行卖出...');
      await this._doSell(state, `FORCED_EXIT(${reason})`);
    }

    dataStore.flushTicks();

    birdeye.priceStream.unsubscribe(address);
    heliusWs.unsubscribe(address);

    this._tokens.delete(address);
    this._stopLossLocks.delete(address);
    birdeye.clearCache(address);
    this._broadcastTokenList();
    this._persistTokens();  // ★ 保存到磁盘
  }

  getTokens() {
    return Array.from(this._tokens.values()).map(s => this._stateSnapshot(s));
  }

  getToken(address) {
    const s = this._tokens.get(address);
    return s ? this._stateSnapshot(s) : null;
  }

  // ★ V5-20: 暴露 _getCurrentCandles 的调用统计, 用于诊断 OHLCV 实时刷新是否生效
  // ★ V5-32: 同时返回 volMergeStats —— 链上量能孤儿桶合并次数, 用于验证 1min K 线下量能补救生效
  getCallStats() {
    return {
      ...(this._gccStats || {}),
      volMergeStats: this._volMergeStats || null,
    };
  }

  // ★ 手动重拉某个代币的历史K线（前端"K线数量过少"时用）
  refetchHistory(address) {
    const state = this._tokens.get(address);
    if (!state) return { ok: false, error: 'token_not_found' };
    histFetchQueue.enqueue(address, state.symbol, (histCandles) => {
      const s = this._tokens.get(address);
      if (!s) return;
      s._histMeta = histCandles?._meta || { requestedBars: HIST_BARS, returnedItems: 0, closedBars: 0, error: 'EMPTY' };
      if (histCandles && histCandles.length > 0) {
        s.historicalCandles = histCandles;
        logger.info('[Monitor] 🔄 %s 手动重拉历史K线: %d 根', state.symbol, histCandles.length);
      }
    });
    return { ok: true, queueSize: histFetchQueue.queueSize() };
  }

  // ── Birdeye WS 实时价格回调（<150ms 延迟） ─────────────────────

  _onBirdeyePrice(address, price, ts) {
    const state = this._tokens.get(address);
    if (!state) return;

    state._lastPriceUsd = price;
    state._lastPriceTs  = ts;

    const tick = { price, ts, source: 'price' };
    state.ticks.push(tick);

    dataStore.appendTick(address, {
      price, ts, source: 'price', symbol: state.symbol,
    });

    // ★ 快速止损检查（持仓中 + 未在卖出中）
    if (state.inPosition && !state._selling && !this._stopLossLocks.has(address)) {
      const sl = checkStopLoss(price, state);
      if (sl.shouldExit) {
        logger.info('[Monitor] ⚡ 快速止损触发 %s @ %.8f | %s | 第%d笔',
          state.symbol, price, sl.reason, state.tradeCount + 1);
        this._stopLossLocks.add(address);
        this._doSell(state, sl.reason).catch(err => {
          logger.error('[Monitor] 快速止损执行失败 %s: %s', state.symbol, err.message);
        }).finally(() => {
          this._stopLossLocks.delete(address);
        });
        return; // 已触发卖出，不再检查 RSI
      }

      // ★★ 实时 RSI 卖出检查（每个 WS tick 都算，不等 K 线收盘）
      this._checkRealtimeRsiSell(state, price);
    }
  }

  /**
   * ★ V5 修复: 实时 RSI 卖出检查
   *   - RSI恐慌卖(>80): 只信任已收盘K线RSI，不用stepRSI（避免K线内波动导致虚假高RSI）
   *   - RSI下穿70: 仍用stepRSI实时检测（下穿检测对精度要求低于绝对值判断）
   */
  _checkRealtimeRsiSell(state, price) {
    const avgGain   = state._rsiAvgGain;
    const avgLoss   = state._rsiAvgLoss;
    const lastClose = state._rsiLastClose;
    if (!Number.isFinite(avgGain) || !Number.isFinite(avgLoss) || !Number.isFinite(lastClose)) return;

    // ★ V5-9: K线数不足时 RSI 不收敛，实时 stepRSI 会乱跳（虚假下穿）
    //         已收盘K线数 = 历史 + 实时，两者都算进来
    const histLen = (state.historicalCandles && state.historicalCandles.length) || 0;
    const liveLen = state._rsiClosedCount || 0;
    const totalClosed = histLen + liveLen;
    const MIN_CANDLES_FOR_SIGNAL = RSI_CONFIG.MIN_CANDLES_FOR_SIGNAL;
    if (totalClosed < MIN_CANDLES_FOR_SIGNAL) return;

    // 用当前实时价格计算实时 RSI
    const rsiNow = stepRSI(avgGain, avgLoss, lastClose, price);
    if (!Number.isFinite(rsiNow)) return;

    const prevRsi = state._wsTickPrevRsi;
    const prevTs  = state._wsTickPrevRsiTs || 0;
    const now = Date.now();

    // ★ V5-15: prevRsi 时效保护
    //   Birdeye WS 偶尔断流几十秒，恢复后第一个 tick 如果直接和旧 prevRsi 比较，
    //   会产生"71.3→32.6"这种虚假大跳（实际是两个不连续采样对比）。
    //   解决：超过 STALE_MS 视为失效，重建 baseline 但不触发下穿判断。
    const STALE_MS = parseInt(process.env.WS_RSI_PREV_STALE_MS || '3000', 10);
    const isStale  = !Number.isFinite(prevRsi) || (now - prevTs) > STALE_MS;

    // 更新上一次的实时 RSI 和时间戳（用于下一次下穿检测）
    state._wsTickPrevRsi   = rsiNow;
    state._wsTickPrevRsiTs = now;

    if (isStale) return; // prev 失效（首次 tick 或断流过久），跳过这次下穿判断

    // ── RSI > 80 恐慌卖 — ★ V5 改为只在主轮询的已收盘K线RSI中触发 ──
    //    stepRSI 在K线内波动剧烈时容易算出虚假高值（如95），
    //    而已收盘K线RSI更稳定、与交易所显示一致。
    //    此处不再处理 RSI_PANIC，由 evaluateSignal 和 _stopLossPoll 负责。

    // ── RSI 下穿 70（实时：prevRsi >= 70 且 rsiNow < 70）──
    //    下穿检测只看方向变化，对绝对值精度要求低，stepRSI可信
    if (prevRsi >= _RSI_SELL && rsiNow < _RSI_SELL) {
      const lastCrossTs = state._lastRsiCrossSellTs ?? 0;
      if (now - lastCrossTs >= 2000) {
        state._lastRsiCrossSellTs = now;
        logger.info('[Monitor] ⚡ WS实时RSI下穿卖出 %s @ %.8f | RSI %.1f→%.1f',
          state.symbol, price, prevRsi, rsiNow);
        this._doSell(state, `RSI_CROSS_DOWN_70_RT(${prevRsi.toFixed(1)}→${rsiNow.toFixed(1)})`).catch(err => {
          logger.error('[Monitor] WS RSI下穿卖出失败 %s: %s', state.symbol, err.message);
        });
      }
    }
  }

  // ★ V5-16: 获取用于 RSI/EMA 计算的 K 线序列
  //   开关开启时：用 Birdeye OHLCV 实时刷新（精准）+ Helius 量能合并
  //   开关关闭时：用 ticks 实时聚合 + 历史K线（旧逻辑）
  //   返回 { closedCandles, currentCandle, source }
  async _getCurrentCandles(state) {
    const address = state.address;
    // ★ V5-20: 调用计数, 用于验证此函数是否真的被调
    if (!this._gccStats) this._gccStats = { totalCalls: 0, ohlcvBranch: 0, ohlcvSuccess: 0, fallbackBranch: 0, lastCallTs: 0 };
    this._gccStats.totalCalls++;
    this._gccStats.lastCallTs = Date.now();

    if (OHLCV_REALTIME_ENABLED) {
      this._gccStats.ohlcvBranch++;
      // 优先尝试 Birdeye OHLCV
      const candles = await birdeye.getRecentOHLCV(address, KLINE_SEC, OHLCV_REALTIME_BARS);
      if (candles && candles.length > 0) {
        this._gccStats.ohlcvSuccess++;
        // ★ 把 Helius 链上交易聚合的 buyVolume/sellVolume 合并进来
        //   遍历 state.ticks 里的 chain tick，按 K 线时间桶累加
        const intervalMs = KLINE_SEC * 1000;
        const volByBucket = new Map();
        for (const t of (state.ticks || [])) {
          if (t.source !== 'chain') continue;
          const bucket = Math.floor(t.ts / intervalMs) * intervalMs;
          let v = volByBucket.get(bucket);
          if (!v) { v = { buyVolume: 0, sellVolume: 0, volume: 0 }; volByBucket.set(bucket, v); }
          const amt = t.solAmount || 0;
          v.volume += amt;
          if (t.isBuy) v.buyVolume += amt; else v.sellVolume += amt;
        }
        // ★ V5-32 修复：先建立 closed-candle openTime 集合，再把"无家可归"的桶
        //   （通常是当前未收盘桶 / 边界对不齐的桶）合并到最近的 closed candle 上，
        //   避免 1 分钟 K 线下大量链上量能因桶错位而被丢弃。
        const candleOpenTimes = new Set(candles.map(c => c.openTime));
        const orphanBuckets   = [];
        for (const [bucket, v] of volByBucket.entries()) {
          if (!candleOpenTimes.has(bucket)) orphanBuckets.push({ bucket, v });
        }
        // 最近的 closed candle openTime（candles 升序，最后一个最新）
        const latestClosedOT = candles.length > 0 ? candles[candles.length - 1].openTime : null;
        // 给每根 K 线注入对应桶的量能（Birdeye OHLCV 自带 volume 字段是 token 数量，不是 SOL，覆盖）
        for (const c of candles) {
          const v = volByBucket.get(c.openTime);
          if (v) {
            c.volume     = v.volume;
            c.buyVolume  = v.buyVolume;
            c.sellVolume = v.sellVolume;
          } else {
            // 没有链上数据时设为 0
            c.buyVolume  = 0;
            c.sellVolume = 0;
          }
        }
        // ★ 把孤儿桶合并到最近的 closed candle（仅当桶时间 > 最新 closed candle 时，
        //   即"当前未收盘桶"或"晚于最新 closed candle 的桶"。早于最新 closed candle
        //   的孤儿桶通常是历史预热段无对应 K 线，忽略即可）
        if (latestClosedOT !== null && orphanBuckets.length > 0) {
          const lastCandle = candles[candles.length - 1];
          let mergedCount = 0, mergedBuy = 0, mergedSell = 0;
          for (const { bucket, v } of orphanBuckets) {
            if (bucket > latestClosedOT) {
              lastCandle.buyVolume  = (lastCandle.buyVolume  || 0) + v.buyVolume;
              lastCandle.sellVolume = (lastCandle.sellVolume || 0) + v.sellVolume;
              lastCandle.volume     = (lastCandle.volume     || 0) + v.volume;
              mergedCount++;
              mergedBuy  += v.buyVolume;
              mergedSell += v.sellVolume;
            }
          }
          if (mergedCount > 0) {
            // 简单计数，便于 /diag 观察是否生效（这里用全局 stats，不是 per-token）
            if (!this._volMergeStats) this._volMergeStats = { totalMerges: 0, totalBuy: 0, totalSell: 0 };
            this._volMergeStats.totalMerges += mergedCount;
            this._volMergeStats.totalBuy    += mergedBuy;
            this._volMergeStats.totalSell   += mergedSell;
          }
        }
        // ★ V5-36: 删除 V5-17 的"用 _lastPriceUsd 覆盖最后一根 closed K 线 close"逻辑
        //   原意图: 让 stepRSI 的 lastClose 是实时的
        //   实际危害: 污染 closedCandles 序列, 导致 calcRSIWithState 算出错误的 lastClosedRsi/avgGain/avgLoss
        //   特别是 _lastPriceUsd 卡死 5 分钟没更新 + closedCandles 也卡死时, 整个 RSI 状态都是错的
        //   现在: closedCandles 保持 Birdeye 原数据不污染, currentCandle 仅在 _lastPriceUsd 新鲜时构造
        const closedOut = candles;
        let currentCandle = null;
        const lastPriceAge = state._lastPriceTs ? (Date.now() - state._lastPriceTs) : Infinity;
        if (closedOut.length > 0 &&
            state._lastPriceUsd && state._lastPriceUsd > 0 &&
            lastPriceAge < PRICE_FRESH_MS_FOR_SIGNAL) {
          // _lastPriceUsd 新鲜, 用它构造 currentCandle (基于最后一根 closed 的副本, 但不写回 closedOut)
          const base = closedOut[closedOut.length - 1];
          currentCandle = { ...base, close: state._lastPriceUsd };
          if (state._lastPriceUsd > currentCandle.high) currentCandle.high = state._lastPriceUsd;
          if (state._lastPriceUsd < currentCandle.low)  currentCandle.low  = state._lastPriceUsd;
        }
        // currentCandle = null 时, 上游 evaluateSignal 调用方 (monitor:989) 会 fallback 到 `price`
        // (那是过期检查后的 BirdeyeWS/HTTP 价格), 仍然安全
        return { closedCandles: closedOut, currentCandle, source: 'birdeye_ohlcv' };
      }
      // Birdeye OHLCV 拉不到 → fallback 到旧逻辑
      logger.debug('[Monitor] %s OHLCV 拉取失败, fallback 到 ticks 聚合', state.symbol);
    }

    // 旧逻辑：ticks 聚合 + 历史K线合并
    this._gccStats.fallbackBranch++;
    const { closed: rawClosedCandles, current: currentCandle } = buildCandles(state.ticks, KLINE_SEC);
    const liveClosed = filterValidCandles(rawClosedCandles);
    let closedCandles = liveClosed;
    if (state.historicalCandles && state.historicalCandles.length > 0) {
      const liveStart = liveClosed.length > 0 ? liveClosed[0].openTime : Infinity;
      const histFiltered = state.historicalCandles.filter(c => c.openTime < liveStart);
      closedCandles = [...histFiltered, ...liveClosed];
    }
    return { closedCandles, currentCandle, source: 'ticks_aggregated' };
  }

  // ── Helius 链上交易回调 ──────────────────────────────────────

  _onChainTrade(address, trade) {
    const state = this._tokens.get(address);
    if (!state) return;

    const now = Date.now();
    const tick = {
      price:     trade.priceSol,
      ts:        trade.ts || now,
      solAmount: trade.solAmount,
      isBuy:     trade.isBuy,
      source:    'chain',
    };

    state.ticks.push(tick);

    dataStore.appendTick(address, {
      ...tick,
      symbol:    state.symbol,
      signature: trade.signature,
      owner:     trade.owner,
    });

    // ★ V5-38: 链上 tick 兜底 _lastPriceUsd 刷新
    //   场景: 低流动性币 BirdeyeWS 长时间不推送 (_lastPriceAge 数十秒到几分钟)
    //         链上突然来一个 3-5 SOL 大单, 价格瞬间跳升 50%+
    //         策略仍用旧的 _lastPriceUsd 决策 → 信号触发后实际成交在新高位
    //   修法: 链上 tick 来时, 如果 _lastPriceUsd 已经太旧, 主动绕过 WS 缓存拉一次 HTTP 价格
    //   节流: 每个币每 30s 最多触发一次 (单币高频大单时不刷爆 CU)
    //         birdeye.getPriceForce 内部还有 60s HTTP 缓存兜底
    const lastPriceAge = state._lastPriceTs ? (now - state._lastPriceTs) : Infinity;
    const lastChainRefreshAge = state._lastChainRefreshTs ? (now - state._lastChainRefreshTs) : Infinity;
    if (lastPriceAge > 30000 && lastChainRefreshAge > 30000) {
      state._lastChainRefreshTs = now;  // 抢占节流锁, 防并发重复刷
      // 异步刷新, 不阻塞 trade 处理本身
      birdeye.getPriceForce(address).then(freshPrice => {
        if (freshPrice && freshPrice > 0) {
          const s = this._tokens.get(address);
          if (s) {
            s._lastPriceUsd = freshPrice;
            s._lastPriceTs  = Date.now();
            logger.debug('[Monitor] %s 链上tick触发价格刷新 → $%s (旧值%ss老)',
              s.symbol, freshPrice, Math.round(lastPriceAge / 1000));
          }
        }
      }).catch(err => {
        logger.debug('[Monitor] %s 链上tick刷价失败: %s', state.symbol, err.message);
      });
    }

    // ★ 链上交易也触发止损检查（用链上价格 × SOL/USD 估算）
    // 链上交易比 Birdeye WS 更快到达，不浪费这个信号
    if (state.inPosition && !state._selling && !this._stopLossLocks.has(address)) {
      // 用最新的 Birdeye USD 价格做止损判断（链上 priceSol 单位不同，不能直接比）
      // 但如果有卖出交易且价格大幅下跌，说明市场在抛售
      const lastUsd = state._lastPriceUsd;
      if (lastUsd && trade.isBuy === false && trade.solAmount > 5) {
        // 大额卖出交易 → 触发紧急价格刷新
        this._urgentStopCheck(address, state);
      }
    }

    logger.debug('[HeliusTrade] %s %s %.4f SOL @ %.10f (%s)',
      state.symbol,
      trade.isBuy ? 'BUY' : 'SELL',
      trade.solAmount,
      trade.priceSol,
      trade.signature?.slice(0, 12) || '?');
  }

  // ── 紧急止损价格刷新（链上检测到大额卖出时触发）────────────
  async _urgentStopCheck(address, state) {
    if (state._selling || this._stopLossLocks.has(address)) return;
    try {
      // 绕过缓存直接拉最新价格
      const price = await birdeye.getPrice(address);
      if (!price || price <= 0) return;
      state._lastPriceUsd = price;
      state._lastPriceTs = Date.now();

      const sl = checkStopLoss(price, state);
      if (sl.shouldExit) {
        logger.info('[Monitor] ⚡ 链上大卖触发止损 %s @ %.8f | %s', state.symbol, price, sl.reason);
        this._stopLossLocks.add(address);
        this._doSell(state, sl.reason).catch(err => {
          logger.error('[Monitor] 紧急止损失败 %s: %s', state.symbol, err.message);
        }).finally(() => {
          this._stopLossLocks.delete(address);
        });
      }
    } catch (_) {}
  }

  // ── 独立止损轮询（每 500ms，不依赖 WS 推送） ─────────────────

  _startStopLossPoller() {
    if (this._slPollTimer) return;
    this._slPollTimer = setInterval(() => this._stopLossPoll(), SL_POLL_SEC * 1000);
  }

  async _stopLossPoll() {
    for (const [address, state] of this._tokens.entries()) {
      if (!state.inPosition || state._selling || this._stopLossLocks.has(address)) continue;

      try {
        // ★ V5: 优先用 WS 缓存价格（10秒内有效），避免对所有持仓币发 HTTP
        //   只有 WS 价格过期超过60秒才发 HTTP 兜底
        let price = birdeye.priceStream.getCachedPrice(address);
        if (price === null) {
          // WS 缓存失效，检查 state 里最近的价格是否够新（60秒内）
          if (state._lastPriceUsd && Date.now() - state._lastPriceTs < 60000) {
            price = state._lastPriceUsd;
          } else {
            price = await birdeye.getPrice(address);
          }
        }
        if (!price || price <= 0) continue;

        state._lastPriceUsd = price;
        state._lastPriceTs = Date.now();

        // ★ V5-21: 最大持仓时间检查 (在所有其他卖出逻辑之前)
        //   防止僵持币(RSI 一直在 30-70 之间徘徊)长期占用资金
        if (MAX_HOLD_SEC > 0 && state.position?.buyTime) {
          const heldSec = Math.round((Date.now() - state.position.buyTime) / 1000);
          if (heldSec >= MAX_HOLD_SEC) {
            const heldH = (heldSec / 3600).toFixed(1);
            const reason = `TIMEOUT_EXIT(${heldH}h>=${(MAX_HOLD_SEC/3600).toFixed(0)}h)`;
            logger.info('[Monitor] ⏰ 超时卖出 %s @ %.8f | %s | 持仓%ds',
              state.symbol, price, reason, heldSec);
            this._stopLossLocks.add(address);
            this._doSell(state, reason).catch(err => {
              logger.error('[Monitor] 超时卖出失败 %s: %s', state.symbol, err.message);
            }).finally(() => {
              this._stopLossLocks.delete(address);
            });
            continue;
          }
        }

        // ── 1. 止损/移动止损检查 ──────────────────────────────
        const sl = checkStopLoss(price, state);
        if (sl.shouldExit) {
          const holdSec = state.position?.buyTime ? Math.round((Date.now() - state.position.buyTime) / 1000) : 0;
          logger.info('[Monitor] ⚡ 止损轮询触发 %s @ %.8f | %s | 持仓%ds',
            state.symbol, price, sl.reason, holdSec);
          this._stopLossLocks.add(address);
          this._doSell(state, sl.reason).catch(err => {
            logger.error('[Monitor] 止损执行失败 %s: %s', state.symbol, err.message);
          }).finally(() => {
            this._stopLossLocks.delete(address);
          });
          continue;
        }

        // ── 2. RSI 卖出检查（双重方式：已收盘K线 + stepRSI实时估算） ──
        // ★ V5-16: K 线来源由 _getCurrentCandles 决定（OHLCV 实时刷新 or ticks 聚合）
        const { closedCandles } = await this._getCurrentCandles(state);
        if (closedCandles && closedCandles.length > 0) {
          if (closedCandles.length >= RSI_CONFIG.RSI_PERIOD + 2) {
            const closes = closedCandles.map(c => c.close);
            const { rsiArray, avgGain, avgLoss } = calcRSIWithState(closes);
            const len     = closes.length;

            // ★ 同时缓存 avgGain/avgLoss/lastClose，供 WS tick 实时 RSI 使用
            const lastCandleTsPoll = closedCandles[len - 1].openTime;
            if (lastCandleTsPoll !== state._rsiLastCandleTs) {
              state._rsiAvgGain     = avgGain;
              state._rsiAvgLoss     = avgLoss;
              state._rsiLastClose   = closes[len - 1];
              state._rsiLastCandleTs = lastCandleTsPoll;
            }
            // ★ V5-9: 保存当前 closedCandles 数量，供 _checkRealtimeRsiSell 判断是否已收敛
            state._rsiClosedCount = len;

            // ★ V5-9: K线数不足 MIN_CANDLES_FOR_SIGNAL 时 RSI 未收敛，跳过所有 RSI 卖出逻辑
            if (len < RSI_CONFIG.MIN_CANDLES_FOR_SIGNAL) {
              continue; // 进入下一轮轮询
            }

            // ★ 用 stepRSI 计算实时 RSI（基于当前价格，而非等K线收盘）
            const rsiRealtime = stepRSI(avgGain, avgLoss, closes[len - 1], price);
            const rsiClosedLast = rsiArray[len - 1];  // 最新已收盘K线RSI（作为 prev 参考）

            // ★ V5-17: _slPollPrevRsi 加时效保护
            //   旧逻辑：直接用 state._slPollPrevRsi，没有时间戳。
            //   如果某轮轮询被卡住或币暂时跳过，再次跑时 prev 是几分钟前的值，
            //   和当前 rsiRealtime 比较会产生虚假下穿（如 71.0→69.4 实际从未发生）。
            //   修复：用 _slPollPrevRsiTs 记录时间戳，超过 STALE 视为失效，跳过本次下穿判断。
            const SL_PREV_STALE_MS = parseInt(process.env.SL_POLL_PREV_STALE_MS || '3000', 10);
            const slPrevTs = state._slPollPrevRsiTs || 0;
            const slIsStale = !Number.isFinite(state._slPollPrevRsi) || (Date.now() - slPrevTs) > SL_PREV_STALE_MS;
            const prevRsiPoll = slIsStale ? NaN : state._slPollPrevRsi;
            // 始终更新本次的 prev（无论这次是否触发）
            state._slPollPrevRsi   = rsiRealtime;
            state._slPollPrevRsiTs = Date.now();

            if (Number.isFinite(rsiRealtime)) {
              // ★ V5: RSI > 80 恐慌卖 — 改为用已收盘K线RSI判断，不用stepRSI
              //   stepRSI在K线内波动时容易算出虚假高值
              if (Number.isFinite(rsiClosedLast) && rsiClosedLast > _RSI_PANIC) {
                const lastPanicTs = state._lastPanicSellTs ?? 0;
                if (Date.now() - lastPanicTs >= 2000) {
                  state._lastPanicSellTs = Date.now();
                  logger.info('[Monitor] ⚡ RSI恐慌卖出(K线) %s @ %.8f | RSI_K=%.1f>%d',
                    state.symbol, price, rsiClosedLast, _RSI_PANIC);
                  this._doSell(state, `RSI_PANIC(K=${rsiClosedLast.toFixed(1)}>${_RSI_PANIC})`).catch(err => {
                    logger.error('[Monitor] RSI恐慌卖出失败 %s: %s', state.symbol, err.message);
                  });
                }
              }
              // RSI 下穿70：支持两种 prev 来源
              //   a) 上次轮询的实时 RSI (prevRsiPoll) — 500ms 间隔的 tick-to-tick 比较
              //   b) 最新已收盘K线 RSI (rsiClosedLast) — K线级别的下穿
              else if (Number.isFinite(prevRsiPoll) && prevRsiPoll >= _RSI_SELL && rsiRealtime < _RSI_SELL) {
                const lastCrossTs = state._lastRsiCrossSellTs ?? 0;
                if (Date.now() - lastCrossTs >= 2000) {
                  state._lastRsiCrossSellTs = Date.now();
                  logger.info('[Monitor] ⚡ RSI下穿卖出(轮询RT) %s @ %.8f | RSI %.1f→%.1f',
                    state.symbol, price, prevRsiPoll, rsiRealtime);
                  this._doSell(state, `RSI_CROSS_DOWN_70(RT:${prevRsiPoll.toFixed(1)}→${rsiRealtime.toFixed(1)})`).catch(err => {
                    logger.error('[Monitor] RSI下穿卖出失败 %s: %s', state.symbol, err.message);
                  });
                }
              }
              // 备用：已收盘K线级别下穿（保留原逻辑作为兜底）
              else if (Number.isFinite(rsiClosedLast)) {
                const rsiPrevClosed = rsiArray[len - 2];
                if (Number.isFinite(rsiPrevClosed) && rsiPrevClosed >= _RSI_SELL && rsiClosedLast < _RSI_SELL) {
                  const candleTs = closedCandles[len - 1].openTime;
                  if (candleTs !== state._lastSellCandle) {
                    state._lastSellCandle = candleTs;
                    logger.info('[Monitor] ⚡ RSI下穿卖出(K线) %s @ %.8f | RSI %.1f→%.1f',
                      state.symbol, price, rsiPrevClosed, rsiClosedLast);
                    this._doSell(state, `RSI_CROSS_DOWN_70(K:${rsiPrevClosed.toFixed(1)}→${rsiClosedLast.toFixed(1)})`).catch(err => {
                      logger.error('[Monitor] RSI下穿卖出失败 %s: %s', state.symbol, err.message);
                    });
                  }
                }
              }
            }
          }
        }
      } catch (_) {}
    }
  }

  // ── 主轮询 ────────────────────────────────────────────────────

  _scheduleNextPoll() {
    if (!this._started) return;
    this._pollTimer = setTimeout(() => this._poll(), POLL_SEC * 1000);
  }

  async _poll() {
    const now = Date.now();
    const addresses = Array.from(this._tokens.keys());

    // ★ V5: 并发控制 — 最多10个同时执行，避免47+币同时发HTTP请求
    const CONCURRENCY = 10;
    for (let i = 0; i < addresses.length; i += CONCURRENCY) {
      const batch = addresses.slice(i, i + CONCURRENCY);
      await Promise.allSettled(batch.map(addr => this._pollOne(addr, now)));
    }
    this._scheduleNextPoll();
  }

  async _pollOne(address, now) {
    const state = this._tokens.get(address);
    if (!state) return;

    // 正在卖出中，跳过此轮
    if (state._selling) return;

    // 1. 获取价格
    // ★ 优先用 BirdeyeWS 已推送的最新价格（state._lastPriceUsd 由 _onBirdeyePrice 实时更新）
    //   只有 WS 价格超过 PRICE_STALE_MS 没更新，才发 HTTP 兜底请求
    //   这样避免每秒对48个币发HTTP，尤其是低流动性币WS长时间不推送的情况
    const PRICE_STALE_MS = parseInt(process.env.PRICE_STALE_MS || '60000', 10); // ★ V5: 默认60秒
    let price;
    const wsAge = state._lastPriceUsd ? now - state._lastPriceTs : Infinity;
    if (state._lastPriceUsd && wsAge < PRICE_STALE_MS) {
      // WS 价格足够新鲜，直接用，不发 HTTP
      price = state._lastPriceUsd;
    } else {
      // WS 价格过期或没有，发 HTTP 兜底
      // ★ V5-13: getPrice 失败时返回 null（不再 throw），失败抑制由 birdeye.js 内部处理
      price = await birdeye.getPrice(address);
      if (price && price > 0) {
        state._lastPriceUsd = price;
        state._lastPriceTs  = now;
      } else {
        // 失败：如果有旧价格，宁可用旧的继续跑 RSI，不要直接 return
        if (!state._lastPriceUsd) return;
        price = state._lastPriceUsd;
      }
    }
    if (!price || price <= 0) return;

    // 2. WS 不可用时补 tick（仅在 HTTP 兜底拉到新价格时才需要，WS 正常时由 _onBirdeyePrice 负责）
    if (wsAge >= PRICE_STALE_MS) {
      const tick = { price, ts: now, source: 'price' };
      state.ticks.push(tick);
      dataStore.appendTick(address, { price, ts: now, source: 'price', symbol: state.symbol });
    }

    // 4. FDV/LP 检查（只用缓存值，巡检会定期刷新）
    //    ★ V5-16: 只在 无持仓 时才退出监控；持仓中即使 FDV/LP 跌破也保留，等正常出场
    const fdv = birdeye.getCachedFdv(address);
    if (fdv !== null && Number.isFinite(fdv)) {
      state.fdv = fdv;  // 更新state
      if (fdv < FDV_EXIT && !state.inPosition) {
        logger.warn('[Monitor] %s FDV=$%d < $%d，退出（无持仓）', state.symbol, Math.round(fdv), FDV_EXIT);
        await this.removeToken(address, `FDV_TOO_LOW($${Math.round(fdv)})`);
        return;
      }
    }
    // LP 退出检查（用 state 中巡检更新的值）
    if (state.lp !== null && Number.isFinite(state.lp) && state.lp < LP_EXIT && !state.inPosition) {
      logger.warn('[Monitor] %s LP=$%d < $%d，退出（无持仓）', state.symbol, Math.round(state.lp), LP_EXIT);
      await this.removeToken(address, `LP_TOO_LOW($${Math.round(state.lp)})`);
      return;
    }

    // 5. 裁剪 ticks（保留最近 2 小时）
    // ★ V5: 用 findIndex+splice 替代 while+shift，O(1) vs O(n)
    const cutoff = now - 2 * 60 * 60 * 1000;
    if (state.ticks.length > 0 && state.ticks[0].ts < cutoff) {
      const idx = state.ticks.findIndex(t => t.ts >= cutoff);
      if (idx > 0) state.ticks.splice(0, idx);
      else if (idx === -1) state.ticks.length = 0;  // 全部过期
    }

    // 6. 聚合 K 线（V5-16: 优先 Birdeye OHLCV 实时刷新，fallback 到 ticks 聚合）
    const { closedCandles, currentCandle } = await this._getCurrentCandles(state);

    // 7. RSI + 量能信号评估
    const realtimePrice = currentCandle?.close ?? price;
    const sigRes = evaluateSignal(closedCandles, realtimePrice, state);
    const { rsi, prevRsi, signal, reason, volume } = sigRes;

    // ★ V5-14: 决策追踪 —— 记录本次评估的完整上下文，方便排查"RSI到80为什么没卖"
    //   只存最近一次到 state，广播时附带
    {
      const lastCloses = closedCandles.slice(-5).map(c => c.close).filter(Number.isFinite);
      const ema99 = lastCloses.length >= 2 ? null : null; // 实际 EMA99 在 rsi.js 内部算，这里存数据特征
      const lastClose = closedCandles.length > 0 ? closedCandles[closedCandles.length-1].close : null;
      const lastCandleTs = closedCandles.length > 0 ? closedCandles[closedCandles.length-1].openTime : null;
      state._signalTrace = {
        ts: now,
        closedCount:    closedCandles.length,
        realtimePrice,
        lastClose,
        lastCandleTs,
        rsi:            Number.isFinite(rsi) ? parseFloat(rsi.toFixed(2)) : null,
        prevRsi:        Number.isFinite(prevRsi) ? parseFloat(prevRsi.toFixed(2)) : null,
        signal:         signal || null,
        reason:         reason || '',
        // 关键去重字段：用于诊断"本应触发的信号被同一K线去重挡住"
        lastBuyCandle:  state._lastBuyCandle  ?? null,
        lastSellCandle: state._lastSellCandle ?? null,
        // 冷却/持仓状态
        inPosition:     !!state.inPosition,
        selling:        !!state._selling,
        cooldownSec:    state._sellCooldownUntil > now ? Math.ceil((state._sellCooldownUntil - now) / 1000) : 0,
        // 最近 5 根 close，方便对比 GMGN 图表
        recentCloses:   lastCloses.map(v => Number(v.toPrecision(6))),
        // ★ V5-35: 买入信号触发时的详细诊断（仅当 signal=BUY）
        buyDiag:        sigRes._diag || null,
      };
    }

    // 8. 记录信号
    if (reason && reason !== '' && reason !== 'rsi_rebase') {
      dataStore.appendSignal({
        ts: now, address, symbol: state.symbol,
        price, rsi: Number.isFinite(rsi) ? parseFloat(rsi.toFixed(2)) : null,
        prevRsi: Number.isFinite(prevRsi) ? parseFloat(prevRsi.toFixed(2)) : null,
        signal, reason, volume, inPosition: state.inPosition,
        tradeCount: state.tradeCount,
      });
    }

    // 9. 广播实时数据
    const histLen = (state.historicalCandles && state.historicalCandles.length) || 0;
    // ★ V5-17: liveClosed 是 _getCurrentCandles 内部变量, 这里访问不到
    //   用 closedCandles.length - histLen 反推, 兜底 0
    const liveLen = Math.max(0, closedCandles.length - histLen);
    wsHub.broadcast({
      type:        'tick',
      address,
      symbol:      state.symbol,
      price,
      fdv,
      lp:          state.lp,
      createdAt:   state.createdAt,
      rsi:         Number.isFinite(rsi) ? parseFloat(rsi.toFixed(2)) : null,
      prevRsi:     Number.isFinite(prevRsi) ? parseFloat(prevRsi.toFixed(2)) : null,
      signal,
      reason,
      closedCount: closedCandles.length,
      // ★ K线诊断：分别看历史/实时/最终合并数和 Birdeye 返回情况
      candleStats: {
        histCount:     histLen,
        liveCount:     liveLen,
        mergedCount:   closedCandles.length,
        histMeta:      state._histMeta || null,
      },
      inPosition:  state.inPosition,
      volume,
      tradeCount:  state.tradeCount,
      cooldown:    state._sellCooldownUntil > now ? Math.ceil((state._sellCooldownUntil - now) / 1000) : 0,
      dryRun:      DRY_RUN,
      ts:          now,
      birdeyeWs:   birdeye.priceStream.isConnected(),
      heliusWs:    heliusWs.isConnected(),
      heliusStats: heliusWs.getStats(),
      // ★ 诊断：这个 token 从 Helius 收到/解析成功的链上交易笔数
      chainStats:  heliusWs.getTokenStats(address),
      // ★ V5-13: 这个 token 的 Birdeye 价格失败状态（None = 正常）
      priceFail:   birdeye.getPriceFailStatus(address),
      // ★ V5-18: X (Twitter) 24h 提及数（每 2 小时刷新一次, null=未拉取）
      xMentions:   xMentions.getMentions(address),
      // ★ V5-14: 决策追踪（最近一次 evaluateSignal 的完整上下文）
      signalTrace: state._signalTrace || null,
    });

    logger.debug('[RSI] %s price=%.6f rsi=%.2f prev=%.2f signal=%s reason=%s trades=%d inPos=%s cool=%ds',
      state.symbol, price, rsi, prevRsi, signal || 'none', reason,
      state.tradeCount, state.inPosition,
      state._sellCooldownUntil > now ? Math.ceil((state._sellCooldownUntil - now) / 1000) : 0);

    // 10. 执行信号
    if (signal === 'BUY' && !state.inPosition && this._canBuy(state, now)) {
      // ★ 买入前强制刷新 FDV（绕过缓存，确保数据最新）
      const freshFdv = await birdeye.getFdvFresh(address);
      if (freshFdv !== null && Number.isFinite(freshFdv) && freshFdv < FDV_EXIT) {
        logger.warn('[Monitor] %s 买入被拒: FDV=$%d < $%d', state.symbol, Math.round(freshFdv), FDV_EXIT);
      } else {
        state.fdv = freshFdv ?? state.fdv;  // 更新最新 FDV
        await this._doBuy(state, price, reason);
      }
    } else if (signal === 'SELL' && state.inPosition && !state._selling) {
      await this._doSell(state, reason);
    }
  }

  // ── 是否可以买入 ────────────────────────────────────────────────

  _canBuy(state, now) {
    // 已在持仓中
    if (state.inPosition) return false;
    // 正在卖出中
    if (state._selling) return false;
    // 冷却期中
    if (now < state._sellCooldownUntil) {
      logger.debug('[Monitor] %s 冷却中，还剩 %ds',
        state.symbol, Math.ceil((state._sellCooldownUntil - now) / 1000));
      return false;
    }
    return true;
  }

  // ── 买入 ────────────────────────────────────────────────────────

  async _doBuy(state, price, reason) {
    const tradeNum = state.tradeCount + 1;
    logger.info('[Monitor] 🟢 BUY #%d %s @ %.8f | %s | DRY_RUN=%s',
      tradeNum, state.symbol, price, reason, DRY_RUN);

    // ★ V5-35: 买入瞬间详细诊断日志，便于排查"RSI 与 GMGN 图表对不上"等问题
    //   一旦观察到怪买入, 直接 grep "BUY_DIAG <symbol>" 拉所有数据
    const diag = state._signalTrace?.buyDiag;
    if (diag) {
      const closesStr = (diag.recentCloses || []).map(v => Number(v).toPrecision(6)).join(',');
      const lastCandleAge = diag.lastCandleTs ? Math.round((Date.now() - diag.lastCandleTs) / 1000) : -1;
      const lastPriceAge  = state._lastPriceTs ? Math.round((Date.now() - state._lastPriceTs) / 1000) : -1;
      logger.info(
        '[Monitor] BUY_DIAG %s | closedCount=%d lastCandleAge=%ds | ' +
        'lastClose=%s realtimePrice=%s | lastClosedRsi=%s rsiRealtime=%s | ' +
        'avgGain=%s avgLoss=%s | _lastPriceUsd=%s _lastPriceAge=%ds | recentCloses=[%s]',
        state.symbol,
        diag.closedCount,
        lastCandleAge,
        Number(diag.lastClose).toPrecision(6),
        Number(diag.realtimePrice).toPrecision(6),
        Number(diag.lastClosedRsi).toFixed(2),
        Number(diag.rsiRealtime).toFixed(2),
        Number(diag.avgGain).toPrecision(4),
        Number(diag.avgLoss).toPrecision(4),
        state._lastPriceUsd ? Number(state._lastPriceUsd).toPrecision(6) : 'null',
        lastPriceAge,
        closesStr
      );
    } else {
      logger.warn('[Monitor] BUY_DIAG %s | (无诊断数据, _signalTrace.buyDiag 为空)', state.symbol);
    }

    state.inPosition = true;

    if (DRY_RUN) {
      const simulatedTokens = Math.floor(TRADE_SOL / price * 1e9);
      state.position = {
        entryPriceUsd : price,
        amountToken   : simulatedTokens,
        solIn         : TRADE_SOL,
        buyTxid       : `DRY_${Date.now()}`,
        buyTime       : Date.now(),
        buyReason     : reason,
        _peakPrice    : price,  // ★ 移动止损：初始峰值 = 买入价
      };
      state.tradeCount++;
      this._addTradeLog(state, { type: 'BUY', symbol: state.symbol, price, reason,
        txid: state.position.buyTxid, solIn: TRADE_SOL, dryRun: true, tradeNum });
      await this._createTradeRecord(state);
      logger.info('[Monitor] ✅ DRY_RUN BUY #%d %s @ %.8f  solIn=%.4f',
        tradeNum, state.symbol, price, TRADE_SOL);
    } else {
      try {
        const result = await trader.buy(state.address, state.symbol, price);

        // ★ 买单成交后，等 500ms 再查一次实际成交价
        //   避免用"信号触发时价格"做止损基准（memecoin 滑点可能很大）
        let actualEntryPrice = price;
        try {
          await new Promise(r => setTimeout(r, 500));
          const postFillPrice = await birdeye.getPrice(state.address);
          if (postFillPrice && postFillPrice > 0) {
            actualEntryPrice = postFillPrice;
            if (Math.abs(postFillPrice - price) / price > 0.02) {
              logger.warn('[Monitor] ⚠️ BUY #%d %s 成交价偏差: 信号=%.6f 实际=%.6f (%.1f%%)',
                tradeNum, state.symbol, price, postFillPrice,
                (postFillPrice - price) / price * 100);
            }
          }
        } catch (_) { /* 查询失败保留信号价 */ }

        state.position = {
          entryPriceUsd : actualEntryPrice,  // ★ 用实际成交后价格，不用信号触发时价格
          signalPriceUsd: price,             // 保留信号价用于参考
          amountToken   : result.amountOut,
          solIn         : result.solIn,
          buyTxid       : result.txid,
          buyTime       : Date.now(),
          buyReason     : reason,
          _peakPrice    : actualEntryPrice,  // ★ 移动止损：初始峰值 = 实际成交价
        };
        state.tradeCount++;
        this._addTradeLog(state, { type: 'BUY', symbol: state.symbol,
          price: actualEntryPrice, signalPrice: price, reason,
          txid: result.txid, solIn: result.solIn, tradeNum });
        await this._createTradeRecord(state);
        logger.info('[Monitor] ✅ BUY #%d %s  solIn=%.4f SOL  entryPrice=%.6f  txid=%s',
          tradeNum, state.symbol, result.solIn, actualEntryPrice, result.txid);
      } catch (err) {
        if (err && err.name === 'PriceDeviationError') {
          // ★ V5-39: 价格偏离过大被拒, 进 30s 冷却避免反复触发
          logger.warn('[Monitor] ⏸ %s 因价格偏离取消下单, 进入 30s 冷却: %s',
            state.symbol, err.message);
          state._sellCooldownUntil = Date.now() + 30000;
        } else {
          logger.error('[Monitor] ❌ BUY #%d %s 失败: %s', tradeNum, state.symbol, err.message);
        }
        state.inPosition = false;
      }
    }
  }

  // ── 卖出（不再退出监控，重置状态等待下一轮） ────────────────────

  async _doSell(state, reason) {
    if (state._selling) return;  // 防并发
    state._selling = true;

    const isStopLoss = reason.includes('STOP_LOSS') || reason.includes('TAKE_PROFIT');
    const tradeNum = state.tradeCount;
    logger.info('[Monitor] 🔴 SELL #%d %s | %s | isStopLoss=%s | DRY_RUN=%s',
      tradeNum, state.symbol, reason, isStopLoss, DRY_RUN);

    if (DRY_RUN) {
      let currentPrice;
      try {
        currentPrice = await birdeye.getPrice(state.address);
      } catch (_) {
        currentPrice = state._lastPriceUsd
          || (state.ticks.length > 0 ? state.ticks[state.ticks.length - 1].price : 0)
          || state.position?.entryPriceUsd || 0;
      }

      const solIn  = state.position?.solIn ?? TRADE_SOL;
      const entryP = state.position?.entryPriceUsd ?? 0;
      const solOut = entryP > 0 ? solIn * (currentPrice / entryP) : 0;
      const pnlPct = entryP > 0 ? (currentPrice - entryP) / entryP * 100 : 0;
      const pnlSol = solOut - solIn;

      state.inPosition = false;
      this._addTradeLog(state, { type: 'SELL', symbol: state.symbol, reason,
        txid: `DRY_${Date.now()}`, solOut, pnlSol, dryRun: true, tradeNum });
      this._finalizeTradeRecord(state, reason, solOut, pnlPct);

      logger.info('[Monitor] ✅ DRY_RUN SELL #%d %s  solIn=%.4f  solOut=%.4f  pnl=%+.4f SOL (%+.1f%%)',
        tradeNum, state.symbol, solIn, solOut, pnlSol, pnlPct);
    } else {
      try {
        const result = await trader.sell(state.address, state.symbol, state.position, isStopLoss);
        const solOut  = Number.isFinite(result.solOut) ? result.solOut : 0;
        const solIn   = state.position?.solIn ?? TRADE_SOL;
        const pnlPct  = solIn > 0 ? (solOut - solIn) / solIn * 100 : 0;
        const pnlSol  = solOut - solIn;

        if (!Number.isFinite(result.solOut)) {
          logger.warn('[Monitor] ⚠️ SELL #%d %s solOut 缺失/异常 (raw=%s)，链上已卖出但无法计算盈亏',
            tradeNum, state.symbol, result.solOut);
        }

        state.inPosition = false;
        this._addTradeLog(state, { type: 'SELL', symbol: state.symbol, reason,
          txid: result.txid, solOut, pnlSol, elapsedMs: result.elapsedMs, tradeNum });
        this._finalizeTradeRecord(state, reason, solOut, pnlPct);

        logger.info('[Monitor] ✅ SELL #%d %s  solIn=%.4f  solOut=%.4f  pnl=%+.4f SOL (%+.1f%%)  耗时=%dms  txid=%s',
          tradeNum, state.symbol, solIn, solOut, pnlSol, pnlPct, result.elapsedMs || 0, result.txid);
      } catch (err) {
        logger.error('[Monitor] ❌ SELL #%d %s 失败: %s', tradeNum, state.symbol, err.message);
        state.inPosition = false;
        this._finalizeTradeRecord(state, `SELL_FAILED(${reason})`, 0, -100);
      }
    }

    // ★ 重置状态，准备下一轮交易
    state._selling = false;
    state.position = null;

    // ★ 设置冷却期
    state._sellCooldownUntil = Date.now() + SELL_COOLDOWN_SEC * 1000;
    // 重置 RSI 穿越防抖（允许新的穿越信号）
    state._lastBuyCandle  = -1;
    state._lastSellCandle = -1;
    state._lastPanicSellTs = 0;
    state._lastRsiCrossSellTs = 0;  // ★ 重置实时RSI下穿防抖
    state._wsTickPrevRsi  = NaN;    // ★ 重置WS tick RSI历史
    state._slPollPrevRsi  = NaN;    // ★ 重置轮询RSI历史

    logger.info('[Monitor] 🔄 %s 第%d笔完成 | 冷却=%ds',
      state.symbol, tradeNum, SELL_COOLDOWN_SEC);
  }

  // ── 辅助工具 ────────────────────────────────────────────────────

  _addTradeLog(state, log) {
    state.tradeLogs.push({ ...log, ts: Date.now() });
    if (state.tradeLogs.length > 500) state.tradeLogs.shift();
    wsHub.broadcast({ type: 'trade_log', ...log, ts: Date.now() });
    this.emit('trade', log);
  }

  async _createTradeRecord(state) {
    if (!state.position) return;

    // ★ V5: 买入时优先用 FDV 缓存中的 LP（_fetchOverview 同时返回 fdv 和 lp）
    //   getFdvFresh 在 _doBuy 前已经调过了，缓存应该是热的
    let realTimeLp = state.lp;
    try {
      const cached = birdeye.getCachedFdv(state.address);
      // getCachedFdv只返回fdv，LP需要从overview缓存中取
      const lp = await birdeye.getLiquidity(state.address); // 会命中缓存
      if (lp !== null && Number.isFinite(lp)) {
        realTimeLp = lp;
        state.lp = lp;
      }
    } catch (_) {}

    const rec = {
      id:         `${state.address}_${state.tradeCount}_${Date.now()}`,
      address:    state.address,
      symbol:     state.symbol,
      tradeNum:   state.tradeCount,
      createdAt:  state.createdAt,  // ★ V5: 代币创建时间
      buyAt:      state.position.buyTime,
      buyTxid:    state.position.buyTxid,
      entryPrice: state.position.entryPriceUsd,
      entryFdv:   state.fdv,
      entryLp:    realTimeLp,
      solIn:      state.position.solIn,
      buyReason:  state.position.buyReason || '',
      dryRun:     DRY_RUN,
      exitAt:     null,
      exitReason: null,
      solOut:     null,
      pnlPct:    null,
      pnlSol:    null,
    };
    state.tradeRecords.push(rec);
    _allTradeRecords.unshift(rec);
    dataStore.appendTrade(rec);

    const cutoff = Date.now() - 24 * 3600 * 1000;
    while (_allTradeRecords.length && _allTradeRecords[_allTradeRecords.length - 1].buyAt < cutoff) {
      _allTradeRecords.pop();
    }
    wsHub.broadcast({ type: 'trade_record', ...rec });
  }

  _finalizeTradeRecord(state, reason, solOut, pnlPct) {
    const rec = state.tradeRecords[state.tradeRecords.length - 1];
    if (!rec) return;

    // ★ 防御 NaN / undefined — 确保写入合法数值，否则前端显示"持仓中"
    const safeSolOut = Number.isFinite(solOut) ? solOut : 0;
    const safePnlPct = Number.isFinite(pnlPct) ? pnlPct : (safeSolOut > 0 && rec.solIn > 0 ? (safeSolOut - rec.solIn) / rec.solIn * 100 : -100);
    const safeSolIn  = state.position?.solIn ?? rec.solIn ?? 0;

    rec.exitAt     = Date.now();
    rec.exitReason = reason;
    rec.solOut     = parseFloat(safeSolOut.toFixed(6));
    rec.pnlPct    = parseFloat(safePnlPct.toFixed(2));
    rec.pnlSol    = parseFloat((safeSolOut - safeSolIn).toFixed(6));

    // ★ 同步更新 _allTradeRecords 中的对应记录（修复引用不一致导致前端显示"持仓中"）
    const globalRec = _allTradeRecords.find(r => r.id === rec.id);
    if (globalRec && globalRec !== rec) {
      globalRec.exitAt     = rec.exitAt;
      globalRec.exitReason = rec.exitReason;
      globalRec.solOut     = rec.solOut;
      globalRec.pnlPct    = rec.pnlPct;
      globalRec.pnlSol    = rec.pnlSol;
    }

    dataStore.updateTrade(rec.id, {
      exitAt:     rec.exitAt,
      exitReason: rec.exitReason,
      solOut:     rec.solOut,
      pnlPct:    rec.pnlPct,
      pnlSol:    rec.pnlSol,
    });

    wsHub.broadcast({ type: 'trade_record', ...rec });
  }

  _stateSnapshot(state) {
    const now = Date.now();
    return {
      address:      state.address,
      symbol:       state.symbol,
      addedAt:      state.addedAt,
      createdAt:    state.createdAt,
      inPosition:   state.inPosition,
      tradeCount:   state.tradeCount,
      cooldown:     state._sellCooldownUntil > now ? Math.ceil((state._sellCooldownUntil - now) / 1000) : 0,
      tradeLogs:    state.tradeLogs,
      tradeRecords: state.tradeRecords,
      dryRun:       DRY_RUN,
      lastPrice:    state._lastPriceUsd,
      lastPriceTs:  state._lastPriceTs,
      fdv:          state.fdv,
      lp:           state.lp,
    };
  }

  _broadcastTokenList() {
    wsHub.broadcast({ type: 'token_list', tokens: this.getTokens() });
  }

  // ── ★ V5: FDV/LP/Age 巡检（分散请求，每轮间隔 OVERVIEW_PATROL_SEC）──────

  _startOverviewPatrol() {
    // 启动后延迟30秒开始第一轮巡检（等WS连接稳定）
    this._patrolTimer = setTimeout(() => this._runOverviewPatrol(), 30000);
  }

  async _runOverviewPatrol() {
    if (!this._started) return;
    const addresses = Array.from(this._tokens.keys());
    if (addresses.length === 0) {
      this._patrolTimer = setTimeout(() => this._runOverviewPatrol(), OVERVIEW_PATROL_SEC * 1000);
      return;
    }

    // 分散请求：每个币之间间隔 2 秒，95个币约3分钟完成一轮
    const INTERVAL_PER_TOKEN = 2000;
    logger.info('[Patrol] 开始 FDV/LP/Age 巡检，%d 个代币，预计 %ds',
      addresses.length, Math.ceil(addresses.length * INTERVAL_PER_TOKEN / 1000));

    for (let i = 0; i < addresses.length; i++) {
      if (!this._started) return;
      const address = addresses[i];
      const state = this._tokens.get(address);
      if (!state) continue;

      try {
        const overview = await birdeye.getOverview(address);
        if (!overview) continue;

        // 更新 state
        if (overview.fdv !== null && Number.isFinite(overview.fdv)) state.fdv = overview.fdv;
        if (overview.liquidity !== null && Number.isFinite(overview.liquidity)) state.lp = overview.liquidity;
        if (overview.createdAt) state.createdAt = overview.createdAt; // ★ 始终更新，确保Age数据存在

        // ★ V5-24: overview 没给 createdAt → 走专门接口兜底 (永久缓存, 已拉过的不会重复请求)
        if (!state.createdAt) {
          try {
            const ts = await birdeye.getCreationInfo(address);
            if (ts) state.createdAt = ts;
          } catch (_) {}
        }

        // ★ FDV 退出检查（V5-16: 持仓中不退出）
        if (state.fdv !== null && Number.isFinite(state.fdv) && state.fdv < FDV_EXIT && !state.inPosition) {
          logger.warn('[Patrol] %s FDV=$%d < $%d，退出监控（无持仓）', state.symbol, Math.round(state.fdv), FDV_EXIT);
          await this.removeToken(address, `FDV_TOO_LOW($${Math.round(state.fdv)})`);
          continue;
        }

        // ★ LP 退出检查（V5-16: 持仓中不退出）
        if (state.lp !== null && Number.isFinite(state.lp) && state.lp < LP_EXIT && !state.inPosition) {
          logger.warn('[Patrol] %s LP=$%d < $%d，退出监控（无持仓）', state.symbol, Math.round(state.lp), LP_EXIT);
          await this.removeToken(address, `LP_TOO_LOW($${Math.round(state.lp)})`);
          continue;
        }

        logger.debug('[Patrol] %s FDV=$%s LP=$%s age=%s',
          state.symbol,
          state.fdv ? Math.round(state.fdv) : '?',
          state.lp ? Math.round(state.lp) : '?',
          state.createdAt ? Math.round((Date.now() - state.createdAt) / 3600000) + 'h' : '?');
      } catch (err) {
        logger.warn('[Patrol] %s 巡检失败: %s', state.symbol, err.message);
      }

      // 等待间隔再查下一个
      if (i < addresses.length - 1) {
        await new Promise(r => setTimeout(r, INTERVAL_PER_TOKEN));
      }
    }

    logger.info('[Patrol] 巡检完成，下次 %ds 后', OVERVIEW_PATROL_SEC);
    this._patrolTimer = setTimeout(() => this._runOverviewPatrol(), OVERVIEW_PATROL_SEC * 1000);
  }

  // ── ★ V6: 监控数满时清理 ─────────────────────────────────
  // V5-12: 维度从"本地 ticks 累计(SOL)"改为"Birdeye 24h volume(USD)"，数据更准确，
  //        不受本地订阅延迟/订阅失败/新币刚加入 ticks 为空 等因素影响。
  //        复用 token_overview 缓存，零额外 API 请求。

  async _evictForNewToken() {
    if (this._tokens.size < MAX_TOKENS) return true; // 有空位

    // 候选：未持仓 且 未在卖出中的币
    const candidates = Array.from(this._tokens.values())
      .filter(s => !s.inPosition && !s._selling);

    if (candidates.length === 0) {
      logger.warn('[Monitor] 监控已满(%d/%d)且所有代币都持仓中，无法清理',
        this._tokens.size, MAX_TOKENS);
      return false;
    }

    // 并发查 Birdeye 24h volume（命中缓存时几乎零开销）
    const withVol = await Promise.all(candidates.map(async (s) => {
      let vol = null;
      try { vol = await birdeye.getV24hUSD(s.address); } catch (_) {}
      // Birdeye 拉取失败则用 0 参与排序（最可能被淘汰，安全选择）
      return { state: s, vol24h: vol ?? 0, volMissing: vol == null };
    }));

    // 按 24h volume 升序，最小的优先淘汰
    withVol.sort((a, b) => a.vol24h - b.vol24h);

    const { state: victim, vol24h, volMissing } = withVol[0];
    const volStr = volMissing ? 'N/A(拉取失败)' : `$${vol24h.toFixed(0)}`;
    logger.info('[Monitor] 🧹 监控已满(%d/%d)，清理 Birdeye 24h 量最低代币 %s (%s)',
      this._tokens.size, MAX_TOKENS, victim.symbol, volStr);
    this.removeToken(victim.address, `EVICTED(v24h=${volStr})`);
    return true;
  }
}

function getAllTradeRecords() {
  const cutoff = Date.now() - 24 * 3600 * 1000;
  const memRecords = _allTradeRecords.filter(r => r.buyAt > cutoff);
  if (memRecords.length === 0) {
    return dataStore.loadTrades().filter(r => r.buyAt > cutoff);
  }
  return memRecords;
}

const monitor = new TokenMonitor();
module.exports = monitor;
module.exports.getAllTradeRecords = getAllTradeRecords;
module.exports.DRY_RUN = DRY_RUN;
