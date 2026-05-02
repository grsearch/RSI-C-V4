'use strict';
// src/heliusWs.js — Helius Enhanced WebSocket 链上交易监听
//
// V4 — 自动模式（auto）：根据代币数量动态切换订阅策略
//
// ★ 模式 "auto"（默认）：
//   - 代币数 ≤ HELIUS_TOKEN_LIMIT（默认15）→ token 精准模式
//   - 代币数 > HELIUS_TOKEN_LIMIT           → 自动升级到 pump 单订阅
//   - 升级后添加/移除代币无需重新订阅，无限扩展
//
// ★ 模式 "token"（手动强制）：
//   每个 token 独立 transactionSubscribe，credits 最省，适合 ≤15 个代币
//
// ★ 模式 "pump"（手动强制）：
//   一个 transactionSubscribe(accountInclude: [PumpAMM])，全量过滤
//   适合大量代币或频繁增删代币的场景
//
// credits 对比（监控 5 个 Pump AMM token）：
//   "token" 模式：~10 笔/秒 × ~0.5KB ≈ 100 credits/s ≈ 860万/天
//   "pump"  模式：~80 笔/秒 × ~0.5KB ≈ 800 credits/s ≈ 6900万/天

const WebSocket = require('ws');
const logger    = require('./logger');

// ── 配置 ────────────────────────────────────────────────────────

const HELIUS_WSS_URL         = process.env.HELIUS_WSS_URL || '';
const HELIUS_GATEKEEPER_URL  = process.env.HELIUS_GATEKEEPER_URL || '';
const HELIUS_API_KEY         = process.env.HELIUS_API_KEY || '';
const HELIUS_RPC_URL         = process.env.HELIUS_RPC_URL || '';

// "multi" | "token" | "pump" | "auto"
// ★ multi = 所有 mint 合并成一个 transactionSubscribe(accountInclude: [mint1, mint2, ...])
//           每次加/删 token 时重订阅。只占 1 个 subscription slot，彻底规避多订阅丢单问题。
//           适合所有场景（Pump / Raydium / 其他 DEX 混合），推荐默认。
// token = 每个 mint 一个独立 transactionSubscribe。credits 最省但容易丢单，不推荐。
// pump  = 一个 transactionSubscribe(accountInclude: [PumpAMM])，全量订阅后过滤。
//         只适合全部代币都在 pumpAMM 上交易的场景。
// auto  = 代币数 ≤ HELIUS_TOKEN_LIMIT 用 token，超过自动切 pump（不推荐，会漏非 Pump 代币）
const CFG_SUB_MODE    = (process.env.HELIUS_SUB_MODE || 'multi').toLowerCase();
// auto 模式下，超过此数量自动切换到 pump 单订阅
const TOKEN_LIMIT     = parseInt(process.env.HELIUS_TOKEN_LIMIT || '50', 10);
// multi 模式：token 增删后延迟多久重订阅（防抖，避免批量添加时频繁重订）
const MULTI_RESUB_DEBOUNCE_MS = parseInt(process.env.HELIUS_MULTI_DEBOUNCE_MS || '500', 10);
// ★ multi 模式：每个订阅最多包含多少个 mint
//   Helius 实测超过 ~60 个会返回 Internal error / Invalid Request
//   超过此数量自动拆分为多个并行订阅；保守值 50 留余量
const MULTI_MAX_MINTS_PER_SUB = parseInt(process.env.HELIUS_MAX_MINTS_PER_SUB || '50', 10);

function getWsUrl() {
  if (HELIUS_GATEKEEPER_URL) {
    let url = HELIUS_GATEKEEPER_URL;
    if (url.startsWith('https://')) url = url.replace('https://', 'wss://');
    if (!url.startsWith('wss://')) url = `wss://${url}`;
    return { url, type: 'gatekeeper' };
  }
  if (HELIUS_WSS_URL) {
    return { url: HELIUS_WSS_URL, type: 'enhanced' };
  }
  const apiKey = HELIUS_API_KEY || extractApiKey(HELIUS_RPC_URL);
  if (!apiKey) return { url: '', type: 'none' };
  return { url: `wss://mainnet.helius-rpc.com/?api-key=${apiKey}`, type: 'enhanced' };
}

function extractApiKey(rpcUrl) {
  const m = (rpcUrl || '').match(/api-key=([a-f0-9-]+)/i);
  return m ? m[1] : '';
}

const PUMP_AMM_PROGRAM = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA';

const LAMPORTS     = 1e9;
const PING_MS      = 25000;
const RECONNECT_MS = 2000;
const MAX_RETRIES  = 999;

// ── HeliusTradeStream ───────────────────────────────────────────

class HeliusTradeStream {
  constructor() {
    this._ws          = null;
    this._pingTimer   = null;
    this._connected   = false;
    this._retryCount  = 0;
    this._connType    = 'none';

    // token → { symbol, onTrade, subId, rpcId }
    this._tokens = new Map();

    // rpcId → tokenAddress | '__pump__'（用于匹配订阅确认）
    this._pendingSubs = new Map();
    this._nextRpcId = 100;

    // pump 模式的 subId
    this._pumpSubId = null;

    // ★ multi 模式的 subId 列表(分批订阅)和重订阅防抖定时器
    this._multiSubIds = [];          // 所有活跃订阅的 subId
    this._multiResubTimer = null;

    // ★ 实际激活的模式（auto模式下动态变化）
    // 初始值根据配置决定
    if (CFG_SUB_MODE === 'pump')       this._activeMode = 'pump';
    else if (CFG_SUB_MODE === 'multi') this._activeMode = 'multi';
    else if (CFG_SUB_MODE === 'auto')  this._activeMode = 'token';  // auto 从 token 起步
    else                               this._activeMode = 'token';

    this._stats = { txReceived: 0, txMatched: 0, txParsed: 0, txSkipped: 0, connType: 'none', subMode: this._activeMode };
  }

  // ── 当前是否使用 pump 模式 ──────────────────────────────────
  _isPumpMode()  { return this._activeMode === 'pump'; }
  _isMultiMode() { return this._activeMode === 'multi'; }

  // ── 生命周期 ────────────────────────────────────────────────

  start() {
    const { url, type } = getWsUrl();
    if (!url) {
      logger.warn('[HeliusWS] ⚠️ 未配置 Helius WebSocket URL，链上量能数据不可用');
      return;
    }
    this._connType = type;
    this._stats.connType = type;
    logger.info('[HeliusWS] 配置模式: %s (当前激活: %s, 代币上限: %d)',
      CFG_SUB_MODE, this._activeMode, TOKEN_LIMIT);
    this._connect(url);
  }

  stop() {
    this._connected = false;
    this._retryCount = MAX_RETRIES + 1;
    if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }
    if (this._ws) {
      try { this._ws.close(); } catch (_) {}
      this._ws = null;
    }
  }

  // ── 连接管理 ────────────────────────────────────────────────

  _connect(wsUrl) {
    const safeUrl = wsUrl.replace(/api-key=[a-f0-9-]+/i, 'api-key=***');
    logger.info('[HeliusWS] 连接 %s (类型: %s) ...', safeUrl, this._connType);

    this._ws = new WebSocket(wsUrl);

    this._ws.on('open', () => {
      logger.info('[HeliusWS] ✅ %s WebSocket 已连接 (模式: %s)', this._connType.toUpperCase(), this._activeMode);
      this._connected  = true;
      this._retryCount = 0;

      this._pingTimer = setInterval(() => {
        if (this._ws?.readyState === WebSocket.OPEN) this._ws.ping();
      }, PING_MS);

      // 重连后恢复订阅（用当前激活的模式）
      if (this._isPumpMode()) {
        this._subscribePumpAmm();
      } else if (this._isMultiMode()) {
        // ★ multi 模式：直接发一个包含所有 mint 的订阅
        this._subscribeMulti();
      } else {
        // ★ 按顺序延迟恢复订阅，每个间隔150ms，避免瞬间大量请求压垮连接
        let i = 0;
        for (const [address, info] of this._tokens.entries()) {
          info.subId = null;
          const delay = i * 150;
          setTimeout(() => {
            if (this._tokens.has(address) && this._connected && !this._isPumpMode() && !this._isMultiMode()) {
              this._subscribeToken(address);
            }
          }, delay);
          i++;
        }
      }
    });

    this._ws.on('message', (data) => this._handleMessage(data));
    this._ws.on('pong', () => {});
    this._ws.on('error', (err) => logger.error('[HeliusWS] 错误: %s', err.message));

    this._ws.on('close', () => {
      logger.warn('[HeliusWS] 连接关闭');
      this._connected = false;
      this._pendingSubs.clear();
      this._pumpSubId = null;
      this._multiSubIds = [];
      if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }

      if (this._retryCount < MAX_RETRIES) {
        this._retryCount++;
        const delay = Math.min(RECONNECT_MS * Math.pow(1.5, this._retryCount - 1), 30000);
        logger.info('[HeliusWS] %ds 后重连 (第%d次)', (delay / 1000).toFixed(0), this._retryCount);
        setTimeout(() => {
          const { url } = getWsUrl();
          if (url) this._connect(url);
        }, delay);
      }
    });
  }

  // ── 订阅：Pump AMM 模式 ───────────────────────────────────

  _subscribePumpAmm() {
    if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;

    const rpcId = this._nextRpcId++;
    this._pendingSubs.set(rpcId, '__pump__');

    this._ws.send(JSON.stringify({
      jsonrpc: '2.0',
      id: rpcId,
      method: 'transactionSubscribe',
      params: [
        { accountInclude: [PUMP_AMM_PROGRAM], failed: false },
        {
          commitment: 'confirmed',
          encoding: 'jsonParsed',
          transactionDetails: 'full',
          maxSupportedTransactionVersion: 0,
        },
      ],
    }));
    logger.info('[HeliusWS] 📡 订阅 Pump AMM program (单订阅模式)');
  }

  // ── 订阅：Multi 模式（所有 mint 按批次合并订阅）─────────────
  // 每批最多 MULTI_MAX_MINTS_PER_SUB 个 mint，超过自动拆分多个订阅。
  // 加/删 token 时：取消所有旧订阅 → 发新的分批订阅。
  _subscribeMulti() {
    if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;

    const mints = Array.from(this._tokens.keys());
    if (mints.length === 0) {
      logger.info('[HeliusWS] multi 模式：无 token，暂不订阅');
      return;
    }

    // 先取消所有旧订阅
    if (this._multiSubIds.length > 0) {
      const oldIds = this._multiSubIds;
      this._multiSubIds = [];
      for (const oldSubId of oldIds) {
        try {
          this._ws.send(JSON.stringify({
            jsonrpc: '2.0', id: this._nextRpcId++,
            method: 'transactionUnsubscribe',
            params: [oldSubId],
          }));
        } catch (_) {}
      }
      logger.info('[HeliusWS] 🔕 取消旧 multi 订阅 %d 个 (subIds=%s)',
        oldIds.length, oldIds.join(','));
    }

    // 分批订阅
    const BATCH = MULTI_MAX_MINTS_PER_SUB;
    const batches = [];
    for (let i = 0; i < mints.length; i += BATCH) {
      batches.push(mints.slice(i, i + BATCH));
    }

    for (let idx = 0; idx < batches.length; idx++) {
      const batch = batches[idx];
      const rpcId = this._nextRpcId++;
      // 用 __multi_{batchIdx}__ 作为 pendingSubs 的 key，便于区分
      this._pendingSubs.set(rpcId, `__multi_${idx}__`);

      this._ws.send(JSON.stringify({
        jsonrpc: '2.0',
        id: rpcId,
        method: 'transactionSubscribe',
        params: [
          { accountInclude: batch, failed: false },
          {
            commitment: 'confirmed',
            encoding: 'jsonParsed',
            transactionDetails: 'full',
            maxSupportedTransactionVersion: 0,
          },
        ],
      }));
    }
    logger.info('[HeliusWS] 📡 订阅 multi 模式，%d 个 mint 分 %d 批 (每批 ≤%d)',
      mints.length, batches.length, BATCH);
  }

  // ★ 防抖触发重订阅：批量 add/remove 时只重订一次
  _scheduleMultiResub() {
    if (this._multiResubTimer) clearTimeout(this._multiResubTimer);
    this._multiResubTimer = setTimeout(() => {
      this._multiResubTimer = null;
      if (this._connected && this._isMultiMode()) this._subscribeMulti();
    }, MULTI_RESUB_DEBOUNCE_MS);
  }

  // ── 订阅：按 Token 精准模式 ───────────────────────────────

  _subscribeToken(tokenAddress) {
    if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;

    const rpcId = this._nextRpcId++;
    this._pendingSubs.set(rpcId, tokenAddress);

    this._ws.send(JSON.stringify({
      jsonrpc: '2.0',
      id: rpcId,
      method: 'transactionSubscribe',
      params: [
        { accountInclude: [tokenAddress], failed: false },
        {
          commitment: 'confirmed',
          encoding: 'jsonParsed',
          transactionDetails: 'full',
          maxSupportedTransactionVersion: 0,
        },
      ],
    }));

    const info = this._tokens.get(tokenAddress);
    if (info) info.rpcId = rpcId;
    logger.info('[HeliusWS] 📡 订阅 token %s (%s)',
      info?.symbol || '?', tokenAddress.slice(0, 8) + '...');
  }

  _unsubscribeToken(tokenAddress) {
    const info = this._tokens.get(tokenAddress);
    if (!info?.subId) return;

    if (this._ws?.readyState === WebSocket.OPEN) {
      const rpcId = this._nextRpcId++;
      this._ws.send(JSON.stringify({
        jsonrpc: '2.0',
        id: rpcId,
        method: 'transactionUnsubscribe',
        params: [info.subId],
      }));
      logger.info('[HeliusWS] 🔕 取消订阅 %s subId=%s', tokenAddress.slice(0, 8) + '...', info.subId);
    }
    info.subId = null;
  }

  // ── 外部接口 ──────────────────────────────────────────────

  subscribe(tokenAddress, symbol, onTrade) {
    this._tokens.set(tokenAddress, {
      symbol, onTrade, subId: null, rpcId: null,
      // ★ 诊断字段：每 token 的链上交易统计
      chainRx:     0,   // Helius 推来多少笔（匹配到这个 mint 的）
      chainParsed: 0,   // 成功解析出 buy/sell 的有几笔
      lastRxTs:    0,   // 最近一笔推送时间
    });
    const count = this._tokens.size;

    // ★ auto 模式：检查是否需要升级到 pump 单订阅
    if (CFG_SUB_MODE === 'auto' && this._activeMode === 'token' && count > TOKEN_LIMIT) {
      logger.info('[HeliusWS] 🔄 代币数 %d > 上限 %d，自动切换到 pump 单订阅模式', count, TOKEN_LIMIT);
      this._upgradeToPump();
      logger.info('[HeliusWS] 📌 注册 token %s (%s)，当前监控 %d 个 (模式=pump[auto升级])',
        symbol, tokenAddress.slice(0, 8) + '...', count);
      return;
    }

    if (this._isMultiMode()) {
      // ★ multi 模式：防抖重订阅（批量 add 时只重订一次）
      if (this._connected) this._scheduleMultiResub();
    } else if (!this._isPumpMode() && this._connected) {
      // token 模式：发送独立订阅（稍微延迟，避免连发）
      setTimeout(() => {
        if (this._tokens.has(tokenAddress) && this._connected && !this._isPumpMode() && !this._isMultiMode()) {
          this._subscribeToken(tokenAddress);
        }
      }, 50);
    }
    // pump 模式：全局订阅已覆盖，无需额外操作

    logger.info('[HeliusWS] 📌 注册 token %s (%s)，当前监控 %d 个 (模式=%s)',
      symbol, tokenAddress.slice(0, 8) + '...', count, this._activeMode);
  }

  unsubscribe(tokenAddress) {
    // token 模式：取消独立订阅（pump/multi 模式不需要）
    if (!this._isPumpMode() && !this._isMultiMode()) {
      this._unsubscribeToken(tokenAddress);
    }
    this._tokens.delete(tokenAddress);

    // multi 模式：删完触发重订阅（防抖）
    if (this._isMultiMode() && this._connected) this._scheduleMultiResub();

    logger.info('[HeliusWS] 🔕 移除 token %s，剩余 %d 个 (模式=%s)',
      tokenAddress.slice(0, 8) + '...', this._tokens.size, this._activeMode);
  }

  // ── auto 模式：token → pump 升级 ──────────────────────────
  // 取消所有独立订阅，发起一个全局 pump 订阅
  _upgradeToPump() {
    if (this._activeMode === 'pump') return;
    this._activeMode = 'pump';
    this._stats.subMode = 'pump';

    if (!this._connected || this._ws?.readyState !== WebSocket.OPEN) return;

    // 取消所有已建立的 token 独立订阅
    for (const [addr, info] of this._tokens.entries()) {
      if (info.subId) {
        const rpcId = this._nextRpcId++;
        this._ws.send(JSON.stringify({
          jsonrpc: '2.0', id: rpcId,
          method: 'transactionUnsubscribe',
          params: [info.subId],
        }));
        info.subId = null;
      }
    }

    // 发起 pump AMM 全局订阅
    this._subscribePumpAmm();
  }

  // ── 消息处理 ──────────────────────────────────────────────

  _handleMessage(rawData) {
    let msg;
    try { msg = JSON.parse(rawData.toString('utf8')); } catch (_) { return; }

    // 订阅确认 / 订阅错误
    if (msg.id !== undefined && (msg.result !== undefined || msg.error !== undefined)) {
      const key = this._pendingSubs.get(msg.id);
      this._pendingSubs.delete(msg.id);

      // ★ 订阅错误：打日志并清理状态（multi 模式下会被后续 _scheduleMultiResub 重试）
      if (msg.error) {
        logger.error('[HeliusWS] ❌ 订阅失败 key=%s error=%s',
          key, JSON.stringify(msg.error));
        return;
      }

      if (key === '__pump__') {
        this._pumpSubId = msg.result;
        logger.info('[HeliusWS] ✅ Pump AMM 订阅确认 subId=%s', msg.result);
      } else if (key && key.startsWith('__multi_')) {
        this._multiSubIds.push(msg.result);
        logger.info('[HeliusWS] ✅ Multi 订阅确认 %s subId=%s (当前活跃批次: %d)',
          key, msg.result, this._multiSubIds.length);
      } else if (key) {
        const info = this._tokens.get(key);
        if (info) {
          info.subId = msg.result;
          logger.info('[HeliusWS] ✅ 订阅确认 %s (%s) subId=%s',
            info.symbol, key.slice(0, 8) + '...', msg.result);
        }
      }
      return;
    }

    // 交易通知
    if (msg.method === 'transactionNotification' && msg.params?.result) {
      this._stats.txReceived++;
      this._parseTransaction(msg.params.result, msg.params.subscription);
    }
  }

  // ── 交易解析 ──────────────────────────────────────────────

  _parseTransaction(result, subscriptionId) {
    try {
      const { transaction: txWrapper, signature } = result;
      if (!txWrapper) return;

      const meta = txWrapper.meta;
      const txData = txWrapper.transaction;
      if (!meta || meta.err) return;

      const postTokenBals = meta.postTokenBalances || [];
      if (postTokenBals.length === 0) return;

      if (this._isPumpMode() || this._isMultiMode()) {
        // ── pump / multi 模式：从交易中提取 mint，匹配监控列表 ──
        const involvedMints = new Set(postTokenBals.map(b => b.mint).filter(Boolean));
        let matched = false;

        for (const mint of involvedMints) {
          const tokenInfo = this._tokens.get(mint);
          if (!tokenInfo) continue;

          matched = true;
          this._stats.txMatched++;
          tokenInfo.chainRx++;
          tokenInfo.lastRxTs = Date.now();
          const trade = this._extractTrade(mint, meta, txData, signature);
          if (trade) {
            this._stats.txParsed++;
            tokenInfo.chainParsed++;
            tokenInfo.onTrade(trade);
          }
        }

        if (!matched) this._stats.txSkipped++;
      } else {
        // ── token 模式：通过 subscriptionId 精准匹配 ──
        let targetToken = null;
        for (const [addr, info] of this._tokens.entries()) {
          if (info.subId === subscriptionId) {
            targetToken = { address: addr, ...info };
            break;
          }
        }

        if (targetToken) {
          this._stats.txMatched++;
          // 注意：targetToken 是 info 的浅拷贝，要从 _tokens 取原对象累加
          const origInfo = this._tokens.get(targetToken.address);
          if (origInfo) { origInfo.chainRx++; origInfo.lastRxTs = Date.now(); }
          const trade = this._extractTrade(targetToken.address, meta, txData, signature);
          if (trade) {
            this._stats.txParsed++;
            if (origInfo) origInfo.chainParsed++;
            targetToken.onTrade(trade);
          }
          return;
        }

        // 兜底：subscriptionId 匹配不到时用 mint 匹配
        const involvedMints = new Set(postTokenBals.map(b => b.mint).filter(Boolean));
        for (const mint of involvedMints) {
          const tokenInfo = this._tokens.get(mint);
          if (!tokenInfo) continue;
          this._stats.txMatched++;
          tokenInfo.chainRx++;
          tokenInfo.lastRxTs = Date.now();
          const trade = this._extractTrade(mint, meta, txData, signature);
          if (trade) {
            this._stats.txParsed++;
            tokenInfo.chainParsed++;
            tokenInfo.onTrade(trade);
          }
        }
      }
    } catch (err) {
      logger.debug('[HeliusWS] 解析交易失败: %s', err.message);
    }
  }

  _extractTrade(tokenAddress, meta, txData, signature) {
    const preTokenBals  = meta.preTokenBalances  || [];
    const postTokenBals = meta.postTokenBalances  || [];
    const preBalances   = meta.preBalances  || [];
    const postBalances  = meta.postBalances || [];

    let accountKeys = [];
    if (txData?.message?.accountKeys) {
      accountKeys = txData.message.accountKeys.map(k =>
        typeof k === 'string' ? k : k.pubkey
      );
    }

    const postEntries = postTokenBals.filter(b => b.mint === tokenAddress);
    const preEntries  = preTokenBals.filter(b => b.mint === tokenAddress);
    if (postEntries.length === 0) return null;

    for (const postEntry of postEntries) {
      const owner = postEntry.owner;
      if (!owner) continue;

      const ownerIndex = accountKeys.indexOf(owner);
      if (ownerIndex < 0 || ownerIndex >= preBalances.length) continue;

      const preEntry = preEntries.find(
        b => b.accountIndex === postEntry.accountIndex || b.owner === owner
      );

      const postAmt = parseFloat(postEntry.uiTokenAmount?.uiAmount ?? '0');
      const preAmt  = preEntry ? parseFloat(preEntry.uiTokenAmount?.uiAmount ?? '0') : 0;
      const tokenDelta = postAmt - preAmt;
      if (Math.abs(tokenDelta) < 1e-12) continue;

      const solDelta = (postBalances[ownerIndex] - preBalances[ownerIndex]) / LAMPORTS;
      const isBuy  = tokenDelta > 0 && solDelta < 0;
      const isSell = tokenDelta < 0 && solDelta > 0;
      if (!isBuy && !isSell) continue;

      return {
        ts: Date.now(),
        signature,
        tokenAddress,
        owner,
        isBuy,
        solAmount:   Math.abs(solDelta),
        tokenAmount: Math.abs(tokenDelta),
        priceSol:    Math.abs(tokenDelta) > 0 ? Math.abs(solDelta) / Math.abs(tokenDelta) : 0,
      };
    }
    return null;
  }

  // ── 状态查询 ──────────────────────────────────────────────

  isConnected() { return this._connected; }
  getSubscriptionCount() { return this._tokens.size; }

  getStats() {
    let confirmedSubs = 0;
    for (const info of this._tokens.values()) {
      if (info.subId) confirmedSubs++;
    }
    let reportedSubs;
    if (this._isPumpMode())      reportedSubs = this._pumpSubId  ? 1 : 0;
    else if (this._isMultiMode()) reportedSubs = this._multiSubIds.length;
    else                          reportedSubs = confirmedSubs;
    return {
      connected:     this._connected,
      connType:      this._connType,
      subMode:       this._activeMode,
      tokens:        this._tokens.size,
      confirmedSubs: reportedSubs,
      retryCount:    this._retryCount,
      ...this._stats,
    };
  }

  // ★ 按 token 查询链上交易接收状态（诊断用）
  getTokenStats(tokenAddress) {
    const info = this._tokens.get(tokenAddress);
    if (!info) return null;
    let subIdShown;
    if (this._isMultiMode()) {
      subIdShown = this._multiSubIds.length > 0
        ? `multi:${this._multiSubIds.length}批(${this._multiSubIds.join(',')})`
        : 'multi:pending';
    }
    else if (this._isPumpMode()) subIdShown = this._pumpSubId  ? `pump:${this._pumpSubId}`   : 'pump:pending';
    else                         subIdShown = info.subId;
    return {
      subId:       subIdShown,
      chainRx:     info.chainRx || 0,
      chainParsed: info.chainParsed || 0,
      lastRxTs:    info.lastRxTs || 0,
    };
  }
}

const heliusWs = new HeliusTradeStream();
module.exports = heliusWs;
