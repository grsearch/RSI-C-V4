'use strict';
// src/dataStore.js — 数据持久化
//
// 空跑模式下：
//   1. 每个 token 的 tick 数据保存到 data/ticks/<address>.json
//   2. 所有交易记录保存到 data/trades.json
//   3. 所有信号（含被过滤的）保存到 data/signals.json
//
// 数据用于后续回测和分析。

const fs     = require('fs');
const path   = require('path');
const logger = require('./logger');

const DATA_DIR   = process.env.DRY_RUN_DATA_DIR || './data';
const TICKS_DIR  = path.join(DATA_DIR, 'ticks');
const TRADES_FILE  = path.join(DATA_DIR, 'trades.json');
const SIGNALS_FILE = path.join(DATA_DIR, 'signals.json');
const TOKENS_FILE  = path.join(DATA_DIR, 'tokens.json');

// 初始化目录
function init() {
  [DATA_DIR, TICKS_DIR].forEach(dir => {
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
      logger.info('[DataStore] 创建目录: %s', dir);
    }
  });

  // 初始化文件
  [TRADES_FILE, SIGNALS_FILE, TOKENS_FILE].forEach(file => {
    if (!fs.existsSync(file)) {
      fs.writeFileSync(file, '[]', 'utf-8');
    }
  });
}

// ── Token 列表持久化 ──────────────────────────────────────────────

function saveTokens(tokens) {
  try {
    // tokens: [{ address, symbol, meta }]
    fs.writeFileSync(TOKENS_FILE, JSON.stringify(tokens, null, 2), 'utf-8');
  } catch (err) {
    logger.error('[DataStore] saveTokens 失败: %s', err.message);
  }
}

function loadTokens() {
  try {
    if (fs.existsSync(TOKENS_FILE)) {
      const data = JSON.parse(fs.readFileSync(TOKENS_FILE, 'utf-8'));
      if (Array.isArray(data)) return data;
    }
  } catch (_) {}
  return [];
}

// ── Tick 数据存储 ──────────────────────────────────────────────────

// 内存缓冲，定期刷盘
const _tickBuffers = new Map(); // address → tick[]
const FLUSH_INTERVAL = 30 * 1000; // 30秒刷盘
let _flushTimer = null;

function appendTick(address, tick) {
  if (!_tickBuffers.has(address)) {
    _tickBuffers.set(address, []);
  }
  _tickBuffers.get(address).push(tick);
}

function flushTicks() {
  for (const [address, newTicks] of _tickBuffers.entries()) {
    if (newTicks.length === 0) continue;
    try {
      const file = path.join(TICKS_DIR, `${address}.json`);
      let existing = [];
      if (fs.existsSync(file)) {
        try {
          existing = JSON.parse(fs.readFileSync(file, 'utf-8'));
        } catch (_) {
          existing = [];
        }
      }
      existing.push(...newTicks);
      // ★ V5: 裁剪超过24小时的旧数据（回测需要足够的历史数据）
      const cutoff = Date.now() - 24 * 60 * 60 * 1000;
      if (existing.length > 0 && existing[0].ts < cutoff) {
        const idx = existing.findIndex(t => t.ts >= cutoff);
        if (idx > 0) existing = existing.slice(idx);
        else if (idx === -1) existing = [];
      }
      fs.writeFileSync(file, JSON.stringify(existing), 'utf-8');
      newTicks.length = 0; // 清空缓冲
    } catch (err) {
      logger.error('[DataStore] flushTicks %s 失败: %s', address, err.message);
    }
  }
}

function startFlush() {
  if (_flushTimer) return;
  _flushTimer = setInterval(flushTicks, FLUSH_INTERVAL);
  // ★ V5: 信号也用缓冲刷盘
  if (!_signalFlushTimer) {
    _signalFlushTimer = setInterval(_flushSignals, SIGNAL_FLUSH_INTERVAL);
  }
  logger.info('[DataStore] 启动 tick 刷盘，间隔 %ds', FLUSH_INTERVAL / 1000);
}

function stopFlush() {
  if (_flushTimer) {
    clearInterval(_flushTimer);
    _flushTimer = null;
  }
  if (_signalFlushTimer) {
    clearInterval(_signalFlushTimer);
    _signalFlushTimer = null;
  }
  flushTicks(); // 最后刷一次
  _flushSignals(); // ★ V5: 信号也刷一次
}

// ── 交易记录存储 ──────────────────────────────────────────────────

function appendTrade(record) {
  try {
    let trades = [];
    if (fs.existsSync(TRADES_FILE)) {
      try {
        trades = JSON.parse(fs.readFileSync(TRADES_FILE, 'utf-8'));
      } catch (_) {
        trades = [];
      }
    }
    trades.push(record);
    fs.writeFileSync(TRADES_FILE, JSON.stringify(trades, null, 2), 'utf-8');
  } catch (err) {
    logger.error('[DataStore] appendTrade 失败: %s', err.message);
  }
}

function updateTrade(id, updates) {
  try {
    let trades = [];
    if (fs.existsSync(TRADES_FILE)) {
      trades = JSON.parse(fs.readFileSync(TRADES_FILE, 'utf-8'));
    }
    const idx = trades.findIndex(t => t.id === id);
    if (idx >= 0) {
      Object.assign(trades[idx], updates);
      fs.writeFileSync(TRADES_FILE, JSON.stringify(trades, null, 2), 'utf-8');
    }
  } catch (err) {
    logger.error('[DataStore] updateTrade 失败: %s', err.message);
  }
}

function loadTrades() {
  try {
    if (fs.existsSync(TRADES_FILE)) {
      return JSON.parse(fs.readFileSync(TRADES_FILE, 'utf-8'));
    }
  } catch (_) {}
  return [];
}

// ── 信号记录存储（★ V5: 缓冲写盘，避免47+币每秒全量读写）──────

const _signalBuffer = [];
const SIGNAL_FLUSH_INTERVAL = 30 * 1000; // 30秒刷盘
let _signalFlushTimer = null;

function appendSignal(signalRecord) {
  _signalBuffer.push(signalRecord);
}

function _flushSignals() {
  if (_signalBuffer.length === 0) return;
  try {
    let signals = [];
    if (fs.existsSync(SIGNALS_FILE)) {
      try {
        signals = JSON.parse(fs.readFileSync(SIGNALS_FILE, 'utf-8'));
      } catch (_) {
        signals = [];
      }
    }
    signals.push(..._signalBuffer);
    _signalBuffer.length = 0;
    // 只保留最近5000条
    if (signals.length > 5000) signals = signals.slice(-5000);
    fs.writeFileSync(SIGNALS_FILE, JSON.stringify(signals), 'utf-8');
  } catch (err) {
    logger.error('[DataStore] _flushSignals 失败: %s', err.message);
  }
}

function loadSignals() {
  try {
    if (fs.existsSync(SIGNALS_FILE)) {
      return JSON.parse(fs.readFileSync(SIGNALS_FILE, 'utf-8'));
    }
  } catch (_) {}
  return [];
}

// ── 加载 token 的 tick 数据（回测用） ──────────────────────────────

function loadTicks(address) {
  try {
    const file = path.join(TICKS_DIR, `${address}.json`);
    if (fs.existsSync(file)) {
      return JSON.parse(fs.readFileSync(file, 'utf-8'));
    }
  } catch (_) {}
  return [];
}

function listTickFiles() {
  try {
    if (!fs.existsSync(TICKS_DIR)) return [];
    return fs.readdirSync(TICKS_DIR)
      .filter(f => f.endsWith('.json'))
      .map(f => ({
        address: f.replace('.json', ''),
        file: path.join(TICKS_DIR, f),
        size: fs.statSync(path.join(TICKS_DIR, f)).size,
      }));
  } catch (_) {
    return [];
  }
}

module.exports = {
  init,
  appendTick,
  flushTicks,
  startFlush,
  stopFlush,
  appendTrade,
  updateTrade,
  loadTrades,
  appendSignal,
  loadSignals,
  loadTicks,
  listTickFiles,
  saveTokens,
  loadTokens,
  DATA_DIR,
};
