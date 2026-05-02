'use strict';
// src/xMentions.js
//
// X (Twitter) Mentions 24h 计数模块
//
// 用 X API v2 GET /2/tweets/counts/recent 拉取过去 24 小时内
// 包含某代币合约地址的推文总数。
//
// 设计要点:
//   - 后台定时器每 X_MENTIONS_INTERVAL_SEC 秒(默认 2 小时)运行一次
//   - 一轮内分批处理所有代币: 每批 BATCH_SIZE 个, 批间隔 BATCH_GAP_SEC 秒
//   - 单代币失败不影响其他; 429 时退避 60 秒
//   - 持久化到磁盘, 重启不丢数据
//   - 没有 X_BEARER_TOKEN 时模块整体 disabled, 不影响主程序

const fs   = require('fs');
const path = require('path');
const logger = require('./logger');

const X_API_BASE = 'https://api.x.com/2';
const X_BEARER   = process.env.X_BEARER_TOKEN || '';
const ENABLED    = !!X_BEARER;

const REFRESH_INTERVAL_SEC = parseInt(process.env.X_MENTIONS_INTERVAL_SEC || '7200', 10);
// ★ V5-19: 改为串行(每次只发 1 个请求)+ 请求间隔
//   X API tweets/counts/recent 限制 300/15min = 平均 3s/请求
//   保守用 15s/请求, 72 个币一轮 ~18 分钟
const REQUEST_GAP_SEC      = parseInt(process.env.X_MENTIONS_REQUEST_GAP_SEC || '15', 10);
const REQUEST_TIMEOUT_MS   = parseInt(process.env.X_MENTIONS_TIMEOUT_MS || '10000', 10);

// 持久化路径
const DATA_DIR  = process.env.DATA_DIR || path.join(__dirname, '..', 'data');
const CACHE_FILE = path.join(DATA_DIR, 'xMentions.json');

// 内存缓存: address → { count, ts, error?, statusCode? }
const _cache = new Map();
let _timer = null;
let _running = false;
let _getAddressList = null;  // monitor 启动后注入: () => string[]

/** 从磁盘加载 */
function _loadFromDisk() {
  try {
    if (fs.existsSync(CACHE_FILE)) {
      const raw = fs.readFileSync(CACHE_FILE, 'utf-8');
      const obj = JSON.parse(raw);
      for (const [addr, val] of Object.entries(obj)) {
        _cache.set(addr, val);
      }
      logger.info('[XMentions] 从磁盘加载 %d 条缓存', _cache.size);
    }
  } catch (err) {
    logger.warn('[XMentions] 加载磁盘缓存失败: %s', err.message);
  }
}

/** 持久化到磁盘 */
function _saveToDisk() {
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    const obj = {};
    for (const [addr, val] of _cache.entries()) obj[addr] = val;
    fs.writeFileSync(CACHE_FILE, JSON.stringify(obj), 'utf-8');
  } catch (err) {
    logger.warn('[XMentions] 持久化失败: %s', err.message);
  }
}

/**
 * 查询单个代币的 24h 提及数
 * @returns {Promise<{count, ts, error?, statusCode?}>}
 */
async function _queryOne(address) {
  const now    = Date.now();
  const start  = new Date(now - 24 * 60 * 60 * 1000).toISOString();
  const end    = new Date(now).toISOString();
  // X API 要求 ISO8601 但不能有毫秒, 还要确保 end 不是未来时间
  const startStr = start.replace(/\.\d{3}Z$/, 'Z');
  const endStr   = new Date(now - 10 * 1000).toISOString().replace(/\.\d{3}Z$/, 'Z');

  const url = `${X_API_BASE}/tweets/counts/recent?query=${encodeURIComponent(address)}&granularity=hour&start_time=${startStr}&end_time=${endStr}`;

  const controller = new AbortController();
  const timeoutId  = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      headers: { 'Authorization': `Bearer ${X_BEARER}` },
      signal: controller.signal,
    });
    if (res.status === 429) {
      // 限流: 读 X 返回的 reset header, 精确等待到解封
      const resetEpoch = parseInt(res.headers.get('x-rate-limit-reset') || '0', 10);
      const waitMs = resetEpoch > 0 ? Math.max(0, resetEpoch * 1000 - Date.now()) : 60000;
      return { count: null, ts: now, error: 'RATE_LIMIT', statusCode: 429, retryAfterMs: waitMs };
    }
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      return { count: null, ts: now, error: `HTTP_${res.status}`, statusCode: res.status, body: text.slice(0, 200) };
    }
    const json = await res.json();
    // 文档: { data: [{ start, end, tweet_count }, ...], meta: { total_tweet_count } }
    const total = json?.meta?.total_tweet_count;
    if (typeof total !== 'number') {
      return { count: null, ts: now, error: 'NO_TOTAL' };
    }
    return { count: total, ts: now };
  } catch (err) {
    return { count: null, ts: now, error: err.name === 'AbortError' ? 'TIMEOUT' : err.message };
  } finally {
    clearTimeout(timeoutId);
  }
}

const _sleep = (ms) => new Promise(r => setTimeout(r, ms));

/** 跑一轮: 遍历所有代币, 分批查询 */
async function _runOnce() {
  if (_running) {
    logger.warn('[XMentions] 上一轮还在运行, 跳过');
    return;
  }
  if (!_getAddressList) {
    logger.warn('[XMentions] 尚未注入 getAddressList, 跳过');
    return;
  }
  _running = true;
  const startTs = Date.now();
  try {
    const addresses = _getAddressList();
    if (!addresses || addresses.length === 0) {
      logger.info('[XMentions] 当前无代币, 跳过');
      return;
    }
    logger.info('[XMentions] 开始一轮刷新, 共 %d 个代币, 串行间隔 %ds, 预计耗时 %ds',
      addresses.length, REQUEST_GAP_SEC, addresses.length * REQUEST_GAP_SEC);

    let okCount = 0, failCount = 0;
    for (let i = 0; i < addresses.length; i++) {
      const addr = addresses[i];
      const res = await _queryOne(addr);
      _cache.set(addr, res);
      if (res.error) {
        failCount++;
        // 命中 429: 精确退避到 reset 时间, 避免后续请求继续触发
        if (res.statusCode === 429 && res.retryAfterMs > 0) {
          const waitSec = Math.ceil(res.retryAfterMs / 1000);
          logger.warn('[XMentions] 命中 429, 退避 %ds 到 X API 解封时间', waitSec);
          await _sleep(res.retryAfterMs + 500); // 加 500ms 缓冲
        }
      } else {
        okCount++;
      }
      // 每查 10 个持久化一次, 避免长时间不刷盘
      if ((i + 1) % 10 === 0) _saveToDisk();
      // 间隔(最后一个不等)
      if (i + 1 < addresses.length) await _sleep(REQUEST_GAP_SEC * 1000);
    }
    _saveToDisk();
    logger.info('[XMentions] 一轮完成, 成功 %d, 失败 %d, 耗时 %ds',
      okCount, failCount, Math.round((Date.now() - startTs) / 1000));
  } catch (err) {
    logger.error('[XMentions] 轮询异常: %s', err.message);
  } finally {
    _running = false;
  }
}

/** 启动后台定时器 */
function start(getAddressList) {
  _getAddressList = getAddressList;
  if (!ENABLED) {
    logger.warn('[XMentions] X_BEARER_TOKEN 未配置, 模块禁用 (前端 X提及列将一直显示 -)');
    return;
  }
  _loadFromDisk();
  logger.info('[XMentions] 已启动, 刷新间隔 %ds (%dh)',
    REFRESH_INTERVAL_SEC, Math.round(REFRESH_INTERVAL_SEC / 3600));
  // 启动 30 秒后跑第一轮(给主程序时间初始化代币列表)
  setTimeout(() => { _runOnce(); }, 30000);
  // 之后每隔 REFRESH_INTERVAL_SEC 秒跑一次
  _timer = setInterval(() => { _runOnce(); }, REFRESH_INTERVAL_SEC * 1000);
}

/** 停止 */
function stop() {
  if (_timer) clearInterval(_timer);
  _timer = null;
}

/**
 * 获取某代币的 24h 提及数 (前端广播时调用)
 * @returns {{count, ts, ageSec, error?}|null} null 表示无数据
 */
function getMentions(address) {
  const entry = _cache.get(address);
  if (!entry) return null;
  return {
    count: entry.count,
    ts: entry.ts,
    ageSec: Math.floor((Date.now() - entry.ts) / 1000),
    error: entry.error || null,
  };
}

/** 是否启用(前端可显示状态) */
function isEnabled() {
  return ENABLED;
}

module.exports = { start, stop, getMentions, isEnabled };
