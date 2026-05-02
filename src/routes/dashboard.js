'use strict';
const express   = require('express');
const router    = express.Router();
const monitor   = require('../monitor');
const reporter  = require('../reporter');
const dataStore = require('../dataStore');
const birdeye   = require('../birdeye');

router.get('/dashboard', (_req, res) => res.json({
  tokens: monitor.getTokens(),
  dryRun: monitor.DRY_RUN,
}));

// ★ V5-20: 实时诊断接口 —— 检查 OHLCV 实时刷新是否生效
// ★ V5-27: 加 CU 估算 + 预警
router.get('/diag', (_req, res) => {
  const ohlcvStats = birdeye.getOhlcvCacheStats();
  const tokens = monitor.getTokens();
  // 抽样 5 个币看 K 线源
  const sample = tokens.slice(0, 5).map(t => ({
    symbol: t.symbol,
    address: t.address.slice(0, 8) + '...',
    closedCount: t.closedCount,
    candleStats: t.candleStats,
  }));

  // ★ V5-27: CU 估算 (基于实际生效环境变量, 不是代码 fallback)
  const ohlcvRefresh    = parseInt(process.env.OHLCV_REFRESH_SEC || '300', 10);
  const overviewPatrol  = parseInt(process.env.OVERVIEW_PATROL_SEC || '7200', 10);
  const ohlcvEnabled    = (process.env.OHLCV_REALTIME_ENABLED || 'true') === 'true';
  const n               = tokens.length;
  // /defi/ohlcv = 40 CU, /defi/token_overview = 25 CU
  const cuOhlcvDay      = ohlcvEnabled ? Math.round(n * 86400 / ohlcvRefresh * 40) : 0;
  const cuOverviewDay   = Math.round(n * 86400 / overviewPatrol * 25);
  const cuTotalDay      = cuOhlcvDay + cuOverviewDay;
  const cuMonth         = cuTotalDay * 30;
  const QUOTA           = 20 * 1e6;
  const status          = cuMonth > QUOTA ? 'OVER_QUOTA' : (cuMonth > QUOTA * 0.8 ? 'WARNING' : 'OK');

  res.json({
    serverTime: new Date().toISOString(),
    env: {
      OHLCV_REALTIME_ENABLED: process.env.OHLCV_REALTIME_ENABLED || '(unset, default true)',
      OHLCV_REFRESH_SEC: process.env.OHLCV_REFRESH_SEC || '(unset, default 300)',
      OHLCV_REALTIME_BARS: process.env.OHLCV_REALTIME_BARS || '(unset, default 120)',
      OVERVIEW_PATROL_SEC: process.env.OVERVIEW_PATROL_SEC || '(unset, default 7200)',
      LOG_LEVEL: process.env.LOG_LEVEL || '(unset, default info)',
    },
    cuEstimate: {
      tokens: n,
      ohlcvCallsPerDay: ohlcvEnabled ? Math.round(n * 86400 / ohlcvRefresh) : 0,
      overviewCallsPerDay: Math.round(n * 86400 / overviewPatrol),
      cuPerDay: cuTotalDay,
      cuPerMonth: cuMonth,
      cuMonthStr: (cuMonth / 1e6).toFixed(1) + 'M',
      quotaMonth: '20M',
      utilizationPct: ((cuMonth / QUOTA) * 100).toFixed(1) + '%',
      status,
      hint: status === 'OVER_QUOTA'
        ? `超配额! 加大 OHLCV_REFRESH_SEC (当前 ${ohlcvRefresh}s, 建议 600)`
        : (status === 'WARNING' ? '接近配额, 注意观察' : '正常'),
    },
    callStats: monitor.getCallStats(),
    ohlcvCache: ohlcvStats,
    // ★ V5-28: WS 推送 + getPrice 调用诊断
    wsStream: birdeye.priceStream.getStats(),
    getPriceStats: birdeye.getPriceStats(),
    tokenCount: tokens.length,
    tokenSample: sample,
  });
});

router.get('/tokens', (_req, res) => res.json(monitor.getTokens()));

// ── 手动添加代币（必须在 /tokens/:address 之前，避免 :address 匹配 "add"）──
router.post('/tokens/add', async (req, res) => {
  const { address, symbol } = req.body || {};
  if (!address) {
    return res.status(400).json({ error: '缺少 address' });
  }
  // ★ V5-16: symbol 自动匹配 —— 没传就从 Birdeye 查
  let sym = symbol;
  if (!sym) {
    try {
      sym = await birdeye.getSymbol(address);
    } catch (_) {}
  }
  if (!sym) sym = address.slice(0, 6); // 兜底
  const added = await monitor.addToken(address, sym, { network: 'solana', source: 'manual' });
  if (!added) {
    return res.status(409).json({ error: '代币已在监控中', address });
  }
  res.json({ ok: true, address, symbol: sym });
});

router.get('/tokens/:address', (req, res) => {
  const t = monitor.getToken(req.params.address);
  if (!t) return res.status(404).json({ error: 'not found' });
  res.json(t);
});

router.delete('/tokens/:address', async (req, res) => {
  await monitor.removeToken(req.params.address, 'manual_delete');
  res.json({ ok: true });
});

// ★ 手动重拉历史K线（Birdeye 429 导致某些币历史K线为空时使用）
router.post('/tokens/:address/refetch-history', (req, res) => {
  const result = monitor.refetchHistory(req.params.address);
  if (!result.ok) return res.status(404).json(result);
  res.json(result);
});

router.get('/trades', (_req, res) => {
  const logs = monitor.getTokens().flatMap(t => t.tradeLogs || []);
  logs.sort((a, b) => b.ts - a.ts);
  res.json(logs.slice(0, 200));
});

router.get('/trade-records', (_req, res) => {
  res.json(monitor.getAllTradeRecords());
});

// 持久化数据统计
router.get('/data-stats', (_req, res) => {
  const files = dataStore.listTickFiles();
  const trades = dataStore.loadTrades();
  const signals = dataStore.loadSignals();
  res.json({
    tickFiles: files.length,
    totalTicks: files.reduce((s, f) => s + Math.floor(f.size / 50), 0),
    tradeCount: trades.length,
    signalCount: signals.length,
    dataDir: dataStore.DATA_DIR,
  });
});

// 信号历史
router.get('/signals', (req, res) => {
  const limit = parseInt(req.query.limit || '100', 10);
  const signals = dataStore.loadSignals();
  res.json(signals.slice(-limit));
});

router.get('/reports', (_req, res) => res.json(reporter.listReports()));

module.exports = router;
