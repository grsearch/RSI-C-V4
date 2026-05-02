'use strict';
// src/reporter.js — 每日 CSV 报告生成器（V2 增加量能字段）

const fs     = require('fs');
const path   = require('path');
const logger = require('./logger');

const REPORTS_DIR = path.join(__dirname, '../public/reports');
const MAX_REPORTS = 7;

if (!fs.existsSync(REPORTS_DIR)) fs.mkdirSync(REPORTS_DIR, { recursive: true });

function recordsToCsv(records) {
  const headers = [
    '币种', '合约地址', 'Age(天)',
    '买入时间', '卖出时间', '持仓时长(分钟)',
    '买入FDV($)', '买入LP($)',
    '买入SOL', '卖出SOL', '盈亏SOL', '盈亏%',
    '买入原因', '退出原因', '空跑', 'GMGN链接',
  ];

  const rows = records.map(r => {
    const fmt = ts => ts ? new Date(ts).toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' }) : '';
    const durMin = (r.buyAt && r.exitAt) ? Math.round((r.exitAt - r.buyAt) / 60000) : '';
    const pnlSol = (r.solOut != null && r.solIn != null)
      ? (r.solOut - r.solIn).toFixed(4) : '';
    const ageDays = r.createdAt ? ((r.buyAt || Date.now()) - r.createdAt) / 86400000 : '';
    const ageStr = ageDays !== '' ? (ageDays < 1 ? (ageDays * 24).toFixed(1) + 'h' : ageDays.toFixed(1) + 'd') : '';
    return [
      r.symbol, r.address, ageStr,
      fmt(r.buyAt), fmt(r.exitAt), durMin,
      r.entryFdv ?? '', r.entryLp ?? '',
      r.solIn    ?? '', r.solOut  ?? '', pnlSol,
      r.pnlPct   ?? '',
      r.buyReason ?? '',
      r.exitReason ?? '持仓中',
      r.dryRun ? '是' : '否',
      `https://gmgn.ai/sol/token/${r.address}`,
    ].map(v => `"${String(v).replace(/"/g, '""')}"`).join(',');
  });

  return [headers.join(','), ...rows].join('\r\n');
}

function generateReport(records) {
  const bjDate  = new Date(Date.now() + 8 * 3600 * 1000);
  const dateStr = bjDate.toISOString().slice(0, 10);
  const filepath = path.join(REPORTS_DIR, `report_${dateStr}.csv`);

  fs.writeFileSync(filepath, '\uFEFF' + recordsToCsv(records), 'utf-8');
  logger.info('[Reporter] ✅ 报告生成: report_%s.csv (%d 笔)', dateStr, records.length);

  const files = fs.readdirSync(REPORTS_DIR)
    .filter(f => f.startsWith('report_') && f.endsWith('.csv'))
    .sort().reverse();
  files.slice(MAX_REPORTS).forEach(f => {
    fs.unlinkSync(path.join(REPORTS_DIR, f));
    logger.info('[Reporter] 🗑  删除旧报告: %s', f);
  });

  return `report_${dateStr}.csv`;
}

function listReports() {
  if (!fs.existsSync(REPORTS_DIR)) return [];
  return fs.readdirSync(REPORTS_DIR)
    .filter(f => f.startsWith('report_') && f.endsWith('.csv'))
    .sort().reverse()
    .map(f => ({
      filename  : f,
      url       : `/reports/${f}`,
      size      : fs.statSync(path.join(REPORTS_DIR, f)).size,
      date      : f.replace('report_', '').replace('.csv', ''),
      createdAt : fs.statSync(path.join(REPORTS_DIR, f)).mtime.toISOString(),
    }));
}

function scheduleDaily(getRecordsFn) {
  function msUntilNext8am() {
    const bjNow = new Date(Date.now() + 8 * 3600 * 1000);
    const target = new Date(Date.UTC(
      bjNow.getUTCFullYear(), bjNow.getUTCMonth(), bjNow.getUTCDate(),
      0, 0, 0, 0
    ));
    let ms = target.getTime() - Date.now();
    if (ms <= 0) ms += 24 * 3600 * 1000;
    return ms;
  }

  function run() {
    const records = getRecordsFn();
    if (records.length > 0) generateReport(records);
    else logger.info('[Reporter] 今日无交易记录，跳过');
    setTimeout(run, msUntilNext8am());
  }

  const ms = msUntilNext8am();
  logger.info('[Reporter] 下次报告: %s',
    new Date(Date.now() + ms).toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' }));
  setTimeout(run, ms);
}

module.exports = { scheduleDaily, generateReport, listReports };
