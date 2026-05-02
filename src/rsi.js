'use strict';
// src/rsi.js — RSI 计算 + 量能过滤 + BUY/SELL 信号逻辑 (V3)
//
// V3 修复：
//   1. buildCandles 区分「价格 tick」和「链上交易 tick」
//      - 价格 tick（来自 Birdeye WS/HTTP，USD 计价）→ 构成 OHLC
//      - 链上 tick（来自 Helius WS，SOL 计价） → 只贡献 volume/buyVolume/sellVolume
//      - 解决了 V2 中 SOL 价格和 USD 价格混入同一数组导致 RSI 错乱的致命 BUG
//
//   2. 止损检查独立于 K 线周期，每个 tick 都检查（快速止损）
//
// 策略：
//   BUY : RSI(7) < 35（超卖区）+ totalVol ≥ 15 SOL + buyVol ≥ 1.2×sellVol
//   SELL: RSI 下穿 70 / RSI > 80 / 止盈 / 止损 / 量能萎缩出场

const RSI_PERIOD   = parseInt(process.env.RSI_PERIOD       || '7',  10);
const RSI_BUY      = parseFloat(process.env.RSI_BUY_LEVEL  || '35');
const RSI_SELL     = parseFloat(process.env.RSI_SELL_LEVEL  || '70');
const RSI_PANIC    = parseFloat(process.env.RSI_PANIC_LEVEL || '80');
const KLINE_SEC    = parseInt(process.env.KLINE_INTERVAL_SEC || '300', 10);

// 量能参数
const VOL_ENABLED         = (process.env.VOL_ENABLED || 'true') === 'true';
const VOL_BUY_MULT        = parseFloat(process.env.VOL_BUY_MULT          || '1.2');
const VOL_SELL_MULT       = parseFloat(process.env.VOL_SELL_MULT         || '999'); // sellVol >= N × buyVol 触发卖出（默认999=禁用）
const VOL_MIN_TOTAL       = parseFloat(process.env.VOL_MIN_TOTAL         || '5');  // 最低总成交量(SOL) // buyVol >= N × sellVol 才买入
const VOL_WINDOW_SEC      = parseInt(process.env.VOL_WINDOW_SEC       || '300', 10);
const VOL_EXIT_CONSECUTIVE = parseInt(process.env.VOL_EXIT_CONSECUTIVE || '3', 10);
const VOL_EXIT_RATIO      = parseFloat(process.env.VOL_EXIT_RATIO     || '0.3');
const VOL_EXIT_LOOKBACK   = parseInt(process.env.VOL_EXIT_LOOKBACK    || '4', 10);
// ★ 量能萎缩出场总开关（默认 false，彻底关闭此出场逻辑）
const VOL_DECAY_EXIT_ENABLED = (process.env.VOL_DECAY_EXIT_ENABLED || 'false') === 'true';
const SKIP_FIRST_CANDLES  = parseInt(process.env.SKIP_FIRST_CANDLES   || '3', 10);

// 止盈止损
const TAKE_PROFIT_PCT = parseFloat(process.env.TAKE_PROFIT_PCT || '100');
// ★ 固定止盈总开关（默认 true，+100% 时止盈卖出）
const TAKE_PROFIT_ENABLED = (process.env.TAKE_PROFIT_ENABLED || 'true') === 'true';
const STOP_LOSS_PCT   = parseFloat(process.env.STOP_LOSS_PCT   || '-50');
// ★ 固定止损总开关（默认 true，-50% 止损兜底）
const STOP_LOSS_ENABLED = (process.env.STOP_LOSS_ENABLED || 'true') === 'true';

// 移动止损（Trailing Stop）
const TRAILING_STOP_ENABLED  = (process.env.TRAILING_STOP_ENABLED  || 'true') === 'true';
const TRAILING_STOP_ACTIVATE = parseFloat(process.env.TRAILING_STOP_ACTIVATE || '30'); // 上涨 30% 后激活
const TRAILING_STOP_PCT      = parseFloat(process.env.TRAILING_STOP_PCT      || '-20'); // 峰值回撤 20% 清仓

// EMA99 买入过滤：价格必须在 EMA99 下方才允许买入
const EMA_PERIOD = parseInt(process.env.EMA_PERIOD || '99', 10);
// ★ EMA99 价格过滤总开关：false=关闭"价格必须 < EMA99"的硬条件（仅靠斜率判断趋势）
const EMA_PRICE_FILTER_ENABLED = (process.env.EMA_PRICE_FILTER_ENABLED || 'true') === 'true';
// ★ EMA99 不足时的行为：strict=拒绝买入(推荐), lenient=跳过该过滤(旧行为,有漏洞)
const EMA_INSUFFICIENT_MODE = (process.env.EMA_INSUFFICIENT_MODE || 'strict').toLowerCase();

// ★ V6: EMA99 斜率过滤 —— 拒绝在 EMA99 下行趋势中买入（"接飞刀"防护）
//   原理：RSI 超卖在上升趋势中是健康回调 → 适合买入
//         RSI 超卖在下降趋势中只是趋势延续 → 容易继续跌（BELIEF 案例）
//   计算：slope = (EMA99_now - EMA99_N根前) / EMA99_N根前
//         slope < EMA_SLOPE_MIN_PCT → 拒绝买入
//   默认 LOOKBACK=5 (5根×5分钟K线=25分钟窗口), MIN_PCT=0 (斜率≥0才买)
const EMA_SLOPE_ENABLED  = (process.env.EMA_SLOPE_ENABLED || 'true') === 'true';
const EMA_SLOPE_LOOKBACK = parseInt(process.env.EMA_SLOPE_LOOKBACK || '5', 10);
const EMA_SLOPE_MIN_PCT  = parseFloat(process.env.EMA_SLOPE_MIN_PCT || '0'); // 0 = 要求持平或上升

// ★ 产生买卖信号所需的最小已收盘K线数（低于此数量完全不产生信号，避免 RSI 不收敛误判）
//   默认 max(SKIP_FIRST_CANDLES, RSI_PERIOD × 3) ≈ 21，这是 Wilder RSI 收敛所需
const MIN_CANDLES_FOR_SIGNAL = parseInt(
  process.env.MIN_CANDLES_FOR_SIGNAL ||
  String(Math.max(parseInt(process.env.SKIP_FIRST_CANDLES || '3', 10), RSI_PERIOD * 3)),
  10
);

// ★ V5-36/V5-37: closedCandles 最新一根超过此时长（秒）就不出 BUY 信号
//   背景: Birdeye OHLCV 缓存 + 低流动性币 BirdeyeWS 长时间不推送
//   现象: lastCandleAge=307s, 用 5 分钟前的数据触发了"RSI=7.93 超卖买入"
//   防御: 数据过期就拒绝出 BUY (SELL 仍然让止损/PANIC 工作, 防止已持仓代币卡死)
//
//   V5-37: 自适应默认值 — 如果 .env 没显式设 MAX_STALE_CANDLE_SEC, 跟随 OHLCV_REFRESH_SEC
//          公式: max(KLINE_SEC + OHLCV_REFRESH_SEC + 30, 90)
//          含义: K 线宽度(等一根新K线)+ OHLCV 缓存周期(等下一次刷新) + 30s 网络/服务端余量, 不低于 90s
//          示例: 1min K 线 + 30s 缓存 → 60+30+30 = 120s
//                1min K 线 + 300s 缓存 → 60+300+30 = 390s (避免全表 STALE_CANDLE)
//                5min K 线 + 300s 缓存 → 300+300+30 = 630s
//          这样即使用户 .env 漂移没同步, 门槛也跟着配置走, 不会硬生生挡住所有信号
const _OHLCV_REFRESH_SEC_FOR_GUARD = parseInt(process.env.OHLCV_REFRESH_SEC || '300', 10);
const MAX_STALE_CANDLE_SEC = process.env.MAX_STALE_CANDLE_SEC
  ? parseInt(process.env.MAX_STALE_CANDLE_SEC, 10)
  : Math.max(KLINE_SEC + _OHLCV_REFRESH_SEC_FOR_GUARD + 30, 90);

function calcEMA(closes, period) {
  if (closes.length < period) return NaN;
  const k = 2 / (period + 1);
  let ema = closes.slice(0, period).reduce((s, v) => s + v, 0) / period;
  for (let i = period; i < closes.length; i++) {
    ema = closes[i] * k + ema * (1 - k);
  }
  return ema;
}

/**
 * 计算 EMA 斜率：返回当前 EMA 和 lookback 根前的 EMA
 * @returns {{ emaNow: number, emaPrev: number, slopePct: number }} 或 null（数据不足）
 */
function calcEMASlope(closes, period, lookback) {
  // 需要 period + lookback 根 close 才能有两个 EMA 点
  if (closes.length < period + lookback) return null;
  const k = 2 / (period + 1);
  let ema = closes.slice(0, period).reduce((s, v) => s + v, 0) / period;
  // 记录倒数第 lookback 个位置的 EMA
  const targetIdx = closes.length - lookback; // emaPrev 的位置
  let emaPrev = NaN;
  for (let i = period; i < closes.length; i++) {
    ema = closes[i] * k + ema * (1 - k);
    if (i === targetIdx - 1) emaPrev = ema;
  }
  if (!Number.isFinite(emaPrev) || emaPrev <= 0) return null;
  const slopePct = (ema - emaPrev) / emaPrev * 100;
  return { emaNow: ema, emaPrev, slopePct };
}

// ── Wilder RSI 计算 ────────────────────────────────────────────────

function calcRSIWithState(closes, period = RSI_PERIOD) {
  const rsiArray = new Array(closes.length).fill(NaN);
  if (closes.length < period + 1) return { rsiArray, avgGain: NaN, avgLoss: NaN };

  let gainSum = 0, lossSum = 0;
  for (let i = 1; i <= period; i++) {
    const diff = closes[i] - closes[i - 1];
    if (diff > 0) gainSum += diff;
    else lossSum += Math.abs(diff);
  }
  let avgGain = gainSum / period;
  let avgLoss = lossSum / period;
  rsiArray[period] = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);

  for (let i = period + 1; i < closes.length; i++) {
    const diff = closes[i] - closes[i - 1];
    const gain = diff > 0 ? diff : 0;
    const loss = diff < 0 ? Math.abs(diff) : 0;
    avgGain = (avgGain * (period - 1) + gain) / period;
    avgLoss = (avgLoss * (period - 1) + loss) / period;
    rsiArray[i] = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);
  }

  return { rsiArray, avgGain, avgLoss };
}

function stepRSI(avgGain, avgLoss, lastClose, newPrice, period = RSI_PERIOD) {
  if (!Number.isFinite(avgGain) || !Number.isFinite(avgLoss)) return NaN;
  const diff = newPrice - lastClose;
  const gain = diff > 0 ? diff : 0;
  const loss = diff < 0 ? Math.abs(diff) : 0;
  const ag = (avgGain * (period - 1) + gain) / period;
  const al = (avgLoss * (period - 1) + loss) / period;
  return al === 0 ? 100 : 100 - 100 / (1 + ag / al);
}

// ── 量能检测 ─────────────────────────────────────────────────────

function checkBuyVolume(closedCandles, currentCandle) {
  if (!VOL_ENABLED) return { pass: true, reason: 'VOL_DISABLED', buyVol: 0, sellVol: 0, ratio: 0 };

  const windowBars = Math.max(1, Math.ceil(VOL_WINDOW_SEC / KLINE_SEC));

  const allCandles = [...closedCandles];
  if (currentCandle) allCandles.push(currentCandle);

  if (allCandles.length < windowBars) {
    return { pass: false, reason: 'VOL_INSUFFICIENT_DATA', buyVol: 0, sellVol: 0, ratio: 0 };
  }

  const windowCandles = allCandles.slice(-windowBars);

  let totalBuy  = 0;
  let totalSell = 0;
  for (const c of windowCandles) {
    totalBuy  += (c.buyVolume  || 0);
    totalSell += (c.sellVolume || 0);
  }

  const total = totalBuy + totalSell;
  const ratio = total > 0 ? totalBuy / total : 0;

  // 没有链上方向数据 → 拒绝买入
  if (total === 0) {
    return { pass: false, reason: 'VOL_NO_DIRECTION_DATA', buyVol: 0, sellVol: 0, ratio: 0 };
  }

  // ★ 最低总成交量门槛
  if (total < VOL_MIN_TOTAL) {
    const mult = totalSell > 0 ? (totalBuy / totalSell).toFixed(1) : '∞';
    return {
      pass: false,
      reason: `VOL_TOO_LOW(${total.toFixed(1)}<${VOL_MIN_TOTAL}SOL,${mult}x,${VOL_WINDOW_SEC}s)`,
      buyVol: totalBuy, sellVol: totalSell, ratio,
    };
  }

  // 核心条件：buyVol >= VOL_BUY_MULT × sellVol
  if (totalBuy >= totalSell * VOL_BUY_MULT) {
    const mult = totalSell > 0 ? (totalBuy / totalSell).toFixed(1) : '∞';
    return {
      pass: true,
      reason: `BUY≥${VOL_BUY_MULT}xSELL(${totalBuy.toFixed(2)}>=${(totalSell*VOL_BUY_MULT).toFixed(2)},${mult}x,${(ratio*100).toFixed(0)}%,${VOL_WINDOW_SEC}s)`,
      buyVol: totalBuy, sellVol: totalSell, ratio,
    };
  }

  const mult = totalSell > 0 ? (totalBuy / totalSell).toFixed(1) : '0';
  return {
    pass: false,
    reason: `BUY<${VOL_BUY_MULT}xSELL(buy=${totalBuy.toFixed(2)},sell=${totalSell.toFixed(2)},${mult}x,${VOL_WINDOW_SEC}s)`,
    buyVol: totalBuy, sellVol: totalSell, ratio,
  };
}

function checkVolumeDecay(closedCandles, tokenState) {
  // ★ 总开关：VOL_DECAY_EXIT_ENABLED=false 时彻底禁用量能萎缩出场
  if (!VOL_DECAY_EXIT_ENABLED) return { shouldExit: false, reason: 'VOL_DECAY_EXIT_DISABLED' };
  if (!VOL_ENABLED) return { shouldExit: false, reason: '' };
  if (closedCandles.length < VOL_EXIT_LOOKBACK + VOL_EXIT_CONSECUTIVE) {
    return { shouldExit: false, reason: 'INSUFFICIENT_DATA' };
  }

  const avgEnd = closedCandles.length - VOL_EXIT_CONSECUTIVE;
  const avgStart = Math.max(0, avgEnd - VOL_EXIT_LOOKBACK);
  const avgCandles = closedCandles.slice(avgStart, avgEnd);
  const avgVol = avgCandles.reduce((s, c) => s + (c.volume || 0), 0) / avgCandles.length;

  if (avgVol <= 0) return { shouldExit: false, reason: 'AVG_VOL_ZERO' };

  const recentCandles = closedCandles.slice(-VOL_EXIT_CONSECUTIVE);
  const allDecayed = recentCandles.every(c => (c.volume || 0) < avgVol * VOL_EXIT_RATIO);

  if (allDecayed) {
    const recentVols = recentCandles.map(c => (c.volume || 0).toFixed(0)).join(',');
    return {
      shouldExit: true,
      reason: `VOL_DECAY(recent=[${recentVols}]<avg=${avgVol.toFixed(0)}×${VOL_EXIT_RATIO})`,
    };
  }

  return { shouldExit: false, reason: '' };
}

// ── 快速止损检查（独立于 K 线，每个 tick 调用） ──────────────────

/**
 * 快速止损/止盈检查，不依赖 RSI，直接看价格偏离
 * @returns {{ shouldExit: boolean, reason: string }}
 */
function checkStopLoss(currentPrice, tokenState) {
  if (!tokenState.inPosition || !tokenState.position?.entryPriceUsd) {
    return { shouldExit: false, reason: '' };
  }

  const entryPrice = tokenState.position.entryPriceUsd;
  const pnl = (currentPrice - entryPrice) / entryPrice * 100;

  // ── 移动止损（Trailing Stop）────────────────────────────────────
  if (TRAILING_STOP_ENABLED && tokenState.position) {
    // 每个 tick 更新峰值价格
    if (!tokenState.position._peakPrice || currentPrice > tokenState.position._peakPrice) {
      tokenState.position._peakPrice = currentPrice;
    }
    const peakPrice = tokenState.position._peakPrice;
    const peakPnl   = (peakPrice - entryPrice) / entryPrice * 100;

    // 上涨达到激活线后，从峰值回撤超过阈值则清仓
    if (peakPnl >= TRAILING_STOP_ACTIVATE) {
      const dropFromPeak = (currentPrice - peakPrice) / peakPrice * 100;
      if (dropFromPeak <= TRAILING_STOP_PCT) {
        return {
          shouldExit: true,
          reason: `TRAILING_STOP(峰值+${peakPnl.toFixed(1)}%,回撤${dropFromPeak.toFixed(1)}%≤${TRAILING_STOP_PCT}%)`
        };
      }
    }
  }

  // ── 固定止盈 / 固定止损 ───────────────────────────────────────
  // ★ 固定止盈已默认禁用（TAKE_PROFIT_ENABLED=false），只走移动止损/RSI 出场
  if (TAKE_PROFIT_ENABLED && pnl >= TAKE_PROFIT_PCT) {
    return { shouldExit: true, reason: `TAKE_PROFIT(+${pnl.toFixed(1)}%≥${TAKE_PROFIT_PCT}%)` };
  }
  // ★ 固定止损已默认禁用（STOP_LOSS_ENABLED=false）
  if (STOP_LOSS_ENABLED && pnl <= STOP_LOSS_PCT) {
    return { shouldExit: true, reason: `STOP_LOSS(${pnl.toFixed(1)}%≤${STOP_LOSS_PCT}%)` };
  }

  return { shouldExit: false, reason: '', pnl };
}

// ── 主信号函数 ─────────────────────────────────────────────────────

function evaluateSignal(closedCandles, realtimePrice, tokenState) {
  const MIN_CANDLES = RSI_PERIOD + 2;
  if (!closedCandles || closedCandles.length < MIN_CANDLES) {
    return { rsi: NaN, prevRsi: NaN, signal: null, reason: 'warming_up', volume: {} };
  }

  if (closedCandles.length < SKIP_FIRST_CANDLES) {
    return { rsi: NaN, prevRsi: NaN, signal: null, reason: `skip_first(${closedCandles.length}/${SKIP_FIRST_CANDLES})`, volume: {} };
  }

  // ★ RSI 收敛门槛：低于 MIN_CANDLES_FOR_SIGNAL 时不产生买卖信号
  //   但依然返回 rsi 供前端展示（只是不用这个 rsi 做交易决策）
  const belowConvergence = closedCandles.length < MIN_CANDLES_FOR_SIGNAL;

  const closes = closedCandles.map(c => c.close);
  const len    = closes.length;

  const { rsiArray, avgGain, avgLoss } = calcRSIWithState(closes, RSI_PERIOD);
  const lastClosedRsi = rsiArray[len - 1];
  const lastClose     = closes[len - 1];

  // ★ 缓存到 tokenState，供 WS tick 快速下穿检测使用（避免重复计算）
  const lastCandleTs = closedCandles[len - 1].openTime;
  if (lastCandleTs !== tokenState._rsiLastCandleTs) {
    tokenState._rsiAvgGain      = avgGain;
    tokenState._rsiAvgLoss      = avgLoss;
    tokenState._rsiLastClose    = lastClose;
    tokenState._rsiLastCandleTs = lastCandleTs;
  }

  const rsiRealtime = stepRSI(avgGain, avgLoss, lastClose, realtimePrice, RSI_PERIOD);

  if (!Number.isFinite(lastClosedRsi) || !Number.isFinite(rsiRealtime)) {
    return { rsi: NaN, prevRsi: NaN, signal: null, reason: 'rsi_nan', volume: {},
             avgGain: NaN, avgLoss: NaN, lastClose: NaN };
  }

  const nowMs          = Date.now();
  // lastCandleTs 已在上方 RSI 缓存块里声明，直接复用
  const lastBuyCandle  = tokenState._lastBuyCandle  ?? -1;
  const lastSellCandle = tokenState._lastSellCandle ?? -1;

  const prevRsiRaw = tokenState._prevRsiRealtime;
  const prevTs     = tokenState._prevRsiTs ?? 0;
  // ★ V5-15: 时效窗口从 10s 收紧到 3s；失效时不 fallback 到 lastClosedRsi
  //   旧逻辑用"几分钟前的已收盘K线RSI"和"当前实时RSI"比较会产生虚假下穿，
  //   例如 lastClosedRsi=71.3、rsiRealtime=32.6 → 日志"71.3→32.6"其实不是真下穿
  const STALE_MS   = 3000;
  const isStale    = !Number.isFinite(prevRsiRaw) || (nowMs - prevTs) > STALE_MS;
  // stale 时 prevRsi = null，下面的下穿判断会跳过（不会误触发）
  const prevRsi    = isStale ? NaN : prevRsiRaw;

  const updateState = () => {
    tokenState._prevRsiRealtime = rsiRealtime;
    tokenState._prevRsiTs       = nowMs;
  };

  // 量能信息
  const latestCandle = closedCandles[len - 1];
  const windowBars = Math.max(1, Math.ceil(VOL_WINDOW_SEC / KLINE_SEC));
  const windowCandles = closedCandles.slice(-windowBars);
  let winBuy = 0, winSell = 0;
  for (const c of windowCandles) {
    winBuy  += (c.buyVolume  || 0);
    winSell += (c.sellVolume || 0);
  }
  const winTotal = winBuy + winSell;
  const volumeInfo = {
    currentVol: latestCandle.volume || 0,
    buyVol:  winBuy,
    sellVol: winSell,
    buyRatio: winTotal > 0 ? winBuy / winTotal : 0,
    windowSec: VOL_WINDOW_SEC,
  };

  // ── SELL 优先（持仓中） ────────────────────────────────────────
  if (tokenState.inPosition) {

    // 1. RSI > 80 恐慌卖
    //    ★ V5: 只信任已收盘K线RSI（lastClosedRsi），不用 stepRSI 实时估算
    //    stepRSI 在K线内波动剧烈时容易算出虚假高值（如95.7），
    //    导致在RSI实际只有40-60的时候误触恐慌卖出
    //    ★ V5-9: K线数不足 MIN_CANDLES_FOR_SIGNAL 时不触发 RSI 卖出（RSI 未收敛会乱跳）
    if (!belowConvergence && lastClosedRsi > RSI_PANIC) {
      const lastPanicTs = tokenState._lastPanicSellTs ?? 0;
      if (nowMs - lastPanicTs >= 2000) {
        tokenState._lastPanicSellTs = nowMs;
        updateState();
        return { rsi: lastClosedRsi, prevRsi, signal: 'SELL',
                 reason: `RSI_PANIC(${lastClosedRsi.toFixed(1)}>${RSI_PANIC})`, volume: volumeInfo };
      }
    }

    // 2. RSI 下穿 70
    //    ★ V5-9: K线数不足 MIN_CANDLES_FOR_SIGNAL 时不触发（RSI 未收敛容易虚假下穿）
    if (!belowConvergence && prevRsi >= RSI_SELL && rsiRealtime < RSI_SELL && lastCandleTs !== lastSellCandle) {
      tokenState._lastSellCandle = lastCandleTs;
      updateState();
      return { rsi: rsiRealtime, prevRsi, signal: 'SELL',
               reason: `RSI_CROSS_DOWN_70(${prevRsi.toFixed(1)}→${rsiRealtime.toFixed(1)})`, volume: volumeInfo };
    }

    // 3. 止盈 / 止损（也在 evaluateSignal 中保留，双重保险）
    const sl = checkStopLoss(realtimePrice, tokenState);
    if (sl.shouldExit) {
      updateState();
      return { rsi: rsiRealtime, prevRsi, signal: 'SELL',
               reason: sl.reason, volume: volumeInfo };
    }

    // 4. 卖压卖出 — 已彻底禁用
    // if (VOL_ENABLED && winTotal > 0 && winSell >= winBuy * VOL_SELL_MULT ...) { ... }

    // 5. 量能萎缩出场（★ 默认已禁用，由 VOL_DECAY_EXIT_ENABLED 控制，函数内部短路返回）
    const volDecay = checkVolumeDecay(closedCandles, tokenState);
    if (volDecay.shouldExit) {
      updateState();
      return { rsi: rsiRealtime, prevRsi, signal: 'SELL',
               reason: volDecay.reason, volume: volumeInfo };
    }
  }

  // ── BUY ────────────────────────────────────────────────────────
  // ★ RSI < 30（超卖区）+ buyVol >= 1.2 × sellVol
  if (!tokenState.inPosition) {
    // ★ V5-9: K线数不足 MIN_CANDLES_FOR_SIGNAL 时不买入（RSI/EMA 都未收敛）
    if (belowConvergence) {
      updateState();
      return { rsi: rsiRealtime, prevRsi, signal: null,
               reason: `INSUFFICIENT_CANDLES(${closedCandles.length}/${MIN_CANDLES_FOR_SIGNAL})`, volume: volumeInfo };
    }
    // ★ V5-36: closedCandles 最新一根太旧就拒绝 BUY（防止用过期数据触发误买）
    //   只挡 BUY, 不挡 SELL (持仓中的代币需要继续让止损/PANIC 工作)
    const lastCandleAge = (nowMs - lastCandleTs) / 1000;
    if (lastCandleAge > MAX_STALE_CANDLE_SEC) {
      updateState();
      return { rsi: rsiRealtime, prevRsi, signal: null,
               reason: `STALE_CANDLE(age=${lastCandleAge.toFixed(0)}s>${MAX_STALE_CANDLE_SEC}s)`,
               volume: volumeInfo };
    }
    if (rsiRealtime < RSI_BUY && lastCandleTs !== lastBuyCandle) {
      // ★ EMA99 过滤：价格必须在 EMA99 下方才允许买入（可通过 EMA_PRICE_FILTER_ENABLED 关闭）
      const ema99 = calcEMA(closes, EMA_PERIOD);
      if (EMA_PRICE_FILTER_ENABLED) {
        if (!Number.isFinite(ema99)) {
          // K线不足 99 根，无法计算 EMA99
          if (EMA_INSUFFICIENT_MODE === 'strict') {
            // ★ 严格模式：直接拒绝买入，避免新币/数据不足时误买
            updateState();
            return { rsi: rsiRealtime, prevRsi, signal: null,
                     reason: `EMA99_INSUFFICIENT_DATA(${closes.length}/${EMA_PERIOD})`, volume: volumeInfo };
          }
          // lenient 模式：跳过 EMA99 过滤（旧行为，不推荐）
        } else if (realtimePrice >= ema99) {
          updateState();
          return { rsi: rsiRealtime, prevRsi, signal: null,
                   reason: `PRICE_ABOVE_EMA99(price=${realtimePrice.toFixed(8)},ema99=${ema99.toFixed(8)})`, volume: volumeInfo };
        }
      } else {
        // 价格过滤关闭：但 strict 模式下 EMA99 数据不足仍然需要拒绝买入（斜率过滤同样需要 EMA99）
        if (!Number.isFinite(ema99) && EMA_INSUFFICIENT_MODE === 'strict') {
          updateState();
          return { rsi: rsiRealtime, prevRsi, signal: null,
                   reason: `EMA99_INSUFFICIENT_DATA(${closes.length}/${EMA_PERIOD})`, volume: volumeInfo };
        }
      }

      // ★ V6: EMA99 斜率过滤 — 拒绝在下行趋势中买入（防接飞刀）
      //   BELIEF 案例：EMA99 持续下行，RSI 超卖只是趋势延续不是反弹信号
      //   Goblin 案例：EMA99 上行，RSI 超卖是健康回调 → 买入机会
      let slopePctForLog = null;  // 用于买入 reason 标注当时斜率
      if (EMA_SLOPE_ENABLED && Number.isFinite(ema99)) {
        const slopeResult = calcEMASlope(closes, EMA_PERIOD, EMA_SLOPE_LOOKBACK);
        if (slopeResult) {
          slopePctForLog = slopeResult.slopePct;
          if (slopeResult.slopePct < EMA_SLOPE_MIN_PCT) {
            updateState();
            return { rsi: rsiRealtime, prevRsi, signal: null,
                     reason: `EMA99_SLOPE_DOWN(slope=${slopeResult.slopePct.toFixed(3)}%<${EMA_SLOPE_MIN_PCT}%,now=${slopeResult.emaNow.toFixed(4)},prev=${slopeResult.emaPrev.toFixed(4)},lb=${EMA_SLOPE_LOOKBACK})`,
                     volume: volumeInfo };
          }
        }
        // slopeResult == null → 数据不足以计算斜率，不阻止买入（已有 EMA99 价格过滤兜底）
      }

      const volCheck = checkBuyVolume(closedCandles, null);
      volumeInfo.buyVol   = volCheck.buyVol;
      volumeInfo.sellVol  = volCheck.sellVol;
      volumeInfo.buyRatio = volCheck.ratio;

      if (volCheck.pass) {
        tokenState._lastBuyCandle = lastCandleTs;
        updateState();
        // ★ V5-33: 买入 reason 带上 EMA99 斜率（slope=N/A 表示数据不足或斜率过滤关闭）
        const slopeStr = slopePctForLog !== null
          ? `slope=${slopePctForLog.toFixed(3)}%`
          : 'slope=N/A';
        return { rsi: rsiRealtime, prevRsi, signal: 'BUY',
                 reason: `RSI_OVERSOLD(${rsiRealtime.toFixed(1)}<${RSI_BUY})+EMA99(${slopeStr})+${volCheck.reason}`,
                 volume: volumeInfo,
                 // ★ V5-35: 详细诊断字段，供 _doBuy 打详细日志
                 _diag: {
                   avgGain, avgLoss, lastClose,
                   realtimePrice,
                   lastClosedRsi,
                   rsiRealtime,
                   lastCandleTs,
                   closedCount: closedCandles.length,
                   recentCloses: closes.slice(-10),
                 }
               };
      }
      // 量能不达标，不标记 lastBuyCandle，下根K线继续检查
    }
  }

  updateState();
  return { rsi: rsiRealtime, prevRsi, signal: null, reason: isStale ? 'rsi_rebase' : '', volume: volumeInfo };
}

// ── K线聚合（V3：分离价格 tick 和链上交易 tick） ─────────────────
//
// tick 格式（两种来源共存于同一数组，用 source 字段区分）：
//
//   价格 tick（Birdeye WS/HTTP）:
//     { price: USD价格, ts, source: 'price' }
//     → 构成 OHLC
//
//   链上交易 tick（Helius WS）:
//     { price: 忽略(SOL计价), ts, solAmount, isBuy, source: 'chain' }
//     → 只贡献 volume / buyVolume / sellVolume
//     → 不参与 OHLC（单位不同！）
//

function buildCandles(ticks, intervalSec = KLINE_SEC) {
  if (!ticks || ticks.length === 0) return { closed: [], current: null };

  const intervalMs = intervalSec * 1000;
  const candles    = [];
  let current      = null;

  for (const tick of ticks) {
    const bucket = Math.floor(tick.ts / intervalMs) * intervalMs;

    const isChainTick = tick.source === 'chain';

    if (!current || current.openTime !== bucket) {
      // 新 K 线
      if (current) candles.push(current);

      if (isChainTick) {
        // 链上 tick 开始一根新K线，但没有价格数据
        // 创建空价格K线，等下一个价格 tick 填入
        current = {
          openTime:   bucket,
          closeTime:  bucket + intervalMs,
          open:       null,    // 等待价格 tick 填入
          high:       null,
          low:        null,
          close:      null,
          volume:     tick.solAmount || 0,
          buyVolume:  tick.isBuy  ? (tick.solAmount || 0) : 0,
          sellVolume: !tick.isBuy ? (tick.solAmount || 0) : 0,
          tickCount:  1,
          priceTickCount: 0,
        };
      } else {
        // 价格 tick 开始新K线
        current = {
          openTime:   bucket,
          closeTime:  bucket + intervalMs,
          open:       tick.price,
          high:       tick.price,
          low:        tick.price,
          close:      tick.price,
          volume:     0,          // volume 只来自链上交易
          buyVolume:  0,
          sellVolume: 0,
          tickCount:  1,
          priceTickCount: 1,
        };
      }
    } else {
      // 同一根 K 线内追加
      if (isChainTick) {
        // 链上 tick：只更新 volume
        current.volume     += (tick.solAmount || 0);
        current.buyVolume  += tick.isBuy  ? (tick.solAmount || 0) : 0;
        current.sellVolume += !tick.isBuy ? (tick.solAmount || 0) : 0;
        current.tickCount++;
      } else {
        // 价格 tick：更新 OHLC
        if (current.open === null) {
          // 这根K线之前只有链上 tick，现在才拿到价格
          current.open  = tick.price;
          current.high  = tick.price;
          current.low   = tick.price;
          current.close = tick.price;
        } else {
          if (tick.price > current.high) current.high = tick.price;
          if (tick.price < current.low)  current.low  = tick.price;
          current.close = tick.price;
        }
        current.tickCount++;
        current.priceTickCount++;
      }
    }
  }

  if (!current) return { closed: candles, current: null };

  const now = Date.now();
  if (now >= current.closeTime) {
    candles.push(current);
    return { closed: candles, current: null };
  }

  return { closed: candles, current };
}

/**
 * 过滤掉 open 为 null 的 K 线（只有链上 tick，没有价格数据的 K 线）
 * RSI 计算前调用
 */
function filterValidCandles(candles) {
  return candles.filter(c => c.open !== null && c.close !== null);
}

module.exports = {
  evaluateSignal,
  buildCandles,
  filterValidCandles,
  calcRSIWithState,
  stepRSI,
  checkBuyVolume,
  checkVolumeDecay,
  checkStopLoss,
  // ★ V5-22: 顶层导出 TRAILING_STOP_*, 修复 monitor.js 解构得到 undefined 的 bug
  //   原本只在 CONFIG 里, 导致启动日志显示 "激活线=+undefined% 移动止损=关闭"
  //   实际业务逻辑用 rsi.js 内部常量, 行为不受影响; 但日志会误导排查
  TRAILING_STOP_ENABLED, TRAILING_STOP_ACTIVATE, TRAILING_STOP_PCT,
  CONFIG: {
    RSI_PERIOD, RSI_BUY, RSI_SELL, RSI_PANIC,
    VOL_ENABLED, VOL_BUY_MULT, VOL_SELL_MULT, VOL_MIN_TOTAL, VOL_WINDOW_SEC,
    VOL_EXIT_CONSECUTIVE, VOL_EXIT_RATIO, VOL_EXIT_LOOKBACK,
    SKIP_FIRST_CANDLES,
    TAKE_PROFIT_PCT, STOP_LOSS_PCT, KLINE_SEC,
    TRAILING_STOP_ENABLED, TRAILING_STOP_ACTIVATE, TRAILING_STOP_PCT,
    EMA_PERIOD, MIN_CANDLES_FOR_SIGNAL,
    EMA_SLOPE_ENABLED, EMA_SLOPE_LOOKBACK, EMA_SLOPE_MIN_PCT,
  },
};
