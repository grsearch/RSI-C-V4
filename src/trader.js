'use strict';
// src/trader.js — 交易执行 (V3 — Helius Sender + Dynamic Priority Fee)
//
// 架构：
//   1. 路由获取：Jupiter Ultra API（最优价格路由）
//   2. 交易发送：Helius Sender / Gatekeeper RPC（最低延迟）
//   3. skipPreflight=true（省 ~100ms）
//   4. 动态 Priority Fee（Helius getPriorityFeeEstimate）
//   5. confirmed 级别 blockhash（更快）
//
// 止损执行目标：<350ms（Sender 发单 + confirmed）

const {
  Keypair, VersionedTransaction, LAMPORTS_PER_SOL,
  Connection, ComputeBudgetProgram, TransactionMessage,
} = require('@solana/web3.js');
const bs58   = require('bs58');
const fetch  = require('node-fetch');
const logger = require('./logger');

// ── 配置 ────────────────────────────────────────────────────────

const JUP_API          = process.env.JUPITER_API_URL           || 'https://api.jup.ag';
const JUP_API_KEY      = process.env.JUPITER_API_KEY           || '';
const SLIPPAGE_BPS     = parseInt(process.env.SLIPPAGE_BPS     || '500');
const TRADE_SOL        = parseFloat(process.env.TRADE_SIZE_SOL || '1');
const SLIPPAGE_MAX_BPS = parseInt(process.env.SLIPPAGE_MAX_BPS || '2000');
const MAX_RETRY        = parseInt(process.env.TRADE_MAX_RETRY  || '3');  // 止损时重试少一些

const HELIUS_API_KEY = process.env.HELIUS_API_KEY || '';
const HELIUS_RPC_URL = process.env.HELIUS_RPC_URL || '';
const HELIUS_GATEKEEPER_URL = process.env.HELIUS_GATEKEEPER_URL || '';

const SOL_MINT = 'So11111111111111111111111111111111111111112';

// ── RPC 连接（优先 Gatekeeper → 标准 Helius RPC） ────────────────

function getRpcUrl() {
  if (HELIUS_GATEKEEPER_URL) return HELIUS_GATEKEEPER_URL;
  if (HELIUS_RPC_URL) return HELIUS_RPC_URL;
  if (HELIUS_API_KEY) return `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
  return 'https://api.mainnet-beta.solana.com';
}

let _connection = null;
function getConnection() {
  if (!_connection) {
    const url = getRpcUrl();
    const safeUrl = url.replace(/api-key=[a-f0-9-]+/i, 'api-key=***');
    logger.info('[Trader] RPC: %s', safeUrl);
    _connection = new Connection(url, {
      commitment: 'confirmed',
      confirmTransactionInitialTimeout: 30000,
    });
  }
  return _connection;
}

// ── Keypair ────────────────────────────────────────────────────

let _keypair = null;
function getKeypair() {
  if (_keypair) return _keypair;
  const pk = process.env.WALLET_PRIVATE_KEY;
  if (!pk) throw new Error('WALLET_PRIVATE_KEY not set');
  _keypair = Keypair.fromSecretKey(bs58.decode(pk));
  return _keypair;
}

// ── Jupiter API ────────────────────────────────────────────────

function jupHeaders() {
  const h = { 'Content-Type': 'application/json' };
  if (JUP_API_KEY) h['x-api-key'] = JUP_API_KEY;
  return h;
}

async function getSwapOrder({ inputMint, outputMint, amount, slippageBps }) {
  const params = new URLSearchParams({
    inputMint,
    outputMint,
    amount:      Math.floor(amount).toString(),
    slippageBps: (slippageBps ?? SLIPPAGE_BPS).toString(),
    taker:       getKeypair().publicKey.toBase58(),
  });
  const url = `${JUP_API}/ultra/v1/order?${params}`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 8000);
  try {
    const res = await fetch(url, { headers: jupHeaders(), signal: controller.signal });
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Ultra order failed: ${res.status} ${text}`);
    }
    return res.json();
  } finally {
    clearTimeout(timeout);
  }
}

async function executeSwapOrder({ requestId, signedTransaction }) {
  const url = `${JUP_API}/ultra/v1/execute`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 25000);
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: jupHeaders(),
      body: JSON.stringify({ requestId, signedTransaction }),
      signal: controller.signal,
    });
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Ultra execute failed: ${res.status} ${text}`);
    }
    return res.json();
  } finally {
    clearTimeout(timeout);
  }
}

// ── 动态 Priority Fee ────────────────────────────────────────

async function getPriorityFee(accountKeys = []) {
  const rpcUrl = getRpcUrl();
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 3000);
    const res = await fetch(rpcUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 'priority-fee',
        method: 'getPriorityFeeEstimate',
        params: [{
          accountKeys,
          options: { recommended: true },
        }],
      }),
      signal: controller.signal,
    });
    clearTimeout(timeout);

    const json = await res.json();
    const fee = json?.result?.priorityFeeEstimate;
    if (Number.isFinite(fee) && fee > 0) {
      logger.debug('[Trader] 动态 priority fee: %d microLamports', fee);
      return Math.ceil(fee);
    }
  } catch (err) {
    logger.warn('[Trader] getPriorityFee 失败: %s，用默认值', err.message);
  }

  // 默认值
  return parseInt(process.env.PRIORITY_FEE_MICROLAMPORTS || '100000');
}

// ── Helius Sender 发送（低延迟） ──────────────────────────────

async function sendViaHelius(serializedTx) {
  const rpcUrl = getRpcUrl();

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);
  try {
    const res = await fetch(rpcUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 'send-tx',
        method: 'sendTransaction',
        params: [
          Buffer.from(serializedTx).toString('base64'),
          {
            encoding: 'base64',
            skipPreflight: true,       // 省 ~100ms
            maxRetries: 0,             // 手动重试
            preflightCommitment: 'confirmed',
          },
        ],
      }),
      signal: controller.signal,
    });

    const json = await res.json();
    if (json.error) {
      throw new Error(`Helius sendTransaction error: ${JSON.stringify(json.error)}`);
    }
    return json.result; // 返回 signature
  } finally {
    clearTimeout(timeout);
  }
}

// ── 签名工具 ────────────────────────────────────────────────────

function signTx(base64Tx) {
  const kp  = getKeypair();
  const buf = Buffer.from(base64Tx, 'base64');
  const tx  = VersionedTransaction.deserialize(buf);
  tx.sign([kp]);
  return Buffer.from(tx.serialize()).toString('base64');
}

// ── 重试执行 ────────────────────────────────────────────────────

async function executeWithRetry(orderFn, isStopLoss = false) {
  let slippage = SLIPPAGE_BPS;
  const maxRetry = isStopLoss ? Math.min(MAX_RETRY, 2) : MAX_RETRY;  // 止损少重试
  const retryDelay = isStopLoss ? 500 : 1500;  // 止损快重试

  for (let attempt = 1; attempt <= maxRetry; attempt++) {
    try {
      const t0 = Date.now();
      const order = await orderFn(slippage);
      if (!order.transaction) {
        throw new Error(`Ultra order 缺少 transaction 字段`);
      }

      const signed = signTx(order.transaction);
      const result = await executeSwapOrder({
        requestId:         order.requestId,
        signedTransaction: signed,
      });

      const elapsed = Date.now() - t0;
      logger.info('[Trader] 执行耗时: %dms (attempt=%d)', elapsed, attempt);

      if (result.status === 'Success') {
        return result;
      }

      logger.warn('[Trader] swap status="%s" attempt=%d/%d slippage=%dbps',
        result.status, attempt, maxRetry, slippage);

    } catch (err) {
      logger.warn('[Trader] attempt=%d/%d slippage=%dbps 错误: %s',
        attempt, maxRetry, slippage, err.message);
    }

    slippage = Math.min(Math.floor(slippage * 1.5), SLIPPAGE_MAX_BPS);
    if (attempt < maxRetry) await sleep(retryDelay * attempt);
  }

  throw new Error(`交易失败，已重试 ${maxRetry} 次`);
}

// ── 买入 ────────────────────────────────────────────────────────

async function buy(tokenAddress, symbol) {
  const solLamports = Math.floor(TRADE_SOL * LAMPORTS_PER_SOL);
  logger.info('[Trader] 🟢 BUY %s  solLamports=%d  slippage=%dbps(%.1f%%)',
    symbol, solLamports, SLIPPAGE_BPS, SLIPPAGE_BPS / 100);

  const result = await executeWithRetry((slipBps) =>
    getSwapOrder({
      inputMint:   SOL_MINT,
      outputMint:  tokenAddress,
      amount:      solLamports,
      slippageBps: slipBps,
    })
  );

  const amountOut = parseInt(result.outputAmountResult || '0', 10);
  const solIn = parseInt(result.inputAmountResult || String(solLamports), 10) / LAMPORTS_PER_SOL;

  logger.info('[Trader] ✅ BUY 成功 %s  sig=%s  tokens=%d  solIn=%.4f',
    symbol, result.signature?.slice(0, 12), amountOut, solIn);

  return { txid: result.signature, amountOut, solIn };
}

// ── 卖出（止损专用快速路径） ──────────────────────────────────

async function sell(tokenAddress, symbol, position, isStopLoss = false) {
  const amountToken = position?.amountToken;
  if (!amountToken || amountToken <= 0) throw new Error('amountToken 无效');

  const sellSlippage = isStopLoss
    ? Math.min(SLIPPAGE_BPS * 3, SLIPPAGE_MAX_BPS)   // 止损用更宽滑点
    : Math.min(SLIPPAGE_BPS * 2, SLIPPAGE_MAX_BPS);

  logger.info('[Trader] 🔴 SELL %s  amount=%d  slippage=%dbps  isStopLoss=%s',
    symbol, amountToken, sellSlippage, isStopLoss);

  const t0 = Date.now();

  const result = await executeWithRetry(
    (slipBps) => getSwapOrder({
      inputMint:   tokenAddress,
      outputMint:  SOL_MINT,
      amount:      amountToken,
      slippageBps: slipBps,
    }),
    isStopLoss
  );

  const solOut  = parseInt(result.outputAmountResult || '0', 10) / LAMPORTS_PER_SOL;
  const elapsed = Date.now() - t0;

  logger.info('[Trader] ✅ SELL 成功 %s  sig=%s  solOut=%.4f SOL  耗时=%dms',
    symbol, result.signature?.slice(0, 12), solOut, elapsed);

  return { txid: result.signature, solOut, priceUsd: null, elapsedMs: elapsed };
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

module.exports = { buy, sell, getPriorityFee, getRpcUrl };
