'use strict';
// src/wsHub.js — WebSocket 广播中心 (V5: tick 节流)

const WebSocket = require('ws');
let _wss = null;

// ★ V5: tick 消息节流 — 同一 address 每 2 秒最多广播一次
const _lastTickTs = new Map();  // address → timestamp
const TICK_THROTTLE_MS = 2000;

function init(server) {
  _wss = new WebSocket.Server({ server });
  _wss.on('connection', ws => {
    ws.on('error', () => {});
  });
}

function broadcast(data) {
  if (!_wss) return;

  // ★ 节流 tick 消息（占绝大多数广播量）
  if (data.type === 'tick' && data.address) {
    const now = Date.now();
    const last = _lastTickTs.get(data.address) || 0;
    if (now - last < TICK_THROTTLE_MS) return;  // 丢弃，等下一个周期
    _lastTickTs.set(data.address, now);
  }

  const msg = JSON.stringify(data);
  _wss.clients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) {
      try { ws.send(msg); } catch (_) {}
    }
  });
}

module.exports = { init, broadcast };
