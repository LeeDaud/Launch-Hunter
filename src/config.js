import fs from 'node:fs';
import path from 'node:path';

(function loadDotEnv() {
  const file = path.resolve('.env');
  if (!fs.existsSync(file)) return;
  for (const line of fs.readFileSync(file, 'utf8').split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const idx = trimmed.indexOf('=');
    if (idx <= 0) continue;
    const k = trimmed.slice(0, idx).trim();
    const v = trimmed.slice(idx + 1).trim();
    if (k && process.env[k] == null) process.env[k] = v;
  }
})();

function envNumber(name, fallback) {
  const raw = process.env[name];
  if (raw == null || raw === '') return fallback;
  const n = Number(raw);
  return Number.isFinite(n) ? n : fallback;
}

function envBool(name, fallback) {
  const raw = String(process.env[name] ?? '').toLowerCase();
  if (!raw) return fallback;
  return raw === '1' || raw === 'true' || raw === 'yes' || raw === 'on';
}

function envAddressList(name) {
  return String(process.env[name] ?? '')
    .split(',')
    .map((v) => v.trim().toLowerCase())
    .filter(Boolean);
}

export const config = {
  port: envNumber('PORT', 3000),
  dbPath: process.env.DB_PATH || './tracker.db',
  baseHttpRpc: process.env.BASE_HTTP_RPC || 'https://mainnet.base.org',
  baseWsRpc: process.env.BASE_WS_RPC || '',
  virtualTokenAddress: String(process.env.VIRTUAL_TOKEN_ADDRESS || '').toLowerCase(),
  counterpartyTokens: envAddressList('COUNTERPARTY_TOKEN_ADDRESSES'),

  backfillBlocks: envNumber('BACKFILL_BLOCKS', 8000),
  logChunkSize: envNumber('LOG_CHUNK_SIZE', 1200),
  replayRecentBlocks: envNumber('REPLAY_RECENT_BLOCKS', 24),
  pollingIntervalMs: envNumber('POLLING_INTERVAL_MS', 2500),
  updateThrottleMs: envNumber('UPDATE_THROTTLE_MS', 2000),

  thresholdFirstMinute: envNumber('RULE_FIRST_THRESHOLD', 3000),
  thresholdSecondMinute: envNumber('RULE_SECOND_THRESHOLD', 6000),
  cooldownMinutes: envNumber('RULE_COOLDOWN_MINUTES', 10),
  metricMode: process.env.METRIC_MODE || 'token_received',

  sellTaxPct: envNumber('SELL_TAX_PCT', 1),
  launchStartTime: envNumber('LAUNCH_START_TIME', 0),
  walletAddress: String(process.env.MY_WALLET_ADDRESS || '').toLowerCase(),
  myWalletFromBlock: envNumber('MY_WALLET_FROM_BLOCK', 0),
  myWalletMaxBackfillBlocks: envNumber('MY_WALLET_MAX_BACKFILL_BLOCKS', 500000),
  curveWindowMinutes: envNumber('CURVE_WINDOW_MINUTES', 30),

  useWsHead: envBool('USE_WS_NEW_HEADS', true),
  maxRpcRetries: envNumber('MAX_RPC_RETRIES', 5),
  rpcBaseBackoffMs: envNumber('RPC_BASE_BACKOFF_MS', 400),

  enableTxFacts: envBool('ENABLE_TX_FACTS', true),
  specialAddressFile: process.env.SPECIAL_ADDRESS_FILE || './src/special-addresses.json',
};

export function buildCounterpartyList() {
  const set = new Set(config.counterpartyTokens);
  if (config.virtualTokenAddress) set.add(config.virtualTokenAddress);
  return Array.from(set);
}

export function clamp(v, min, max) {
  return Math.min(max, Math.max(min, v));
}

export function calcBuyTaxPct(nowSec, launchStartTime) {
  if (!launchStartTime || launchStartTime <= 0) return 1;
  const minutes = Math.max(0, Math.floor((nowSec - launchStartTime) / 60));
  return clamp(99 - minutes, 1, 99);
}
