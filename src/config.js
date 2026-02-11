import fs from 'node:fs';
import path from 'node:path';

(function loadDotEnv() {
  const file = path.resolve('.env');
  if (!fs.existsSync(file)) return;
  const lines = fs.readFileSync(file, 'utf8').split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const idx = trimmed.indexOf('=');
    if (idx <= 0) continue;
    const key = trimmed.slice(0, idx).trim();
    const value = trimmed.slice(idx + 1).trim();
    if (key && process.env[key] == null) {
      process.env[key] = value;
    }
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
  const raw = String(process.env[name] ?? '');
  return raw
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
  cooldownMinutes: envNumber('RULE_COOLDOWN_MINUTES', 10),
  thresholdFirstMinute: envNumber('RULE_FIRST_THRESHOLD', 3000),
  thresholdSecondMinute: envNumber('RULE_SECOND_THRESHOLD', 6000),
  metricMode: process.env.METRIC_MODE || 'token_received',
  useWsHead: envBool('USE_WS_NEW_HEADS', true),
  maxRpcRetries: envNumber('MAX_RPC_RETRIES', 5),
  rpcBaseBackoffMs: envNumber('RPC_BASE_BACKOFF_MS', 400),
  enableTxFacts: envBool('ENABLE_TX_FACTS', false),
};

export function buildCounterpartyList() {
  const set = new Set(config.counterpartyTokens);
  if (config.virtualTokenAddress) set.add(config.virtualTokenAddress);
  return Array.from(set);
}
