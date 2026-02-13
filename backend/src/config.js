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
  host: process.env.HOST || '0.0.0.0',
  frontendOrigin: String(process.env.FRONTEND_ORIGIN || '').trim(),
  dbPath: process.env.DB_PATH || './data/app.db',
  baseHttpRpc: process.env.BASE_HTTP_RPC || 'https://mainnet.base.org',
  baseWsRpc: process.env.BASE_WS_RPC || '',
  spotPairAddress: String(process.env.SPOT_PAIR_ADDRESS || '').toLowerCase(),
  spotPairRefreshMs: envNumber('SPOT_PAIR_REFRESH_MS', 5000),
  autoDiscoverSpotPair: envBool('AUTO_DISCOVER_SPOT_PAIR', true),
  autoDiscoverSpotPairIntervalMs: envNumber('AUTO_DISCOVER_SPOT_PAIR_INTERVAL_MS', 60000),
  externalDexFactoryAddress: String(process.env.EXTERNAL_DEX_FACTORY_ADDRESS || '').toLowerCase(),
  externalWethAddress: String(process.env.EXTERNAL_WETH_ADDRESS || '0x4200000000000000000000000000000000000006').toLowerCase(),
  externalMinLiquidityUsd: envNumber('EXTERNAL_MIN_LIQUIDITY_USD', 50000),
  wethUsdFallback: envNumber('WETH_USD_FALLBACK', 0),
  virtualsLaunchPoolFactoryAddress: String(process.env.VIRTUALS_LAUNCHPOOL_FACTORY_ADDRESS || '').toLowerCase(),
  virtualsProtocolAddress: String(process.env.VIRTUALS_PROTOCOL_ADDRESS || '0xF66DeA7b3e897cD44A5a231c61B6B4423d613259').toLowerCase(),
  launchPoolScanBlocks: envNumber('LAUNCHPOOL_SCAN_BLOCKS', 30000),
  internalPriceDecimals: envNumber('INTERNAL_PRICE_DECIMALS', 18),
  protocolAlertInflowVirtual: envNumber('PROTOCOL_ALERT_INFLOW_VIRTUAL', 5000),
  protocolAutoDiscoverEnabled: envBool('PROTOCOL_AUTO_DISCOVER_ENABLED', true),
  protocolAutoDiscoverIntervalMs: envNumber('PROTOCOL_AUTO_DISCOVER_INTERVAL_MS', 60000),
  protocolAutoDiscoverBlocks: envNumber('PROTOCOL_AUTO_DISCOVER_BLOCKS', 2500),
  protocolAutoDiscoverMinInteractions: envNumber('PROTOCOL_AUTO_DISCOVER_MIN_INTERACTIONS', 8),
  protocolAutoDiscoverMinDistinctTokens: envNumber('PROTOCOL_AUTO_DISCOVER_MIN_DISTINCT_TOKENS', 2),
  protocolAutoDiscoverMinVirtualBalance: envNumber('PROTOCOL_AUTO_DISCOVER_MIN_VIRTUAL_BALANCE', 1000),
  protocolAutoDiscoverMaxCandidates: envNumber('PROTOCOL_AUTO_DISCOVER_MAX_CANDIDATES', 30),
  requireVirtualQuoteForSpot: envBool('REQUIRE_VIRTUAL_QUOTE_FOR_SPOT', true),
  virtualUsdPairAddress: String(process.env.VIRTUAL_USD_PAIR_ADDRESS || '').toLowerCase(),
  virtualUsdFallback: envNumber('VIRTUAL_USD_FALLBACK', 0),
  usdStableTokenAddresses: envAddressList('USD_STABLE_TOKEN_ADDRESSES').length
    ? envAddressList('USD_STABLE_TOKEN_ADDRESSES')
    : [
      '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913', // USDC (Base)
      '0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca', // USDbC (legacy)
    ],
  virtualTokenAddress: String(
    process.env.VIRTUAL_CA
    || process.env.VIRTUAL_TOKEN_ADDRESS
    || '0x0b3e328455c4059eeb9e3f84b5543f74e24e7e1b'
  ).toLowerCase(),
  counterpartyTokens: envAddressList('COUNTERPARTY_TOKEN_ADDRESSES'),

  backfillBlocks: envNumber('BACKFILL_BLOCKS', 8000),
  tokenStartBlock: envNumber('TOKEN_START_BLOCK', 0),
  autoDiscoverTokenStart: envBool('AUTO_DISCOVER_TOKEN_START', true),
  autoDiscoverLaunchStartTime: envBool('AUTO_DISCOVER_LAUNCH_START_TIME', true),
  tokenStartProbeCoarseSpan: envNumber('TOKEN_START_PROBE_COARSE_SPAN', 200000),
  tokenStartProbeFineSpan: envNumber('TOKEN_START_PROBE_FINE_SPAN', 10000),
  tokenStartProbeMicroSpan: envNumber('TOKEN_START_PROBE_MICRO_SPAN', 500),
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
  debugTrackingLogs: envBool('DEBUG_TRACKING_LOGS', false),
  specialAddressFile: process.env.SPECIAL_ADDRESS_FILE || './backend/src/special-addresses.json',
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
  const deltaSec = Math.max(0, Number(nowSec || 0) - Number(launchStartTime || 0));
  const minutes = Math.floor(deltaSec / 60);
  return clamp(99 - minutes, 1, 99);
}
