import { decodeEventLog, formatUnits, isAddress, keccak256, parseAbiItem, toBytes, zeroAddress } from 'viem';
import {
  UNISWAP_V2_FACTORY_ABI,
  UNISWAP_V2_PAIR_ABI,
  VIRTUALS_LAUNCHPOOL_FACTORY_ABI,
  VIRTUALS_LAUNCHPOOL_EVENT_ABI,
  VIRTUALS_LAUNCHPOOL_PRICE_ABI,
} from './abi.js';
import { config } from './config.js';
import { logger } from './logger.js';
import { nowSec, safeAddress, toFloat } from './utils.js';

const POOL_CREATED_EVENTS = [
  ...VIRTUALS_LAUNCHPOOL_EVENT_ABI,
  parseAbiItem('event PoolCreated(address indexed token, address indexed pool)'),
  parseAbiItem('event PoolCreated(address indexed token, address pool)'),
  parseAbiItem('event PoolCreated(address token, address pool)'),
  parseAbiItem('event LaunchPoolCreated(address indexed token, address indexed pool)'),
  parseAbiItem('event LaunchPoolCreated(address indexed token, address pool)'),
  parseAbiItem('event LaunchPoolCreated(address token, address pool)'),
];

const INTERNAL_EVENT_TOPICS = new Set([
  keccak256(toBytes('Buy(address,uint256,uint256)')),
  keccak256(toBytes('Buy(address,uint256)')),
  keccak256(toBytes('Sell(address,uint256,uint256)')),
  keccak256(toBytes('Sell(address,uint256)')),
  keccak256(toBytes('PriceUpdate(uint256)')),
  keccak256(toBytes('PriceUpdated(uint256)')),
]);

function pow10(n) {
  return 10n ** BigInt(Math.max(0, Number(n || 0)));
}

function ratioToDecimalString(numerRaw, numerDecimals, denomRaw, denomDecimals, precision = 18) {
  const num = BigInt(numerRaw || 0n);
  const den = BigInt(denomRaw || 0n);
  if (den <= 0n || num <= 0n) return '0';
  const p = Math.max(2, Math.min(30, Number(precision || 18)));
  const scaled = num * pow10(denomDecimals + p);
  const base = den * pow10(numerDecimals);
  if (base <= 0n) return '0';
  const q = scaled / base;
  const s = q.toString();
  if (s.length <= p) return `0.${s.padStart(p, '0')}`.replace(/\.?0+$/, '') || '0';
  const head = s.slice(0, -p);
  const tail = s.slice(-p).replace(/0+$/, '');
  return tail ? `${head}.${tail}` : head;
}

function sameCoreView(a, b) {
  if (!a && !b) return true;
  if (!a || !b) return false;
  return [
    'stage',
    'source',
    'pair_address',
    'launch_pool_address',
    'quote_token',
    'spot_price_virtual_str',
    'usd_per_virtual_str',
    'pair_auto_discovered',
  ].every((k) => String(a[k] ?? '') === String(b[k] ?? ''));
}

export class PriceService {
  constructor({ rpc, getTokenMeta, discoverDexPairAddress, onDexPairAutoDiscovered }) {
    this.rpc = rpc;
    this.getTokenMeta = getTokenMeta;
    this.discoverDexPairAddress = discoverDexPairAddress;
    this.onDexPairAutoDiscovered = onDexPairAutoDiscovered;
    this.launchPoolCache = new Map();
    this.tokenState = new Map();
    this.virtualUsd = null;
    this.virtualUsdFetchedAtMs = 0;
  }

  getState(tokenAddress) {
    const key = safeAddress(tokenAddress);
    let st = this.tokenState.get(key);
    if (!st) {
      st = {
        lastView: null,
        lastPriceRefreshMs: 0,
        lastPoolEventBlock: 0,
      };
      this.tokenState.set(key, st);
    }
    return st;
  }

  async refresh({ tokenAddress, tokenStartBlock = 0, latestBlock = 0, configuredDexPairAddress = '', force = false }) {
    const token = safeAddress(tokenAddress);
    if (!isAddress(token)) return { changed: false, view: null };
    const st = this.getState(token);
    await this.refreshVirtualUsdRate(force);

    const stageInfo = await this.detectStage(token, latestBlock);
    const dexView = await this.readDexPrice(token, configuredDexPairAddress, force);
    const useInternal = stageInfo.internal;

    let view = null;
    if (useInternal) {
      const launchPoolAddress = await this.resolveLaunchPoolAddress(token, tokenStartBlock, latestBlock);
      const poolActivity = await this.hasInternalPoolActivity(st, launchPoolAddress, latestBlock, force);
      const minGap = Math.max(1000, Number(config.spotPairRefreshMs || 5000));
      if (isAddress(launchPoolAddress) && (force || poolActivity || Date.now() - st.lastPriceRefreshMs >= minGap)) {
        const internal = await this.readInternalPoolPrice(token, launchPoolAddress);
        st.lastPriceRefreshMs = Date.now();
        if (internal && Number(internal.spot_price_virtual || 0) > 0) {
          view = {
            stage: 'internal',
            source: 'internal_pool',
            stage_reason: stageInfo.reason,
            launch_pool_address: launchPoolAddress,
            pair_address: launchPoolAddress,
            quote_token: safeAddress(config.virtualTokenAddress),
            pair_auto_discovered: false,
            spot_price_virtual: internal.spot_price_virtual,
            spot_price_virtual_str: internal.spot_price_virtual_str,
            usd_per_virtual: this.virtualUsd?.usd_per_virtual || null,
            usd_per_virtual_str: this.virtualUsd?.usd_per_virtual_str || null,
            usd_source: this.virtualUsd?.source || null,
            updated_at: nowSec(),
          };
        }
      }
    }

    if (!view && dexView) {
      view = {
        stage: 'external',
        source: 'dex_pool',
        stage_reason: stageInfo.reason,
        launch_pool_address: null,
        pair_address: dexView.pair_address,
        quote_token: dexView.quote_token,
        pair_auto_discovered: Boolean(dexView.pair_auto_discovered),
        spot_price_virtual: dexView.spot_price_virtual,
        spot_price_virtual_str: dexView.spot_price_virtual_str,
        usd_per_virtual: this.virtualUsd?.usd_per_virtual || null,
        usd_per_virtual_str: this.virtualUsd?.usd_per_virtual_str || null,
        usd_source: this.virtualUsd?.source || null,
        updated_at: nowSec(),
      };
    }

    if (!view) {
      view = {
        stage: useInternal ? 'internal' : 'external',
        source: useInternal ? 'internal_pool_unavailable' : 'dex_pool_unavailable',
        stage_reason: stageInfo.reason,
        launch_pool_address: null,
        pair_address: null,
        quote_token: null,
        pair_auto_discovered: false,
        spot_price_virtual: 0,
        spot_price_virtual_str: '0',
        usd_per_virtual: this.virtualUsd?.usd_per_virtual || null,
        usd_per_virtual_str: this.virtualUsd?.usd_per_virtual_str || null,
        usd_source: this.virtualUsd?.source || null,
        updated_at: nowSec(),
      };
    }

    const changed = !sameCoreView(st.lastView, view);
    st.lastView = view;
    return { changed, view };
  }

  async detectStage(tokenAddress, latestBlock) {
    const factory = safeAddress(config.externalDexFactoryAddress);
    const weth = safeAddress(config.externalWethAddress);
    if (!isAddress(factory) || !isAddress(weth)) {
      return { internal: true, reason: 'external_factory_not_configured' };
    }
    try {
      const pair = safeAddress(await this.rpc.readContract({
        address: factory,
        abi: UNISWAP_V2_FACTORY_ABI,
        functionName: 'getPair',
        args: [tokenAddress, weth],
      }));
      if (!isAddress(pair) || pair === zeroAddress) {
        return { internal: true, reason: 'external_pair_missing' };
      }

      const liqUsd = await this.estimatePairLiquidityUsd(pair, tokenAddress);
      if (Number.isFinite(liqUsd)) {
        if (liqUsd < Number(config.externalMinLiquidityUsd || 50000)) {
          return { internal: true, reason: 'external_liquidity_below_threshold', external_pair: pair, liquidity_usd: liqUsd };
        }
        return { internal: false, reason: 'external_liquidity_ready', external_pair: pair, liquidity_usd: liqUsd };
      }
      return { internal: false, reason: 'external_pair_exists_unknown_liquidity', external_pair: pair };
    } catch (err) {
      logger.warn('detect stage failed, fallback internal', { token: tokenAddress, error: String(err?.message || err) });
      return { internal: true, reason: 'external_detect_error' };
    }
  }

  async estimatePairLiquidityUsd(pairAddress, tokenAddress) {
    const [token0Raw, token1Raw, reserves] = await Promise.all([
      this.rpc.readPairToken0(pairAddress),
      this.rpc.readPairToken1(pairAddress),
      this.rpc.readPairReserves(pairAddress),
    ]);
    const token0 = safeAddress(token0Raw);
    const token1 = safeAddress(token1Raw);
    if (token0 !== tokenAddress && token1 !== tokenAddress) return NaN;
    const quoteToken = token0 === tokenAddress ? token1 : token0;
    const reserve0 = BigInt(reserves?.reserve0 ?? reserves?.[0] ?? 0n);
    const reserve1 = BigInt(reserves?.reserve1 ?? reserves?.[1] ?? 0n);
    const quoteReserve = token0 === tokenAddress ? reserve1 : reserve0;
    if (quoteReserve <= 0n) return 0;

    const quoteMeta = await this.getTokenMeta(quoteToken);
    const quoteAmount = toFloat(quoteReserve.toString(), Number(quoteMeta?.decimals || 18));
    if (!Number.isFinite(quoteAmount) || quoteAmount <= 0) return 0;

    const stableSet = new Set((config.usdStableTokenAddresses || []).map((v) => safeAddress(v)));
    if (stableSet.has(quoteToken)) return quoteAmount * 2;
    if (quoteToken === safeAddress(config.virtualTokenAddress) && Number(this.virtualUsd?.usd_per_virtual || 0) > 0) {
      return quoteAmount * Number(this.virtualUsd.usd_per_virtual) * 2;
    }
    if (quoteToken === safeAddress(config.externalWethAddress) && Number(config.wethUsdFallback || 0) > 0) {
      return quoteAmount * Number(config.wethUsdFallback) * 2;
    }
    return NaN;
  }

  async readDexPrice(tokenAddress, configuredDexPairAddress, force) {
    let pairAutoDiscovered = false;
    let pairAddress = safeAddress(configuredDexPairAddress);
    if (!isAddress(pairAddress) && config.autoDiscoverSpotPair && this.discoverDexPairAddress) {
      const autoPair = safeAddress(await this.discoverDexPairAddress(force));
      if (isAddress(autoPair)) {
        pairAddress = autoPair;
        pairAutoDiscovered = true;
        this.onDexPairAutoDiscovered?.(autoPair);
      }
    }
    if (!isAddress(pairAddress)) return null;
    try {
      const [token0Raw, token1Raw, reserves] = await Promise.all([
        this.rpc.readPairToken0(pairAddress),
        this.rpc.readPairToken1(pairAddress),
        this.rpc.readPairReserves(pairAddress),
      ]);
      const token0 = safeAddress(token0Raw);
      const token1 = safeAddress(token1Raw);
      if (token0 !== tokenAddress && token1 !== tokenAddress) return null;
      const quoteToken = token0 === tokenAddress ? token1 : token0;
      const quoteMeta = await this.getTokenMeta(quoteToken);
      const tokenMeta = await this.getTokenMeta(tokenAddress);
      const reserve0 = BigInt(reserves?.reserve0 ?? reserves?.[0] ?? 0n);
      const reserve1 = BigInt(reserves?.reserve1 ?? reserves?.[1] ?? 0n);
      const tokenReserve = token0 === tokenAddress ? reserve0 : reserve1;
      const quoteReserve = token0 === tokenAddress ? reserve1 : reserve0;
      if (tokenReserve <= 0n || quoteReserve <= 0n) return null;
      const spotPriceStr = ratioToDecimalString(
        quoteReserve,
        Number(quoteMeta?.decimals || 18),
        tokenReserve,
        Number(tokenMeta?.decimals || 18),
        18,
      );
      const spotPrice = Number(spotPriceStr);
      if (!Number.isFinite(spotPrice) || spotPrice <= 0) return null;
      return {
        pair_address: pairAddress,
        quote_token: quoteToken,
        pair_auto_discovered: pairAutoDiscovered,
        spot_price_virtual: spotPrice,
        spot_price_virtual_str: spotPriceStr,
      };
    } catch (err) {
      logger.warn('read dex price failed', { token: tokenAddress, pairAddress, error: String(err?.message || err) });
      return null;
    }
  }

  async resolveLaunchPoolAddress(tokenAddress, tokenStartBlock, latestBlock) {
    const cached = this.launchPoolCache.get(tokenAddress);
    if (cached && isAddress(cached)) return cached;

    const factory = safeAddress(config.virtualsLaunchPoolFactoryAddress);
    const protocol = safeAddress(config.virtualsProtocolAddress);
    const latest = Math.max(0, Number(latestBlock || 0));
    const span = Math.max(2000, Number(config.launchPoolScanBlocks || 30000));
    const startHint = Math.max(0, Number(tokenStartBlock || 0));
    const from = startHint > 0 ? Math.max(0, startHint - span) : Math.max(0, latest - span);
    const to = startHint > 0 ? Math.min(latest, startHint + span) : latest;

    const logDebug = (msg, meta) => {
      if (config.debugTrackingLogs) logger.info(msg, meta);
    };

    const candidates = ['getLaunchPool', 'launchPoolOf', 'pools', 'tokenToPool'];
    if (isAddress(factory)) {
      for (const functionName of candidates) {
        try {
          const pool = safeAddress(await this.rpc.readContract({
            address: factory,
            abi: VIRTUALS_LAUNCHPOOL_FACTORY_ABI,
            functionName,
            args: [tokenAddress],
          }));
          if (isAddress(pool) && pool !== zeroAddress) {
            this.launchPoolCache.set(tokenAddress, pool);
            logDebug('launch pool resolved', {
              mode: 'factory_mapping',
              token: tokenAddress,
              factory,
              functionName,
              pool,
            });
            return pool;
          }
        } catch {
          // try next selector
        }
      }
    }

    const scanTargets = [];
    if (isAddress(factory)) scanTargets.push({ mode: 'factory_logs', address: factory });
    if (isAddress(protocol) && protocol !== factory) scanTargets.push({ mode: 'protocol_logs', address: protocol });

    for (const t of scanTargets) {
      logDebug('launch pool scan start', {
        mode: t.mode,
        token: tokenAddress,
        scanAddress: t.address,
        fromBlock: from,
        toBlock: to,
        span,
      });
      const pool = await this.scanPoolCreatedLogsPaged({
        scanAddress: t.address,
        tokenAddress,
        fromBlock: from,
        toBlock: to,
        mode: t.mode,
      });
      if (isAddress(pool) && pool !== zeroAddress) {
        this.launchPoolCache.set(tokenAddress, pool);
        logDebug('launch pool resolved', {
          mode: t.mode,
          token: tokenAddress,
          scanAddress: t.address,
          fromBlock: from,
          toBlock: to,
          pool,
        });
        return pool;
      }
    }

    return '';
  }

  decodePoolCreatedLog(log) {
    for (const ev of POOL_CREATED_EVENTS) {
      try {
        const decoded = decodeEventLog({
          abi: [ev],
          topics: log.topics,
          data: log.data,
          strict: false,
        });
        const args = decoded?.args || {};
        const createdToken = safeAddress(args.token ?? args._token ?? args[0]);
        const pool = safeAddress(args.pool ?? args.poolAddress ?? args.launchPool ?? args[1]);
        if (isAddress(createdToken) && isAddress(pool) && pool !== zeroAddress) {
          return { token: createdToken, pool };
        }
      } catch {
        // ignore and try next event signature
      }
    }
    return null;
  }

  async scanPoolCreatedLogsPaged({ scanAddress, tokenAddress, fromBlock, toBlock, mode }) {
    if (!isAddress(scanAddress)) return '';
    const targetToken = safeAddress(tokenAddress);
    if (!isAddress(targetToken)) return '';

    const from = Math.max(0, Number(fromBlock || 0));
    const to = Math.max(from, Number(toBlock || 0));
    const page = Math.max(500, Math.min(5000, Number(config.logChunkSize || 1200)));

    for (let cursor = from; cursor <= to; cursor += page) {
      const end = Math.min(to, cursor + page - 1);
      try {
        const logs = await this.rpc.getLogs({
          address: scanAddress,
          fromBlock: BigInt(cursor),
          toBlock: BigInt(end),
        });
        for (const log of logs) {
          const decoded = this.decodePoolCreatedLog(log);
          if (!decoded) continue;
          if (decoded.token === targetToken) {
            if (config.debugTrackingLogs) {
              logger.info('launch pool scan hit', {
                mode,
                scanAddress,
                token: targetToken,
                pool: decoded.pool,
                blockNumber: Number(log.blockNumber || 0),
                txHash: String(log.transactionHash || '').toLowerCase(),
              });
            }
            return decoded.pool;
          }
        }
      } catch (err) {
        logger.warn('scan launch pool failed', {
          mode,
          token: targetToken,
          scanAddress,
          fromBlock: cursor,
          toBlock: end,
          error: String(err?.message || err),
        });
      }
    }
    return '';
  }

  async hasInternalPoolActivity(state, launchPoolAddress, latestBlock, force) {
    if (!isAddress(launchPoolAddress) || !Number.isFinite(Number(latestBlock || 0))) return false;
    const latest = Math.max(0, Number(latestBlock || 0));
    const from = force
      ? Math.max(0, latest - 50)
      : Math.max(0, Number(state.lastPoolEventBlock || 0) + 1);
    if (from > latest) return false;
    try {
      const logs = await this.rpc.getLogs({
        address: launchPoolAddress,
        fromBlock: BigInt(from),
        toBlock: BigInt(latest),
      });
      state.lastPoolEventBlock = latest;
      if (!logs.length) return false;
      return logs.some((l) => {
        const topic0 = String(l?.topics?.[0] || '').toLowerCase();
        return INTERNAL_EVENT_TOPICS.has(topic0);
      }) || logs.length > 0;
    } catch {
      return false;
    }
  }

  async readInternalPoolPrice(tokenAddress, launchPoolAddress) {
    const directFns = ['getCurrentPrice', 'currentPrice', 'price'];
    for (const functionName of directFns) {
      try {
        const raw = BigInt(await this.rpc.readContract({
          address: launchPoolAddress,
          abi: VIRTUALS_LAUNCHPOOL_PRICE_ABI,
          functionName,
          args: [],
        }));
        if (raw > 0n) {
          const priceStr = String(formatUnits(raw, Number(config.internalPriceDecimals || 18)));
          const price = Number(priceStr);
          if (Number.isFinite(price) && price > 0) {
            return { spot_price_virtual: price, spot_price_virtual_str: priceStr };
          }
        }
      } catch {
        // try next function
      }
    }

    const readFirst = async (names) => {
      for (const functionName of names) {
        try {
          const v = await this.rpc.readContract({
            address: launchPoolAddress,
            abi: VIRTUALS_LAUNCHPOOL_PRICE_ABI,
            functionName,
            args: [],
          });
          return BigInt(v || 0n);
        } catch {
          // continue
        }
      }
      return 0n;
    };

    const [virtualBoughtRaw, tokenSoldRaw, virtualMeta, tokenMeta] = await Promise.all([
      readFirst(['totalVirtualBought', 'cumulativeVirtualBought']),
      readFirst(['totalTokenSold', 'cumulativeTokenSold']),
      this.getTokenMeta(safeAddress(config.virtualTokenAddress)),
      this.getTokenMeta(tokenAddress),
    ]);
    if (virtualBoughtRaw <= 0n || tokenSoldRaw <= 0n) return null;

    const priceStr = ratioToDecimalString(
      virtualBoughtRaw,
      Number(virtualMeta?.decimals || 18),
      tokenSoldRaw,
      Number(tokenMeta?.decimals || 18),
      18,
    );
    const price = Number(priceStr);
    if (!Number.isFinite(price) || price <= 0) return null;
    return { spot_price_virtual: price, spot_price_virtual_str: priceStr };
  }

  async refreshVirtualUsdRate(force = false) {
    const pairAddress = safeAddress(config.virtualUsdPairAddress);
    if (!isAddress(pairAddress)) {
      if (Number(config.virtualUsdFallback || 0) > 0) {
        this.virtualUsd = {
          source: 'env_fallback',
          usd_per_virtual: Number(config.virtualUsdFallback),
          usd_per_virtual_str: String(config.virtualUsdFallback),
          updated_at: nowSec(),
        };
      }
      return;
    }
    const nowMs = Date.now();
    const minGap = Math.max(1000, Number(config.spotPairRefreshMs || 5000));
    if (!force && nowMs - this.virtualUsdFetchedAtMs < minGap) return;
    try {
      const [token0Raw, token1Raw, reserves] = await Promise.all([
        this.rpc.readPairToken0(pairAddress),
        this.rpc.readPairToken1(pairAddress),
        this.rpc.readPairReserves(pairAddress),
      ]);
      const token0 = safeAddress(token0Raw);
      const token1 = safeAddress(token1Raw);
      const virtual = safeAddress(config.virtualTokenAddress);
      if (token0 !== virtual && token1 !== virtual) return;

      const quoteToken = token0 === virtual ? token1 : token0;
      const stableSet = new Set((config.usdStableTokenAddresses || []).map((v) => safeAddress(v)).filter((v) => isAddress(v)));
      if (stableSet.size && !stableSet.has(quoteToken)) return;

      const quoteMeta = await this.getTokenMeta(quoteToken);
      const virtualMeta = await this.getTokenMeta(virtual);

      const reserve0 = BigInt(reserves?.reserve0 ?? reserves?.[0] ?? 0n);
      const reserve1 = BigInt(reserves?.reserve1 ?? reserves?.[1] ?? 0n);
      const virtualReserve = token0 === virtual ? reserve0 : reserve1;
      const quoteReserve = token0 === virtual ? reserve1 : reserve0;
      if (virtualReserve <= 0n || quoteReserve <= 0n) return;

      const usdPerVirtualStr = ratioToDecimalString(
        quoteReserve,
        Number(quoteMeta?.decimals || 18),
        virtualReserve,
        Number(virtualMeta?.decimals || 18),
        18,
      );
      const usdPerVirtual = Number(usdPerVirtualStr);
      if (!Number.isFinite(usdPerVirtual) || usdPerVirtual <= 0) return;

      this.virtualUsd = {
        source: 'virtual_usd_pair',
        pair_address: pairAddress,
        quote_token: quoteToken,
        usd_per_virtual: usdPerVirtual,
        usd_per_virtual_str: usdPerVirtualStr,
        updated_at: nowSec(),
      };
      this.virtualUsdFetchedAtMs = nowMs;
    } catch (err) {
      logger.warn('virtual usd refresh failed', { error: String(err?.message || err) });
    }
  }
}
