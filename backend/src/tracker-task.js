import { decodeEventLog, formatUnits, isAddress, parseAbiItem, zeroAddress } from 'viem';
import { buildCounterpartyList, calcBuyTaxPct, config } from './config.js';
import { logger } from './logger.js';
import { ERC20_ABI } from './abi.js';
import { PriceService } from './price-service.js';
import { minuteFloor, nowSec, safeAddress, sleep, toFloat } from './utils.js';

const TRANSFER_EVENT = parseAbiItem('event Transfer(address indexed from, address indexed to, uint256 value)');

function chunkRanges(fromBlock, toBlock, span) {
  const out = [];
  let cursor = fromBlock;
  while (cursor <= toBlock) {
    const end = Math.min(cursor + span - 1, toBlock);
    out.push([cursor, end]);
    cursor = end + 1;
  }
  return out;
}

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

export class TokenTrackerTask {
  constructor({ tokenAddress, db, rpc, ruleEngine, runtimeConfig, specialAddresses, onUpdate }) {
    this.tokenAddress = safeAddress(tokenAddress);
    this.db = db;
    this.rpc = rpc;
    this.ruleEngine = ruleEngine;
    this.runtimeConfig = runtimeConfig;
    this.specialAddresses = specialAddresses || [];
    this.onUpdate = onUpdate;

    this.running = false;
    this.stopFlag = false;
    this.backfillDone = false;
    this.unwatchHead = null;
    this.lastKnownHead = 0;
    this.lastEmitAt = 0;
    this.lastChainTimeSec = 0;
    this.currentMetricMode = config.metricMode === 'virtual_spent' ? 'virtual_spent' : 'token_received';

    this.blockTsCache = new Map();
    this.tokenMetaCache = new Map();
    this.counterpartyList = buildCounterpartyList();
    this.staticSpecialMap = new Map(this.specialAddresses.map((s) => [safeAddress(s.address), s]));
    this.dynamicSpecialMap = new Map();
    this.specialMap = new Map(this.staticSpecialMap);
    this.myWalletSet = new Set();
    this.scanQueue = Promise.resolve();
    this.pairSpot = null;
    this.pairSpotFetchedAtMs = 0;
    this.spotPairAutoDetectLastAtMs = 0;
    this.virtualUsd = null;
    this.virtualUsdFetchedAtMs = 0;
    this.unifiedPrice = null;
    this.protocolBalances = {
      sell_wall_token_balance: 0,
      launch_pool_token_balance: 0,
      updated_at: 0,
    };
    this.walletBackfillQueued = false;
    this.walletBackfillRunning = false;
    this.walletBackfillStatus = 'idle';
    this.walletBackfillLastAt = 0;
    this.walletBackfillLastError = '';
    this.walletBackfillLastReason = '';
    this.lastIngest = {
      start: 0,
      end: 0,
      log_count: 0,
      tx_count: 0,
      no_transfers_in_window: false,
      updated_at: 0,
      wallet_filtered: false,
    };
    this.priceService = new PriceService({
      rpc: this.rpc,
      getTokenMeta: async (addr) => this.ensureTokenMeta(addr),
      discoverDexPairAddress: async () => this.tryAutoDetectSpotPairAddress(),
      onDexPairAutoDiscovered: (addr) => {
        if (isAddress(addr)) this.runtimeConfig.spotPairAddress = safeAddress(addr);
      },
    });
    this._updateSpecialMap();
    this.refreshMyWalletSet();
  }

  _updateSpecialMap() {
    const next = new Map(this.staticSpecialMap);
    for (const [addr, row] of this.dynamicSpecialMap.entries()) next.set(addr, row);
    this.specialMap = next;
  }

  _setDynamicSpecialAddress(address, row) {
    const addr = safeAddress(address);
    if (!isAddress(addr) || addr === zeroAddress) return;
    this.dynamicSpecialMap.set(addr, {
      address: addr,
      label: String(row?.label || 'Dynamic Protocol Address'),
      type: String(row?.type || 'dynamic_special'),
      category: String(row?.category || row?.type || 'dynamic_special'),
    });
    this._updateSpecialMap();
  }

  _findSpecialAddressByType(type) {
    const t = String(type || '').trim().toLowerCase();
    for (const row of this.specialMap.values()) {
      if (String(row?.type || row?.category || '').toLowerCase() === t) {
        return safeAddress(row.address);
      }
    }
    return '';
  }

  async start() {
    this.running = true;
    this.stopFlag = false;
    this.db.setMeta(this.tokenAddress, { running: 1 });

    await this.ensureTokenMeta(this.tokenAddress);
    for (const addr of this.counterpartyList) {
      if (addr) await this.ensureTokenMeta(addr);
    }

    const latest = Number(await this.rpc.getBlockNumber());
    this.lastKnownHead = latest;
    const state = this.db.getMeta(this.tokenAddress);
    const configuredStart = Math.max(0, Number(this.runtimeConfig.tokenStartBlock || config.tokenStartBlock || 0));
    let discoveredStart = 0;
    if (!state?.last_processed_block && configuredStart <= 0 && config.autoDiscoverTokenStart) {
      discoveredStart = await this.discoverTokenStartBlock(latest);
      if (discoveredStart > 0) this.runtimeConfig.tokenStartBlock = discoveredStart;
    }
    if (!state?.last_processed_block && Number(this.runtimeConfig.launchStartTime || 0) <= 0 && config.autoDiscoverLaunchStartTime) {
      const candidateBlock = discoveredStart > 0 ? discoveredStart : configuredStart;
      if (candidateBlock > 0) {
        try {
          const launchTs = await this.getBlockTimestamp(candidateBlock);
          if (Number.isFinite(Number(launchTs)) && Number(launchTs) > 0) {
            this.runtimeConfig.launchStartTime = Number(launchTs);
            logger.info('launch start time auto-discovered', {
              token: this.tokenAddress,
              block: candidateBlock,
              launchStartTime: this.runtimeConfig.launchStartTime,
            });
          }
        } catch (err) {
          logger.warn('launch start auto-discovery failed', { error: String(err?.message || err) });
        }
      }
    }
    const startFrom = state?.last_processed_block
      ? Number(state.last_processed_block) + 1
      : configuredStart > 0
        ? configuredStart
        : discoveredStart > 0
          ? discoveredStart
        : Math.max(0, latest - config.backfillBlocks);

    logger.info('task start', {
      token: this.tokenAddress,
      startFrom,
      latest,
      configuredStart,
      discoveredStart,
      counterpartyCount: this.counterpartyList.length,
      specialCount: this.specialAddresses.length,
    });

    if (startFrom <= latest) {
      await this.enqueueScan(startFrom, latest, false, null);
    }
    await this.refreshPairSpotPrice(true);
    const initTokenMeta = await this.ensureTokenMeta(this.tokenAddress);
    await this.refreshProtocolBalances(Number(initTokenMeta?.decimals || 18));

    this.backfillDone = true;
    this.emitUpdate(true, 'backfill_complete');
    this.requestWalletBackfill('initial');

    this.unwatchHead = this.rpc.subscribeNewHeads((bn) => {
      this.lastKnownHead = Math.max(this.lastKnownHead, bn);
    });

    while (!this.stopFlag) {
      try {
        const currentHead = Number(await this.rpc.getBlockNumber());
        this.lastKnownHead = Math.max(this.lastKnownHead, currentHead);

        const meta = this.db.getMeta(this.tokenAddress);
        const lastProcessed = Number(meta?.last_processed_block || 0);
        if (this.lastKnownHead >= lastProcessed) {
          const replayFrom = Math.max(0, lastProcessed - config.replayRecentBlocks + 1);
          await this.enqueueScan(replayFrom, this.lastKnownHead, true, null);
        }
        await this.refreshPairSpotPrice(false);
        const loopTokenMeta = await this.ensureTokenMeta(this.tokenAddress);
        await this.refreshProtocolBalances(Number(loopTokenMeta?.decimals || 18));

        this.emitUpdate(false, 'tick');
      } catch (err) {
        logger.error('task loop failed', { error: String(err?.message || err) });
      }

      await sleep(config.pollingIntervalMs);
    }

    if (this.unwatchHead) {
      try { this.unwatchHead(); } catch {}
    }

    this.running = false;
    this.db.setMeta(this.tokenAddress, { running: 0 });
    this.emitUpdate(true, 'stopped');
  }

  async hasTransferInRange(fromBlock, toBlock) {
    if (toBlock < fromBlock) return false;
    const logs = await this.rpc.getLogs({
      address: this.tokenAddress,
      event: TRANSFER_EVENT,
      fromBlock: BigInt(fromBlock),
      toBlock: BigInt(toBlock),
    });
    return logs.length > 0;
  }

  async discoverTokenStartBlock(latestBlock) {
    const latest = Math.max(0, Number(latestBlock || 0));
    if (latest <= 0) return 0;

    const coarseSpan = Math.max(10000, Number(config.tokenStartProbeCoarseSpan || 200000));
    const fineSpan = Math.max(1000, Number(config.tokenStartProbeFineSpan || 10000));
    const microSpan = Math.max(100, Number(config.tokenStartProbeMicroSpan || 500));

    let coarseStart = -1;
    let coarseEnd = -1;
    for (let s = 0; s <= latest; s += coarseSpan) {
      const e = Math.min(latest, s + coarseSpan - 1);
      if (await this.hasTransferInRange(s, e)) {
        coarseStart = s;
        coarseEnd = e;
        break;
      }
    }
    if (coarseStart < 0) return 0;

    let fineStart = coarseStart;
    let fineEnd = coarseEnd;
    for (let s = coarseStart; s <= coarseEnd; s += fineSpan) {
      const e = Math.min(coarseEnd, s + fineSpan - 1);
      if (await this.hasTransferInRange(s, e)) {
        fineStart = s;
        fineEnd = e;
        break;
      }
    }

    let microStart = fineStart;
    let microEnd = fineEnd;
    for (let s = fineStart; s <= fineEnd; s += microSpan) {
      const e = Math.min(fineEnd, s + microSpan - 1);
      if (await this.hasTransferInRange(s, e)) {
        microStart = s;
        microEnd = e;
        break;
      }
    }

    const logs = await this.rpc.getLogs({
      address: this.tokenAddress,
      event: TRANSFER_EVENT,
      fromBlock: BigInt(microStart),
      toBlock: BigInt(microEnd),
    });
    if (!logs.length) return microStart;
    let first = Number(logs[0].blockNumber);
    for (const log of logs) first = Math.min(first, Number(log.blockNumber));

    logger.info('token start block discovered', {
      token: this.tokenAddress,
      firstTransferBlock: first,
      latest,
      coarseSpan,
      fineSpan,
      microSpan,
    });
    return first;
  }

  async stop() {
    this.stopFlag = true;
  }

  setMetricMode(mode) {
    this.currentMetricMode = mode === 'virtual_spent' ? 'virtual_spent' : 'token_received';
  }

  refreshMyWalletSet() {
    const list = Array.isArray(this.runtimeConfig?.myWallets) ? this.runtimeConfig.myWallets : [];
    this.myWalletSet = new Set(list.map((x) => String(x || '').trim().toLowerCase()).filter((x) => isAddress(x)));
  }

  onRuntimeUpdated({ myWalletsChanged, myWalletFromBlockChanged, spotPairChanged } = {}) {
    if (myWalletsChanged) this.refreshMyWalletSet();
    if (spotPairChanged) {
      this.pairSpot = null;
      this.pairSpotFetchedAtMs = 0;
      this.refreshPairSpotPrice(true).catch(() => {});
    }
    if (myWalletsChanged || myWalletFromBlockChanged) {
      this.requestWalletBackfill('runtime_update');
      this.emitUpdate(true, 'wallets_updated');
    }
  }

  enqueueScan(fromBlock, toBlock, isReplay, walletFilterSet) {
    this.scanQueue = this.scanQueue
      .then(() => this.scanRange(fromBlock, toBlock, isReplay, walletFilterSet))
      .catch((err) => logger.error('scan queue failed', { error: String(err?.message || err) }));
    return this.scanQueue;
  }

  requestWalletBackfill(reason) {
    if (!this.running || this.stopFlag) return;
    if (!this.myWalletSet.size) return;
    this.walletBackfillQueued = true;
    this.walletBackfillStatus = 'queued';
    this.walletBackfillLastReason = String(reason || '');
    this.walletBackfillLastError = '';
    this.walletBackfillLastAt = nowSec();
    this.emitUpdate(true, 'wallet_backfill_queued');
    setTimeout(() => this.runWalletBackfill(reason), 0);
  }

  async runWalletBackfill(reason) {
    if (!this.walletBackfillQueued || this.walletBackfillRunning || this.stopFlag) return;
    this.walletBackfillQueued = false;
    this.walletBackfillRunning = true;
    this.walletBackfillStatus = 'running';
    this.walletBackfillLastReason = String(reason || '');
    this.walletBackfillLastError = '';
    this.walletBackfillLastAt = nowSec();
    this.emitUpdate(true, 'wallet_backfill_running');
    try {
      const head = Number(await this.rpc.getBlockNumber());
      const configuredFrom = Number(this.runtimeConfig.myWalletFromBlock || 0);
      const fallbackFrom = Math.max(0, head - Math.max(1, Number(config.myWalletMaxBackfillBlocks || 500000)));
      const fromBlock = configuredFrom > 0 ? configuredFrom : fallbackFrom;
      logger.info('wallet directed backfill start', {
        token: this.tokenAddress,
        reason,
        fromBlock,
        head,
        wallets: this.myWalletSet.size,
      });
      await this.enqueueScan(fromBlock, head, true, this.myWalletSet);
      this.db.rebuildMyWalletStatsFromHistory(this.tokenAddress, Array.from(this.myWalletSet));
      this.walletBackfillStatus = 'done';
      this.walletBackfillLastAt = nowSec();
      this.walletBackfillLastError = '';
      this.emitUpdate(true, 'wallet_backfill_done');
    } catch (err) {
      this.walletBackfillStatus = 'error';
      this.walletBackfillLastAt = nowSec();
      this.walletBackfillLastError = String(err?.message || err);
      logger.warn('wallet directed backfill failed', { error: String(err?.message || err) });
      this.emitUpdate(true, 'wallet_backfill_error');
    } finally {
      this.walletBackfillRunning = false;
      if (this.walletBackfillQueued) setTimeout(() => this.runWalletBackfill('requeue'), 0);
    }
  }

  async ensureTokenMeta(tokenAddress) {
    const address = safeAddress(tokenAddress);
    if (!address) return { decimals: 18, totalSupply: '0' };
    if (this.tokenMetaCache.has(address)) return this.tokenMetaCache.get(address);

    const cached = this.db.getTokenMeta(address);
    if (cached && Number(cached.decimals) > 0) {
      const data = { decimals: Number(cached.decimals), totalSupply: String(cached.total_supply || '0') };
      this.tokenMetaCache.set(address, data);
      return data;
    }

    const decimals = Number(await this.rpc.readDecimals(address));
    let totalSupply = '0';
    try {
      totalSupply = String(await this.rpc.readTotalSupply(address));
    } catch {
      totalSupply = '0';
    }

    const data = { decimals, totalSupply };
    this.db.upsertTokenMeta(address, decimals, totalSupply, null);
    this.tokenMetaCache.set(address, data);
    return data;
  }

  async getBlockTimestamp(blockNumber) {
    const bn = Number(blockNumber);
    if (this.blockTsCache.has(bn)) return this.blockTsCache.get(bn);

    const cached = this.db.getBlockTimestamp(bn);
    if (cached != null) {
      this.blockTsCache.set(bn, cached);
      return cached;
    }

    const block = await this.rpc.getBlock(BigInt(bn));
    const ts = Number(block.timestamp);
    this.blockTsCache.set(bn, ts);
    this.db.setBlockTimestamp(bn, ts);
    return ts;
  }

  async fillMissingBlockTimestamps(logs) {
    const missing = new Set();
    for (const log of logs) {
      const bn = Number(log.blockNumber);
      if (!this.blockTsCache.has(bn) && this.db.getBlockTimestamp(bn) == null) missing.add(bn);
    }

    for (const bn of missing) {
      const block = await this.rpc.getBlock(BigInt(bn));
      const ts = Number(block.timestamp);
      this.blockTsCache.set(bn, ts);
      this.db.setBlockTimestamp(bn, ts);
    }
  }

  async scanRange(fromBlock, toBlock, isReplay, walletFilterSet = null) {
    if (toBlock < fromBlock) return;

    const ranges = chunkRanges(fromBlock, toBlock, config.logChunkSize);
    for (const [start, end] of ranges) {
      if (this.stopFlag) return;
      logger.info('range pull start', {
        token: this.tokenAddress,
        start,
        end,
        replay: isReplay,
        walletFiltered: Boolean(walletFilterSet && walletFilterSet.size),
      });

      const logs = await this.rpc.getLogs({
        address: this.tokenAddress,
        event: TRANSFER_EVENT,
        fromBlock: BigInt(start),
        toBlock: BigInt(end),
      });
      const virtualAddress = safeAddress(config.virtualTokenAddress);
      const virtualLogs = (isAddress(virtualAddress) && virtualAddress !== this.tokenAddress)
        ? await this.rpc.getLogs({
          address: virtualAddress,
          event: TRANSFER_EVENT,
          fromBlock: BigInt(start),
          toBlock: BigInt(end),
        })
        : [];
      logger.info('range pull fetched', {
        token: this.tokenAddress,
        start,
        end,
        logCount: logs.length,
        virtualLogCount: virtualLogs.length,
      });

      await this.fillMissingBlockTimestamps([...logs, ...virtualLogs]);

      const transferRows = [];
      for (const log of logs) {
        if (!log.transactionHash || log.logIndex == null) continue;
        let decoded;
        try {
          decoded = decodeEventLog({ abi: [TRANSFER_EVENT], topics: log.topics, data: log.data });
        } catch {
          continue;
        }

        const bn = Number(log.blockNumber);
        const ts = this.blockTsCache.get(bn) ?? this.db.getBlockTimestamp(bn) ?? await this.getBlockTimestamp(bn);

        transferRows.push({
          token_address: this.tokenAddress,
          tx_hash: safeAddress(log.transactionHash),
          log_index: Number(log.logIndex),
          block_number: bn,
          timestamp: ts,
          from_address: safeAddress(decoded.args.from),
          to_address: safeAddress(decoded.args.to),
          amount: BigInt(decoded.args.value).toString(),
        });
      }

      const virtualTransferRows = [];
      for (const log of virtualLogs) {
        if (!log.transactionHash || log.logIndex == null) continue;
        let decoded;
        try {
          decoded = decodeEventLog({ abi: [TRANSFER_EVENT], topics: log.topics, data: log.data });
        } catch {
          continue;
        }
        const bn = Number(log.blockNumber);
        const ts = this.blockTsCache.get(bn) ?? this.db.getBlockTimestamp(bn) ?? await this.getBlockTimestamp(bn);
        virtualTransferRows.push({
          token_address: virtualAddress,
          tx_hash: safeAddress(log.transactionHash),
          log_index: Number(log.logIndex),
          block_number: bn,
          timestamp: ts,
          from_address: safeAddress(decoded.args.from),
          to_address: safeAddress(decoded.args.to),
          amount: BigInt(decoded.args.value).toString(),
        });
      }

      const filteredRows = walletFilterSet && walletFilterSet.size
        ? transferRows.filter((r) => walletFilterSet.has(r.from_address) || walletFilterSet.has(r.to_address))
        : transferRows;
      const rangeTxHashes = Array.from(new Set(filteredRows.map((r) => r.tx_hash).filter(Boolean)));

      const { inserted, newTxHashes, insertedRows } = this.db.insertTransfers(filteredRows);
      this.db.applyMyWalletTransferRows(this.tokenAddress, insertedRows, this.myWalletSet);
      this.db.insertSpecialFlows(this.extractSpecialFromTransfers(insertedRows));
      const protocolRows = [
        ...this.buildProtocolFlowsFromTransfers(transferRows, { asset: 'token' }),
        ...this.buildProtocolFlowsFromTransfers(virtualTransferRows, { asset: 'virtual' }),
      ];
      if (protocolRows.length) this.db.insertProtocolFlows(protocolRows);

      let missingFactTxHashes = [];
      if (config.enableTxFacts && rangeTxHashes.length) {
        missingFactTxHashes = this.db.getMissingFactTxHashes(this.tokenAddress, rangeTxHashes);
        if (missingFactTxHashes.length) {
          await this.processTxFacts(missingFactTxHashes);
        }
      }

      const prev = Number(this.db.getMeta(this.tokenAddress)?.last_processed_block || 0);
      this.db.setMeta(this.tokenAddress, { last_processed_block: isReplay ? Math.max(prev, end) : end, running: 1 });
      const endTs = this.blockTsCache.get(end) ?? this.db.getBlockTimestamp(end) ?? await this.getBlockTimestamp(end);
      this.lastChainTimeSec = Math.max(this.lastChainTimeSec, Number(endTs || 0));
      this.lastIngest = {
        start,
        end,
        log_count: logs.length,
        tx_count: rangeTxHashes.length,
        no_transfers_in_window: logs.length === 0,
        updated_at: nowSec(),
        wallet_filtered: Boolean(walletFilterSet && walletFilterSet.size),
      };
      this.emitUpdate(false, 'range_processed');

      logger.info('range done', {
        token: this.tokenAddress,
        start,
        end,
        logs: logs.length,
        inserted,
        newTxCount: newTxHashes.length,
        missingFactTxCount: missingFactTxHashes.length,
        rangeTxCount: rangeTxHashes.length,
          replay: isReplay,
          walletFiltered: Boolean(walletFilterSet && walletFilterSet.size),
        });
      if (config.debugTrackingLogs) {
        logger.info('range debug', {
          token: this.tokenAddress,
          start,
          end,
          logs: logs.length,
          txHashesInRange: rangeTxHashes.length,
          txFactsQueued: missingFactTxHashes.length,
        });
      }
    }
    await this.refreshPairSpotPrice(false);
    const tokenMeta = await this.ensureTokenMeta(this.tokenAddress);
    await this.refreshProtocolBalances(Number(tokenMeta?.decimals || 18));
  }

  async refreshPairSpotPrice(force = false) {
    try {
      const latestBlock = Number(this.lastKnownHead || await this.rpc.getBlockNumber());
      const tokenStartBlock = Number(this.runtimeConfig.tokenStartBlock || config.tokenStartBlock || 0);
      const configuredDexPairAddress = safeAddress(this.runtimeConfig.spotPairAddress || config.spotPairAddress);
      const result = await this.priceService.refresh({
        tokenAddress: this.tokenAddress,
        tokenStartBlock,
        latestBlock,
        configuredDexPairAddress,
        force,
      });
      this.unifiedPrice = result?.view || null;
      this.pairSpotFetchedAtMs = Date.now();
      if (this.unifiedPrice && Number(this.unifiedPrice.spot_price_virtual || 0) > 0) {
        if (isAddress(this.unifiedPrice.launch_pool_address || '')) {
          this._setDynamicSpecialAddress(this.unifiedPrice.launch_pool_address, {
            label: 'Virtuals LaunchPool',
            type: 'launch_pool',
            category: 'launch_pool',
          });
        }
        this.pairSpot = {
          source: String(this.unifiedPrice.source || 'price_service'),
          pair_address: this.unifiedPrice.pair_address || null,
          quote_token: this.unifiedPrice.quote_token || null,
          pair_auto_discovered: Boolean(this.unifiedPrice.pair_auto_discovered),
          spot_price: Number(this.unifiedPrice.spot_price_virtual || 0),
          spot_price_str: String(this.unifiedPrice.spot_price_virtual_str || this.unifiedPrice.spot_price_virtual || '0'),
          spot_mcap: 0,
          updated_at: nowSec(),
          stage: String(this.unifiedPrice.stage || ''),
          stage_reason: String(this.unifiedPrice.stage_reason || ''),
          launch_pool_address: this.unifiedPrice.launch_pool_address || null,
        };
      }
      if (this.unifiedPrice?.usd_per_virtual) {
        this.virtualUsd = {
          source: this.unifiedPrice.usd_source || 'price_service',
          usd_per_virtual: Number(this.unifiedPrice.usd_per_virtual || 0),
          usd_per_virtual_str: String(this.unifiedPrice.usd_per_virtual_str || this.unifiedPrice.usd_per_virtual || ''),
          updated_at: nowSec(),
        };
      }
      if (result?.changed) this.emitUpdate(true, 'price_updated');
    } catch (err) {
      logger.warn('price service refresh failed', { error: String(err?.message || err) });
    }
  }

  async refreshVirtualUsdRate(force = false) {
    const pairAddress = safeAddress(config.virtualUsdPairAddress);
    if (!isAddress(pairAddress)) {
      if (Number(config.virtualUsdFallback || 0) > 0) {
        this.virtualUsd = {
          source: 'env_fallback',
          usd_per_virtual: Number(config.virtualUsdFallback),
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
      if (Array.isArray(config.usdStableTokenAddresses) && config.usdStableTokenAddresses.length) {
        const stableSet = new Set(config.usdStableTokenAddresses.map((v) => safeAddress(v)).filter((v) => isAddress(v)));
        if (stableSet.size && !stableSet.has(quoteToken)) return;
      }
      const quoteMeta = await this.ensureTokenMeta(quoteToken);
      const virtualMeta = await this.ensureTokenMeta(virtual);

      const reserve0 = BigInt(reserves?.reserve0 ?? reserves?.[0] ?? 0n);
      const reserve1 = BigInt(reserves?.reserve1 ?? reserves?.[1] ?? 0n);
      const virtualReserve = token0 === virtual ? reserve0 : reserve1;
      const quoteReserve = token0 === virtual ? reserve1 : reserve0;
      if (virtualReserve <= 0n || quoteReserve <= 0n) return;

      const usdPerVirtualStr = ratioToDecimalString(
        quoteReserve,
        Number(quoteMeta.decimals || 18),
        virtualReserve,
        Number(virtualMeta.decimals || 18),
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

  async tryAutoDetectSpotPairAddress() {
    const recentTransfers = this.db.getRecentTransfers(this.tokenAddress, 1200);
    if (!recentTransfers.length) return '';

    const flowMap = new Map();
    const addFlow = (addr, inAmt, outAmt) => {
      const a = safeAddress(addr);
      if (!isAddress(a) || a === zeroAddress) return;
      const prev = flowMap.get(a) || { in: 0n, out: 0n, count: 0 };
      prev.in += BigInt(inAmt);
      prev.out += BigInt(outAmt);
      prev.count += 1;
      flowMap.set(a, prev);
    };

    for (const t of recentTransfers) {
      const amt = BigInt(t.amount || '0');
      if (amt <= 0n) continue;
      addFlow(t.to_address, amt, 0n);
      addFlow(t.from_address, 0n, amt);
    }

    const candidates = Array.from(flowMap.entries())
      .filter(([, v]) => v.in > 0n && v.out > 0n && v.count >= 3)
      .map(([addr, v]) => ({
        addr,
        score: v.in + v.out,
        count: v.count,
      }))
      .sort((a, b) => (a.score === b.score ? b.count - a.count : (b.score > a.score ? 1 : -1)))
      .slice(0, 30);

    const virtual = safeAddress(config.virtualTokenAddress);
    let bestAddr = '';
    let bestScore = 0n;
    for (const c of candidates) {
      try {
        const [token0Raw, token1Raw, reserves] = await Promise.all([
          this.rpc.readPairToken0(c.addr),
          this.rpc.readPairToken1(c.addr),
          this.rpc.readPairReserves(c.addr),
        ]);
        const token0 = safeAddress(token0Raw);
        const token1 = safeAddress(token1Raw);
        if (token0 !== this.tokenAddress && token1 !== this.tokenAddress) continue;
        const quoteToken = token0 === this.tokenAddress ? token1 : token0;
        if (config.requireVirtualQuoteForSpot && isAddress(virtual) && quoteToken !== virtual) continue;
        const reserve0 = BigInt(reserves?.reserve0 ?? reserves?.[0] ?? 0n);
        const reserve1 = BigInt(reserves?.reserve1 ?? reserves?.[1] ?? 0n);
        const tokenReserve = token0 === this.tokenAddress ? reserve0 : reserve1;
        const quoteReserve = token0 === this.tokenAddress ? reserve1 : reserve0;
        if (tokenReserve <= 0n || quoteReserve <= 0n) continue;
        const score = tokenReserve * quoteReserve;
        if (score > bestScore) {
          bestScore = score;
          bestAddr = c.addr;
        }
      } catch {
        // not a v2 pair or call failed
      }
    }
    return bestAddr;
  }

  extractSpecialFromTransfers(rows) {
    const events = [];
    for (const row of rows || []) {
      const from = this.specialMap.get(row.from_address);
      const to = this.specialMap.get(row.to_address);
      if (from) {
        events.push({
          token_address: row.token_address,
          tx_hash: row.tx_hash,
          block_number: row.block_number,
          timestamp: row.timestamp,
          special_address: row.from_address,
          label: from.label,
          category: from.category,
          asset: 'TOKEN',
          direction: 'out',
          amount: row.amount,
        });
      }
      if (to) {
        events.push({
          token_address: row.token_address,
          tx_hash: row.tx_hash,
          block_number: row.block_number,
          timestamp: row.timestamp,
          special_address: row.to_address,
          label: to.label,
          category: to.category,
          asset: 'TOKEN',
          direction: 'in',
          amount: row.amount,
        });
      }
    }
    return events;
  }

  buildProtocolFlowsFromTransfers(rows, { asset } = {}) {
    const flows = [];
    const kind = String(asset || 'token').toLowerCase() === 'virtual' ? 'virtual' : 'token';
    for (const row of rows || []) {
      const from = this.specialMap.get(row.from_address);
      const to = this.specialMap.get(row.to_address);
      if (!from && !to) continue;
      const amount = String(row.amount || '0');
      if (from) {
        flows.push({
          token: this.tokenAddress,
          block_number: Number(row.block_number || 0),
          timestamp: Number(row.timestamp || 0),
          address: String(row.from_address || '').toLowerCase(),
          type: String(from.type || from.category || 'unknown'),
          direction: 'out',
          amount_token: kind === 'token' ? amount : '0',
          amount_virtual: kind === 'virtual' ? amount : '0',
          tx_hash: String(row.tx_hash || '').toLowerCase(),
        });
      }
      if (to) {
        flows.push({
          token: this.tokenAddress,
          block_number: Number(row.block_number || 0),
          timestamp: Number(row.timestamp || 0),
          address: String(row.to_address || '').toLowerCase(),
          type: String(to.type || to.category || 'unknown'),
          direction: 'in',
          amount_token: kind === 'token' ? amount : '0',
          amount_virtual: kind === 'virtual' ? amount : '0',
          tx_hash: String(row.tx_hash || '').toLowerCase(),
        });
      }
    }
    return flows;
  }

  async processTxFacts(txHashes) {
    const concurrency = 6;
    for (let i = 0; i < txHashes.length; i += concurrency) {
      if (this.stopFlag) return;
      const chunk = txHashes.slice(i, i + concurrency);
      const settled = await Promise.allSettled(chunk.map(async (txHash) => {
        const receipt = await this.rpc.getTransactionReceipt(txHash);
        return this.classifyTxFromReceipt(receipt);
      }));
      for (const one of settled) {
        if (one.status !== 'fulfilled') continue;
        const parsed = one.value;
        if (parsed.fact) {
          this.db.applyTxFact(parsed.fact);
          this.db.applyMyWalletFact(parsed.fact, this.myWalletSet);
        }
        if (parsed.specialEvents.length) this.db.insertSpecialFlows(parsed.specialEvents);
      }
    }
  }

  async classifyTxFromReceipt(receipt) {
    const counterpartySet = new Set(this.counterpartyList.map((v) => safeAddress(v)));
    const token = this.tokenAddress;

    const tokenDelta = new Map();
    const virtualDelta = new Map();
    const specialEvents = [];
    let sawToken = false;
    let sawVirtual = false;

    const pushSpecial = ({ contract, from, to, amount, blockNumber, timestamp, txHash }) => {
      const fromSpecial = this.specialMap.get(from);
      const toSpecial = this.specialMap.get(to);
      const asset = contract === token ? 'TOKEN' : contract === safeAddress(config.virtualTokenAddress) ? 'VIRTUAL' : 'OTHER';
      if (fromSpecial) {
        specialEvents.push({
          token_address: token,
          tx_hash: txHash,
          block_number: blockNumber,
          timestamp,
          special_address: from,
          label: fromSpecial.label,
          category: fromSpecial.category,
          asset,
          direction: 'out',
          amount: amount.toString(),
        });
      }
      if (toSpecial) {
        specialEvents.push({
          token_address: token,
          tx_hash: txHash,
          block_number: blockNumber,
          timestamp,
          special_address: to,
          label: toSpecial.label,
          category: toSpecial.category,
          asset,
          direction: 'in',
          amount: amount.toString(),
        });
      }
    };

    const blockNumber = Number(receipt.blockNumber);
    const timestamp = await this.getBlockTimestamp(blockNumber);

    for (const log of receipt.logs || []) {
      if (!log?.topics?.length || !log.address) continue;
      let decoded;
      try {
        decoded = decodeEventLog({ abi: [TRANSFER_EVENT], topics: log.topics, data: log.data, strict: false });
      } catch {
        continue;
      }

      const contract = safeAddress(log.address);
      const from = safeAddress(decoded.args.from);
      const to = safeAddress(decoded.args.to);
      const amount = BigInt(decoded.args.value || 0n);
      if (amount <= 0n) continue;

      pushSpecial({ contract, from, to, amount, blockNumber, timestamp, txHash: safeAddress(receipt.transactionHash) });

      if (contract === token) {
        sawToken = true;
        tokenDelta.set(from, (tokenDelta.get(from) || 0n) - amount);
        tokenDelta.set(to, (tokenDelta.get(to) || 0n) + amount);
      }
      if (counterpartySet.has(contract)) {
        sawVirtual = true;
        virtualDelta.set(from, (virtualDelta.get(from) || 0n) - amount);
        virtualDelta.set(to, (virtualDelta.get(to) || 0n) + amount);
      }
    }

    if (!sawToken) return { fact: null, specialEvents };

    const allWallets = new Set([...tokenDelta.keys(), ...virtualDelta.keys()]);
    const addressFacts = [];
    let totalSpent = 0n;
    let totalReceived = 0n;
    let totalSoldToken = 0n;
    let totalSoldVirtual = 0n;

    for (const addr of allWallets) {
      if (addr === zeroAddress) continue;
      const td = tokenDelta.get(addr) || 0n;
      const vd = virtualDelta.get(addr) || 0n;

      if (td > 0n && vd < 0n) {
        addressFacts.push({
          wallet_address: addr,
          role: 'buyer',
          spent_virtual_amount: (-vd).toString(),
          received_token_amount: td.toString(),
          sold_virtual_amount: '0',
          sold_token_amount: '0',
        });
        totalSpent += -vd;
        totalReceived += td;
      }

      if (td < 0n && vd > 0n) {
        addressFacts.push({
          wallet_address: addr,
          role: 'seller',
          spent_virtual_amount: '0',
          received_token_amount: '0',
          sold_virtual_amount: vd.toString(),
          sold_token_amount: (-td).toString(),
        });
        totalSoldToken += -td;
        totalSoldVirtual += vd;
      }
    }

    let classification = 'transfer_or_unknown';
    let kind = 'unknown';
    if (sawVirtual && totalSpent > 0n && totalReceived > 0n) {
      classification = 'suspected_buy';
      kind = 'buy';
    } else if (sawVirtual && totalSoldToken > 0n && totalSoldVirtual > 0n) {
      classification = 'suspected_sell';
      kind = 'sell';
    } else if (sawToken) {
      kind = 'transfer';
    }

    let actorAddress = null;
    let actorReceivedToken = 0n;
    for (const [addr, td] of tokenDelta.entries()) {
      if (addr === zeroAddress) continue;
      if (td > actorReceivedToken) {
        actorReceivedToken = td;
        actorAddress = addr;
      }
    }

    const fact = {
      token_address: token,
      tx_hash: safeAddress(receipt.transactionHash),
      block_number: blockNumber,
      timestamp,
      actor_address: actorAddress,
      kind,
      classification,
      spent_virtual_amount: totalSpent.toString(),
      received_token_amount: totalReceived.toString(),
      received_virtual_amount: totalSoldVirtual.toString(),
      sold_token_amount: totalSoldToken.toString(),
      addressFacts,
    };

    return { fact, specialEvents };
  }

  computeHoldersAndLeaderboard(tokenDecimals, virtualDecimals, totalSupplyRaw) {
    const transfers = this.db.getAllTransfers(this.tokenAddress);
    const facts = this.db.getAllFactAddresses(this.tokenAddress);
    const map = new Map();

    const getRow = (addr) => {
      const a = safeAddress(addr);
      if (!map.has(a)) {
        map.set(a, {
          address: a,
          balance: 0n,
          cumulativeIn: 0n,
          cumulativeOut: 0n,
          txCount: 0,
          lastActive: 0,
          spentVirtualSum: 0n,
          receivedTokenSum: 0n,
          delta5mToken: 0n,
        });
      }
      return map.get(a);
    };

    const ts5m = nowSec() - 5 * 60;

    for (const t of transfers) {
      const amt = BigInt(t.amount || '0');
      const ts = Number(t.timestamp || 0);
      const from = getRow(t.from_address);
      const to = getRow(t.to_address);

      from.balance -= amt;
      from.cumulativeOut += amt;
      from.txCount += 1;
      from.lastActive = Math.max(from.lastActive, ts);
      if (ts >= ts5m) from.delta5mToken -= amt;

      to.balance += amt;
      to.cumulativeIn += amt;
      to.txCount += 1;
      to.lastActive = Math.max(to.lastActive, ts);
      if (ts >= ts5m) to.delta5mToken += amt;
    }

    for (const f of facts) {
      const row = getRow(f.wallet_address);
      row.spentVirtualSum += BigInt(f.spent_virtual_amount || '0');
      row.receivedTokenSum += BigInt(f.received_token_amount || '0');
      row.lastActive = Math.max(row.lastActive, Number(f.timestamp || 0));
    }

    const totalSupply = toFloat(totalSupplyRaw || '0', tokenDecimals);
    const sellTax = Number(this.runtimeConfig.sellTaxPct || 1) / 100;
    const holders = Array.from(map.values()).filter((r) => r.address && r.address !== zeroAddress);
    const totalPositiveBalance = holders.reduce((acc, x) => acc + (x.balance > 0n ? x.balance : 0n), 0n);

    const enriched = holders.map((h) => {
      const balance = toFloat(h.balance, tokenDecimals);
      const cumIn = toFloat(h.cumulativeIn, tokenDecimals);
      const cumOut = toFloat(h.cumulativeOut, tokenDecimals);
      const spentVirtual = toFloat(h.spentVirtualSum, virtualDecimals);
      const receivedToken = toFloat(h.receivedTokenSum, tokenDecimals);
      const costPerToken = receivedToken > 0 ? spentVirtual / receivedToken : 0;
      const breakevenPrice = costPerToken > 0 ? costPerToken / (1 - sellTax) : 0;
      const breakevenMcap = breakevenPrice > 0 ? breakevenPrice * totalSupply : 0;
      const sharePct = totalPositiveBalance > 0n && h.balance > 0n
        ? (Number((h.balance * 1000000n) / totalPositiveBalance) / 10000)
        : 0;

      return {
        address: h.address,
        balance,
        cumulative_in: cumIn,
        cumulative_out: cumOut,
        tx_count: h.txCount,
        last_active_time: h.lastActive,
        spent_virtual_sum: spentVirtual,
        received_token_sum: receivedToken,
        cost_per_token: costPerToken,
        breakeven_mcap: breakevenMcap,
        share_pct: sharePct,
        delta_5m_token: toFloat(h.delta5mToken, tokenDecimals),
      };
    });

    const whales = enriched
      .slice()
      .sort((a, b) => (b.balance === a.balance ? b.last_active_time - a.last_active_time : b.balance - a.balance))
      .slice(0, 100);

    const heat = enriched
      .slice()
      .sort((a, b) => b.delta_5m_token - a.delta_5m_token)
      .slice(0, 100)
      .map((r) => ({ address: r.address, netflow: r.delta_5m_token, netflow_token: r.delta_5m_token, netflow_virtual: 0, trades_5m: r.tx_count }));

    const active = enriched
      .slice()
      .sort((a, b) => (b.tx_count === a.tx_count ? b.last_active_time - a.last_active_time : b.tx_count - a.tx_count))
      .slice(0, 100)
      .map((r) => ({
        address: r.address,
        trades_5m: r.tx_count,
        spent_virtual_5m: r.spent_virtual_sum,
        received_token_5m: r.received_token_sum,
        last_active_time: r.last_active_time,
      }));

    return { holders: enriched, leaderboard: { whales, heat, active, source: 'holders' } };
  }

  computeSpecialStats(tokenDecimals, virtualDecimals) {
    const now = nowSec();
    const rows = this.db.getSpecialFlowsSince(this.tokenAddress, now - 3600);
    const map = new Map();

    const getRow = (flow) => {
      if (!map.has(flow.special_address)) {
        map.set(flow.special_address, {
          address: flow.special_address,
          label: flow.label,
          category: flow.category,
          net_virtual_5m: 0,
          net_virtual_1h: 0,
          net_virtual_cum: 0,
          net_token_5m: 0,
          net_token_cum: 0,
          last_active_time: 0,
        });
      }
      return map.get(flow.special_address);
    };

    for (const r of rows) {
      const row = getRow(r);
      const sign = r.direction === 'in' ? 1 : -1;
      const ts = Number(r.timestamp || 0);
      const is5m = ts >= now - 300;
      const amount = r.asset === 'VIRTUAL' ? toFloat(r.amount, virtualDecimals) : toFloat(r.amount, tokenDecimals);

      if (r.asset === 'VIRTUAL') {
        row.net_virtual_1h += sign * amount;
        row.net_virtual_cum += sign * amount;
        if (is5m) row.net_virtual_5m += sign * amount;
      }
      if (r.asset === 'TOKEN') {
        row.net_token_cum += sign * amount;
        if (is5m) row.net_token_5m += sign * amount;
      }

      row.last_active_time = Math.max(row.last_active_time, ts);
    }

    return Array.from(map.values()).sort((a, b) => b.net_virtual_5m - a.net_virtual_5m);
  }

  async refreshProtocolBalances(tokenDecimals) {
    try {
      const sellWall = this._findSpecialAddressByType('protocol_lock');
      const launchPool = this._findSpecialAddressByType('launch_pool');
      let sellWallRaw = 0n;
      let launchPoolRaw = 0n;
      if (isAddress(sellWall)) {
        try {
          sellWallRaw = BigInt(await this.rpc.readContract({
            address: this.tokenAddress,
            abi: ERC20_ABI,
            functionName: 'balanceOf',
            args: [sellWall],
          }));
        } catch {}
      }
      if (isAddress(launchPool)) {
        try {
          launchPoolRaw = BigInt(await this.rpc.readContract({
            address: this.tokenAddress,
            abi: ERC20_ABI,
            functionName: 'balanceOf',
            args: [launchPool],
          }));
        } catch {}
      }
      this.protocolBalances = {
        sell_wall_token_balance: toFloat(sellWallRaw.toString(), tokenDecimals),
        launch_pool_token_balance: toFloat(launchPoolRaw.toString(), tokenDecimals),
        updated_at: nowSec(),
      };
    } catch {
      // ignore
    }
  }

  computeProtocolActivity(tokenDecimals, virtualDecimals) {
    const now = nowSec();
    const rows = this.db.getProtocolFlowsSince(this.tokenAddress, now - 3600);
    let inflow1m = 0;
    let outflow1m = 0;
    let inflow5m = 0;
    let outflow5m = 0;
    let interaction1m = 0;
    const executorTx5m = new Set();
    const byAddress = new Map();

    for (const r of rows) {
      const ts = Number(r.timestamp || 0);
      const in1m = ts >= now - 60;
      const in5m = ts >= now - 300;
      const directionIn = String(r.direction || '') === 'in';
      const amountVirtual = toFloat(r.amount_virtual || '0', virtualDecimals);
      const amountToken = toFloat(r.amount_token || '0', tokenDecimals);
      const addr = safeAddress(r.address);
      if (!byAddress.has(addr)) {
        byAddress.set(addr, {
          address: addr,
          type: String(r.type || 'unknown'),
          inflow_virtual_1m: 0,
          outflow_virtual_1m: 0,
          net_virtual_1m: 0,
          inflow_virtual_5m: 0,
          outflow_virtual_5m: 0,
          net_virtual_5m: 0,
          net_token_5m: 0,
          interaction_count_5m: 0,
        });
      }
      const row = byAddress.get(addr);
      if (in1m) {
        interaction1m += 1;
        if (directionIn) inflow1m += amountVirtual;
        else outflow1m += amountVirtual;
        if (directionIn) row.inflow_virtual_1m += amountVirtual;
        else row.outflow_virtual_1m += amountVirtual;
      }
      if (in5m) {
        if (directionIn) inflow5m += amountVirtual;
        else outflow5m += amountVirtual;
        if (directionIn) row.inflow_virtual_5m += amountVirtual;
        else row.outflow_virtual_5m += amountVirtual;
        row.interaction_count_5m += 1;
        row.net_token_5m += directionIn ? amountToken : -amountToken;
        if (String(r.type || '') === 'protocol_executor') executorTx5m.add(String(r.tx_hash || ''));
      }
      row.net_virtual_1m = row.inflow_virtual_1m - row.outflow_virtual_1m;
      row.net_virtual_5m = row.inflow_virtual_5m - row.outflow_virtual_5m;
    }

    const net1m = inflow1m - outflow1m;
    const net5m = inflow5m - outflow5m;
    const alert = inflow1m >= Number(config.protocolAlertInflowVirtual || 5000);
    const leaderboard = Array.from(byAddress.values())
      .sort((a, b) => b.net_virtual_1m - a.net_virtual_1m)
      .slice(0, 20);

    return {
      protocol_inflow_1m: inflow1m,
      protocol_outflow_1m: outflow1m,
      net_flow_1m: net1m,
      protocol_inflow_5m: inflow5m,
      protocol_outflow_5m: outflow5m,
      net_flow_5m: net5m,
      interaction_count_1m: interaction1m,
      executor_interactions_5m: executorTx5m.size,
      sell_wall_token_balance: Number(this.protocolBalances.sell_wall_token_balance || 0),
      launch_pool_token_balance: Number(this.protocolBalances.launch_pool_token_balance || 0),
      alert,
      alert_threshold_virtual: Number(config.protocolAlertInflowVirtual || 5000),
      by_address: leaderboard,
      updated_at: now,
    };
  }

  computeMyWalletStats(tokenMeta, virtualMeta) {
    const wallets = Array.from(this.myWalletSet);
    const tokenDecimals = tokenMeta.decimals;
    const virtualDecimals = virtualMeta.decimals;
    const sellTax = Number(this.runtimeConfig.sellTaxPct || 1) / 100;
    const totalSupply = toFloat(tokenMeta.totalSupply || '0', tokenDecimals);
    const rows = this.db.getMyWalletStats(this.tokenAddress, wallets);

    const mapped = rows.map((r) => {
      const spentVirtual = toFloat(r.spent_virtual_sum || '0', virtualDecimals);
      const receivedToken = toFloat(r.received_token_sum || '0', tokenDecimals);
      const soldToken = toFloat(r.sold_token_sum || '0', tokenDecimals);
      const receivedVirtual = toFloat(r.received_virtual_sum || '0', virtualDecimals);
      const transferIn = toFloat(r.transfer_in_sum || '0', tokenDecimals);
      const transferOut = toFloat(r.transfer_out_sum || '0', tokenDecimals);
      const balance = toFloat((BigInt(r.total_in_sum || '0') - BigInt(r.total_out_sum || '0')).toString(), tokenDecimals);
      const costPerToken = receivedToken > 0 ? spentVirtual / receivedToken : 0;
      const effectiveCostPerToken = costPerToken > 0 ? costPerToken / (1 - sellTax) : 0;
      const breakevenMcap = effectiveCostPerToken > 0 ? effectiveCostPerToken * totalSupply : 0;
      return {
        address: r.wallet_address,
        balance,
        spent_virtual_sum: spentVirtual,
        received_token_sum: receivedToken,
        sold_token_sum: soldToken,
        received_virtual_sum: receivedVirtual,
        transfer_in_sum: transferIn,
        transfer_out_sum: transferOut,
        cost_per_token: costPerToken,
        effective_cost_per_token: effectiveCostPerToken,
        breakeven_mcap: breakevenMcap,
        tx_count: Number(r.tx_count || 0),
        last_active_time: Number(r.last_active_time || 0),
        raw: {
          spent_virtual_sum: String(r.spent_virtual_sum || '0'),
          received_token_sum: String(r.received_token_sum || '0'),
          sold_token_sum: String(r.sold_token_sum || '0'),
          received_virtual_sum: String(r.received_virtual_sum || '0'),
          transfer_in_sum: String(r.transfer_in_sum || '0'),
          transfer_out_sum: String(r.transfer_out_sum || '0'),
          total_in_sum: String(r.total_in_sum || '0'),
          total_out_sum: String(r.total_out_sum || '0'),
        },
      };
    });

    const sum = {
      spent_virtual_sum: 0n,
      received_token_sum: 0n,
      sold_token_sum: 0n,
      received_virtual_sum: 0n,
      transfer_in_sum: 0n,
      transfer_out_sum: 0n,
      total_in_sum: 0n,
      total_out_sum: 0n,
    };
    for (const r of rows) {
      sum.spent_virtual_sum += BigInt(r.spent_virtual_sum || '0');
      sum.received_token_sum += BigInt(r.received_token_sum || '0');
      sum.sold_token_sum += BigInt(r.sold_token_sum || '0');
      sum.received_virtual_sum += BigInt(r.received_virtual_sum || '0');
      sum.transfer_in_sum += BigInt(r.transfer_in_sum || '0');
      sum.transfer_out_sum += BigInt(r.transfer_out_sum || '0');
      sum.total_in_sum += BigInt(r.total_in_sum || '0');
      sum.total_out_sum += BigInt(r.total_out_sum || '0');
    }

    const spentVirtualTotal = toFloat(sum.spent_virtual_sum, virtualDecimals);
    const receivedTokenTotal = toFloat(sum.received_token_sum, tokenDecimals);
    const soldTokenTotal = toFloat(sum.sold_token_sum, tokenDecimals);
    const receivedVirtualTotal = toFloat(sum.received_virtual_sum, virtualDecimals);
    const transferInTotal = toFloat(sum.transfer_in_sum, tokenDecimals);
    const transferOutTotal = toFloat(sum.transfer_out_sum, tokenDecimals);
    const balanceTotal = toFloat((sum.total_in_sum - sum.total_out_sum).toString(), tokenDecimals);
    const costPerTokenTotal = receivedTokenTotal > 0 ? spentVirtualTotal / receivedTokenTotal : 0;
    const effectiveCostTotal = costPerTokenTotal > 0 ? costPerTokenTotal / (1 - sellTax) : 0;
    const breakevenMcapTotal = effectiveCostTotal > 0 ? effectiveCostTotal * totalSupply : 0;

    return {
      addresses: wallets,
      rows: mapped,
      status: {
        state: this.walletBackfillStatus,
        last_at: this.walletBackfillLastAt,
        last_reason: this.walletBackfillLastReason,
        last_error: this.walletBackfillLastError || null,
        queued: this.walletBackfillQueued,
        running: this.walletBackfillRunning,
      },
      total: {
        balance: balanceTotal,
        spent_virtual_sum: spentVirtualTotal,
        received_token_sum: receivedTokenTotal,
        sold_token_sum: soldTokenTotal,
        received_virtual_sum: receivedVirtualTotal,
        transfer_in_sum: transferInTotal,
        transfer_out_sum: transferOutTotal,
        cost_per_token: costPerTokenTotal,
        effective_cost_per_token: effectiveCostTotal,
        breakeven_mcap: breakevenMcapTotal,
        raw: Object.fromEntries(Object.entries(sum).map(([k, v]) => [k, v.toString()])),
      },
    };
  }

  buildCurves(tokenMeta, myWalletSummary, spotMcapOverride = 0) {
    const tokenDecimals = tokenMeta.decimals;
    const virtualMeta = this.tokenMetaCache.get(safeAddress(config.virtualTokenAddress)) || { decimals: 18 };
    const virtualDecimals = virtualMeta.decimals;
    const totalSupply = toFloat(tokenMeta.totalSupply || '0', tokenDecimals);

    const nowMinute = minuteFloor(nowSec());
    const windowMinutes = Math.max(10, Number(this.runtimeConfig.curveWindowMinutes || 30));
    const start = nowMinute - (windowMinutes - 1) * 60;

    const buyFacts = this.db.getRecentBuyFacts(this.tokenAddress, start);
    const perMinute = new Map();
    for (const f of buyFacts) {
      const m = minuteFloor(Number(f.timestamp || 0));
      const cur = perMinute.get(m) || { spent: 0n, recv: 0n };
      cur.spent += BigInt(f.spent_virtual_amount || '0');
      cur.recv += BigInt(f.received_token_amount || '0');
      perMinute.set(m, cur);
    }

    let lastMcap = 0;
    const emvSeries = [];
    const rmvSeries = [];

    const rmv = myWalletSummary?.total?.breakeven_mcap || 0;

    for (let m = start; m <= nowMinute; m += 60) {
      const item = perMinute.get(m);
      if (item && item.recv > 0n) {
        const price = toFloat(item.spent, virtualDecimals) / toFloat(item.recv, tokenDecimals);
        if (Number.isFinite(price) && price > 0) {
          lastMcap = price * totalSupply;
        }
      }

      const taxPct = calcBuyTaxPct(m, Number(this.runtimeConfig.launchStartTime || 0));
      const emv = lastMcap > 0 ? lastMcap / (1 - taxPct / 100) : 0;
      emvSeries.push({ minute_ts: m, value: emv, tax_pct: taxPct, mcap: lastMcap });
      rmvSeries.push({ minute_ts: m, value: rmv });
    }

    if (spotMcapOverride > 0 && emvSeries.length) {
      const last = emvSeries[emvSeries.length - 1];
      const taxPct = Number(last.tax_pct || 1);
      last.mcap = spotMcapOverride;
      last.value = spotMcapOverride / Math.max(1e-9, 1 - taxPct / 100);
    }

    return { emvSeries, rmvSeries };
  }

  computeSpotAndRecentTrades(tokenMeta, virtualMeta) {
    const tokenDecimals = Number(tokenMeta?.decimals || 18);
    const virtualDecimals = Number(virtualMeta?.decimals || 18);
    const totalSupply = toFloat(tokenMeta?.totalSupply || '0', tokenDecimals);
    const recentFacts = this.db.getRecentFacts(this.tokenAddress, 200);

    const buys = recentFacts.filter((f) => {
      if (String(f.classification || '') !== 'suspected_buy') return false;
      const recv = BigInt(f.received_token_amount || '0');
      const spent = BigInt(f.spent_virtual_amount || '0');
      return recv > 0n && spent > 0n;
    });

    const now = nowSec();
    const windowSec = 300;
    const windowBuys = buys.filter((f) => Number(f.timestamp || 0) >= now - windowSec);
    const calcOn = windowBuys.length ? windowBuys : buys.slice(0, 30);

    let sumSpentFloat = 0;
    let sumGrossTokenFloat = 0;
    for (const f of calcOn) {
      const spentFloat = toFloat(f.spent_virtual_amount || '0', virtualDecimals);
      const receivedFloat = toFloat(f.received_token_amount || '0', tokenDecimals);
      if (!Number.isFinite(spentFloat) || !Number.isFinite(receivedFloat) || spentFloat <= 0 || receivedFloat <= 0) continue;
      const taxPct = calcBuyTaxPct(Number(f.timestamp || now), Number(this.runtimeConfig.launchStartTime || 0));
      const grossToken = receivedFloat / Math.max(1e-9, 1 - taxPct / 100);
      if (!Number.isFinite(grossToken) || grossToken <= 0) continue;
      sumSpentFloat += spentFloat;
      sumGrossTokenFloat += grossToken;
    }

    const weightedSpotPrice = sumGrossTokenFloat > 0
      ? (sumSpentFloat / sumGrossTokenFloat)
      : 0;

    const latestBuy = buys[0] || null;
    const latestSpotPrice = latestBuy
      ? (() => {
        const spentFloat = toFloat(latestBuy.spent_virtual_amount || '0', virtualDecimals);
        const receivedFloat = toFloat(latestBuy.received_token_amount || '0', tokenDecimals);
        const taxPct = calcBuyTaxPct(Number(latestBuy.timestamp || now), Number(this.runtimeConfig.launchStartTime || 0));
        const grossToken = receivedFloat / Math.max(1e-9, 1 - taxPct / 100);
        return spentFloat > 0 && grossToken > 0 ? spentFloat / grossToken : 0;
      })()
      : 0;

    let spotPriceVirtual = weightedSpotPrice > 0 ? weightedSpotPrice : latestSpotPrice;
    let spotPriceVirtualStr = spotPriceVirtual > 0 ? String(spotPriceVirtual) : '0';
    let spotMcapVirtual = spotPriceVirtual > 0 ? spotPriceVirtual * totalSupply : 0;
    let priceSource = 'inferred_facts';
    let priceStage = null;
    let launchPoolAddress = null;
    let stageReason = null;
    if (this.unifiedPrice && Number(this.unifiedPrice.spot_price_virtual || 0) > 0) {
      spotPriceVirtual = Number(this.unifiedPrice.spot_price_virtual || 0);
      spotPriceVirtualStr = String(this.unifiedPrice.spot_price_virtual_str || this.unifiedPrice.spot_price_virtual || '0');
      spotMcapVirtual = spotPriceVirtual > 0 ? spotPriceVirtual * totalSupply : 0;
      priceSource = String(this.unifiedPrice.source || 'price_service');
      priceStage = String(this.unifiedPrice.stage || '');
      launchPoolAddress = this.unifiedPrice.launch_pool_address || null;
      stageReason = String(this.unifiedPrice.stage_reason || '');
    } else if (this.pairSpot && Number(this.pairSpot.spot_price || 0) > 0) {
      spotPriceVirtual = Number(this.pairSpot.spot_price || 0);
      spotPriceVirtualStr = String(this.pairSpot.spot_price_str || this.pairSpot.spot_price || '0');
      spotMcapVirtual = spotPriceVirtual > 0 ? spotPriceVirtual * totalSupply : 0;
      priceSource = String(this.pairSpot.source || 'pair_reserves');
      priceStage = String(this.pairSpot.stage || '');
      launchPoolAddress = this.pairSpot.launch_pool_address || null;
      stageReason = String(this.pairSpot.stage_reason || '');
    }

    const usdPerVirtual = Number(this.virtualUsd?.usd_per_virtual || config.virtualUsdFallback || 0);
    const spotPriceUsd = usdPerVirtual > 0 ? spotPriceVirtual * usdPerVirtual : 0;
    const spotMcapUsd = usdPerVirtual > 0 ? spotMcapVirtual * usdPerVirtual : 0;

    const factsByTx = new Map();
    for (const f of recentFacts) factsByTx.set(String(f.tx_hash || ''), f);

    const recentTransfers = this.db.getRecentTransfers(this.tokenAddress, 400);
    const recentTransfersRaw = recentTransfers.slice(0, 120).map((t) => ({
      tx_hash: String(t.tx_hash || ''),
      log_index: Number(t.log_index || 0),
      block_number: Number(t.block_number || 0),
      timestamp: Number(t.timestamp || 0),
      from_address: String(t.from_address || ''),
      to_address: String(t.to_address || ''),
      amount_raw: String(t.amount || '0'),
      amount_token: toFloat(t.amount || '0', tokenDecimals),
    }));
    const recentTrades = recentTransfers.slice(0, 120).map((t) => {
      const txHash = String(t.tx_hash || '');
      const fact = factsByTx.get(txHash);
      const classification = String(fact?.classification || 'transfer');
      const spentVirtual = toFloat(fact?.spent_virtual_amount || '0', virtualDecimals);
      const receivedToken = toFloat(fact?.received_token_amount || '0', tokenDecimals);
      const receivedVirtual = toFloat(fact?.received_virtual_amount || '0', virtualDecimals);
      const soldToken = toFloat(fact?.sold_token_amount || '0', tokenDecimals);
      const side = classification === 'suspected_buy'
        ? 'buy'
        : classification === 'suspected_sell'
          ? 'sell'
          : 'transfer';
      const buyTaxPct = calcBuyTaxPct(Number(t.timestamp || now), Number(this.runtimeConfig.launchStartTime || 0));
      const price = side === 'buy'
        ? (() => {
          const grossToken = receivedToken > 0 ? (receivedToken / Math.max(1e-9, 1 - buyTaxPct / 100)) : 0;
          return grossToken > 0 ? (spentVirtual / grossToken) : 0;
        })()
        : side === 'sell'
          ? (soldToken > 0 ? receivedVirtual / soldToken : 0)
          : 0;
      return {
        tx_hash: txHash,
        log_index: Number(t.log_index || 0),
        block_number: Number(t.block_number || 0),
        timestamp: Number(t.timestamp || 0),
        classification,
        side,
        from_address: String(t.from_address || ''),
        to_address: String(t.to_address || ''),
        amount_token: toFloat(t.amount || '0', tokenDecimals),
        actor_address: String(fact?.actor_address || ''),
        buy_tax_pct: buyTaxPct,
        spent_virtual: spentVirtual,
        received_token: receivedToken,
        received_virtual: receivedVirtual,
        sold_token: soldToken,
        price,
      };
    });

    return {
      spot_price: spotPriceVirtual,
      spot_price_str: spotPriceVirtualStr,
      spot_mcap: spotMcapVirtual,
      spot_price_virtual: spotPriceVirtual,
      spot_price_virtual_str: spotPriceVirtualStr,
      spot_mcap_virtual: spotMcapVirtual,
      spot_price_usd: spotPriceUsd,
      spot_mcap_usd: spotMcapUsd,
      usd_per_virtual: usdPerVirtual > 0 ? usdPerVirtual : null,
      usd_per_virtual_str: this.virtualUsd?.usd_per_virtual_str || (usdPerVirtual > 0 ? String(usdPerVirtual) : null),
      usd_source: this.virtualUsd?.source || (usdPerVirtual > 0 ? 'env_fallback' : null),
      source: priceSource,
      stage: priceStage,
      stage_reason: stageReason,
      launch_pool_address: launchPoolAddress,
      quote_token: this.pairSpot?.quote_token || null,
      pair_address: this.pairSpot?.pair_address || null,
      pair_auto_discovered: Boolean(this.pairSpot?.pair_auto_discovered),
      last_buy_price: latestSpotPrice,
      weighted_window_price: weightedSpotPrice,
      source_trade_count: calcOn.length,
      window_sec: windowSec,
      recent_trades: recentTrades,
      recent_transfers_raw: recentTransfersRaw,
    };
  }

  buildSnapshot() {
    const nowMinuteTs = minuteFloor(nowSec());
    const tokenMeta = this.tokenMetaCache.get(this.tokenAddress) || { decimals: 18, totalSupply: '0' };
    const virtualMeta = this.tokenMetaCache.get(safeAddress(config.virtualTokenAddress)) || { decimals: 18, totalSupply: '0' };
    const tokenDecimals = tokenMeta.decimals;
    const virtualDecimals = virtualMeta.decimals;

    const bucketRows = this.db.getRecentBuckets(this.tokenAddress, nowMinuteTs - 59 * 60);
    const minuteMap = new Map();
    for (const b of bucketRows) {
      const minute = Number(b.minute_ts);
      const raw = this.currentMetricMode === 'virtual_spent' ? BigInt(b.virtual_spent_sum || '0') : BigInt(b.token_received_sum || '0');
      const dec = this.currentMetricMode === 'virtual_spent' ? virtualDecimals : tokenDecimals;
      minuteMap.set(minute, Number(formatUnits(raw, dec)));
    }

    const m0 = nowMinuteTs;
    const m1 = nowMinuteTs - 60;
    const m2 = nowMinuteTs - 120;
    const m3 = nowMinuteTs - 180;
    const m4 = nowMinuteTs - 240;

    const near1 = Number(minuteMap.get(m0) || 0);
    const near2Seq = [
      { minute_ts: m2, value: Number(minuteMap.get(m2) || 0) },
      { minute_ts: m1, value: Number(minuteMap.get(m1) || 0) },
    ];
    const near5 = [m0, m1, m2, m3, m4].reduce((a, x) => a + Number(minuteMap.get(x) || 0), 0);

    const holdersResult = this.computeHoldersAndLeaderboard(tokenDecimals, virtualDecimals, tokenMeta.totalSupply);
    const myWalletSummary = this.computeMyWalletStats(tokenMeta, virtualMeta);
    const walletHolder = holdersResult.holders.find((h) => safeAddress(h.address) === safeAddress(this.runtimeConfig.walletAddress));

    const ruleResult = this.ruleEngine.evaluate({
      tokenAddress: this.tokenAddress,
      metricMode: this.currentMetricMode,
      nowMinuteTs,
      minuteValueMap: minuteMap,
      lastSignal: this.db.getLastSignal(this.tokenAddress),
      leaderboard: holdersResult.leaderboard,
    });

    if (ruleResult.triggered) this.db.insertSignal(ruleResult.signal);
    const lastSignal = this.db.getLastSignal(this.tokenAddress);

    const minuteSeries = [];
    for (let i = 29; i >= 0; i -= 1) {
      const minute = nowMinuteTs - i * 60;
      minuteSeries.push({ minute_ts: minute, value: Number(minuteMap.get(minute) || 0) });
    }

    const priceView = this.computeSpotAndRecentTrades(tokenMeta, virtualMeta);
    const curves = this.buildCurves(tokenMeta, myWalletSummary, Number(priceView.spot_mcap || 0));
    const specialStats = this.computeSpecialStats(tokenDecimals, virtualDecimals);
    const protocolActivity = this.computeProtocolActivity(tokenDecimals, virtualDecimals);

    const taxNowSec = this.lastChainTimeSec > 0 ? this.lastChainTimeSec : nowSec();
    return {
      token: this.tokenAddress,
      running: this.running,
      backfill_done: this.backfillDone,
      last_processed_block: Number(this.db.getMeta(this.tokenAddress)?.last_processed_block || 0),
      metric_mode: this.currentMetricMode,
      windows: {
        near_1m: near1,
        near_2m_sequence: near2Seq,
        near_5m: near5,
      },
      signal_state: {
        passed_now: ruleResult.triggered,
        reason: ruleResult.reason || 'triggered',
        pair: ruleResult.pair,
        cooldown_until: Number(ruleResult.cooldownUntil || lastSignal?.cooldown_until || 0),
        cooling_down: Number(ruleResult.cooldownUntil || lastSignal?.cooldown_until || 0) > nowSec(),
      },
      tax: {
        launch_start_time: Number(this.runtimeConfig.launchStartTime || 0),
        buy_tax_pct: calcBuyTaxPct(taxNowSec, Number(this.runtimeConfig.launchStartTime || 0)),
        sell_tax_pct: Number(this.runtimeConfig.sellTaxPct || 1),
        reference_time: taxNowSec,
      },
      minute_series: minuteSeries,
      curves,
      price: {
        spot_price: priceView.spot_price,
        spot_price_str: priceView.spot_price_str,
        spot_price_virtual: priceView.spot_price_virtual,
        spot_price_virtual_str: priceView.spot_price_virtual_str,
        spot_price_usd: priceView.spot_price_usd,
        spot_mcap: priceView.spot_mcap,
        spot_mcap_virtual: priceView.spot_mcap_virtual,
        spot_mcap_usd: priceView.spot_mcap_usd,
        usd_per_virtual: priceView.usd_per_virtual,
        usd_per_virtual_str: priceView.usd_per_virtual_str,
        usd_source: priceView.usd_source,
        source: priceView.source,
        stage: priceView.stage,
        stage_reason: priceView.stage_reason,
        launch_pool_address: priceView.launch_pool_address,
        quote_token: priceView.quote_token,
        pair_address: priceView.pair_address,
        pair_auto_discovered: priceView.pair_auto_discovered,
        last_buy_price: priceView.last_buy_price,
        weighted_window_price: priceView.weighted_window_price,
        source_trade_count: priceView.source_trade_count,
        window_sec: priceView.window_sec,
      },
      recent_trades: priceView.recent_trades || [],
      recent_transfers_raw: priceView.recent_transfers_raw || [],
      holder_stats: holdersResult.holders,
      leaderboard: holdersResult.leaderboard,
      my_wallet: {
        address: this.runtimeConfig.walletAddress || null,
        breakeven_mcap: walletHolder?.breakeven_mcap || 0,
        cost_per_token: walletHolder?.cost_per_token || 0,
      },
      my_wallets: myWalletSummary,
      special_stats: specialStats,
      protocol_activity: protocolActivity,
      ingest: {
        ...this.lastIngest,
        counterparty_configured: this.counterpartyList.length > 0,
        counterparty_count: this.counterpartyList.length,
      },
      decimals: {
        token: tokenDecimals,
        virtual: virtualDecimals,
      },
      token_meta: {
        total_supply: tokenMeta.totalSupply,
      },
    };
  }

  emitUpdate(force, eventType) {
    const now = Date.now();
    if (!force && now - this.lastEmitAt < config.updateThrottleMs) return;
    this.lastEmitAt = now;
    this.onUpdate?.({ type: eventType, ts: now, snapshot: this.buildSnapshot() });
  }
}

