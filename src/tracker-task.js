import { decodeEventLog, formatUnits, parseAbiItem, zeroAddress } from 'viem';
import { config, buildCounterpartyList } from './config.js';
import { logger } from './logger.js';
import { minuteFloor, nowSec, safeAddress, sleep } from './utils.js';

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

export class TokenTrackerTask {
  constructor({ tokenAddress, db, rpc, ruleEngine, onUpdate }) {
    this.tokenAddress = safeAddress(tokenAddress);
    this.db = db;
    this.rpc = rpc;
    this.ruleEngine = ruleEngine;
    this.onUpdate = onUpdate;

    this.running = false;
    this.stopFlag = false;
    this.backfillDone = false;
    this.unwatchHead = null;
    this.lastKnownHead = 0;
    this.lastEmitAt = 0;
    this.currentMetricMode = config.metricMode === 'virtual_spent' ? 'virtual_spent' : 'token_received';

    this.blockTsCache = new Map();
    this.decimalsCache = new Map();
    this.counterpartyList = buildCounterpartyList();
  }

  async start() {
    this.running = true;
    this.stopFlag = false;
    this.db.setMeta(this.tokenAddress, { running: 1 });

    await this.ensureTokenDecimals(this.tokenAddress);
    for (const addr of this.counterpartyList) {
      if (addr) await this.ensureTokenDecimals(addr);
    }

    const latest = Number(await this.rpc.getBlockNumber());
    this.lastKnownHead = latest;
    const state = this.db.getMeta(this.tokenAddress);
    const startFrom = state?.last_processed_block
      ? Number(state.last_processed_block) + 1
      : Math.max(0, latest - config.backfillBlocks);

    logger.info('task start', {
      token: this.tokenAddress,
      startFrom,
      latest,
      counterpartyCount: this.counterpartyList.length,
    });

    if (startFrom <= latest) {
      await this.scanRange(startFrom, latest, false);
    }

    this.backfillDone = true;
    this.emitUpdate(true, 'backfill_complete');

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
          await this.scanRange(replayFrom, this.lastKnownHead, true);
        }

        this.emitUpdate(false, 'tick');
      } catch (err) {
        logger.error('task loop failed', { error: String(err?.message || err) });
      }

      await sleep(config.pollingIntervalMs);
    }

    if (this.unwatchHead) {
      try {
        this.unwatchHead();
      } catch {
        // ignore
      }
    }

    this.running = false;
    this.db.setMeta(this.tokenAddress, { running: 0 });
    this.emitUpdate(true, 'stopped');
  }

  async stop() {
    this.stopFlag = true;
  }

  setMetricMode(mode) {
    this.currentMetricMode = mode === 'virtual_spent' ? 'virtual_spent' : 'token_received';
  }

  async ensureTokenDecimals(tokenAddress) {
    const address = safeAddress(tokenAddress);
    if (!address) return 18;
    if (this.decimalsCache.has(address)) return this.decimalsCache.get(address);

    const dbMeta = this.db.getTokenMeta(address);
    if (dbMeta) {
      this.decimalsCache.set(address, Number(dbMeta.decimals));
      return Number(dbMeta.decimals);
    }

    const decimals = Number(await this.rpc.readDecimals(address));
    this.db.upsertTokenDecimals(address, decimals, null);
    this.decimalsCache.set(address, decimals);
    return decimals;
  }

  async getBlockTimestamp(blockNumber) {
    const bn = Number(blockNumber);
    if (this.blockTsCache.has(bn)) return this.blockTsCache.get(bn);

    const cachedDb = this.db.getBlockTimestamp(bn);
    if (cachedDb != null) {
      this.blockTsCache.set(bn, cachedDb);
      return cachedDb;
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
      if (!this.blockTsCache.has(bn) && this.db.getBlockTimestamp(bn) == null) {
        missing.add(bn);
      }
    }
    for (const bn of missing) {
      const block = await this.rpc.getBlock(BigInt(bn));
      const ts = Number(block.timestamp);
      this.blockTsCache.set(bn, ts);
      this.db.setBlockTimestamp(bn, ts);
    }
  }

  async scanRange(fromBlock, toBlock, isReplay) {
    if (toBlock < fromBlock) return;

    const ranges = chunkRanges(fromBlock, toBlock, config.logChunkSize);
    for (const [start, end] of ranges) {
      if (this.stopFlag) return;

      const logs = await this.rpc.getLogs({
        address: this.tokenAddress,
        event: TRANSFER_EVENT,
        fromBlock: BigInt(start),
        toBlock: BigInt(end),
      });

      await this.fillMissingBlockTimestamps(logs);

      const transferRows = [];
      for (const log of logs) {
        if (!log.transactionHash || log.logIndex == null) continue;
        let decoded;
        try {
          decoded = decodeEventLog({
            abi: [TRANSFER_EVENT],
            topics: log.topics,
            data: log.data,
          });
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

      const { inserted, newTxHashes } = this.db.insertTransfers(transferRows);
      if (config.enableTxFacts && newTxHashes.length) {
        await this.processTxFacts(newTxHashes);
      }

      const meta = this.db.getMeta(this.tokenAddress);
      const prev = Number(meta?.last_processed_block || 0);
      const next = isReplay ? Math.max(prev, end) : end;
      this.db.setMeta(this.tokenAddress, { last_processed_block: next, running: 1 });
      this.emitUpdate(false, 'range_processed');

      logger.info('range done', {
        token: this.tokenAddress,
        start,
        end,
        logs: logs.length,
        inserted,
        newTxCount: newTxHashes.length,
        replay: isReplay,
      });
    }
  }

  async processTxFacts(txHashes) {
    for (const txHash of txHashes) {
      if (this.stopFlag) return;
      const receipt = await this.rpc.getTransactionReceipt(txHash);
      const fact = await this.classifyTxFromReceipt(receipt);
      if (!fact) continue;
      this.db.applyTxFact(fact);
    }
  }

  async classifyTxFromReceipt(receipt) {
    const counterpartySet = new Set(this.counterpartyList.map((v) => safeAddress(v)));
    const token = this.tokenAddress;

    const tokenDelta = new Map();
    const virtualDelta = new Map();
    let sawTokenTransfer = false;
    let sawVirtualTransfer = false;

    for (const log of receipt.logs || []) {
      if (!log?.topics?.length || !log.address) continue;

      let decoded;
      try {
        decoded = decodeEventLog({
          abi: [TRANSFER_EVENT],
          topics: log.topics,
          data: log.data,
          strict: false,
        });
      } catch {
        continue;
      }

      const contract = safeAddress(log.address);
      const from = safeAddress(decoded.args.from);
      const to = safeAddress(decoded.args.to);
      const amount = BigInt(decoded.args.value || 0n);
      if (amount <= 0n) continue;

      if (contract === token) {
        sawTokenTransfer = true;
        tokenDelta.set(from, (tokenDelta.get(from) || 0n) - amount);
        tokenDelta.set(to, (tokenDelta.get(to) || 0n) + amount);
      }

      if (counterpartySet.has(contract)) {
        sawVirtualTransfer = true;
        virtualDelta.set(from, (virtualDelta.get(from) || 0n) - amount);
        virtualDelta.set(to, (virtualDelta.get(to) || 0n) + amount);
      }
    }

    if (!sawTokenTransfer) return null;

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
        const spent = -vd;
        const recv = td;
        addressFacts.push({
          wallet_address: addr,
          role: 'buyer',
          spent_virtual_amount: spent.toString(),
          received_token_amount: recv.toString(),
          sold_virtual_amount: '0',
          sold_token_amount: '0',
        });
        totalSpent += spent;
        totalReceived += recv;
      }

      if (td < 0n && vd > 0n) {
        const soldToken = -td;
        const soldVirtual = vd;
        addressFacts.push({
          wallet_address: addr,
          role: 'seller',
          spent_virtual_amount: '0',
          received_token_amount: '0',
          sold_virtual_amount: soldVirtual.toString(),
          sold_token_amount: soldToken.toString(),
        });
        totalSoldToken += soldToken;
        totalSoldVirtual += soldVirtual;
      }
    }

    let classification = 'transfer_or_unknown';
    if (sawVirtualTransfer && totalSpent > 0n && totalReceived > 0n) {
      classification = 'suspected_buy';
    } else if (sawVirtualTransfer && totalSoldToken > 0n && totalSoldVirtual > 0n) {
      classification = 'suspected_sell';
    }

    const blockNumber = Number(receipt.blockNumber);
    const timestamp = await this.getBlockTimestamp(blockNumber);

    return {
      token_address: token,
      tx_hash: safeAddress(receipt.transactionHash),
      block_number: blockNumber,
      timestamp,
      classification,
      spent_virtual_amount: totalSpent.toString(),
      received_token_amount: totalReceived.toString(),
      addressFacts,
    };
  }

  buildSnapshot() {
    const nowMinuteTs = minuteFloor(nowSec());
    const startMinute = nowMinuteTs - 9 * 60;
    const buckets = this.db.getRecentBuckets(this.tokenAddress, startMinute);
    const tokenDecimals = this.decimalsCache.get(this.tokenAddress) ?? 18;
    const virtualDecimals = this.decimalsCache.get(config.virtualTokenAddress) ?? 18;
    const modeDecimals = this.currentMetricMode === 'virtual_spent' ? virtualDecimals : tokenDecimals;
    const map = new Map();
    for (const row of buckets) {
      const minuteTs = Number(row.minute_ts);
      const raw = this.currentMetricMode === 'virtual_spent'
        ? BigInt(row.virtual_spent_sum || '0')
        : BigInt(row.token_received_sum || '0');
      const value = this._rawToNumber(raw, modeDecimals);
      map.set(minuteTs, value);
    }

    const m0 = nowMinuteTs;
    const m1 = nowMinuteTs - 60;
    const m2 = nowMinuteTs - 120;
    const m3 = nowMinuteTs - 180;
    const m4 = nowMinuteTs - 240;

    const near1 = Number(map.get(m0) || 0);
    const near2Sequence = [
      { minute_ts: m2, value: Number(map.get(m2) || 0) },
      { minute_ts: m1, value: Number(map.get(m1) || 0) },
    ];
    const near5 = [m0, m1, m2, m3, m4].reduce((acc, minute) => acc + Number(map.get(minute) || 0), 0);

    let rawLeaderboard = this.db.getLeaderboard(this.tokenAddress, this.currentMetricMode, 50, nowMinuteTs);
    if (!rawLeaderboard.whales.length && !rawLeaderboard.heat.length && !rawLeaderboard.active.length) {
      rawLeaderboard = this.db.getHolderLeaderboardFromTransfers(this.tokenAddress, 50, nowSec());
    }
    const leaderboard = this._formatLeaderboard(rawLeaderboard, tokenDecimals, virtualDecimals);
    const lastSignal = this.db.getLastSignal(this.tokenAddress);
    const ruleResult = this.ruleEngine.evaluate({
      tokenAddress: this.tokenAddress,
      metricMode: this.currentMetricMode,
      nowMinuteTs,
      minuteValueMap: map,
      lastSignal,
      leaderboard,
    });

    if (ruleResult.triggered) {
      this.db.insertSignal(ruleResult.signal);
    }

    const cooldownUntil = Number(ruleResult.cooldownUntil || lastSignal?.cooldown_until || 0);

    const series = [];
    for (let i = 9; i >= 0; i -= 1) {
      const minuteTs = nowMinuteTs - i * 60;
      series.push({ minute_ts: minuteTs, value: Number(map.get(minuteTs) || 0) });
    }

    const meta = this.db.getMeta(this.tokenAddress);
    return {
      token: this.tokenAddress,
      running: this.running,
      backfill_done: this.backfillDone,
      last_processed_block: Number(meta?.last_processed_block || 0),
      metric_mode: this.currentMetricMode,
      windows: {
        near_1m: near1,
        near_2m_sequence: near2Sequence,
        near_5m: near5,
      },
      signal_state: {
        passed_now: ruleResult.triggered,
        reason: ruleResult.reason || 'triggered',
        pair: ruleResult.pair,
        cooldown_until: cooldownUntil,
        cooling_down: cooldownUntil > nowSec(),
      },
      minute_series: series,
      leaderboard,
      decimals: {
        token: tokenDecimals,
        virtual: virtualDecimals,
      },
    };
  }

  _rawToNumber(raw, decimals) {
    try {
      const n = Number(formatUnits(BigInt(raw), decimals));
      return Number.isFinite(n) ? n : 0;
    } catch {
      return 0;
    }
  }

  _formatLeaderboard(raw, tokenDecimals, virtualDecimals) {
    const whales = (raw.whales || []).map((r) => {
      const spent = this._rawToNumber(r.cumulative_spent_virtual, virtualDecimals);
      const received = this._rawToNumber(r.cumulative_received_token, tokenDecimals);
      const avg = received > 0 ? spent / received : 0;
      return {
        ...r,
        cumulative_spent_virtual_raw: r.cumulative_spent_virtual,
        cumulative_received_token_raw: r.cumulative_received_token,
        cumulative_spent_virtual: spent,
        cumulative_received_token: received,
        weighted_avg_price: avg,
      };
    });

    const heat = (raw.heat || []).map((r) => ({
      ...r,
      netflow_raw: r.netflow,
      netflow_token_raw: r.netflow_token,
      netflow_virtual_raw: r.netflow_virtual,
      netflow: this.currentMetricMode === 'virtual_spent'
        ? this._rawToNumber(r.netflow, virtualDecimals)
        : this._rawToNumber(r.netflow, tokenDecimals),
      netflow_token: this._rawToNumber(r.netflow_token, tokenDecimals),
      netflow_virtual: this._rawToNumber(r.netflow_virtual, virtualDecimals),
    }));

    const active = (raw.active || []).map((r) => ({
      ...r,
      spent_virtual_5m_raw: r.spent_virtual_5m,
      received_token_5m_raw: r.received_token_5m,
      spent_virtual_5m: this._rawToNumber(r.spent_virtual_5m, virtualDecimals),
      received_token_5m: this._rawToNumber(r.received_token_5m, tokenDecimals),
    }));

    return { whales, heat, active, source: raw.source || 'facts' };
  }

  emitUpdate(force, eventType) {
    const now = Date.now();
    if (!force && now - this.lastEmitAt < config.updateThrottleMs) return;
    this.lastEmitAt = now;

    const snapshot = this.buildSnapshot();
    this.onUpdate?.({
      type: eventType,
      ts: now,
      snapshot,
    });
  }
}
