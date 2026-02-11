import { createPublicClient, decodeEventLog, http, isAddress, parseAbiItem } from 'viem';
import { base } from 'viem/chains';

const transferEvent = parseAbiItem(
  'event Transfer(address indexed from, address indexed to, uint256 value)'
);

function chunkRange(from, to, size) {
  const ranges = [];
  let start = from;
  while (start <= to) {
    const end = start + size - 1n <= to ? start + size - 1n : to;
    ranges.push([start, end]);
    start = end + 1n;
  }
  return ranges;
}

export class TokenTracker {
  constructor({ db, rpcUrl, onUpdate, backfillBlocks = 8000, pollIntervalMs = 2000, batchBlockSpan = 1200 }) {
    this.db = db;
    this.rpcUrl = rpcUrl;
    this.onUpdate = onUpdate;
    this.backfillBlocks = backfillBlocks;
    this.pollIntervalMs = pollIntervalMs;
    this.batchBlockSpan = batchBlockSpan;

    this.client = createPublicClient({
      chain: base,
      transport: http(this.rpcUrl),
    });

    this.currentTask = null;
  }

  getStatus() {
    if (!this.currentTask) {
      return { running: false, token: null, lastBlock: null, startedAt: null };
    }
    return {
      running: true,
      token: this.currentTask.token,
      lastBlock: this.currentTask.lastBlock,
      startedAt: this.currentTask.startedAt,
      backfillDone: this.currentTask.backfillDone,
    };
  }

  async start(tokenAddress) {
    const token = tokenAddress.toLowerCase();
    if (!isAddress(token)) {
      throw new Error('Invalid token address');
    }

    await this.stop();

    const state = this.db.getState(token);
    const latest = await this.client.getBlockNumber();
    const startFrom = state?.last_scanned_block
      ? BigInt(state.last_scanned_block) + 1n
      : latest - BigInt(this.backfillBlocks) > 0n
        ? latest - BigInt(this.backfillBlocks)
        : 0n;

    const task = {
      token,
      startedAt: Date.now(),
      stopFlag: false,
      runningPromise: null,
      lastBlock: Number(startFrom - 1n >= 0n ? startFrom - 1n : 0n),
      backfillDone: false,
    };

    this.currentTask = task;
    this.db.upsertState(token, task.lastBlock);

    task.runningPromise = this._run(task, startFrom);
    return this.getStatus();
  }

  async stop() {
    if (!this.currentTask) return;
    this.currentTask.stopFlag = true;
    try {
      await this.currentTask.runningPromise;
    } catch {
      // ignore tracker-loop cancel errors
    }
    this.currentTask = null;
  }

  async _run(task, startFrom) {
    try {
      let fromBlock = startFrom;

      const head = await this.client.getBlockNumber();
      if (fromBlock <= head) {
        await this._scanRange(task, fromBlock, head);
      }

      task.backfillDone = true;
      this._notify(task, 'backfill_complete');

      while (!task.stopFlag) {
        const latest = await this.client.getBlockNumber();
        const nextFrom = BigInt(task.lastBlock) + 1n;
        if (nextFrom <= latest) {
          await this._scanRange(task, nextFrom, latest);
        }

        const oneHourAgo = Math.floor(Date.now() / 1000) - 3600;
        this.db.cleanupOldFlowEvents(task.token, oneHourAgo - 300);

        this._notify(task, 'tick');
        await this._sleep(this.pollIntervalMs);
      }
    } catch (err) {
      if (!task.stopFlag) {
        this._notify(task, 'error', err);
      }
    }
  }

  async _scanRange(task, fromBlock, toBlock) {
    const ranges = chunkRange(fromBlock, toBlock, BigInt(this.batchBlockSpan));

    for (const [from, to] of ranges) {
      if (task.stopFlag) return;

      const logs = await this.client.getLogs({
        address: task.token,
        event: transferEvent,
        fromBlock: from,
        toBlock: to,
      });

      const blockTsCache = new Map();

      for (const log of logs) {
        if (task.stopFlag) return;
        if (!log.transactionHash) continue;

        const decoded = decodeEventLog({
          abi: [transferEvent],
          data: log.data,
          topics: log.topics,
        });

        const fromAddr = String(decoded.args.from).toLowerCase();
        const toAddr = String(decoded.args.to).toLowerCase();
        const amount = BigInt(decoded.args.value);
        const bnum = Number(log.blockNumber);

        let ts = blockTsCache.get(log.blockNumber);
        if (!ts) {
          const block = await this.client.getBlock({ blockNumber: log.blockNumber });
          ts = Number(block.timestamp);
          blockTsCache.set(log.blockNumber, ts);
        }

        const transfer = {
          token_address: task.token,
          tx_hash: log.transactionHash.toLowerCase(),
          block_number: bnum,
          timestamp: ts,
          from_address: fromAddr,
          to_address: toAddr,
          amount: amount.toString(),
        };

        this.db.applyTransfer(transfer);
      }

      task.lastBlock = Number(to);
      this.db.setLastScannedBlock(task.token, task.lastBlock);
      this._notify(task, 'range_processed');
    }
  }

  _notify(task, type, error = null) {
    if (!this.onUpdate) return;

    const leaderboard = this.db.getTopWallets(task.token, 100);
    const payload = {
      type,
      token: task.token,
      status: this.getStatus(),
      summary: {
        wallet_count: this.db.getWalletCount(task.token),
        updated_at: Date.now(),
      },
      leaderboard,
      error: error ? String(error.message || error) : null,
    };

    this.onUpdate(payload);
  }

  _sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
