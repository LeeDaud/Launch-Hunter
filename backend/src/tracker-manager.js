import fs from 'node:fs';
import path from 'node:path';
import { isAddress } from 'viem';
import { config } from './config.js';
import { RpcService } from './rpc.js';
import { RuleEngine } from './rules.js';
import { TokenTrackerTask } from './tracker-task.js';
import { logger } from './logger.js';
import { SPECIAL_ADDRESSES, normalizeSpecialAddressRows } from './virtuals-special-addresses.js';

export class TrackerManager {
  constructor({ db }) {
    this.db = db;
    this.rpc = new RpcService();
    this.ruleEngine = new RuleEngine({
      thresholdFirstMinute: config.thresholdFirstMinute,
      thresholdSecondMinute: config.thresholdSecondMinute,
      cooldownMinutes: config.cooldownMinutes,
    });

    this.runtimeConfig = {
      launchStartTime: Number(config.launchStartTime || 0),
      walletAddress: String(config.walletAddress || '').toLowerCase(),
      myWallets: this.db.getMyWallets().map((x) => String(x.addr || '').toLowerCase()).filter((x) => isAddress(x)),
      myWalletFromBlock: Number(config.myWalletFromBlock || 0),
      tokenStartBlock: Number(config.tokenStartBlock || 0),
      spotPairAddress: String(config.spotPairAddress || '').toLowerCase(),
      sellTaxPct: Number(config.sellTaxPct || 1),
      curveWindowMinutes: Number(config.curveWindowMinutes || 30),
    };

    this.specialAddresses = this._loadSpecialAddresses();

    this.activeTask = null;
    this.activeTaskId = 0;
    this.taskPromise = null;
    this.subscribers = new Set();
  }

  _loadSpecialAddresses() {
    const base = normalizeSpecialAddressRows(SPECIAL_ADDRESSES);
    try {
      const file = path.resolve(config.specialAddressFile);
      const raw = fs.readFileSync(file, 'utf8').replace(/^\uFEFF/, '');
      const rows = normalizeSpecialAddressRows(JSON.parse(raw));
      const byAddr = new Map(base.map((r) => [r.address, r]));
      for (const r of rows) byAddr.set(r.address, r);
      return Array.from(byAddr.values()).filter((r) => isAddress(r.address));
    } catch (err) {
      logger.warn('failed to load special addresses', { error: String(err?.message || err) });
      return base;
    }
  }

  subscribe(fn) {
    this.subscribers.add(fn);
    return () => this.subscribers.delete(fn);
  }

  broadcast(payload) {
    for (const fn of this.subscribers) {
      try {
        fn(payload);
      } catch (err) {
        logger.warn('subscriber push failed', { error: String(err?.message || err) });
      }
    }
  }

  getStatus() {
    if (!this.activeTask) {
      return { running: false, token: null, runtime: this.runtimeConfig };
    }
    const snap = this.activeTask.buildSnapshot();
    return {
      running: true,
      token: this.activeTask.tokenAddress,
      backfill_done: snap.backfill_done,
      last_processed_block: snap.last_processed_block,
      metric_mode: snap.metric_mode,
      runtime: this.runtimeConfig,
    };
  }

  async start(tokenAddress) {
    await this.stop({ waitMs: 4000 });

    const taskId = Date.now();
    this.activeTaskId = taskId;

    this.activeTask = new TokenTrackerTask({
      tokenAddress,
      db: this.db,
      rpc: this.rpc,
      ruleEngine: this.ruleEngine,
      runtimeConfig: this.runtimeConfig,
      specialAddresses: this.specialAddresses,
      onUpdate: (payload) => {
        if (this.activeTaskId !== taskId) return;
        this.broadcast(payload);
      },
    });

    this.taskPromise = this.activeTask.start().catch((err) => {
      logger.error('task crashed', { error: String(err?.message || err) });
      if (this.activeTaskId !== taskId) return;
      this.broadcast({
        type: 'error',
        ts: Date.now(),
        error: String(err?.message || err),
        snapshot: this.activeTask?.buildSnapshot?.() || null,
      });
    });

    return this.getStatus();
  }

  async stop({ waitMs = 5000 } = {}) {
    if (!this.activeTask) return;
    const taskToStop = this.activeTask;
    const promiseToWait = this.taskPromise;
    this.activeTaskId = 0;
    this.activeTask = null;
    this.taskPromise = null;
    await taskToStop.stop();

    if (!promiseToWait) return;
    try {
      if (waitMs > 0) {
        await Promise.race([
          promiseToWait,
          new Promise((resolve) => setTimeout(resolve, waitMs)),
        ]);
      } else {
        await promiseToWait;
      }
    } catch {
      // ignore
    }
  }

  setMetricMode(mode) {
    if (this.activeTask) this.activeTask.setMetricMode(mode);
    this.broadcast({ type: 'metric_mode_changed', ts: Date.now(), snapshot: this.getSnapshot() });
  }

  updateRuntimeSettings({ launchStartTime, walletAddress, sellTaxPct, curveWindowMinutes, myWallets, myWalletFromBlock, tokenStartBlock, spotPairAddress }) {
    let walletsChanged = false;
    let fromBlockChanged = false;
    let pairChanged = false;

    if (launchStartTime != null && Number.isFinite(Number(launchStartTime))) {
      this.runtimeConfig.launchStartTime = Number(launchStartTime);
    }
    if (walletAddress != null) {
      const value = String(walletAddress).trim().toLowerCase();
      this.runtimeConfig.walletAddress = value;
    }
    if (sellTaxPct != null && Number.isFinite(Number(sellTaxPct))) {
      this.runtimeConfig.sellTaxPct = Number(sellTaxPct);
    }
    if (Array.isArray(myWallets)) {
      const normalized = Array.from(new Set(
        myWallets
          .map((x) => String(x || '').trim().toLowerCase())
          .filter((x) => isAddress(x))
      ));
      const prev = JSON.stringify(this.runtimeConfig.myWallets || []);
      const next = JSON.stringify(normalized);
      walletsChanged = prev !== next;
      this.runtimeConfig.myWallets = normalized;
      this.db.saveMyWallets(normalized.map((addr) => ({ addr, label: '' })));
    }
    if (myWalletFromBlock != null && Number.isFinite(Number(myWalletFromBlock))) {
      const val = Math.max(0, Number(myWalletFromBlock));
      fromBlockChanged = val !== Number(this.runtimeConfig.myWalletFromBlock || 0);
      this.runtimeConfig.myWalletFromBlock = val;
    }
    if (tokenStartBlock != null && Number.isFinite(Number(tokenStartBlock))) {
      this.runtimeConfig.tokenStartBlock = Math.max(0, Number(tokenStartBlock));
    }
    if (spotPairAddress != null) {
      const next = String(spotPairAddress || '').trim().toLowerCase();
      pairChanged = next !== String(this.runtimeConfig.spotPairAddress || '');
      this.runtimeConfig.spotPairAddress = isAddress(next) ? next : '';
    }
    if (curveWindowMinutes != null && Number.isFinite(Number(curveWindowMinutes))) {
      this.runtimeConfig.curveWindowMinutes = Math.max(10, Math.min(120, Number(curveWindowMinutes)));
    }

    if (this.activeTask && (walletsChanged || fromBlockChanged || pairChanged)) {
      this.activeTask.onRuntimeUpdated({
        myWalletsChanged: walletsChanged,
        myWalletFromBlockChanged: fromBlockChanged,
        spotPairChanged: pairChanged,
      });
    }

    this.broadcast({
      type: 'runtime_updated',
      ts: Date.now(),
      snapshot: this.getSnapshot(),
      runtime: this.runtimeConfig,
    });

    return this.runtimeConfig;
  }

  getSnapshot() {
    return this.activeTask ? this.activeTask.buildSnapshot() : null;
  }
}
