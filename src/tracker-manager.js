import { config } from './config.js';
import { RpcService } from './rpc.js';
import { RuleEngine } from './rules.js';
import { TokenTrackerTask } from './tracker-task.js';
import { logger } from './logger.js';

export class TrackerManager {
  constructor({ db }) {
    this.db = db;
    this.rpc = new RpcService();
    this.ruleEngine = new RuleEngine({
      thresholdFirstMinute: config.thresholdFirstMinute,
      thresholdSecondMinute: config.thresholdSecondMinute,
      cooldownMinutes: config.cooldownMinutes,
    });

    this.activeTask = null;
    this.taskPromise = null;
    this.subscribers = new Set();
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
      return {
        running: false,
        token: null,
      };
    }

    const snap = this.activeTask.buildSnapshot();
    return {
      running: true,
      token: this.activeTask.tokenAddress,
      backfill_done: snap.backfill_done,
      last_processed_block: snap.last_processed_block,
      metric_mode: snap.metric_mode,
    };
  }

  async start(tokenAddress) {
    await this.stop();

    this.activeTask = new TokenTrackerTask({
      tokenAddress,
      db: this.db,
      rpc: this.rpc,
      ruleEngine: this.ruleEngine,
      onUpdate: (payload) => this.broadcast(payload),
    });

    this.taskPromise = this.activeTask.start().catch((err) => {
      logger.error('task crashed', { error: String(err?.message || err) });
      this.broadcast({
        type: 'error',
        ts: Date.now(),
        error: String(err?.message || err),
        snapshot: this.activeTask?.buildSnapshot?.() || null,
      });
    });

    return this.getStatus();
  }

  async stop() {
    if (!this.activeTask) return;

    await this.activeTask.stop();
    if (this.taskPromise) {
      try {
        await this.taskPromise;
      } catch {
        // swallow
      }
    }

    this.activeTask = null;
    this.taskPromise = null;
  }

  setMetricMode(mode) {
    if (this.activeTask) {
      this.activeTask.setMetricMode(mode);
      this.broadcast({
        type: 'metric_mode_changed',
        ts: Date.now(),
        snapshot: this.activeTask.buildSnapshot(),
      });
    }
  }

  getSnapshot() {
    return this.activeTask ? this.activeTask.buildSnapshot() : null;
  }
}
