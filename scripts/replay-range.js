import path from 'node:path';
import { TrackerDB } from '../src/db.js';
import { RpcService } from '../src/rpc.js';
import { RuleEngine } from '../src/rules.js';
import { TokenTrackerTask } from '../src/tracker-task.js';
import { config } from '../src/config.js';

const token = String(process.env.REPLAY_TOKEN || '').toLowerCase();
const from = Number(process.env.REPLAY_FROM_BLOCK || 0);
const to = Number(process.env.REPLAY_TO_BLOCK || 0);

if (!token || !Number.isFinite(from) || !Number.isFinite(to) || from <= 0 || to < from) {
  console.error('Usage: set REPLAY_TOKEN/REPLAY_FROM_BLOCK/REPLAY_TO_BLOCK then run npm run replay');
  process.exit(1);
}

const db = new TrackerDB(path.resolve(config.dbPath));
const rpc = new RpcService();
const ruleEngine = new RuleEngine({
  thresholdFirstMinute: Number(process.env.RULE_FIRST_THRESHOLD || config.thresholdFirstMinute),
  thresholdSecondMinute: Number(process.env.RULE_SECOND_THRESHOLD || config.thresholdSecondMinute),
  cooldownMinutes: Number(process.env.RULE_COOLDOWN_MINUTES || config.cooldownMinutes),
});

const task = new TokenTrackerTask({
  tokenAddress: token,
  db,
  rpc,
  ruleEngine,
  onUpdate: (payload) => {
    if (payload.type === 'range_processed') {
      const snap = payload.snapshot;
      console.log(`processed block=${snap.last_processed_block}, near1=${snap.windows.near_1m}, near5=${snap.windows.near_5m}`);
    }
  },
});

task.running = true;
await task.ensureTokenDecimals(token);
await task.scanRange(from, to, false);

const snapshot = task.buildSnapshot();
console.log('Replay done. Current signal state:');
console.log(JSON.stringify(snapshot.signal_state, null, 2));

db.close();
