import express from 'express';
import cors from 'cors';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { TrackerDB } from './db.js';
import { TokenTracker } from './tracker.js';
import { createApi } from './api.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = Number(process.env.PORT || 3000);
const RPC_URL =
  process.env.BASE_RPC_URL ||
  'https://mainnet.base.org';
const BACKFILL_BLOCKS = Number(process.env.BACKFILL_BLOCKS || 8000);
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS || 2000);
const DB_PATH = process.env.DB_PATH || path.join(__dirname, 'tracker.db');

const db = new TrackerDB(DB_PATH);
const tracker = new TokenTracker({
  db,
  rpcUrl: RPC_URL,
  backfillBlocks: BACKFILL_BLOCKS,
  pollIntervalMs: POLL_INTERVAL_MS,
  onUpdate: null,
});

const app = express();
app.use(cors());
app.use(express.json({ limit: '1mb' }));

app.use('/api', createApi({ tracker, db }));
app.use(express.static(path.join(__dirname, 'public')));

app.get('*', (_req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
  console.log(`Using Base RPC: ${RPC_URL}`);
});

process.on('SIGINT', async () => {
  await tracker.stop();
  process.exit(0);
});
