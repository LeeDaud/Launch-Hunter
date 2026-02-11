import express from 'express';
import cors from 'cors';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { config } from './config.js';
import { logger } from './logger.js';
import { TrackerDB } from './db.js';
import { TrackerManager } from './tracker-manager.js';
import { createApi } from './api.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const root = path.resolve(__dirname, '..');

const db = new TrackerDB(path.resolve(root, config.dbPath));
const tracker = new TrackerManager({ db });

const app = express();
app.use(cors());
app.use(express.json({ limit: '1mb' }));

app.use('/api', createApi({ tracker, db }));
app.use(express.static(path.join(root, 'public')));

app.get('*', (_req, res) => {
  res.sendFile(path.join(root, 'public', 'index.html'));
});

const server = app.listen(config.port, () => {
  logger.info('server started', {
    port: config.port,
    dbPath: config.dbPath,
    baseHttpRpc: config.baseHttpRpc,
    baseWsRpc: config.baseWsRpc || '(disabled)',
  });
});

async function shutdown() {
  logger.info('shutting down');
  try {
    await tracker.stop();
  } catch {
    // ignore
  }
  try {
    db.close();
  } catch {
    // ignore
  }
  server.close(() => process.exit(0));
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
