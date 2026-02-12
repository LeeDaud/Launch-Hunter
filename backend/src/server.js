import express from 'express';
import cors from 'cors';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { config } from './config.js';
import { logger } from './logger.js';
import { TrackerDB } from './db.js';
import { TrackerManager } from './tracker-manager.js';
import { createApi } from './api.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const backendRoot = path.resolve(__dirname, '..');
const projectRoot = path.resolve(backendRoot, '..');
const frontendPublicRoot = path.join(projectRoot, 'frontend', 'public');

const dbFile = path.resolve(backendRoot, config.dbPath);
fs.mkdirSync(path.dirname(dbFile), { recursive: true });
const db = new TrackerDB(dbFile);
const tracker = new TrackerManager({ db });

const app = express();
const allowedOrigin = config.frontendOrigin;
app.use(cors({
  origin(origin, callback) {
    if (!origin) return callback(null, true);
    if (origin === allowedOrigin) return callback(null, true);
    return callback(null, false);
  },
}));
app.use(express.json({ limit: '1mb' }));

app.get('/healthz', (_req, res) => {
  res.type('text/plain').send('ok');
});

app.use('/api', createApi({ tracker, db }));
if (fs.existsSync(frontendPublicRoot)) {
  app.use(express.static(frontendPublicRoot));
  app.get('*', (_req, res) => {
    res.sendFile(path.join(frontendPublicRoot, 'index.html'));
  });
}

const server = app.listen(config.port, config.host, () => {
  logger.info('server started', {
    host: config.host,
    port: config.port,
    dbPath: config.dbPath,
    frontendOrigin: config.frontendOrigin || '(not set)',
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
