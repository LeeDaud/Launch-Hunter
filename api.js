import express from 'express';

export function createApi({ tracker, db }) {
  const router = express.Router();
  const sseClients = new Set();

  function broadcast(eventName, data) {
    const payload = `event: ${eventName}\ndata: ${JSON.stringify(data)}\n\n`;
    for (const res of sseClients) {
      res.write(payload);
    }
  }

  tracker.onUpdate = (payload) => {
    broadcast('update', payload);
  };

  router.get('/health', (_req, res) => {
    res.json({ ok: true, now: Date.now() });
  });

  router.get('/status', (_req, res) => {
    res.json(tracker.getStatus());
  });

  router.post('/track/start', async (req, res) => {
    try {
      const token = String(req.body?.token || '').trim().toLowerCase();
      if (!token) {
        return res.status(400).json({ error: 'token is required' });
      }

      const status = await tracker.start(token);
      const leaderboard = db.getTopWallets(token, 100);
      const payload = {
        type: 'started',
        token,
        status,
        summary: {
          wallet_count: db.getWalletCount(token),
          updated_at: Date.now(),
        },
        leaderboard,
      };
      broadcast('update', payload);

      res.json({ ok: true, status });
    } catch (err) {
      res.status(400).json({ error: String(err.message || err) });
    }
  });

  router.post('/track/stop', async (_req, res) => {
    await tracker.stop();
    const payload = {
      type: 'stopped',
      token: null,
      status: tracker.getStatus(),
      summary: {
        wallet_count: 0,
        updated_at: Date.now(),
      },
      leaderboard: [],
    };
    broadcast('update', payload);

    res.json({ ok: true, status: tracker.getStatus() });
  });

  router.get('/leaderboard', (req, res) => {
    const token = String(req.query.token || '').trim().toLowerCase();
    if (!token) {
      return res.status(400).json({ error: 'token query is required' });
    }
    const limit = Number(req.query.limit || 100);
    const rows = db.getTopWallets(token, Number.isFinite(limit) ? limit : 100);
    res.json({ token, count: rows.length, rows });
  });

  router.get('/events', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();

    sseClients.add(res);
    res.write(`event: connected\ndata: ${JSON.stringify({ ok: true, ts: Date.now() })}\n\n`);

    const heartbeat = setInterval(() => {
      res.write(`event: ping\ndata: ${Date.now()}\n\n`);
    }, 15000);

    req.on('close', () => {
      clearInterval(heartbeat);
      sseClients.delete(res);
    });
  });

  return router;
}
