import express from 'express';
import { isAddress } from 'viem';

export function createApi({ tracker, db }) {
  const router = express.Router();
  const clients = new Set();

  const send = (res, event, data) => {
    res.write(`event: ${event}\n`);
    res.write(`data: ${JSON.stringify(data)}\n\n`);
  };

  const broadcast = (event, data) => {
    for (const res of clients) {
      try { send(res, event, data); } catch { clients.delete(res); }
    }
  };

  tracker.subscribe((payload) => {
    broadcast('update', payload);
    if (payload?.snapshot?.signal_state?.passed_now) broadcast('signal', payload);
  });

  router.get('/health', (_req, res) => {
    res.json({ ok: true, now: Date.now() });
  });

  router.get('/status', (_req, res) => {
    res.json({ ok: true, status: tracker.getStatus(), snapshot: tracker.getSnapshot() });
  });

  router.get('/records/transfers', (req, res) => {
    const token = String(req.query?.token || '').trim().toLowerCase();
    if (!isAddress(token)) return res.status(400).json({ ok: false, error: 'invalid token query param' });
    const limit = Math.max(1, Math.min(1000, Number(req.query?.limit || 50)));
    res.json({ ok: true, token, rows: db.getRecentTransfers(token, limit) });
  });

  router.get('/records/facts', (req, res) => {
    const token = String(req.query?.token || '').trim().toLowerCase();
    if (!isAddress(token)) return res.status(400).json({ ok: false, error: 'invalid token query param' });
    const limit = Math.max(1, Math.min(1000, Number(req.query?.limit || 50)));
    res.json({ ok: true, token, rows: db.getRecentFacts(token, limit) });
  });

  router.post('/track/start', async (req, res) => {
    try {
      const token = String(req.body?.token_address || req.body?.token || '').trim().toLowerCase();
      if (!isAddress(token)) return res.status(400).json({ ok: false, error: 'invalid token_address' });

      const launchStartTime = req.body?.launch_start_time;
      const walletAddress = req.body?.wallet_address;
      const sellTaxPct = req.body?.sell_tax_pct;
      const myWallets = req.body?.myWallets;
      const myWalletFromBlock = req.body?.my_wallet_from_block;
      if (launchStartTime != null || walletAddress != null || sellTaxPct != null || myWallets != null || myWalletFromBlock != null) {
        tracker.updateRuntimeSettings({ launchStartTime, walletAddress, sellTaxPct, myWallets, myWalletFromBlock });
      }

      const status = await tracker.start(token);
      res.json({ ok: true, status, snapshot: tracker.getSnapshot() });
    } catch (err) {
      res.status(500).json({ ok: false, error: String(err?.message || err) });
    }
  });

  router.post('/track/stop', async (_req, res) => {
    await tracker.stop();
    res.json({ ok: true, status: tracker.getStatus() });
  });

  router.post('/settings/mode', (req, res) => {
    const mode = String(req.body?.metric_mode || '').trim();
    if (mode !== 'token_received' && mode !== 'virtual_spent') {
      return res.status(400).json({ ok: false, error: 'metric_mode must be token_received or virtual_spent' });
    }
    tracker.setMetricMode(mode);
    res.json({ ok: true, metric_mode: mode, snapshot: tracker.getSnapshot() });
  });

  router.post('/settings/runtime', (req, res) => {
    const runtime = tracker.updateRuntimeSettings({
      launchStartTime: req.body?.launch_start_time,
      walletAddress: req.body?.wallet_address,
      sellTaxPct: req.body?.sell_tax_pct,
      myWallets: req.body?.myWallets,
      myWalletFromBlock: req.body?.my_wallet_from_block,
      curveWindowMinutes: req.body?.curve_window_minutes,
    });
    res.json({ ok: true, runtime, snapshot: tracker.getSnapshot() });
  });

  router.get('/events', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();

    clients.add(res);
    send(res, 'connected', { ok: true, ts: Date.now() });

    const snapshot = tracker.getSnapshot();
    if (snapshot) send(res, 'update', { type: 'snapshot', ts: Date.now(), snapshot });

    const timer = setInterval(() => send(res, 'ping', { ts: Date.now() }), 15000);

    req.on('close', () => {
      clearInterval(timer);
      clients.delete(res);
    });
  });

  return router;
}
