import express from 'express';
import { isAddress } from 'viem';
import { ProtocolService } from './services/protocol-service.js';

export function createApi({ tracker, db }) {
  const router = express.Router();
  const clients = new Set();
  const etherscanApiKey = String(process.env.ETHERSCAN_API_KEY || '').trim();
  const protocolService = new ProtocolService({ tracker });

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
    if (payload?.snapshot?.token) {
      const protocolPayload = protocolService.buildPayloadFromSnapshot(payload.snapshot);
      broadcast('protocol_update', protocolPayload);
    }
  });

  router.get('/health', (_req, res) => {
    res.json({ ok: true, now: Date.now() });
  });

  router.get('/status', (_req, res) => {
    res.json({ ok: true, status: tracker.getStatus(), snapshot: tracker.getSnapshot() });
  });

  router.get('/protocol-monitor', (req, res) => {
    const token = String(req.query?.token || '').trim().toLowerCase();
    const result = protocolService.getProtocolMonitor(token);
    if (!result.ok) return res.status(400).json(result);
    return res.json(result);
  });

  router.get('/protocol-activity', (req, res) => {
    const token = String(req.query?.token || '').trim().toLowerCase();
    const result = protocolService.getProtocolMonitor(token);
    if (!result.ok) return res.status(400).json(result);
    return res.json({
      ok: true,
      token: result.token,
      summary: result.summary,
      addresses: result.addresses,
      potentialAddresses: result.potentialAddresses,
      deprecated: true,
    });
  });

  router.get('/special-protocol-flows', (req, res) => {
    const token = String(req.query?.token || '').trim().toLowerCase();
    const result = protocolService.getProtocolMonitor(token);
    if (!result.ok) return res.status(400).json(result);
    return res.json({
      ok: true,
      token: result.token,
      specialFlows: result.specialFlows,
      deprecated: true,
    });
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

  router.get('/explorer/tokentx', async (req, res) => {
    if (!etherscanApiKey) {
      return res.status(500).json({ ok: false, error: 'ETHERSCAN_API_KEY is not configured' });
    }

    const address = String(req.query?.address || '').trim().toLowerCase();
    const contractAddress = String(req.query?.contractaddress || '').trim().toLowerCase();
    const chainId = String(req.query?.chainid || process.env.ETHERSCAN_CHAIN_ID || '8453').trim();
    const startBlock = String(req.query?.startblock || '0').trim();
    const endBlock = String(req.query?.endblock || '99999999').trim();
    const page = String(req.query?.page || '1').trim();
    const offset = String(req.query?.offset || '100').trim();
    const sort = String(req.query?.sort || 'desc').trim().toLowerCase();

    if (!isAddress(address)) return res.status(400).json({ ok: false, error: 'invalid address' });
    if (contractAddress && !isAddress(contractAddress)) return res.status(400).json({ ok: false, error: 'invalid contractaddress' });
    if (!/^\d+$/.test(chainId)) return res.status(400).json({ ok: false, error: 'invalid chainid' });
    if (!/^\d+$/.test(startBlock) || !/^\d+$/.test(endBlock) || !/^\d+$/.test(page) || !/^\d+$/.test(offset)) {
      return res.status(400).json({ ok: false, error: 'invalid pagination or block range' });
    }
    if (sort !== 'asc' && sort !== 'desc') return res.status(400).json({ ok: false, error: 'sort must be asc or desc' });

    const url = new URL('https://api.etherscan.io/v2/api');
    url.searchParams.set('apikey', etherscanApiKey);
    url.searchParams.set('chainid', chainId);
    url.searchParams.set('module', 'account');
    url.searchParams.set('action', 'tokentx');
    url.searchParams.set('address', address);
    if (contractAddress) url.searchParams.set('contractaddress', contractAddress);
    url.searchParams.set('startblock', startBlock);
    url.searchParams.set('endblock', endBlock);
    url.searchParams.set('page', page);
    url.searchParams.set('offset', offset);
    url.searchParams.set('sort', sort);

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);
    try {
      const upstreamRes = await fetch(url, { method: 'GET', signal: controller.signal });
      const upstream = await upstreamRes.json();
      return res.status(upstreamRes.ok ? 200 : 502).json({
        ok: upstreamRes.ok,
        upstream_status: upstreamRes.status,
        request: {
          chainid: chainId,
          address,
          contractaddress: contractAddress || null,
          startblock: startBlock,
          endblock: endBlock,
          page,
          offset,
          sort,
        },
        upstream,
      });
    } catch (err) {
      return res.status(502).json({ ok: false, error: String(err?.message || err) });
    } finally {
      clearTimeout(timeout);
    }
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
      const tokenStartBlock = req.body?.token_start_block;
      const spotPairAddress = req.body?.spot_pair_address;
      if (launchStartTime != null || walletAddress != null || sellTaxPct != null || myWallets != null || myWalletFromBlock != null || tokenStartBlock != null || spotPairAddress != null) {
        tracker.updateRuntimeSettings({ launchStartTime, walletAddress, sellTaxPct, myWallets, myWalletFromBlock, tokenStartBlock, spotPairAddress });
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
      tokenStartBlock: req.body?.token_start_block,
      spotPairAddress: req.body?.spot_pair_address,
      curveWindowMinutes: req.body?.curve_window_minutes,
    });
    res.json({ ok: true, runtime, snapshot: tracker.getSnapshot() });
  });

  router.get('/events', (req, res) => {
    req.socket.setKeepAlive(true);
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache, no-transform');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders();

    clients.add(res);
    send(res, 'connected', { ok: true, ts: Date.now() });

    const snapshot = tracker.getSnapshot();
    if (snapshot) send(res, 'update', { type: 'snapshot', ts: Date.now(), snapshot });
    if (snapshot?.token) send(res, 'protocol_update', protocolService.buildPayloadFromSnapshot(snapshot));

    const timer = setInterval(() => {
      res.write(': heartbeat\n\n');
      send(res, 'ping', { ts: Date.now() });
    }, 15000);

    req.on('close', () => {
      clearInterval(timer);
      clients.delete(res);
    });
  });

  return router;
}
