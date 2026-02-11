import Database from 'better-sqlite3';

export class TrackerDB {
  constructor(path = 'tracker.db') {
    this.db = new Database(path);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL');
    this.db.pragma('foreign_keys = ON');
    this._prepareSchema();
    this._prepareStatements();
  }

  _prepareSchema() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS tracker_state (
        token_address TEXT PRIMARY KEY,
        last_scanned_block INTEGER NOT NULL DEFAULT 0,
        started_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS transfers (
        token_address TEXT NOT NULL,
        tx_hash TEXT NOT NULL,
        block_number INTEGER NOT NULL,
        timestamp INTEGER NOT NULL,
        from_address TEXT NOT NULL,
        to_address TEXT NOT NULL,
        amount TEXT NOT NULL,
        PRIMARY KEY (token_address, tx_hash)
      );

      CREATE INDEX IF NOT EXISTS idx_transfers_token_block ON transfers (token_address, block_number);
      CREATE INDEX IF NOT EXISTS idx_transfers_token_time ON transfers (token_address, timestamp);
      CREATE INDEX IF NOT EXISTS idx_transfers_token_from ON transfers (token_address, from_address);
      CREATE INDEX IF NOT EXISTS idx_transfers_token_to ON transfers (token_address, to_address);

      CREATE TABLE IF NOT EXISTS wallet_stats (
        token_address TEXT NOT NULL,
        wallet_address TEXT NOT NULL,
        current_balance TEXT NOT NULL DEFAULT '0',
        total_in TEXT NOT NULL DEFAULT '0',
        total_out TEXT NOT NULL DEFAULT '0',
        tx_count INTEGER NOT NULL DEFAULT 0,
        last_active_time INTEGER NOT NULL DEFAULT 0,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY (token_address, wallet_address)
      );

      CREATE INDEX IF NOT EXISTS idx_wallet_stats_token_addr ON wallet_stats (token_address, wallet_address);
      CREATE INDEX IF NOT EXISTS idx_wallet_stats_token_last_active ON wallet_stats (token_address, last_active_time);

      CREATE TABLE IF NOT EXISTS wallet_flow_events (
        token_address TEXT NOT NULL,
        wallet_address TEXT NOT NULL,
        tx_hash TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        delta TEXT NOT NULL,
        direction TEXT NOT NULL,
        PRIMARY KEY (token_address, wallet_address, tx_hash, direction)
      );

      CREATE INDEX IF NOT EXISTS idx_flow_token_wallet_time ON wallet_flow_events (token_address, wallet_address, timestamp);
      CREATE INDEX IF NOT EXISTS idx_flow_token_time ON wallet_flow_events (token_address, timestamp);
    `);
  }

  _prepareStatements() {
    this.stmtUpsertState = this.db.prepare(`
      INSERT INTO tracker_state (token_address, last_scanned_block, started_at, updated_at)
      VALUES (@token_address, @last_scanned_block, @started_at, @updated_at)
      ON CONFLICT(token_address) DO UPDATE SET
        last_scanned_block = excluded.last_scanned_block,
        updated_at = excluded.updated_at
    `);

    this.stmtGetState = this.db.prepare(`
      SELECT token_address, last_scanned_block, started_at, updated_at
      FROM tracker_state
      WHERE token_address = ?
    `);

    this.stmtInsertTransfer = this.db.prepare(`
      INSERT OR IGNORE INTO transfers (
        token_address, tx_hash, block_number, timestamp, from_address, to_address, amount
      ) VALUES (
        @token_address, @tx_hash, @block_number, @timestamp, @from_address, @to_address, @amount
      )
    `);

    this.stmtUpsertWallet = this.db.prepare(`
      INSERT INTO wallet_stats (
        token_address, wallet_address, current_balance, total_in, total_out, tx_count, last_active_time, updated_at
      ) VALUES (
        @token_address, @wallet_address, @current_balance, @total_in, @total_out, @tx_count, @last_active_time, @updated_at
      )
      ON CONFLICT(token_address, wallet_address) DO UPDATE SET
        current_balance = excluded.current_balance,
        total_in = excluded.total_in,
        total_out = excluded.total_out,
        tx_count = excluded.tx_count,
        last_active_time = excluded.last_active_time,
        updated_at = excluded.updated_at
    `);

    this.stmtGetWallet = this.db.prepare(`
      SELECT current_balance, total_in, total_out, tx_count, last_active_time
      FROM wallet_stats
      WHERE token_address = ? AND wallet_address = ?
    `);

    this.stmtInsertFlow = this.db.prepare(`
      INSERT OR IGNORE INTO wallet_flow_events (
        token_address, wallet_address, tx_hash, timestamp, delta, direction
      ) VALUES (
        @token_address, @wallet_address, @tx_hash, @timestamp, @delta, @direction
      )
    `);

    this.stmtDeleteFlowsOlderThan = this.db.prepare(`
      DELETE FROM wallet_flow_events
      WHERE token_address = ? AND timestamp < ?
    `);

    this.stmtSelectWalletStats = this.db.prepare(`
      SELECT wallet_address, current_balance, total_in, total_out, tx_count, last_active_time
      FROM wallet_stats
      WHERE token_address = ?
    `);

    this.stmtSelectRecentFlow = this.db.prepare(`
      SELECT wallet_address, timestamp, delta
      FROM wallet_flow_events
      WHERE token_address = ? AND timestamp >= ?
    `);

    this.stmtCountWallets = this.db.prepare(`
      SELECT COUNT(*) AS cnt FROM wallet_stats WHERE token_address = ?
    `);

    this.txApplyTransfer = this.db.transaction((transfer) => {
      const inserted = this.stmtInsertTransfer.run(transfer);
      if (inserted.changes === 0) return false;

      const ts = transfer.timestamp;
      const amount = BigInt(transfer.amount);

      this._applyWalletDelta({
        token: transfer.token_address,
        wallet: transfer.from_address,
        delta: -amount,
        inAmount: 0n,
        outAmount: amount,
        ts,
      });

      this._applyWalletDelta({
        token: transfer.token_address,
        wallet: transfer.to_address,
        delta: amount,
        inAmount: amount,
        outAmount: 0n,
        ts,
      });

      this.stmtInsertFlow.run({
        token_address: transfer.token_address,
        wallet_address: transfer.from_address,
        tx_hash: transfer.tx_hash,
        timestamp: transfer.timestamp,
        delta: (-amount).toString(),
        direction: 'out',
      });

      this.stmtInsertFlow.run({
        token_address: transfer.token_address,
        wallet_address: transfer.to_address,
        tx_hash: transfer.tx_hash,
        timestamp: transfer.timestamp,
        delta: amount.toString(),
        direction: 'in',
      });

      return true;
    });
  }

  _applyWalletDelta({ token, wallet, delta, inAmount, outAmount, ts }) {
    const existing = this.stmtGetWallet.get(token, wallet);
    const current = existing
      ? {
          balance: BigInt(existing.current_balance),
          totalIn: BigInt(existing.total_in),
          totalOut: BigInt(existing.total_out),
          txCount: Number(existing.tx_count),
          lastActive: Number(existing.last_active_time),
        }
      : {
          balance: 0n,
          totalIn: 0n,
          totalOut: 0n,
          txCount: 0,
          lastActive: 0,
        };

    this.stmtUpsertWallet.run({
      token_address: token,
      wallet_address: wallet,
      current_balance: (current.balance + delta).toString(),
      total_in: (current.totalIn + inAmount).toString(),
      total_out: (current.totalOut + outAmount).toString(),
      tx_count: current.txCount + 1,
      last_active_time: Math.max(current.lastActive, ts),
      updated_at: Date.now(),
    });
  }

  applyTransfer(transfer) {
    return this.txApplyTransfer(transfer);
  }

  upsertState(token, lastBlock) {
    const now = Date.now();
    this.stmtUpsertState.run({
      token_address: token,
      last_scanned_block: Number(lastBlock),
      started_at: now,
      updated_at: now,
    });
  }

  getState(token) {
    return this.stmtGetState.get(token) || null;
  }

  setLastScannedBlock(token, block) {
    const state = this.getState(token);
    const now = Date.now();
    this.stmtUpsertState.run({
      token_address: token,
      last_scanned_block: Number(block),
      started_at: state?.started_at ?? now,
      updated_at: now,
    });
  }

  cleanupOldFlowEvents(token, olderThanTs) {
    this.stmtDeleteFlowsOlderThan.run(token, olderThanTs);
  }

  getTopWallets(token, limit = 100) {
    const nowSec = Math.floor(Date.now() / 1000);
    const ts5m = nowSec - 5 * 60;
    const ts1h = nowSec - 60 * 60;

    const stats = this.stmtSelectWalletStats.all(token);
    const recentFlows = this.stmtSelectRecentFlow.all(token, ts1h);

    const net5mMap = new Map();
    const net1hMap = new Map();

    for (const row of recentFlows) {
      const wallet = row.wallet_address;
      const delta = BigInt(row.delta);
      net1hMap.set(wallet, (net1hMap.get(wallet) || 0n) + delta);
      if (Number(row.timestamp) >= ts5m) {
        net5mMap.set(wallet, (net5mMap.get(wallet) || 0n) + delta);
      }
    }

    const sorted = stats.sort((a, b) => {
      const av = BigInt(a.current_balance);
      const bv = BigInt(b.current_balance);
      if (av === bv) return Number(b.last_active_time) - Number(a.last_active_time);
      return av > bv ? -1 : 1;
    });

    return sorted.slice(0, limit).map((r, i) => ({
      rank: i + 1,
      address: r.wallet_address,
      current_balance: String(r.current_balance),
      net_5m: (net5mMap.get(r.wallet_address) || 0n).toString(),
      net_1h: (net1hMap.get(r.wallet_address) || 0n).toString(),
      total_in: String(r.total_in),
      total_out: String(r.total_out),
      tx_count: Number(r.tx_count),
      last_active_time: Number(r.last_active_time),
    }));
  }

  getWalletCount(token) {
    const row = this.stmtCountWallets.get(token);
    return Number(row?.cnt || 0);
  }
}
