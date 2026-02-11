import Database from 'better-sqlite3';

export class TrackerDB {
  constructor(dbPath) {
    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL');
    this.db.pragma('foreign_keys = ON');
    this._initSchema();
    this._prepare();
  }

  _initSchema() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS meta (
        token_address TEXT PRIMARY KEY,
        last_processed_block INTEGER NOT NULL DEFAULT 0,
        running INTEGER NOT NULL DEFAULT 0,
        updated_at INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS token_meta (
        token_address TEXT PRIMARY KEY,
        decimals INTEGER NOT NULL,
        symbol TEXT,
        updated_at INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS transfers (
        token_address TEXT NOT NULL,
        tx_hash TEXT NOT NULL,
        log_index INTEGER NOT NULL,
        block_number INTEGER NOT NULL,
        timestamp INTEGER NOT NULL,
        from_address TEXT NOT NULL,
        to_address TEXT NOT NULL,
        amount TEXT NOT NULL,
        PRIMARY KEY (token_address, tx_hash, log_index)
      );
      CREATE INDEX IF NOT EXISTS idx_transfers_token_block ON transfers(token_address, block_number);
      CREATE INDEX IF NOT EXISTS idx_transfers_token_time ON transfers(token_address, timestamp);
      CREATE INDEX IF NOT EXISTS idx_transfers_token_tx ON transfers(token_address, tx_hash);

      CREATE TABLE IF NOT EXISTS tx_facts (
        token_address TEXT NOT NULL,
        tx_hash TEXT NOT NULL,
        block_number INTEGER NOT NULL,
        timestamp INTEGER NOT NULL,
        classification TEXT NOT NULL,
        spent_virtual_amount TEXT NOT NULL DEFAULT '0',
        received_token_amount TEXT NOT NULL DEFAULT '0',
        PRIMARY KEY (token_address, tx_hash)
      );
      CREATE INDEX IF NOT EXISTS idx_tx_facts_token_time ON tx_facts(token_address, timestamp);

      CREATE TABLE IF NOT EXISTS tx_fact_addresses (
        token_address TEXT NOT NULL,
        tx_hash TEXT NOT NULL,
        wallet_address TEXT NOT NULL,
        role TEXT NOT NULL,
        spent_virtual_amount TEXT NOT NULL DEFAULT '0',
        received_token_amount TEXT NOT NULL DEFAULT '0',
        timestamp INTEGER NOT NULL,
        PRIMARY KEY (token_address, tx_hash, wallet_address, role)
      );
      CREATE INDEX IF NOT EXISTS idx_fact_addr_token_wallet ON tx_fact_addresses(token_address, wallet_address);
      CREATE INDEX IF NOT EXISTS idx_fact_addr_token_time ON tx_fact_addresses(token_address, timestamp);

      CREATE TABLE IF NOT EXISTS addr_stats (
        token_address TEXT NOT NULL,
        wallet_address TEXT NOT NULL,
        cumulative_spent_virtual TEXT NOT NULL DEFAULT '0',
        cumulative_received_token TEXT NOT NULL DEFAULT '0',
        cumulative_sell_virtual TEXT NOT NULL DEFAULT '0',
        cumulative_sold_token TEXT NOT NULL DEFAULT '0',
        buy_trade_count INTEGER NOT NULL DEFAULT 0,
        sell_trade_count INTEGER NOT NULL DEFAULT 0,
        total_tx_count INTEGER NOT NULL DEFAULT 0,
        last_active_time INTEGER NOT NULL DEFAULT 0,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY (token_address, wallet_address)
      );
      CREATE INDEX IF NOT EXISTS idx_addr_stats_spent ON addr_stats(token_address, cumulative_spent_virtual);
      CREATE INDEX IF NOT EXISTS idx_addr_stats_active ON addr_stats(token_address, last_active_time);

      CREATE TABLE IF NOT EXISTS addr_minute_stats (
        token_address TEXT NOT NULL,
        wallet_address TEXT NOT NULL,
        minute_ts INTEGER NOT NULL,
        spent_virtual TEXT NOT NULL DEFAULT '0',
        received_token TEXT NOT NULL DEFAULT '0',
        sold_virtual TEXT NOT NULL DEFAULT '0',
        sold_token TEXT NOT NULL DEFAULT '0',
        tx_count INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (token_address, wallet_address, minute_ts)
      );
      CREATE INDEX IF NOT EXISTS idx_addr_minute_token_time ON addr_minute_stats(token_address, minute_ts);

      CREATE TABLE IF NOT EXISTS minute_buckets (
        token_address TEXT NOT NULL,
        minute_ts INTEGER NOT NULL,
        token_received_sum TEXT NOT NULL DEFAULT '0',
        virtual_spent_sum TEXT NOT NULL DEFAULT '0',
        buy_tx_count INTEGER NOT NULL DEFAULT 0,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY (token_address, minute_ts)
      );
      CREATE INDEX IF NOT EXISTS idx_bucket_token_time ON minute_buckets(token_address, minute_ts);

      CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token_address TEXT NOT NULL,
        triggered_at INTEGER NOT NULL,
        bucket_t INTEGER NOT NULL,
        bucket_t_value TEXT NOT NULL,
        bucket_t1 INTEGER NOT NULL,
        bucket_t1_value TEXT NOT NULL,
        metric_mode TEXT NOT NULL,
        top_addr_netflow TEXT,
        top_addr_spent TEXT,
        cooldown_until INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS block_timestamps (
        block_number INTEGER PRIMARY KEY,
        timestamp INTEGER NOT NULL
      );
    `);
  }

  _prepare() {
    const transferColumns = new Set(
      this.db.prepare('PRAGMA table_info(transfers)').all().map((row) => String(row.name)),
    );
    this.transferHasLogIndex = transferColumns.has('log_index');

    this.stmtGetMeta = this.db.prepare('SELECT * FROM meta WHERE token_address = ?');
    this.stmtUpsertMeta = this.db.prepare(`
      INSERT INTO meta(token_address, last_processed_block, running, updated_at)
      VALUES(@token_address, @last_processed_block, @running, @updated_at)
      ON CONFLICT(token_address) DO UPDATE SET
        last_processed_block = excluded.last_processed_block,
        running = excluded.running,
        updated_at = excluded.updated_at
    `);

    this.stmtGetTokenMeta = this.db.prepare('SELECT * FROM token_meta WHERE token_address = ?');
    this.stmtUpsertTokenMeta = this.db.prepare(`
      INSERT INTO token_meta(token_address, decimals, symbol, updated_at)
      VALUES(@token_address, @decimals, @symbol, @updated_at)
      ON CONFLICT(token_address) DO UPDATE SET
        decimals = excluded.decimals,
        symbol = excluded.symbol,
        updated_at = excluded.updated_at
    `);

    this.stmtInsertTransfer = this.transferHasLogIndex
      ? this.db.prepare(`
          INSERT OR IGNORE INTO transfers(
            token_address, tx_hash, log_index, block_number, timestamp, from_address, to_address, amount
          ) VALUES (
            @token_address, @tx_hash, @log_index, @block_number, @timestamp, @from_address, @to_address, @amount
          )
        `)
      : this.db.prepare(`
          INSERT OR IGNORE INTO transfers(
            token_address, tx_hash, block_number, timestamp, from_address, to_address, amount
          ) VALUES (
            @token_address, @tx_hash, @block_number, @timestamp, @from_address, @to_address, @amount
          )
        `);

    this.stmtInsertTxFact = this.db.prepare(`
      INSERT OR IGNORE INTO tx_facts(
        token_address, tx_hash, block_number, timestamp, classification, spent_virtual_amount, received_token_amount
      ) VALUES(
        @token_address, @tx_hash, @block_number, @timestamp, @classification, @spent_virtual_amount, @received_token_amount
      )
    `);

    this.stmtInsertTxFactAddress = this.db.prepare(`
      INSERT OR IGNORE INTO tx_fact_addresses(
        token_address, tx_hash, wallet_address, role, spent_virtual_amount, received_token_amount, timestamp
      ) VALUES(
        @token_address, @tx_hash, @wallet_address, @role, @spent_virtual_amount, @received_token_amount, @timestamp
      )
    `);

    this.stmtGetAddrStats = this.db.prepare(`
      SELECT * FROM addr_stats WHERE token_address = ? AND wallet_address = ?
    `);

    this.stmtUpsertAddrStats = this.db.prepare(`
      INSERT INTO addr_stats(
        token_address, wallet_address, cumulative_spent_virtual, cumulative_received_token,
        cumulative_sell_virtual, cumulative_sold_token, buy_trade_count, sell_trade_count,
        total_tx_count, last_active_time, updated_at
      ) VALUES(
        @token_address, @wallet_address, @cumulative_spent_virtual, @cumulative_received_token,
        @cumulative_sell_virtual, @cumulative_sold_token, @buy_trade_count, @sell_trade_count,
        @total_tx_count, @last_active_time, @updated_at
      )
      ON CONFLICT(token_address, wallet_address) DO UPDATE SET
        cumulative_spent_virtual = excluded.cumulative_spent_virtual,
        cumulative_received_token = excluded.cumulative_received_token,
        cumulative_sell_virtual = excluded.cumulative_sell_virtual,
        cumulative_sold_token = excluded.cumulative_sold_token,
        buy_trade_count = excluded.buy_trade_count,
        sell_trade_count = excluded.sell_trade_count,
        total_tx_count = excluded.total_tx_count,
        last_active_time = excluded.last_active_time,
        updated_at = excluded.updated_at
    `);

    this.stmtGetMinuteBucket = this.db.prepare(`
      SELECT token_received_sum, virtual_spent_sum, buy_tx_count
      FROM minute_buckets
      WHERE token_address = ? AND minute_ts = ?
    `);

    this.stmtUpsertMinuteBucket = this.db.prepare(`
      INSERT INTO minute_buckets(token_address, minute_ts, token_received_sum, virtual_spent_sum, buy_tx_count, updated_at)
      VALUES(@token_address, @minute_ts, @token_received_sum, @virtual_spent_sum, @buy_tx_count, @updated_at)
      ON CONFLICT(token_address, minute_ts) DO UPDATE SET
        token_received_sum = excluded.token_received_sum,
        virtual_spent_sum = excluded.virtual_spent_sum,
        buy_tx_count = excluded.buy_tx_count,
        updated_at = excluded.updated_at
    `);

    this.stmtGetAddrMinute = this.db.prepare(`
      SELECT spent_virtual, received_token, sold_virtual, sold_token, tx_count
      FROM addr_minute_stats
      WHERE token_address = ? AND wallet_address = ? AND minute_ts = ?
    `);

    this.stmtUpsertAddrMinute = this.db.prepare(`
      INSERT INTO addr_minute_stats(
        token_address, wallet_address, minute_ts, spent_virtual, received_token, sold_virtual, sold_token, tx_count
      ) VALUES(
        @token_address, @wallet_address, @minute_ts, @spent_virtual, @received_token, @sold_virtual, @sold_token, @tx_count
      )
      ON CONFLICT(token_address, wallet_address, minute_ts) DO UPDATE SET
        spent_virtual = excluded.spent_virtual,
        received_token = excluded.received_token,
        sold_virtual = excluded.sold_virtual,
        sold_token = excluded.sold_token,
        tx_count = excluded.tx_count
    `);

    this.stmtRecentBuckets = this.db.prepare(`
      SELECT minute_ts, token_received_sum, virtual_spent_sum
      FROM minute_buckets
      WHERE token_address = ? AND minute_ts >= ?
      ORDER BY minute_ts DESC
    `);

    this.stmtInsertSignal = this.db.prepare(`
      INSERT INTO signals(
        token_address, triggered_at, bucket_t, bucket_t_value, bucket_t1, bucket_t1_value,
        metric_mode, top_addr_netflow, top_addr_spent, cooldown_until
      ) VALUES(
        @token_address, @triggered_at, @bucket_t, @bucket_t_value, @bucket_t1, @bucket_t1_value,
        @metric_mode, @top_addr_netflow, @top_addr_spent, @cooldown_until
      )
    `);

    this.stmtLastSignal = this.db.prepare(`
      SELECT * FROM signals WHERE token_address = ? ORDER BY id DESC LIMIT 1
    `);

    this.stmtAddrStatsForToken = this.db.prepare(`
      SELECT wallet_address, cumulative_spent_virtual, cumulative_received_token,
             cumulative_sell_virtual, cumulative_sold_token, total_tx_count, last_active_time
      FROM addr_stats
      WHERE token_address = ?
    `);

    this.stmtAddrMinuteWindow = this.db.prepare(`
      SELECT wallet_address, minute_ts, spent_virtual, received_token, sold_virtual, sold_token, tx_count
      FROM addr_minute_stats
      WHERE token_address = ? AND minute_ts >= ?
    `);

    this.stmtGetBlockTs = this.db.prepare('SELECT timestamp FROM block_timestamps WHERE block_number = ?');
    this.stmtUpsertBlockTs = this.db.prepare(`
      INSERT INTO block_timestamps(block_number, timestamp)
      VALUES(?, ?)
      ON CONFLICT(block_number) DO UPDATE SET timestamp = excluded.timestamp
    `);

    this.stmtRecentTransfers = this.transferHasLogIndex
      ? this.db.prepare(`
          SELECT token_address, tx_hash, log_index, block_number, timestamp, from_address, to_address, amount
          FROM transfers
          WHERE token_address = ?
          ORDER BY block_number DESC, log_index DESC
          LIMIT ?
        `)
      : this.db.prepare(`
          SELECT token_address, tx_hash, 0 AS log_index, block_number, timestamp, from_address, to_address, amount
          FROM transfers
          WHERE token_address = ?
          ORDER BY block_number DESC
          LIMIT ?
        `);

    this.stmtAllTransfersForToken = this.db.prepare(`
      SELECT tx_hash, block_number, timestamp, from_address, to_address, amount
      FROM transfers
      WHERE token_address = ?
      ORDER BY block_number DESC
    `);

    this.stmtRecentFacts = this.db.prepare(`
      SELECT token_address, tx_hash, block_number, timestamp, classification, spent_virtual_amount, received_token_amount
      FROM tx_facts
      WHERE token_address = ?
      ORDER BY block_number DESC
      LIMIT ?
    `);

    this.txInsertTransfers = this.db.transaction((rows) => {
      const newTxHashes = new Set();
      let inserted = 0;
      for (const row of rows) {
        const result = this.stmtInsertTransfer.run(row);
        if (result.changes > 0) {
          inserted += 1;
          newTxHashes.add(row.tx_hash);
        }
      }
      return { inserted, newTxHashes: Array.from(newTxHashes) };
    });

    this.txApplyFact = this.db.transaction((fact) => {
      const factInsert = this.stmtInsertTxFact.run(fact);
      if (factInsert.changes === 0) return false;

      const minuteTs = Math.floor(fact.timestamp / 60) * 60;
      for (const addr of fact.addressFacts) {
        this.stmtInsertTxFactAddress.run({
          token_address: fact.token_address,
          tx_hash: fact.tx_hash,
          wallet_address: addr.wallet_address,
          role: addr.role,
          spent_virtual_amount: addr.spent_virtual_amount,
          received_token_amount: addr.received_token_amount,
          timestamp: fact.timestamp,
        });

        const existing = this.stmtGetAddrStats.get(fact.token_address, addr.wallet_address);
        const current = existing
          ? {
              spent: BigInt(existing.cumulative_spent_virtual),
              recv: BigInt(existing.cumulative_received_token),
              sellVirtual: BigInt(existing.cumulative_sell_virtual),
              soldToken: BigInt(existing.cumulative_sold_token),
              buyTrades: Number(existing.buy_trade_count),
              sellTrades: Number(existing.sell_trade_count),
              totalTx: Number(existing.total_tx_count),
              lastActive: Number(existing.last_active_time),
            }
          : {
              spent: 0n,
              recv: 0n,
              sellVirtual: 0n,
              soldToken: 0n,
              buyTrades: 0,
              sellTrades: 0,
              totalTx: 0,
              lastActive: 0,
            };

        const spentDelta = BigInt(addr.spent_virtual_amount || '0');
        const recvDelta = BigInt(addr.received_token_amount || '0');
        const soldVirtualDelta = BigInt(addr.sold_virtual_amount || '0');
        const soldTokenDelta = BigInt(addr.sold_token_amount || '0');

        this.stmtUpsertAddrStats.run({
          token_address: fact.token_address,
          wallet_address: addr.wallet_address,
          cumulative_spent_virtual: (current.spent + spentDelta).toString(),
          cumulative_received_token: (current.recv + recvDelta).toString(),
          cumulative_sell_virtual: (current.sellVirtual + soldVirtualDelta).toString(),
          cumulative_sold_token: (current.soldToken + soldTokenDelta).toString(),
          buy_trade_count: current.buyTrades + (spentDelta > 0n || recvDelta > 0n ? 1 : 0),
          sell_trade_count: current.sellTrades + (soldVirtualDelta > 0n || soldTokenDelta > 0n ? 1 : 0),
          total_tx_count: current.totalTx + 1,
          last_active_time: Math.max(current.lastActive, fact.timestamp),
          updated_at: Date.now(),
        });

        const minuteExisting = this.stmtGetAddrMinute.get(fact.token_address, addr.wallet_address, minuteTs);
        const minuteCurrent = minuteExisting
          ? {
              spent: BigInt(minuteExisting.spent_virtual),
              recv: BigInt(minuteExisting.received_token),
              soldVirtual: BigInt(minuteExisting.sold_virtual),
              soldToken: BigInt(minuteExisting.sold_token),
              tx: Number(minuteExisting.tx_count),
            }
          : {
              spent: 0n,
              recv: 0n,
              soldVirtual: 0n,
              soldToken: 0n,
              tx: 0,
            };

        this.stmtUpsertAddrMinute.run({
          token_address: fact.token_address,
          wallet_address: addr.wallet_address,
          minute_ts: minuteTs,
          spent_virtual: (minuteCurrent.spent + spentDelta).toString(),
          received_token: (minuteCurrent.recv + recvDelta).toString(),
          sold_virtual: (minuteCurrent.soldVirtual + soldVirtualDelta).toString(),
          sold_token: (minuteCurrent.soldToken + soldTokenDelta).toString(),
          tx_count: minuteCurrent.tx + 1,
        });
      }

      if (fact.classification === 'suspected_buy') {
        const minute = this.stmtGetMinuteBucket.get(fact.token_address, minuteTs);
        const tokenSum = (BigInt(minute?.token_received_sum || '0') + BigInt(fact.received_token_amount)).toString();
        const virtualSum = (BigInt(minute?.virtual_spent_sum || '0') + BigInt(fact.spent_virtual_amount)).toString();
        const buyCount = Number(minute?.buy_tx_count || 0) + 1;

        this.stmtUpsertMinuteBucket.run({
          token_address: fact.token_address,
          minute_ts: minuteTs,
          token_received_sum: tokenSum,
          virtual_spent_sum: virtualSum,
          buy_tx_count: buyCount,
          updated_at: Date.now(),
        });
      }

      return true;
    });
  }

  close() {
    this.db.close();
  }

  getMeta(tokenAddress) {
    return this.stmtGetMeta.get(tokenAddress) || null;
  }

  setMeta(tokenAddress, partial) {
    const prev = this.getMeta(tokenAddress);
    this.stmtUpsertMeta.run({
      token_address: tokenAddress,
      last_processed_block: partial.last_processed_block ?? prev?.last_processed_block ?? 0,
      running: partial.running ?? prev?.running ?? 0,
      updated_at: Date.now(),
    });
  }

  insertTransfers(rows) {
    return this.txInsertTransfers(rows);
  }

  applyTxFact(fact) {
    return this.txApplyFact(fact);
  }

  upsertTokenDecimals(tokenAddress, decimals, symbol = null) {
    this.stmtUpsertTokenMeta.run({
      token_address: tokenAddress,
      decimals,
      symbol,
      updated_at: Date.now(),
    });
  }

  getTokenMeta(tokenAddress) {
    return this.stmtGetTokenMeta.get(tokenAddress) || null;
  }

  setBlockTimestamp(blockNumber, timestamp) {
    this.stmtUpsertBlockTs.run(Number(blockNumber), Number(timestamp));
  }

  getBlockTimestamp(blockNumber) {
    const row = this.stmtGetBlockTs.get(Number(blockNumber));
    return row ? Number(row.timestamp) : null;
  }

  getRecentBuckets(tokenAddress, fromMinuteTs) {
    return this.stmtRecentBuckets.all(tokenAddress, fromMinuteTs);
  }

  getRecentTransfers(tokenAddress, limit = 50) {
    return this.stmtRecentTransfers.all(tokenAddress, limit);
  }

  getRecentFacts(tokenAddress, limit = 50) {
    return this.stmtRecentFacts.all(tokenAddress, limit);
  }

  getHolderLeaderboardFromTransfers(tokenAddress, limit = 50, nowSec = Math.floor(Date.now() / 1000)) {
    const rows = this.stmtAllTransfersForToken.all(tokenAddress);
    const map = new Map();
    const ts5m = nowSec - 5 * 60;

    const getState = (addr) => {
      const key = String(addr || '').toLowerCase();
      if (!map.has(key)) {
        map.set(key, {
          address: key,
          balance: 0n,
          totalIn: 0n,
          totalOut: 0n,
          net5m: 0n,
          txCount: 0,
          lastActive: 0,
        });
      }
      return map.get(key);
    };

    for (const row of rows) {
      const amount = BigInt(row.amount || '0');
      const ts = Number(row.timestamp || 0);
      const from = getState(row.from_address);
      const to = getState(row.to_address);

      from.balance -= amount;
      from.totalOut += amount;
      from.txCount += 1;
      from.lastActive = Math.max(from.lastActive, ts);
      if (ts >= ts5m) from.net5m -= amount;

      to.balance += amount;
      to.totalIn += amount;
      to.txCount += 1;
      to.lastActive = Math.max(to.lastActive, ts);
      if (ts >= ts5m) to.net5m += amount;
    }

    const all = Array.from(map.values()).filter((x) => x.address && x.address !== '0x0000000000000000000000000000000000000000');

    const whales = all
      .slice()
      .sort((a, b) => (a.balance === b.balance ? b.lastActive - a.lastActive : a.balance > b.balance ? -1 : 1))
      .slice(0, limit)
      .map((x) => ({
        address: x.address,
        cumulative_spent_virtual: x.totalOut.toString(),
        cumulative_received_token: x.totalIn.toString(),
        total_tx_count: x.txCount,
        last_active_time: x.lastActive,
        holder_balance: x.balance.toString(),
      }));

    const heat = all
      .slice()
      .sort((a, b) => (a.net5m === b.net5m ? b.txCount - a.txCount : a.net5m > b.net5m ? -1 : 1))
      .slice(0, limit)
      .map((x) => ({
        address: x.address,
        netflow: x.net5m.toString(),
        netflow_token: x.net5m.toString(),
        netflow_virtual: '0',
        trades_5m: x.txCount,
      }));

    const active = all
      .slice()
      .sort((a, b) => (b.txCount === a.txCount ? b.lastActive - a.lastActive : b.txCount - a.txCount))
      .slice(0, limit)
      .map((x) => ({
        address: x.address,
        trades_5m: x.txCount,
        spent_virtual_5m: x.totalOut.toString(),
        received_token_5m: x.totalIn.toString(),
        last_active_time: x.lastActive,
      }));

    return { whales, heat, active, source: 'transfers' };
  }

  getLastSignal(tokenAddress) {
    return this.stmtLastSignal.get(tokenAddress) || null;
  }

  insertSignal(signal) {
    this.stmtInsertSignal.run(signal);
  }

  getLeaderboard(tokenAddress, metricMode, limit = 50, nowMinuteTs) {
    const windowStart = nowMinuteTs - 4 * 60;
    const whales = this.stmtAddrStatsForToken
      .all(tokenAddress)
      .sort((a, b) => {
        const av = BigInt(a.cumulative_spent_virtual || '0');
        const bv = BigInt(b.cumulative_spent_virtual || '0');
        if (av === bv) {
          return Number(b.last_active_time || 0) - Number(a.last_active_time || 0);
        }
        return av > bv ? -1 : 1;
      })
      .slice(0, limit)
      .map((row) => ({
        address: row.wallet_address,
        cumulative_spent_virtual: row.cumulative_spent_virtual,
        cumulative_received_token: row.cumulative_received_token,
        last_active_time: Number(row.last_active_time),
        total_tx_count: Number(row.total_tx_count),
      }));

    const agg = new Map();
    const rows = this.stmtAddrMinuteWindow.all(tokenAddress, windowStart);
    for (const row of rows) {
      const addr = row.wallet_address;
      const current = agg.get(addr) || {
        spentVirtual: 0n,
        receivedToken: 0n,
        soldVirtual: 0n,
        soldToken: 0n,
        trades: 0,
        lastMinute: 0,
      };

      current.spentVirtual += BigInt(row.spent_virtual || '0');
      current.receivedToken += BigInt(row.received_token || '0');
      current.soldVirtual += BigInt(row.sold_virtual || '0');
      current.soldToken += BigInt(row.sold_token || '0');
      current.trades += Number(row.tx_count || 0);
      current.lastMinute = Math.max(current.lastMinute, Number(row.minute_ts || 0));

      agg.set(addr, current);
    }

    const heat = Array.from(agg.entries())
      .map(([address, v]) => {
        const netToken = v.receivedToken - v.soldToken;
        const netVirtual = v.spentVirtual - v.soldVirtual;
        return {
          address,
          netflow: metricMode === 'virtual_spent' ? netVirtual.toString() : netToken.toString(),
          netflow_virtual: netVirtual.toString(),
          netflow_token: netToken.toString(),
          trades_5m: v.trades,
        };
      })
      .sort((a, b) => {
        const av = BigInt(a.netflow);
        const bv = BigInt(b.netflow);
        if (av === bv) return b.trades_5m - a.trades_5m;
        return av > bv ? -1 : 1;
      })
      .slice(0, limit);

    const active = Array.from(agg.entries())
      .map(([address, v]) => ({
        address,
        trades_5m: v.trades,
        spent_virtual_5m: v.spentVirtual.toString(),
        received_token_5m: v.receivedToken.toString(),
        last_active_time: v.lastMinute,
      }))
      .sort((a, b) => {
        if (a.trades_5m === b.trades_5m) return b.last_active_time - a.last_active_time;
        return b.trades_5m - a.trades_5m;
      })
      .slice(0, limit);

    return { whales, heat, active };
  }
}
