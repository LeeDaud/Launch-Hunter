import Database from 'better-sqlite3';

function nowMs() {
  return Date.now();
}

export class TrackerDB {
  constructor(dbPath) {
    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL');
    this.db.pragma('foreign_keys = ON');
    this._initSchema();
    this._prepare();
  }

  _columnNames(table) {
    return new Set(this.db.prepare(`PRAGMA table_info(${table})`).all().map((r) => String(r.name)));
  }

  _ensureColumn(table, column, ddl) {
    const cols = this._columnNames(table);
    if (cols.has(column)) return;
    this.db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${ddl}`);
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
        decimals INTEGER NOT NULL DEFAULT 18,
        total_supply TEXT NOT NULL DEFAULT '0',
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

      CREATE TABLE IF NOT EXISTS tx_fact_addresses (
        token_address TEXT NOT NULL,
        tx_hash TEXT NOT NULL,
        wallet_address TEXT NOT NULL,
        role TEXT NOT NULL,
        spent_virtual_amount TEXT NOT NULL DEFAULT '0',
        received_token_amount TEXT NOT NULL DEFAULT '0',
        sold_virtual_amount TEXT NOT NULL DEFAULT '0',
        sold_token_amount TEXT NOT NULL DEFAULT '0',
        timestamp INTEGER NOT NULL,
        PRIMARY KEY (token_address, tx_hash, wallet_address, role)
      );

      CREATE TABLE IF NOT EXISTS minute_buckets (
        token_address TEXT NOT NULL,
        minute_ts INTEGER NOT NULL,
        token_received_sum TEXT NOT NULL DEFAULT '0',
        virtual_spent_sum TEXT NOT NULL DEFAULT '0',
        buy_tx_count INTEGER NOT NULL DEFAULT 0,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY (token_address, minute_ts)
      );

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

      CREATE TABLE IF NOT EXISTS special_flows (
        token_address TEXT NOT NULL,
        tx_hash TEXT NOT NULL,
        block_number INTEGER NOT NULL,
        timestamp INTEGER NOT NULL,
        special_address TEXT NOT NULL,
        label TEXT NOT NULL,
        category TEXT NOT NULL,
        asset TEXT NOT NULL,
        direction TEXT NOT NULL,
        amount TEXT NOT NULL,
        UNIQUE(token_address, tx_hash, special_address, asset, direction, amount)
      );

      CREATE TABLE IF NOT EXISTS block_timestamps (
        block_number INTEGER PRIMARY KEY,
        timestamp INTEGER NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_transfers_token_block ON transfers(token_address, block_number);
      CREATE INDEX IF NOT EXISTS idx_transfers_token_time ON transfers(token_address, timestamp);
      CREATE INDEX IF NOT EXISTS idx_transfers_token_tx ON transfers(token_address, tx_hash);
      CREATE INDEX IF NOT EXISTS idx_facts_token_time ON tx_facts(token_address, timestamp);
      CREATE INDEX IF NOT EXISTS idx_fact_addr_token_time ON tx_fact_addresses(token_address, timestamp);
      CREATE INDEX IF NOT EXISTS idx_bucket_token_time ON minute_buckets(token_address, minute_ts);
      CREATE INDEX IF NOT EXISTS idx_special_token_time ON special_flows(token_address, timestamp);
    `);

    this._ensureColumn('token_meta', 'total_supply', "TEXT NOT NULL DEFAULT '0'");
    this._ensureColumn('tx_fact_addresses', 'sold_virtual_amount', "TEXT NOT NULL DEFAULT '0'");
    this._ensureColumn('tx_fact_addresses', 'sold_token_amount', "TEXT NOT NULL DEFAULT '0'");
  }

  _prepare() {
    const transferCols = this._columnNames('transfers');
    this.transferHasLogIndex = transferCols.has('log_index');

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
      INSERT INTO token_meta(token_address, decimals, total_supply, symbol, updated_at)
      VALUES(@token_address, @decimals, @total_supply, @symbol, @updated_at)
      ON CONFLICT(token_address) DO UPDATE SET
        decimals = excluded.decimals,
        total_supply = excluded.total_supply,
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
        token_address, tx_hash, wallet_address, role, spent_virtual_amount, received_token_amount, sold_virtual_amount, sold_token_amount, timestamp
      ) VALUES(
        @token_address, @tx_hash, @wallet_address, @role, @spent_virtual_amount, @received_token_amount, @sold_virtual_amount, @sold_token_amount, @timestamp
      )
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

    this.stmtRecentBuckets = this.db.prepare(`
      SELECT minute_ts, token_received_sum, virtual_spent_sum, buy_tx_count
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

    this.stmtInsertSpecial = this.db.prepare(`
      INSERT OR IGNORE INTO special_flows(
        token_address, tx_hash, block_number, timestamp, special_address, label, category, asset, direction, amount
      ) VALUES(
        @token_address, @tx_hash, @block_number, @timestamp, @special_address, @label, @category, @asset, @direction, @amount
      )
    `);

    this.stmtSpecialSince = this.db.prepare(`
      SELECT * FROM special_flows
      WHERE token_address = ? AND timestamp >= ?
      ORDER BY timestamp DESC
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

    this.stmtAllTransfers = this.db.prepare(`
      SELECT token_address, tx_hash, block_number, timestamp, from_address, to_address, amount
      FROM transfers
      WHERE token_address = ?
      ORDER BY block_number DESC
    `);

    this.stmtFactAddresses = this.db.prepare(`
      SELECT * FROM tx_fact_addresses
      WHERE token_address = ?
      ORDER BY timestamp DESC
    `);

    this.stmtFacts = this.db.prepare(`
      SELECT * FROM tx_facts
      WHERE token_address = ?
      ORDER BY block_number DESC
      LIMIT ?
    `);

    this.stmtRecentBuyFacts = this.db.prepare(`
      SELECT * FROM tx_facts
      WHERE token_address = ? AND classification = 'suspected_buy' AND timestamp >= ?
      ORDER BY timestamp ASC
    `);

    this.stmtGetBlockTs = this.db.prepare('SELECT timestamp FROM block_timestamps WHERE block_number = ?');
    this.stmtUpsertBlockTs = this.db.prepare(`
      INSERT INTO block_timestamps(block_number, timestamp)
      VALUES(?, ?)
      ON CONFLICT(block_number) DO UPDATE SET timestamp = excluded.timestamp
    `);

    this.txInsertTransfers = this.db.transaction((rows) => {
      const newTxHashes = new Set();
      const insertedRows = [];
      for (const row of rows) {
        const ret = this.stmtInsertTransfer.run(row);
        if (ret.changes > 0) {
          newTxHashes.add(row.tx_hash);
          insertedRows.push(row);
        }
      }
      return { inserted: insertedRows.length, newTxHashes: Array.from(newTxHashes), insertedRows };
    });

    this.txApplyFact = this.db.transaction((fact) => {
      const ret = this.stmtInsertTxFact.run(fact);
      if (ret.changes === 0) return false;

      const minuteTs = Math.floor(fact.timestamp / 60) * 60;
      for (const a of fact.addressFacts || []) {
        this.stmtInsertTxFactAddress.run({
          token_address: fact.token_address,
          tx_hash: fact.tx_hash,
          wallet_address: a.wallet_address,
          role: a.role,
          spent_virtual_amount: a.spent_virtual_amount || '0',
          received_token_amount: a.received_token_amount || '0',
          sold_virtual_amount: a.sold_virtual_amount || '0',
          sold_token_amount: a.sold_token_amount || '0',
          timestamp: fact.timestamp,
        });
      }

      if (fact.classification === 'suspected_buy') {
        const m = this.stmtGetMinuteBucket.get(fact.token_address, minuteTs);
        this.stmtUpsertMinuteBucket.run({
          token_address: fact.token_address,
          minute_ts: minuteTs,
          token_received_sum: (BigInt(m?.token_received_sum || '0') + BigInt(fact.received_token_amount || '0')).toString(),
          virtual_spent_sum: (BigInt(m?.virtual_spent_sum || '0') + BigInt(fact.spent_virtual_amount || '0')).toString(),
          buy_tx_count: Number(m?.buy_tx_count || 0) + 1,
          updated_at: nowMs(),
        });
      }

      return true;
    });

    this.txInsertSpecial = this.db.transaction((events) => {
      let count = 0;
      for (const e of events || []) {
        const r = this.stmtInsertSpecial.run(e);
        if (r.changes > 0) count += 1;
      }
      return count;
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
      updated_at: nowMs(),
    });
  }

  getTokenMeta(tokenAddress) {
    return this.stmtGetTokenMeta.get(tokenAddress) || null;
  }

  upsertTokenMeta(tokenAddress, decimals, totalSupply = '0', symbol = null) {
    this.stmtUpsertTokenMeta.run({
      token_address: tokenAddress,
      decimals: Number(decimals || 18),
      total_supply: String(totalSupply || '0'),
      symbol,
      updated_at: nowMs(),
    });
  }

  insertTransfers(rows) {
    return this.txInsertTransfers(rows);
  }

  applyTxFact(fact) {
    return this.txApplyFact(fact);
  }

  insertSpecialFlows(events) {
    return this.txInsertSpecial(events);
  }

  getRecentBuckets(tokenAddress, fromMinuteTs) {
    return this.stmtRecentBuckets.all(tokenAddress, fromMinuteTs);
  }

  getLastSignal(tokenAddress) {
    return this.stmtLastSignal.get(tokenAddress) || null;
  }

  insertSignal(signal) {
    this.stmtInsertSignal.run(signal);
  }

  getRecentTransfers(tokenAddress, limit = 50) {
    return this.stmtRecentTransfers.all(tokenAddress, limit);
  }

  getRecentFacts(tokenAddress, limit = 50) {
    return this.stmtFacts.all(tokenAddress, limit);
  }

  getAllTransfers(tokenAddress) {
    return this.stmtAllTransfers.all(tokenAddress);
  }

  getAllFactAddresses(tokenAddress) {
    return this.stmtFactAddresses.all(tokenAddress);
  }

  getRecentBuyFacts(tokenAddress, fromTs) {
    return this.stmtRecentBuyFacts.all(tokenAddress, fromTs);
  }

  getSpecialFlowsSince(tokenAddress, fromTs) {
    return this.stmtSpecialSince.all(tokenAddress, fromTs);
  }

  setBlockTimestamp(blockNumber, timestamp) {
    this.stmtUpsertBlockTs.run(Number(blockNumber), Number(timestamp));
  }

  getBlockTimestamp(blockNumber) {
    const row = this.stmtGetBlockTs.get(Number(blockNumber));
    return row ? Number(row.timestamp) : null;
  }
}
