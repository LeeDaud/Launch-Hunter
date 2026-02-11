# Virtuals On-Chain Realtime Decision Assistant (Base)

Node.js + viem + Express + SQLite + SSE implementation for single-token realtime tracking (with structure ready for multi-token extension).

## What it does

- Input a token CA in frontend and click `开始追踪`.
- Backend automatically stops previous task and switches to new token task.
- Cold-start backfills recent blocks (`BACKFILL_BLOCKS`, default 8000), then enters realtime incremental mode.
- Data source uses only on-chain raw data:
  - token `Transfer` logs via `eth_getLogs`
  - tx receipt logs to classify buy/sell/unknown and detect counterparty flow
- Supports two metric modes:
  - `token_received`: minute sum of received token in suspected-buy facts (tax-after actual received)
  - `virtual_spent`: minute sum of spent VIRTUAL in suspected-buy facts
- Supports `My Wallets` (multi-address):
  - parses wallet-involved tx from receipts
  - computes spent/received/cost/effective cost/breakeven mcap per wallet and total
  - supports directed historical backfill for wallet addresses
- Rule engine (core):
  - if minute bucket `t > 3000` and `t+1 > 6000`, trigger entry signal
  - cooldown per token default 10 minutes
- Realtime SSE push for overview + leaderboard + signal alerts.

## Required config

Copy `.env.example` to `.env`, then fill:

- `BASE_HTTP_RPC` (Base HTTP RPC)
- `BASE_WS_RPC` (Base WS RPC, optional but recommended)
- `VIRTUAL_TOKEN_ADDRESS` (Base VIRTUAL contract address)

Optional:

- `COUNTERPARTY_TOKEN_ADDRESSES` comma-separated extra counterparties
- `METRIC_MODE` = `token_received` or `virtual_spent`
- rule thresholds/cooldown
- `MY_WALLET_FROM_BLOCK` directed backfill start block (0 means auto)
- `MY_WALLET_MAX_BACKFILL_BLOCKS` auto backfill length when `MY_WALLET_FROM_BLOCK=0`

## Run

```bash
npm install
npm start
```

Open `http://localhost:5000` if you launched with `PORT=5000`.

## API

- `POST /api/track/start` body:
  - `token_address`
  - `myWallets: string[]` (optional)
  - `my_wallet_from_block` (optional)
- `POST /api/track/stop`
- `POST /api/settings/mode` body `{ "metric_mode": "token_received|virtual_spent" }`
- `POST /api/settings/runtime` supports:
  - `launch_start_time`
  - `wallet_address`
  - `sell_tax_pct`
  - `myWallets`
  - `my_wallet_from_block`
- `GET /api/status`
- `GET /api/events` (SSE)

## Storage schema

Core tables requested in requirement are included:

- `transfers`: raw transfer logs (strong dedup on `token_address + tx_hash + log_index`)
- `tx_facts`: per tx interpreted fact (`classification`, `spent_virtual_amount`, `received_token_amount`)
- `addr_stats`: per address cumulative stats
- `meta`: token runtime status + `last_processed_block`
- `my_wallets`: configured wallet list
- `my_wallet_stats`: incremental per-wallet stats:
  - `spent_virtual_sum`, `received_token_sum`, `sold_token_sum`, `received_virtual_sum`
  - `transfer_in_sum`, `transfer_out_sum`, `total_in_sum`, `total_out_sum`

Extended tables for performance/stability:

- `addr_minute_stats`: per address minute buckets for 5m views
- `minute_buckets`: per token minute aggregation
- `signals`: rule trigger records
- `token_meta`: decimals cache
- `block_timestamps`: `blockNumber -> timestamp` cache (avoid repeated block calls)

## Stability details

- Incremental `eth_getLogs` with chunked ranges (`LOG_CHUNK_SIZE`)
- Replay pull of recent blocks every loop (`REPLAY_RECENT_BLOCKS`) to reduce miss risk
- Directed wallet backfill runs in queue and only processes transfers involving configured `myWallets`
- All writes idempotent by unique keys; replay cannot double-count
- RPC retry with exponential backoff + jitter
- SSE updates throttled (`UPDATE_THROTTLE_MS`, default 2s)

## Rule semantics implemented

For latest closed minute pair (`t`, `t+1`):

- `bucket(t) > RULE_FIRST_THRESHOLD`
- `bucket(t+1) > RULE_SECOND_THRESHOLD`

then emit signal and enter cooldown (`RULE_COOLDOWN_MINUTES`).

Signal record keeps:

- trigger time
- bucket values
- top netflow address and top cumulative-spent address at trigger time

## Verify rule correctness

### Method 1: replay historical block range

Set env and run:

```bash
# Windows PowerShell
$env:REPLAY_TOKEN="0xYourToken"
$env:REPLAY_FROM_BLOCK="<start>"
$env:REPLAY_TO_BLOCK="<end>"
npm run replay
```

Script scans only that range and prints current signal state.

### Method 2: lower thresholds for fast trigger check

In `.env` temporarily set:

- `RULE_FIRST_THRESHOLD=1`
- `RULE_SECOND_THRESHOLD=2`
- `RULE_COOLDOWN_MINUTES=1`

Then start tracking an active token. You should quickly observe front-end signal popup and cooldown state changes.

### Validate My Wallet cost/breakeven correctness

1. In UI, fill `My Wallets` with one or more addresses and click `Apply Wallets`.
2. Start tracking token and wait for wallet directed backfill completion.
3. Confirm `My Wallets` panel fields change with incoming txs:
   - buy: `spent V` and `received T` increase
   - sell: `sold T` and `received V` increase
4. Validate formulas:
   - `costPerToken = spentVirtualSum / receivedTokenSum`
   - `effectiveCostPerToken = costPerToken / (1 - sellTaxPct)`
   - `breakevenMcap = effectiveCostPerToken * totalSupply`
5. Total row should equal sum of wallet detail rows.

## Project structure

```text
.
├─ public/
│  └─ index.html
├─ scripts/
│  └─ replay-range.js
├─ src/
│  ├─ abi.js
│  ├─ api.js
│  ├─ config.js
│  ├─ db.js
│  ├─ logger.js
│  ├─ rpc.js
│  ├─ rules.js
│  ├─ server.js
│  ├─ tracker-manager.js
│  ├─ tracker-task.js
│  └─ utils.js
├─ .env.example
├─ package.json
└─ README.md
```

## Notes

- Classification is tx-receipt heuristic based on co-occurrence of token transfer and counterparty transfer (default VIRTUAL).
- Unknown txs are still stored in raw `transfers`, but only `suspected_buy` contributes to minute tax-after volume metric.
- Amounts are stored as bigint strings in DB; display converts by decimals when needed.
