# Virtuals 实时决策系统技术文档

## 1. 系统概览

本系统用于在 Base 链上对单个 Token 进行实时追踪，输出分钟级交易信号、持仓统计、我的钱包成本/回本市值，并通过 SSE 推送给前端。

- 前端：`frontend/public/index.html`（静态页面，Vercel 部署）
- 后端：`backend/src/server.js`（Express + SSE，Linux 常驻）
- 存储：SQLite（`better-sqlite3`）

## 2. 数据来源

系统只使用链上原始数据，不依赖第三方聚合指标。

### 2.1 RPC 来源

- `BASE_HTTP_RPC`：主数据入口（必填）
- `BASE_WS_RPC`：新区块订阅（可选）

### 2.2 链上原始事件

1. Token `Transfer` 日志（目标 token）
2. 交易回执中的 `Transfer` 日志（用于识别买卖行为）
3. 区块时间戳 `block.timestamp`
4. 合约元数据：`decimals`、`totalSupply`

### 2.3 对手币（默认 VIRTUAL）

通过 `VIRTUAL_CA` + `COUNTERPARTY_TOKEN_ADDRESSES` 组成对手币集合，回执中若同时出现目标 token 与对手币流动，则参与买卖分类。

## 3. 数据处理链路

### 3.1 扫描与增量更新

1. 启动追踪后先回扫历史区块（`BACKFILL_BLOCKS`）
2. 进入实时轮询 + 重放近期区块（`REPLAY_RECENT_BLOCKS`）降低漏数风险
3. 区间拉取采用分片（`LOG_CHUNK_SIZE`）
4. 所有写入以唯一键去重，支持重放幂等

### 3.2 事实表生成（Tx Facts）

每个交易回执计算地址维度增量：

- `tokenDelta[address]`
- `virtualDelta[address]`

判定逻辑：

- 买：`tokenDelta > 0` 且 `virtualDelta < 0`
- 卖：`tokenDelta < 0` 且 `virtualDelta > 0`

交易分类：

- `suspected_buy`
- `suspected_sell`
- `transfer_or_unknown`

### 3.3 分钟桶聚合

仅 `suspected_buy` 会写入 `minute_buckets`：

- `token_received_sum`
- `virtual_spent_sum`
- `buy_tx_count`

系统支持两种统计口径：

- `token_received`
- `virtual_spent`

## 4. 核心计算逻辑

## 4.1 规则信号（两分钟突破）

在最新闭合的两根分钟桶上计算：

- `t` 分钟值 `v0`
- `t+1` 分钟值 `v1`

触发条件：

- `v0 > RULE_FIRST_THRESHOLD`（默认 3000）
- `v1 > RULE_SECOND_THRESHOLD`（默认 6000）

命中后进入冷却：

- `cooldown_until = now + RULE_COOLDOWN_MINUTES * 60`

### 4.2 买入税率与卖出税率

- 动态买入税：`buy_tax_pct = clamp(99 - floor((now - launch_start_time)/60), 1, 99)`
- 卖出税：`SELL_TAX_PCT`（默认 1）

### 4.3 持仓与成本

对地址统计：

- `balance = cumulative_in - cumulative_out`
- `cost_per_token = spent_virtual_sum / received_token_sum`（收到 token > 0 时）
- `breakeven_price = cost_per_token / (1 - sell_tax)`
- `breakeven_mcap = breakeven_price * total_supply`

### 4.4 我的钱包（多地址）

支持 `myWallets[]` 与定向回扫。钱包统计由两部分叠加：

1. 转账层：`transfer_in/out`, `total_in/out`
2. 买卖事实层：`spent_virtual_sum`, `received_token_sum`, `sold_token_sum`, `received_virtual_sum`

核心字段：

- `cost_per_token`
- `effective_cost_per_token`
- `breakeven_mcap`
- `balance`

### 4.5 曲线（EMV / RMV）

- `RMV`：我的钱包总回本市值（常量序列）
- `EMV`：从买单事实推导出的分钟市值并按当时买入税反推

## 5. 数据库表说明（关键）

- `transfers`：原始转账日志
- `tx_facts`：交易级事实
- `tx_fact_addresses`：地址级买卖事实
- `minute_buckets`：分钟聚合
- `signals`：信号记录
- `my_wallets` / `my_wallet_stats`：我的钱包配置与统计
- `special_flows`：特殊地址流向
- `meta`：任务状态（是否运行、已处理区块）
- `token_meta`：decimals/totalSupply 缓存
- `block_timestamps`：区块时间缓存

## 6. 对外接口与实时推送

### 6.1 HTTP API

- `GET /healthz`：健康检查（返回 `ok`）
- `GET /api/status`：当前完整快照
- `POST /api/track/start`：开始追踪
- `POST /api/track/stop`：停止追踪
- `POST /api/settings/mode`：切换口径
- `POST /api/settings/runtime`：更新运行参数

### 6.2 SSE

`GET /api/events`

- `Content-Type: text/event-stream`
- `Cache-Control: no-cache, no-transform`
- `X-Accel-Buffering: no`
- 15 秒心跳，支持 Nginx 长连接反代

## 7. 精度与一致性策略

1. 链上金额落库为字符串（bigint），展示时按 decimals 转换
2. 关键写入使用 `INSERT OR IGNORE` + 主键/唯一键保证幂等
3. 重放近期区块抵抗 RPC 抖动和短时漏日志
4. 规则基于“闭合分钟桶”，避免未闭合分钟噪声

## 8. 已知边界

1. `suspected_buy/sell` 属于回执启发式分类，不等同于 DEX 路由级真相还原
2. 当链上出现复杂路由/聚合器拆单时，单笔行为可能被归类为 `transfer_or_unknown`
3. 若 `VIRTUAL_CA` 或对手币配置错误，买卖识别与分钟聚合会偏差

