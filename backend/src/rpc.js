import { createPublicClient, http, webSocket } from 'viem';
import { base } from 'viem/chains';
import { ERC20_ABI, UNISWAP_V2_PAIR_ABI } from './abi.js';
import { config } from './config.js';
import { logger } from './logger.js';
import { sleep } from './utils.js';

export class RpcService {
  constructor() {
    this.httpClient = createPublicClient({
      chain: base,
      transport: http(config.baseHttpRpc),
    });

    this.wsClient = config.baseWsRpc
      ? createPublicClient({ chain: base, transport: webSocket(config.baseWsRpc) })
      : null;
  }

  async withRetry(label, fn) {
    const maxRetries = Math.max(1, config.maxRpcRetries);
    const baseWait = Math.max(50, config.rpcBaseBackoffMs);
    let lastErr = null;

    for (let i = 0; i < maxRetries; i += 1) {
      try {
        return await fn();
      } catch (err) {
        lastErr = err;
        const wait = baseWait * Math.pow(2, i) + Math.floor(Math.random() * 120);
        logger.warn('rpc failed', { label, attempt: i + 1, wait, error: String(err?.message || err) });
        if (i < maxRetries - 1) await sleep(wait);
      }
    }
    throw lastErr;
  }

  async getBlockNumber() {
    return this.withRetry('getBlockNumber', async () => this.httpClient.getBlockNumber());
  }

  async getLogs(params) {
    return this.withRetry('getLogs', async () => this.httpClient.getLogs(params));
  }

  async getBlock(blockNumber) {
    return this.withRetry('getBlock', async () => this.httpClient.getBlock({ blockNumber }));
  }

  async getTransactionReceipt(hash) {
    return this.withRetry('getTransactionReceipt', async () => this.httpClient.getTransactionReceipt({ hash }));
  }

  async readDecimals(tokenAddress) {
    return this.withRetry('readDecimals', async () => this.httpClient.readContract({
      address: tokenAddress,
      abi: ERC20_ABI,
      functionName: 'decimals',
    }));
  }

  async readTotalSupply(tokenAddress) {
    return this.withRetry('readTotalSupply', async () => this.httpClient.readContract({
      address: tokenAddress,
      abi: ERC20_ABI,
      functionName: 'totalSupply',
    }));
  }

  async readPairToken0(pairAddress) {
    return this.withRetry('readPairToken0', async () => this.httpClient.readContract({
      address: pairAddress,
      abi: UNISWAP_V2_PAIR_ABI,
      functionName: 'token0',
    }));
  }

  async readPairToken1(pairAddress) {
    return this.withRetry('readPairToken1', async () => this.httpClient.readContract({
      address: pairAddress,
      abi: UNISWAP_V2_PAIR_ABI,
      functionName: 'token1',
    }));
  }

  async readPairReserves(pairAddress) {
    return this.withRetry('readPairReserves', async () => this.httpClient.readContract({
      address: pairAddress,
      abi: UNISWAP_V2_PAIR_ABI,
      functionName: 'getReserves',
    }));
  }

  async readContract({ address, abi, functionName, args = [] }) {
    return this.withRetry(`readContract:${functionName}`, async () => this.httpClient.readContract({
      address,
      abi,
      functionName,
      args,
    }));
  }

  subscribeNewHeads(onBlock) {
    if (!this.wsClient || !config.useWsHead) return null;
    try {
      const unwatch = this.wsClient.watchBlocks({
        onBlock: (block) => {
          const bn = Number(block.number);
          if (Number.isFinite(bn)) onBlock(bn);
        },
        onError: (err) => logger.warn('watch block error', { error: String(err?.message || err) }),
      });
      logger.info('ws newHeads subscribed');
      return unwatch;
    } catch (err) {
      logger.warn('ws subscribe failed, polling only', { error: String(err?.message || err) });
      return null;
    }
  }
}
