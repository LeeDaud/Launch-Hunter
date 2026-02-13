import { isAddress } from 'viem';
import { config } from '../config.js';
import { logger } from '../logger.js';
import { safeAddress } from '../utils.js';

export class ProtocolService {
  constructor({ tracker }) {
    this.tracker = tracker;
  }

  buildPayloadFromSnapshot(snapshot) {
    const token = safeAddress(snapshot?.token || '');
    const protocol = snapshot?.protocol_activity || {};
    const specialRows = Array.isArray(snapshot?.special_stats) ? snapshot.special_stats : [];
    const addrRows = Array.isArray(protocol?.by_address) ? protocol.by_address : [];
    const potential = Array.isArray(protocol?.potential_addresses) ? protocol.potential_addresses : [];

    const thresholdV = Number(protocol.alert_threshold_virtual || config.protocolAlertInflowVirtual || 5000);
    const in1m = Number(protocol.protocol_inflow_1m || 0);
    const out1m = Number(protocol.protocol_outflow_1m || 0);
    const in5m = Number(protocol.protocol_inflow_5m || 0);
    const out5m = Number(protocol.protocol_outflow_5m || 0);
    const net1m = Number(protocol.net_flow_1m || 0);
    const net5m = Number(protocol.net_flow_5m || 0);
    const alertLevel = in1m >= thresholdV * 2 ? 'critical' : in1m >= thresholdV ? 'warn' : 'none';
    const alertMessage = alertLevel === 'none'
      ? 'No abnormal inflow'
      : `1m inflow ${in1m.toFixed(4)} V exceeds threshold ${thresholdV}`;

    if (config.debugTrackingLogs) {
      logger.info('protocol monitor payload', {
        token,
        net1m,
        net5m,
        in1m,
        specialCount: specialRows.length,
        addressCount: addrRows.length,
        potentialCount: potential.length,
      });
    }

    return {
      token,
      summary: {
        net1mV: net1m,
        net1mInV: in1m,
        net1mOutV: out1m,
        net5mV: net5m,
        net5mInV: in5m,
        net5mOutV: out5m,
        executor5m: Number(protocol.executor_interactions_5m || 0),
        sellWallBalanceT: Number(protocol.sell_wall_token_balance || 0),
        launchPoolBalanceT: Number(protocol.launch_pool_token_balance || 0),
        alert: {
          level: alertLevel,
          thresholdV,
          message: alertMessage,
        },
        potential: potential.length,
        updatedAt: Number(protocol.updated_at || Math.floor(Date.now() / 1000)),
      },
      specialFlows: specialRows.map((r) => ({
        label: String(r.label || ''),
        category: String(r.category || ''),
        v5m: Number(r.net_virtual_5m || 0),
        v1h: Number(r.net_virtual_1h || 0),
        cumV: Number(r.net_virtual_cum || 0),
        t5m: Number(r.net_token_5m || 0),
        cumT: Number(r.net_token_cum || 0),
      })),
      addresses: addrRows.map((r) => ({
        address: String(r.address || ''),
        type: String(r.type || ''),
        net1mV: Number(r.net_virtual_1m || 0),
        net5mV: Number(r.net_virtual_5m || 0),
        net5mT: Number(r.net_token_5m || 0),
        interactions5m: Number(r.interaction_count_5m || 0),
      })),
      potentialAddresses: potential.map((r) => ({
        address: String(r.address || ''),
        type: String(r.type || 'potential_protocol_address'),
        interactions: Number(r.interactions || 0),
        distinctTokens: Number(r.distinct_tokens || 0),
        virtualBalance: Number(r.virtual_balance || 0),
      })),
    };
  }

  getProtocolMonitor(tokenAddress) {
    const reqToken = safeAddress(tokenAddress || '');
    if (!isAddress(reqToken)) return { ok: false, error: 'invalid token' };
    const snapshot = this.tracker.getSnapshot();
    if (!snapshot || safeAddress(snapshot.token) !== reqToken) {
      return {
        ok: true,
        token: reqToken,
        summary: {
          net1mV: 0,
          net1mInV: 0,
          net1mOutV: 0,
          net5mV: 0,
          net5mInV: 0,
          net5mOutV: 0,
          executor5m: 0,
          sellWallBalanceT: 0,
          launchPoolBalanceT: 0,
          alert: { level: 'none', thresholdV: Number(config.protocolAlertInflowVirtual || 5000), message: 'No active token tracking' },
          potential: 0,
          updatedAt: Math.floor(Date.now() / 1000),
        },
        specialFlows: [],
        addresses: [],
        potentialAddresses: [],
      };
    }
    return { ok: true, ...this.buildPayloadFromSnapshot(snapshot) };
  }
}
