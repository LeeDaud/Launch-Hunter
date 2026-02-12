export class RuleEngine {
  constructor({ thresholdFirstMinute, thresholdSecondMinute, cooldownMinutes }) {
    this.thresholdFirstMinute = thresholdFirstMinute;
    this.thresholdSecondMinute = thresholdSecondMinute;
    this.cooldownMinutes = cooldownMinutes;
  }

  evaluate({ tokenAddress, metricMode, nowMinuteTs, minuteValueMap, lastSignal, leaderboard }) {
    const latestClosed = nowMinuteTs - 60;
    const t = latestClosed - 60;
    const t1 = latestClosed;

    const v0 = Number(minuteValueMap.get(t) || 0);
    const v1 = Number(minuteValueMap.get(t1) || 0);

    const cooldownUntil = Number(lastSignal?.cooldown_until || 0);
    if (cooldownUntil > Math.floor(Date.now() / 1000)) {
      return {
        triggered: false,
        reason: 'cooldown',
        cooldownUntil,
        pair: { t, t1, v0, v1 },
      };
    }

    const passed = v0 > this.thresholdFirstMinute && v1 > this.thresholdSecondMinute;
    if (!passed) {
      return {
        triggered: false,
        reason: 'threshold_not_met',
        cooldownUntil,
        pair: { t, t1, v0, v1 },
      };
    }

    const nowSec = Math.floor(Date.now() / 1000);
    const newCooldown = nowSec + this.cooldownMinutes * 60;

    return {
      triggered: true,
      signal: {
        token_address: tokenAddress,
        triggered_at: nowSec,
        bucket_t: t,
        bucket_t_value: String(v0),
        bucket_t1: t1,
        bucket_t1_value: String(v1),
        metric_mode: metricMode,
        top_addr_netflow: leaderboard?.heat?.[0]?.address || null,
        top_addr_spent: leaderboard?.whales?.[0]?.address || null,
        cooldown_until: newCooldown,
      },
      pair: { t, t1, v0, v1 },
      cooldownUntil: newCooldown,
    };
  }
}
