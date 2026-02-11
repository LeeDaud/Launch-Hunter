import { formatUnits } from 'viem';

export function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function minuteFloor(tsSec) {
  return Math.floor(tsSec / 60) * 60;
}

export function nowSec() {
  return Math.floor(Date.now() / 1000);
}

export function safeAddress(v) {
  return String(v || '').toLowerCase();
}

export function formatAmount(amount, decimals, fixed = 6) {
  try {
    const raw = formatUnits(BigInt(amount), decimals);
    const [intPart, fracPart = ''] = raw.split('.');
    if (!fracPart) return intPart;
    return `${intPart}.${fracPart.slice(0, fixed)}`;
  } catch {
    return '0';
  }
}

export function toFloat(amount, decimals) {
  return Number(formatUnits(BigInt(amount), decimals));
}

export function bigintMax(a, b) {
  return a > b ? a : b;
}
