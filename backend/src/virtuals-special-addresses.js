export const SPECIAL_ADDRESSES = [
  {
    address: '0x0b3e328455c4059eeb9e3f84b5543f74e24e7e1b',
    label: '$VIRTUAL Token (Base)',
    type: 'quote_token',
  },
  {
    address: '0xe2890629ef31b32132003c02b29a50a025deee8a',
    label: 'Sell Wall / Lock Wallet',
    type: 'protocol_lock',
  },
  {
    address: '0xf8dd39c71a278fe9f4377d009d7627ef140f809e',
    label: 'Protocol Executor',
    type: 'protocol_executor',
  },
];

export function normalizeSpecialAddressRows(rows) {
  return (rows || [])
    .map((r) => ({
      address: String(r.address || '').trim().toLowerCase(),
      label: String(r.label || 'Unknown'),
      type: String(r.type || r.category || 'unknown'),
      category: String(r.category || r.type || 'unknown'),
    }))
    .filter((r) => /^0x[a-f0-9]{40}$/.test(r.address));
}
