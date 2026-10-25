// What counts as a good record. Bad input is reported by field name, never thrown.
const CURRENCIES = new Set(['USD', 'EUR', 'GBP']);

export function validateRecord(record) {
  if (record === null || typeof record !== 'object' || Array.isArray(record)) {
    return { ok: false, errors: ['record'] };
  }
  const errors = [];
  if (typeof record.id !== 'string' || !/^rec-\d{4,}$/.test(record.id)) errors.push('id');
  if (typeof record.customer !== 'string' || record.customer === '') errors.push('customer');
  if (typeof record.amount !== 'number' || !Number.isFinite(record.amount) || record.amount <= 0 || record.amount > 10000) {
    errors.push('amount');
  }
  if (!CURRENCIES.has(record.currency)) errors.push('currency');
  if (typeof record.timestamp !== 'string' || Number.isNaN(Date.parse(record.timestamp))) errors.push('timestamp');
  return { ok: errors.length === 0, errors };
}
