// Fake data only (plan rule). The same seed always gives the same 1,000 records:
// 930 good and unique, 50 bad (10 of each kind), and 20 exact copies of good ones.
const BAD_KINDS = ['no-id', 'negative-amount', 'bad-currency', 'bad-timestamp', 'not-an-object'];

function random(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function generateRecords(seed = 1) {
  const rand = random(seed);
  const pick = (list) => list[Math.floor(rand() * list.length)];
  const good = (n) => ({
    id: `rec-${String(n).padStart(4, '0')}`,
    customer: `cust-${1 + Math.floor(rand() * 200)}`,
    amount: Math.round((1 + rand() * 999) * 100) / 100,
    currency: pick(['USD', 'EUR', 'GBP']),
    timestamp: new Date(Date.UTC(2026, 0, 1) + Math.floor(rand() * 86400 * 1000 * 90)).toISOString(),
  });

  const valid = Array.from({ length: 930 }, (_, i) => good(i + 1));
  const bad = [];
  BAD_KINDS.forEach((kind, k) => {
    for (let i = 0; i < 10; i += 1) {
      const base = good(1001 + k * 10 + i);
      if (kind === 'no-id') delete base.id;
      if (kind === 'negative-amount') base.amount = -base.amount;
      if (kind === 'bad-currency') base.currency = 'XXX';
      if (kind === 'bad-timestamp') base.timestamp = 'not a date';
      bad.push(kind === 'not-an-object' ? pick(['garbage', null, 42, ['x']]) : base);
    }
  });
  const duplicates = Array.from({ length: 20 }, (_, i) => ({ ...valid[i * 40] }));

  const records = [...valid, ...bad, ...duplicates];
  for (let i = records.length - 1; i > 0; i -= 1) {
    const j = Math.floor(rand() * (i + 1));
    [records[i], records[j]] = [records[j], records[i]];
  }
  return { records, expected: { valid: 930, rejected: 50, duplicate: 20 } };
}
