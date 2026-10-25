import { validateRecord } from './validate.mjs';

// The pipeline's rules, kept apart from AWS so they can be tested with no account.
// The store is the one thing AWS will supply later (DynamoDB): `has(id)` and
// `put(id)`, where put returns false when the id was already stored.
export function createMemoryStore() {
  const seen = new Set();
  return {
    async has(id) { return seen.has(id); },
    async put(id) {
      if (seen.has(id)) return false;
      seen.add(id);
      return true;
    },
  };
}

function withTimeout(promise, ms) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error('timeout')), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

export async function processRecords(records, { store, handler = async () => {}, maxAttempts = 3, timeoutMs = 5000 }) {
  const counts = { valid: 0, rejected: 0, duplicate: 0, deadLettered: 0 };
  const results = [];
  const deadLetter = [];

  for (const record of records) {
    const id = record !== null && typeof record === 'object' ? record.id : undefined;
    const path = ['received'];
    const finish = (status, extra = {}) => {
      path.push(status);
      results.push({ id, status, path, attempts: 0, ...extra });
    };

    const check = validateRecord(record);
    if (!check.ok) {
      counts.rejected += 1;
      finish('rejected', { errors: check.errors });
      continue;
    }
    path.push('validated');

    if (await store.has(id)) {
      counts.duplicate += 1;
      finish('duplicate');
      continue;
    }

    let attempts = 0;
    let failure = null;
    while (attempts < maxAttempts) {
      attempts += 1;
      try {
        await withTimeout(Promise.resolve(handler(record)), timeoutMs);
        failure = null;
        break;
      } catch (error) {
        failure = error.message;
      }
    }

    if (failure !== null) {
      counts.deadLettered += 1;
      deadLetter.push({ id, attempts, reason: failure, record });
      finish('dead-lettered', { attempts, reason: failure });
      continue;
    }

    path.push('handled');
    if (await store.put(id)) {
      counts.valid += 1;
      finish('valid', { attempts });
    } else {
      counts.duplicate += 1;
      finish('duplicate', { attempts });
    }
  }
  return { counts, results, deadLetter };
}
