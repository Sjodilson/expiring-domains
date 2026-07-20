// DNS-uppslag (rolling). Kollar A, MX, NS för varje domän.

import { promises as dns } from 'node:dns';
import { runPool } from './util.mjs';

const CONCURRENCY = 40;
const TIMEOUT_MS = 5_000;

dns.setServers(['1.1.1.1', '8.8.8.8', '9.9.9.9']);

async function withTimeout(promise, ms) {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('timeout')), ms);
    promise.then(
      (v) => { clearTimeout(t); resolve(v); },
      (e) => { clearTimeout(t); reject(e); }
    );
  });
}

async function tryResolve(fn, domain) {
  try {
    const r = await withTimeout(fn(domain), TIMEOUT_MS);
    return Array.isArray(r) ? r.length > 0 : !!r;
  } catch {
    return false;
  }
}

async function lookupOne(domain) {
  const [a, mx, ns] = await Promise.all([
    tryResolve(dns.resolve4, domain),
    tryResolve(dns.resolveMx, domain),
    tryResolve(dns.resolveNs, domain)
  ]);
  return { a, mx, ns };
}

// Kollar om nysläppta domäner registrerats igen (aktiva DNS-records = tagen).
// Redan tagna domäner kollas inte om.
export async function checkAvailability(released, cache, { log }) {
  const todo = released.filter((r) => r.taken !== 1).map((r) => r.domain);
  if (todo.length === 0) return { checked: 0, taken: 0 };
  log(`  Tillgänglighet: kollar ${todo.length.toLocaleString('sv-SE')} nysläppta domäner`);
  let taken = 0;
  await runPool(todo, CONCURRENCY, async (domain) => {
    const r = await lookupOne(domain);
    const isTaken = r.a || r.mx || r.ns;
    if (isTaken) taken++;
    cache.setAvailability(domain, isTaken);
  });
  log(`  Tillgänglighet: ${taken} nyregistrerade av ${todo.length} kollade`);
  return { checked: todo.length, taken };
}

export async function enrichDns(domains, cache, { maxPerRun, log }) {
  if (domains.length === 0) return { processed: 0 };
  const todo = domains.slice(0, maxPerRun);
  log(`  DNS: kör ${todo.length.toLocaleString('sv-SE')} av ${domains.length.toLocaleString('sv-SE')} domäner som behöver uppdatering`);

  let counts = { a: 0, mx: 0, ns: 0 };
  await runPool(
    todo,
    CONCURRENCY,
    async (domain) => {
      const r = await lookupOne(domain);
      cache.setDns(domain, r.a, r.mx, r.ns);
      if (r.a) counts.a++;
      if (r.mx) counts.mx++;
      if (r.ns) counts.ns++;
    },
    (done, total) => {
      if (done % 1000 === 0 || done === total) {
        log(`    DNS: ${done}/${total}`);
      }
    }
  );
  log(`  DNS-resultat: ${counts.a} A, ${counts.mx} MX, ${counts.ns} NS`);
  return { processed: todo.length, ...counts };
}
