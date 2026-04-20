// Wayback Machine CDX-uppslag (rolling).

import { runPool } from './util.mjs';

const CDX_URL = 'https://web.archive.org/cdx/search/cdx';
const CONCURRENCY = 6;
const TIMEOUT_MS = 15_000;

async function lookupOne(domain) {
  // Hämta första snapshot + räkna alla. limit=-1 visar nyaste; limit=1 äldsta.
  // För räkning använder vi showNumPages eller helt enkelt limit=10000 fl=timestamp.
  // Praktiskt: fråga efter äldsta + ett enkelt count via collapse.
  const u = new URL(CDX_URL);
  u.searchParams.set('url', domain);
  u.searchParams.set('output', 'json');
  u.searchParams.set('fl', 'timestamp');
  u.searchParams.set('collapse', 'timestamp:6'); // gruppera per månad
  u.searchParams.set('limit', '500');

  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(u, {
      signal: ctrl.signal,
      headers: { 'User-Agent': 'expiring-domains-builder/2.0' }
    });
    if (!res.ok) {
      // 4xx från CDX = ofta inga snapshots, behandla som 0
      if (res.status === 404) return { first: null, count: 0 };
      throw new Error(`HTTP ${res.status}`);
    }
    const arr = await res.json();
    if (!Array.isArray(arr) || arr.length <= 1) return { first: null, count: 0 };
    // arr[0] = headers, resten = data
    const data = arr.slice(1);
    let first = null;
    for (const row of data) {
      const ts = row[0];
      if (typeof ts === 'string' && ts.length >= 4) {
        if (!first || ts < first) first = ts;
      }
    }
    return { first, count: data.length };
  } finally {
    clearTimeout(t);
  }
}

export async function enrichWayback(domains, cache, { maxPerRun, log }) {
  if (domains.length === 0) return { processed: 0 };
  const todo = domains.slice(0, maxPerRun);
  log(`  Wayback: kör ${todo.length.toLocaleString('sv-SE')} av ${domains.length.toLocaleString('sv-SE')} domäner som behöver uppdatering`);

  let ok = 0;
  let err = 0;
  await runPool(
    todo,
    CONCURRENCY,
    async (domain) => {
      try {
        const r = await lookupOne(domain);
        cache.setWayback(domain, r.first, r.count);
        ok++;
      } catch {
        err++;
        // Spara ändå med nollat resultat så vi inte fastnar — men markera nyligen kollat.
        cache.setWayback(domain, null, 0);
      }
    },
    (done, total) => {
      if (done % 200 === 0 || done === total) {
        log(`    Wayback: ${done}/${total} (${ok} ok, ${err} fel)`);
      }
    }
  );
  return { processed: todo.length, ok, err };
}
