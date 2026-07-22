// DNS-uppslag (rolling). Kollar A, MX, NS för varje domän.

import { promises as dns } from 'node:dns';
import { runPool } from './util.mjs';

const CONCURRENCY = 40;
const TIMEOUT_MS = 5_000;
const DAS_REQUESTS_PER_SECOND = 25;

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
    return { value: Array.isArray(r) ? r.length > 0 : !!r, error: null };
  } catch (err) {
    const code = err?.code || (err?.message === 'timeout' ? 'ETIMEOUT' : 'EUNKNOWN');
    // ENODATA/ENOTFOUND är riktiga negativa DNS-svar. Övriga fel är tillfälliga
    // eller tekniska och får aldrig lagras som "inga records".
    if (code === 'ENODATA' || code === 'ENOTFOUND') return { value: false, error: null };
    return { value: null, error: code };
  }
}

async function lookupOne(domain) {
  const [aResult, mxResult, nsResult] = await Promise.all([
    tryResolve(dns.resolve4, domain),
    tryResolve(dns.resolveMx, domain),
    tryResolve(dns.resolveNs, domain)
  ]);
  const errors = [aResult.error, mxResult.error, nsResult.error].filter(Boolean);
  const active = aResult.value === true || mxResult.value === true || nsResult.value === true;
  return {
    a: aResult.value,
    mx: mxResult.value,
    ns: nsResult.value,
    active,
    status: errors.length ? (active ? 'partial' : 'error') : 'ok',
    error: errors.length ? [...new Set(errors)].join(', ') : null
  };
}

async function checkDas(domain) {
  const host = domain.endsWith('.nu') ? 'free.iis.nu' : 'free.iis.se';
  try {
    const res = await fetch(`http://${host}/free?q=${encodeURIComponent(domain)}`, {
      headers: { 'User-Agent': 'expiring-domains-builder/2.0' },
      signal: AbortSignal.timeout(TIMEOUT_MS)
    });
    if (!res.ok) throw new Error(`HTTP_${res.status}`);
    const body = (await res.text()).trim().toLowerCase();
    if (body.startsWith('occupied ')) return { taken: true, error: null };
    if (body.startsWith('free ')) return { taken: false, error: null };
    return { taken: null, error: `OVÄNTAT_SVAR: ${body.slice(0, 80)}` };
  } catch (err) {
    return { taken: null, error: err?.name === 'TimeoutError' ? 'ETIMEOUT' : (err?.message || 'EUNKNOWN') };
  }
}

function hoursSince(iso) {
  if (!iso) return Infinity;
  const ms = Date.now() - new Date(iso).getTime();
  return Number.isFinite(ms) ? ms / 3600000 : Infinity;
}

function availabilityDue(row, today) {
  if (row.taken === 1) return false;
  if (row.avail_status == null) return true;
  const ageDays = Math.max(0, Math.floor((today.getTime() - new Date(`${row.release_at}T00:00:00Z`).getTime()) / 86400000));
  const sinceAttempt = hoursSince(row.avail_attempted_at || row.avail_checked_at);
  if (row.avail_status === 'error') return sinceAttempt >= 6;
  if (ageDays === 0) return sinceAttempt >= 0.75;
  if (ageDays <= 7) return sinceAttempt >= 23;
  return sinceAttempt >= 24 * 7;
}

async function runDasBatches(items, worker, onProgress) {
  let done = 0;
  for (let i = 0; i < items.length; i += DAS_REQUESTS_PER_SECOND) {
    const batch = items.slice(i, i + DAS_REQUESTS_PER_SECOND);
    await Promise.all(batch.map(worker));
    done += batch.length;
    onProgress?.(done, items.length);
    if (done < items.length) await new Promise((resolve) => setTimeout(resolve, 1000));
  }
}

// Registerstatus hämtas från Internetstiftelsens Free/DAS. När en domän är
// upptagen sparas även dess faktiska DNS-resultat. Tekniska fel blir "okänd".
export async function checkAvailability(released, cache, { log }) {
  const today = new Date(`${new Date().toISOString().slice(0, 10)}T00:00:00Z`);
  const todo = released.filter((r) => availabilityDue(r, today));
  const dnsRepair = released.filter((r) =>
    r.taken === 1 &&
    !(r.dns_a || r.dns_mx || r.dns_ns) &&
    (r.dns_status == null || (r.dns_status === 'error' && hoursSince(r.dns_attempted_at) >= 6))
  );

  let taken = 0;
  let free = 0;
  let errors = 0;
  if (todo.length) {
    log(`  Tillgänglighet (DAS): kollar ${todo.length.toLocaleString('sv-SE')} nysläppta domäner`);
    await runDasBatches(
      todo,
      async (row) => {
        const result = await checkDas(row.domain);
        if (result.error) {
          errors++;
          cache.setAvailabilityError(row.domain, result.error);
          return;
        }
        cache.setAvailability(row.domain, result.taken);
        if (result.taken) {
          taken++;
          const dnsResult = await lookupOne(row.domain);
          if (dnsResult.status === 'error') cache.setDnsError(row.domain, dnsResult.error);
          else cache.setDns(row.domain, dnsResult.a, dnsResult.mx, dnsResult.ns, dnsResult.status, dnsResult.error);
        } else {
          free++;
        }
      },
      (done, total) => {
        if (done % 250 === 0 || done === total) log(`    DAS: ${done}/${total}`);
      }
    );
    log(`  Tillgänglighet: ${taken} upptagna, ${free} lediga, ${errors} kontrollfel`);
  }

  if (dnsRepair.length) {
    log(`  DNS-reparation: kollar ${dnsRepair.length.toLocaleString('sv-SE')} redan upptagna domäner`);
    await runPool(dnsRepair, CONCURRENCY, async (row) => {
      cache.setAvailability(row.domain, true);
      const result = await lookupOne(row.domain);
      if (result.status === 'error') cache.setDnsError(row.domain, result.error);
      else cache.setDns(row.domain, result.a, result.mx, result.ns, result.status, result.error);
    });
  }

  return { checked: todo.length, taken, free, errors, dnsRepaired: dnsRepair.length };
}

// Äldre guldkorn kommer direkt från rankningslistorna och saknar normalt ett
// känt frisläppningsdatum. Cachen väljer vilka kandidater som är förfallna för
// kontroll; här utförs endast den budgeterade DAS-batchen.
export async function checkRankedAvailability(candidates, cache, { log }) {
  if (candidates.length === 0) return { checked: 0, occupied: 0, free: 0, errors: 0 };

  let occupied = 0;
  let free = 0;
  let errors = 0;
  log(`  Rankade kandidater (DAS): kollar ${candidates.length.toLocaleString('sv-SE')} domäner`);
  await runDasBatches(
    candidates,
    async (row) => {
      const result = await checkDas(row.domain);
      if (result.error) {
        errors++;
        cache.setRankedAvailabilityError(row.domain, result.error);
        return;
      }
      cache.setRankedAvailability(row.domain, result.taken);
      if (result.taken) occupied++;
      else free++;
    },
    (done, total) => {
      if (done % 250 === 0 || done === total) log(`    Rankad DAS: ${done}/${total}`);
    }
  );
  log(`  Rankad tillgänglighet: ${occupied} upptagna, ${free} lediga, ${errors} kontrollfel`);
  return { checked: candidates.length, occupied, free, errors };
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
      if (r.status === 'error') cache.setDnsError(domain, r.error);
      else cache.setDns(domain, r.a, r.mx, r.ns, r.status, r.error);
      if (r.a === true) counts.a++;
      if (r.mx === true) counts.mx++;
      if (r.ns === true) counts.ns++;
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
