// Gratis Domain Rating från Ahrefs. Resultaten lagras i den beständiga
// berikningscachen så att samma domän normalt bara behöver kontrolleras en
// gång per månad.

const DR_URL = 'https://api.ahrefs.com/v3/public/domain-rating-free';
const TIMEOUT_MS = 15_000;
// Ahrefs API har som standard 60 anrop/minut. 1,1 sekunder mellan starter ger
// marginal för klockdrift och tillfällig throttling.
const MIN_INTERVAL_MS = parseInt(process.env.AHREFS_DR_INTERVAL_MS || '1100', 10);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function lookupAhrefsDr(domain, {
  apiKey = process.env.AHREFS_API_KEY,
  fetchImpl = fetch
} = {}) {
  if (!apiKey) throw new Error('AHREFS_API_KEY saknas');

  const url = new URL(DR_URL);
  url.searchParams.set('target', domain);
  url.searchParams.set('output', 'json');

  const res = await fetchImpl(url, {
    signal: AbortSignal.timeout(TIMEOUT_MS),
    headers: {
      Accept: 'application/json',
      Authorization: `Bearer ${apiKey}`,
      'User-Agent': 'expiring-domains-builder/2.0'
    }
  });
  if (!res.ok) throw new Error(`Ahrefs HTTP ${res.status}`);

  const data = await res.json();
  const rating = Number(data?.domain_rating?.domain_rating);
  if (!Number.isFinite(rating) || rating < 0 || rating > 100) {
    throw new Error('Oväntat DR-svar från Ahrefs');
  }
  return rating;
}

export async function enrichAhrefsDr(domains, cache, {
  maxPerRun,
  log,
  apiKey = process.env.AHREFS_API_KEY,
  lookup = lookupAhrefsDr,
  minIntervalMs = MIN_INTERVAL_MS
}) {
  if (!apiKey) {
    log('  Ahrefs DR: hoppar över (AHREFS_API_KEY saknas)');
    return { processed: 0, ok: 0, errors: 0, skipped: domains.length };
  }
  if (!Number.isFinite(maxPerRun) || maxPerRun <= 0 || domains.length === 0) {
    return { processed: 0, ok: 0, errors: 0, skipped: domains.length };
  }

  const todo = domains.slice(0, Math.floor(maxPerRun));
  log(`  Ahrefs DR: kör ${todo.length.toLocaleString('sv-SE')} av ${domains.length.toLocaleString('sv-SE')} domäner som behöver uppdatering`);

  let ok = 0;
  let errors = 0;
  let processed = 0;
  let lastStartedAt = 0;
  let fatalError = null;

  for (const domain of todo) {
    const waitMs = Math.max(0, lastStartedAt + Math.max(0, minIntervalMs) - Date.now());
    if (waitMs) await sleep(waitMs);
    lastStartedAt = Date.now();

    try {
      const rating = await lookup(domain, { apiKey });
      cache.setAhrefsDr(domain, rating);
      ok++;
    } catch (err) {
      const message = err?.name === 'TimeoutError' ? 'Ahrefs timeout' : (err?.message || 'Ahrefs okänt fel');
      cache.setAhrefsDrError(domain, message);
      errors++;
      // En ogiltig/otillåten nyckel löses inte genom att försöka tusentals
      // gånger i samma körning. Lämna resten i kön till nästa build.
      if (/HTTP (401|403)\b/.test(message)) fatalError = message;
    }
    processed++;

    if (processed % 25 === 0 || processed === todo.length || fatalError) {
      log(`    Ahrefs DR: ${processed}/${todo.length} (${ok} ok, ${errors} fel)`);
    }
    if (fatalError) break;
  }

  if (fatalError) log(`  Ahrefs DR avbröts: ${fatalError}`);
  return { processed, ok, errors, skipped: Math.max(0, domains.length - processed) };
}
