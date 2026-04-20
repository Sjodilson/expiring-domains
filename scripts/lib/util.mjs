// Hjälpfunktioner för HTTP, parallella jobb m.m.

export async function fetchJson(url) {
  const res = await fetch(url, { headers: { 'User-Agent': 'expiring-domains-builder/2.0' } });
  if (!res.ok) throw new Error(`HTTP ${res.status} för ${url}`);
  return await res.json();
}

export async function fetchText(url) {
  const res = await fetch(url, { headers: { 'User-Agent': 'expiring-domains-builder/2.0' } });
  if (!res.ok) throw new Error(`HTTP ${res.status} för ${url}`);
  return await res.text();
}

export async function fetchBuffer(url) {
  const res = await fetch(url, { headers: { 'User-Agent': 'expiring-domains-builder/2.0' } });
  if (!res.ok) throw new Error(`HTTP ${res.status} för ${url}`);
  return Buffer.from(await res.arrayBuffer());
}

// Kör jobb parallellt med given samtidighet.
// onProgress(done, total) anropas regelbundet.
export async function runPool(items, concurrency, worker, onProgress) {
  const total = items.length;
  let done = 0;
  let cursor = 0;
  let nextLog = 0;
  const results = new Array(total);

  async function next() {
    while (true) {
      const i = cursor++;
      if (i >= total) return;
      try {
        results[i] = await worker(items[i], i);
      } catch (err) {
        results[i] = { __error: err.message };
      }
      done++;
      if (onProgress && (done === total || done >= nextLog)) {
        onProgress(done, total);
        nextLog = done + Math.max(1, Math.floor(total / 20));
      }
    }
  }

  const workers = [];
  for (let i = 0; i < Math.min(concurrency, total); i++) workers.push(next());
  await Promise.all(workers);
  return results;
}
