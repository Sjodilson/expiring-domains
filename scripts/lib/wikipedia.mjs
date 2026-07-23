// Svenska Wikipedias externa länkdump ger ett kandidatuniversum av .se/.nu-
// domäner som faktiskt har citerats från Wikipedia. Dumpen streamas och den
// lilla extraherade mängden cachas mellan GitHub Actions-körningar.

import { existsSync, readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { createGunzip } from 'node:zlib';
import { createInterface } from 'node:readline';
import { Readable } from 'node:stream';

const WIKIPEDIA_URL =
  process.env.WIKIPEDIA_URL ||
  'https://dumps.wikimedia.org/svwiki/latest/svwiki-latest-externallinks.sql.gz';
const CACHE_MAX_AGE_DAYS = parseInt(process.env.WIKIPEDIA_REFRESH_DAYS || '30', 10);

function withMetadata(map, sourceVersion, fetchedAt) {
  Object.defineProperties(map, {
    sourceVersion: { value: sourceVersion, enumerable: false },
    fetchedAt: { value: fetchedAt, enumerable: false }
  });
  return map;
}

export function extractWikipediaDomains(line, counts) {
  // MediaWiki lagrar domänindex i omvänd ordning:
  //   https://se.internetstiftelsen.www.  -> internetstiftelsen.se
  // Vi behöver bara de två första omvända etiketterna för registrerbar
  // andranivådomän under .se/.nu.
  const re = /\(\d+,\d+,'https?:\/\/((?:se|nu)\.[^']*)','/g;
  let match;
  while ((match = re.exec(line)) !== null) {
    const labels = match[1].replace(/\.$/, '').split('.');
    if (labels.length < 2) continue;
    const tld = labels[0];
    const base = labels[1].toLowerCase();
    if (!/^[a-z0-9åäö](?:[a-z0-9åäö-]{0,61}[a-z0-9åäö])?$/.test(base)) continue;
    const domain = `${base}.${tld}`;
    counts.set(domain, (counts.get(domain) || 0) + 1);
  }
}

export async function loadWikipediaDomainMap(log, cacheDir) {
  mkdirSync(cacheDir, { recursive: true });
  const cachePath = join(cacheDir, 'wikipedia-se-nu.json');

  if (existsSync(cachePath)) {
    try {
      const cached = JSON.parse(readFileSync(cachePath, 'utf8'));
      const age = (Date.now() - new Date(cached.fetchedAt).getTime()) / 86400000;
      if (age < CACHE_MAX_AGE_DAYS) {
        const map = new Map(Object.entries(cached.domains).map(([domain, links]) => [domain, Number(links)]));
        log(`  Wikipedia-länkar (cache, ${age.toFixed(0)}d): ${map.size.toLocaleString('sv-SE')} .se/.nu`);
        return withMetadata(map, cached.sourceVersion || cached.fetchedAt, cached.fetchedAt);
      }
      log(`  Wikipedia-cache är ${age.toFixed(0)}d gammal — laddar om`);
    } catch {
      log('  Wikipedia-cache korrupt — laddar om');
    }
  }

  log(`→ Hämtar svenska Wikipedias externa länkar: ${WIKIPEDIA_URL}`);
  const res = await fetch(WIKIPEDIA_URL, {
    headers: { 'User-Agent': 'expiring-domains-builder/2.0' }
  });
  if (!res.ok) throw new Error(`Wikipedia HTTP ${res.status}`);

  const counts = new Map();
  const rl = createInterface({
    input: Readable.fromWeb(res.body).pipe(createGunzip())
  });
  let rows = 0;
  let lastLog = Date.now();
  for await (const line of rl) {
    if (!line.startsWith('INSERT INTO `externallinks` VALUES ')) continue;
    extractWikipediaDomains(line, counts);
    rows++;
    if (Date.now() - lastLog > 30000) {
      log(`    ${rows.toLocaleString('sv-SE')} insertblock · ${counts.size.toLocaleString('sv-SE')} .se/.nu`);
      lastLog = Date.now();
    }
  }

  const fetchedAt = new Date().toISOString();
  const sourceVersion = res.headers.get('last-modified') || fetchedAt.slice(0, 10);
  writeFileSync(cachePath, JSON.stringify({
    sourceVersion,
    fetchedAt,
    domains: Object.fromEntries(counts)
  }), 'utf8');
  log(`  Wikipedia: ${counts.size.toLocaleString('sv-SE')} .se/.nu-domäner cachade`);
  return withMetadata(counts, sourceVersion, fetchedAt);
}
