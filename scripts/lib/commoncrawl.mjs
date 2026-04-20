// Common Crawl Web Graph: domain-level vertices.
// Laddar ner domain-vertices.txt.gz (~850 MB), stream-filtrerar .se/.nu.
// Cachar extraherad data i cache/cc-se-nu.json (uppdateras var 30:e dag).

import { existsSync, readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { createGunzip } from 'node:zlib';
import { createInterface } from 'node:readline';
import { Readable } from 'node:stream';

const GRAPHINFO_URL = 'https://index.commoncrawl.org/graphinfo.json';
const CC_DATA_BASE = 'https://data.commoncrawl.org/projects/hyperlinkgraph';
const CACHE_MAX_AGE_DAYS = parseInt(process.env.CC_REFRESH_DAYS || '30', 10);

/**
 * Ladda CC-domändata (antal hosts per domän) för .se/.nu.
 * Returnerar Map<domain, numHosts>.
 *
 * Cachar resultatet i cacheDir/cc-se-nu.json.
 */
export async function loadCcDomainMap(log, cacheDir) {
  mkdirSync(cacheDir, { recursive: true });
  const cachePath = join(cacheDir, 'cc-se-nu.json');

  // Kolla om cache är färsk nog
  if (existsSync(cachePath)) {
    try {
      const cached = JSON.parse(readFileSync(cachePath, 'utf8'));
      const age = (Date.now() - new Date(cached.fetchedAt).getTime()) / 86400000;
      if (age < CACHE_MAX_AGE_DAYS) {
        const map = new Map(Object.entries(cached.domains));
        log(`  CC web graph (cache, ${age.toFixed(0)}d gammal): ${map.size.toLocaleString('sv-SE')} .se/.nu domäner`);
        return map;
      }
      log(`  CC web graph cache är ${age.toFixed(0)}d gammal — laddar om`);
    } catch {
      log('  CC web graph cache korrupt — laddar om');
    }
  }

  // Hämta senaste crawl-ID
  log(`→ Hämtar CC graphinfo…`);
  const res0 = await fetch(GRAPHINFO_URL, {
    headers: { 'User-Agent': 'expiring-domains-builder/2.0' }
  });
  if (!res0.ok) throw new Error(`graphinfo HTTP ${res0.status}`);
  const info = await res0.json();
  const latest = info[0];
  if (!latest?.id) throw new Error('Kunde inte hitta senaste CC web graph');
  const crawlId = latest.id;
  log(`  Senaste CC web graph: ${crawlId}`);

  // Ladda ner domain-vertices (stream + gunzip + filter)
  const verticesUrl = `${CC_DATA_BASE}/${crawlId}/domain/${crawlId}-domain-vertices.txt.gz`;
  log(`→ Hämtar CC domain-vertices: ${verticesUrl}`);
  log(`  (detta kan ta 1–3 min beroende på nätverket)`);

  const res = await fetch(verticesUrl, {
    headers: { 'User-Agent': 'expiring-domains-builder/2.0' }
  });
  if (!res.ok) throw new Error(`CC vertices HTTP ${res.status}`);

  const nodeStream = Readable.fromWeb(res.body);
  const gunzip = createGunzip();
  const rl = createInterface({ input: nodeStream.pipe(gunzip) });

  const map = new Map();
  let lineCount = 0;
  let lastLog = Date.now();

  for await (const line of rl) {
    lineCount++;
    // Logga progress var 30:e sekund
    if (Date.now() - lastLog > 30000) {
      log(`    ${(lineCount / 1e6).toFixed(1)}M rader bearbetade, ${map.size.toLocaleString('sv-SE')} .se/.nu hittade`);
      lastLog = Date.now();
    }

    // Format: id \t reversed_domain \t num_hosts
    const t1 = line.indexOf('\t');
    if (t1 === -1) continue;
    const t2 = line.indexOf('\t', t1 + 1);
    if (t2 === -1) continue;

    const revDomain = line.slice(t1 + 1, t2);
    // Reversed: se.example → bara .se/.nu intressanta
    if (!revDomain.startsWith('se.') && !revDomain.startsWith('nu.')) continue;

    const numHosts = parseInt(line.slice(t2 + 1), 10) || 0;
    // Reversera tillbaka: se.example → example.se
    const parts = revDomain.split('.');
    const domain = parts.reverse().join('.');
    map.set(domain, numHosts);
  }

  log(`  CC web graph: ${lineCount.toLocaleString('sv-SE')} total, ${map.size.toLocaleString('sv-SE')} .se/.nu`);

  // Spara cache
  const cacheData = {
    crawlId,
    fetchedAt: new Date().toISOString(),
    totalLines: lineCount,
    domains: Object.fromEntries(map)
  };
  writeFileSync(cachePath, JSON.stringify(cacheData), 'utf8');
  log(`  Cachad till ${cachePath}`);

  return map;
}
