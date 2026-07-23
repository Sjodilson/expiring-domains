// Common Crawl Web Graph: domain-level vertices.
// Laddar ner domain-vertices.txt.gz (~850 MB), stream-filtrerar .se/.nu.
// Cachar extraherad data i cache/cc-se-nu.json (uppdateras var 30:e dag).

import { existsSync, readFileSync, writeFileSync, mkdirSync, unlinkSync } from 'node:fs';
import { join } from 'node:path';
import { createGunzip } from 'node:zlib';
import { createInterface } from 'node:readline';
import { Readable } from 'node:stream';

const GRAPHINFO_URL = 'https://index.commoncrawl.org/graphinfo.json';
const CC_DATA_BASE = 'https://data.commoncrawl.org/projects/hyperlinkgraph';
const CACHE_MAX_AGE_DAYS = parseInt(process.env.CC_REFRESH_DAYS || '30', 10);

function withMetadata(map, { sourceVersion, fetchedAt, historical = false }) {
  Object.defineProperties(map, {
    sourceVersion: { value: sourceVersion, enumerable: false },
    fetchedAt: { value: fetchedAt, enumerable: false },
    historical: { value: historical, enumerable: false }
  });
  return map;
}

async function fetchGraphInfo() {
  const res = await fetch(GRAPHINFO_URL, {
    headers: { 'User-Agent': 'expiring-domains-builder/2.0' }
  });
  if (!res.ok) throw new Error(`graphinfo HTTP ${res.status}`);
  const info = await res.json();
  if (!Array.isArray(info) || !info[0]?.id) {
    throw new Error('Kunde inte hitta Common Crawls webbgrafer');
  }
  return info;
}

async function downloadDomainMap(crawlId, log) {
  const verticesUrl = `${CC_DATA_BASE}/${crawlId}/domain/${crawlId}-domain-vertices.txt.gz`;
  log(`→ Hämtar CC domain-vertices: ${verticesUrl}`);
  log('  (detta kan ta 1–3 min beroende på nätverket)');

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
    if (!revDomain.startsWith('se.') && !revDomain.startsWith('nu.')) continue;

    const numHosts = parseInt(line.slice(t2 + 1), 10) || 0;
    const domain = revDomain.split('.').reverse().join('.');
    map.set(domain, numHosts);
  }

  log(`  CC web graph: ${lineCount.toLocaleString('sv-SE')} total, ${map.size.toLocaleString('sv-SE')} .se/.nu`);
  return { map, lineCount };
}

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
        const map = new Map(
          Object.entries(cached.domains).map(([domain, hosts]) => [domain, Number(hosts) || 0])
        );
        log(`  CC web graph (cache, ${age.toFixed(0)}d gammal): ${map.size.toLocaleString('sv-SE')} .se/.nu domäner`);
        return withMetadata(map, {
          sourceVersion: cached.crawlId || cached.fetchedAt,
          fetchedAt: cached.fetchedAt
        });
      }
      log(`  CC web graph cache är ${age.toFixed(0)}d gammal — laddar om`);
    } catch {
      log('  CC web graph cache korrupt — laddar om');
    }
  }

  // Hämta senaste crawl-ID
  log(`→ Hämtar CC graphinfo…`);
  const info = await fetchGraphInfo();
  const latest = info[0];
  const crawlId = latest.id;
  log(`  Senaste CC web graph: ${crawlId}`);

  const { map, lineCount } = await downloadDomainMap(crawlId, log);
  const fetchedAt = new Date().toISOString();

  // Spara cache
  const cacheData = {
    crawlId,
    fetchedAt,
    totalLines: lineCount,
    domains: Object.fromEntries(map)
  };
  writeFileSync(cachePath, JSON.stringify(cacheData), 'utf8');
  log(`  Cachad till ${cachePath}`);

  return withMetadata(map, { sourceVersion: crawlId, fetchedAt });
}

function graphYear(id) {
  const match = String(id).match(/cc-main-(\d{4})/);
  return match ? parseInt(match[1], 10) : null;
}

function historicalTargets(info) {
  const latestYear = graphYear(info[0]?.id) ?? new Date().getUTCFullYear();
  const perYear = new Map();
  for (const graph of info) {
    const year = graphYear(graph.id);
    if (!year || year >= latestYear || (graph.stats?.domain?.nodes ?? 0) <= 0) continue;
    // graphinfo är sorterad nyast först. Skriv över inom året så att vi väljer
    // den tidigaste användbara grafen och får största möjliga tidsavstånd till
    // den aktuella grafen.
    perYear.set(year, graph);
  }
  return [...perYear.entries()]
    .sort((a, b) => b[0] - a[0])
    .map(([, graph]) => graph);
}

/**
 * Hämtar högst en äldre årsgraph åt gången. Den extraherade .se/.nu-mängden
 * ligger kvar som en liten pending-cache tills hela grafen har importerats i
 * SQLite-cachen. På så sätt laddas aldrig samma ~850 MB-fil varje timme.
 */
export async function loadHistoricalCcDomainMap(
  log,
  cacheDir,
  { completedIds = new Set(), allowDownload = false } = {}
) {
  mkdirSync(cacheDir, { recursive: true });
  const pendingPath = join(cacheDir, 'cc-history-pending.json');

  if (existsSync(pendingPath)) {
    try {
      const pending = JSON.parse(readFileSync(pendingPath, 'utf8'));
      if (!completedIds.has(pending.crawlId) && pending?.domains) {
        const map = new Map(
          Object.entries(pending.domains).map(([domain, hosts]) => [domain, Number(hosts) || 0])
        );
        log(`  Historisk CC (pending ${pending.crawlId}): ${map.size.toLocaleString('sv-SE')} .se/.nu`);
        return withMetadata(map, {
          sourceVersion: pending.crawlId,
          fetchedAt: pending.fetchedAt,
          historical: true
        });
      }
      unlinkSync(pendingPath);
    } catch {
      log('  Historisk CC pending-cache korrupt — tar om den vid nästa fullkörning');
      unlinkSync(pendingPath);
    }
  }

  if (!allowDownload) return null;

  const info = await fetchGraphInfo();
  const target = historicalTargets(info).find((graph) => !completedIds.has(graph.id));
  if (!target) {
    log('  Historisk CC: alla årsgraphar är redan importerade');
    return null;
  }

  log(`→ Historisk CC: extraherar ${target.id}`);
  const { map, lineCount } = await downloadDomainMap(target.id, log);
  const fetchedAt = new Date().toISOString();
  writeFileSync(pendingPath, JSON.stringify({
    crawlId: target.id,
    fetchedAt,
    totalLines: lineCount,
    domains: Object.fromEntries(map)
  }), 'utf8');
  log(`  Historisk CC pending-cache: ${pendingPath}`);

  return withMetadata(map, {
    sourceVersion: target.id,
    fetchedAt,
    historical: true
  });
}

export function clearHistoricalCcPending(cacheDir, crawlId) {
  const pendingPath = join(cacheDir, 'cc-history-pending.json');
  if (!existsSync(pendingPath)) return false;
  try {
    const pending = JSON.parse(readFileSync(pendingPath, 'utf8'));
    if (pending.crawlId !== crawlId) return false;
    unlinkSync(pendingPath);
    return true;
  } catch {
    return false;
  }
}
