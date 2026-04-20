// Bygger public/domains.sqlite + public/meta.json från Internetstiftelsens
// bardate-data, berikat med ordlista, Tranco, Majestic, Wayback och DNS.

import { mkdirSync, rmSync, existsSync, statSync, writeFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';

import { fetchJson, fetchText, fetchBuffer } from './lib/util.mjs';
import { EnrichmentCache } from './lib/cache.mjs';
import { enrichWayback } from './lib/wayback.mjs';
import { enrichDns } from './lib/dnscheck.mjs';
import { loadMajesticMap } from './lib/backlinks.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, '..');
const PUBLIC_DIR = join(ROOT, 'public');
const CACHE_DIR = join(ROOT, 'cache');
const DB_PATH = join(PUBLIC_DIR, 'domains.sqlite');
const META_PATH = join(PUBLIC_DIR, 'meta.json');
const CACHE_DB = join(CACHE_DIR, 'enrichment.sqlite');

const SOURCES = [
  { tld: 'se', url: 'https://data.internetstiftelsen.se/bardate_domains.json' },
  { tld: 'nu', url: 'https://data.internetstiftelsen.se/bardate_domains_nu.json' }
];

const WORDLIST_URL =
  process.env.WORDLIST_URL ||
  'https://raw.githubusercontent.com/martinlindhe/wordlist_swedish/master/swe_wordlist';

const TRANCO_URL =
  process.env.TRANCO_URL || 'https://tranco-list.eu/top-1m.csv.zip';

// Hur stora batchar vi gör per build (resten görs nästa körning)
const WAYBACK_MAX_PER_RUN = parseInt(process.env.WAYBACK_MAX || '2000', 10);
const WAYBACK_REFRESH_DAYS = parseInt(process.env.WAYBACK_REFRESH_DAYS || '30', 10);
const DNS_MAX_PER_RUN = parseInt(process.env.DNS_MAX || '8000', 10);
const DNS_REFRESH_DAYS = parseInt(process.env.DNS_REFRESH_DAYS || '14', 10);

const VOWELS = new Set(['a', 'e', 'i', 'o', 'u', 'y', 'å', 'ä', 'ö']);

const log = (...a) => console.log(...a);
const warn = (...a) => console.warn('!', ...a);

// ─── Källor ────────────────────────────────────────────────────────────────
async function fetchBardate(url) {
  log(`→ Hämtar ${url}`);
  const json = await fetchJson(url);
  if (!json || !Array.isArray(json.data)) throw new Error(`Oväntat format från ${url}`);
  return json.data;
}

async function loadWordSet() {
  if (process.env.SKIP_WORDS === '1') { warn('SKIP_WORDS=1'); return null; }
  try {
    log(`→ Hämtar ${WORDLIST_URL}`);
    const txt = await fetchText(WORDLIST_URL);
    const set = new Set();
    for (const line of txt.split(/\r?\n/)) {
      const w = line.trim().toLowerCase();
      if (w && !w.includes(' ') && w.length >= 2) set.add(w);
    }
    log(`  Ordlista: ${set.size.toLocaleString('sv-SE')} ord`);
    return set;
  } catch (err) {
    warn(`Ordlista misslyckades: ${err.message}`);
    return null;
  }
}

async function loadTrancoMap() {
  if (process.env.SKIP_TRANCO === '1') { warn('SKIP_TRANCO=1'); return null; }
  try {
    const AdmZip = (await import('adm-zip')).default;
    log(`→ Hämtar ${TRANCO_URL}`);
    const buf = await fetchBuffer(TRANCO_URL);
    const zip = new AdmZip(buf);
    const csvEntry = zip.getEntries().find((e) => e.entryName.endsWith('.csv'));
    if (!csvEntry) throw new Error('Ingen csv i Tranco-zip');
    const csv = csvEntry.getData().toString('utf8');
    const map = new Map();
    let count = 0;
    for (const line of csv.split('\n')) {
      const idx = line.indexOf(',');
      if (idx === -1) continue;
      const rank = parseInt(line.slice(0, idx), 10);
      const domain = line.slice(idx + 1).trim().toLowerCase();
      if (!rank || !domain) continue;
      if (domain.endsWith('.se') || domain.endsWith('.nu')) {
        map.set(domain, rank);
        count++;
      }
    }
    log(`  Tranco: ${count.toLocaleString('sv-SE')} relevanta domäner`);
    return map;
  } catch (err) {
    warn(`Tranco misslyckades: ${err.message}`);
    return null;
  }
}

async function loadMajestic() {
  if (process.env.SKIP_MAJESTIC === '1') { warn('SKIP_MAJESTIC=1'); return null; }
  try {
    return await loadMajesticMap(log);
  } catch (err) {
    warn(`Majestic misslyckades: ${err.message}`);
    return null;
  }
}

// ─── Mönster ───────────────────────────────────────────────────────────────
function isPalindrome(s) {
  if (s.length < 2) return false;
  for (let i = 0, j = s.length - 1; i < j; i++, j--) if (s[i] !== s[j]) return false;
  return true;
}
function hasRepeatedChar(s) {
  for (let i = 1; i < s.length; i++) if (s[i] === s[i - 1]) return true;
  return false;
}
function isCvcv(s) {
  if (s.length < 4) return false;
  if (!/^[a-zåäö]+$/.test(s)) return false;
  let pat = '';
  for (const ch of s) pat += VOWELS.has(ch) ? 'V' : 'C';
  return /^(CV)+$/.test(pat) || /^(VC)+$/.test(pat);
}

// ─── Bygg sqlite ───────────────────────────────────────────────────────────
function buildDatabase(rows, words, tranco, majestic, cacheRows) {
  if (existsSync(DB_PATH)) rmSync(DB_PATH);
  const db = new Database(DB_PATH);
  db.pragma('journal_mode = OFF');
  db.pragma('synchronous = OFF');
  db.pragma('temp_store = MEMORY');

  db.exec(`
    CREATE TABLE domains (
      name              TEXT NOT NULL,
      base              TEXT NOT NULL,
      tld               TEXT NOT NULL,
      release_at        TEXT NOT NULL,
      length            INTEGER NOT NULL,
      has_digit         INTEGER NOT NULL,
      has_hyphen        INTEGER NOT NULL,
      only_digits       INTEGER NOT NULL,
      only_letters      INTEGER NOT NULL,
      is_palindrome     INTEGER NOT NULL,
      has_repeat        INTEGER NOT NULL,
      is_cvcv           INTEGER NOT NULL,
      is_word           INTEGER NOT NULL,
      tranco_rank       INTEGER,
      majestic_rank     INTEGER,
      majestic_refsubnets INTEGER,
      wayback_first     TEXT,
      wayback_count     INTEGER,
      wayback_checked   INTEGER NOT NULL,
      dns_a             INTEGER,
      dns_mx            INTEGER,
      dns_ns            INTEGER,
      dns_checked       INTEGER NOT NULL
    );
  `);

  const insert = db.prepare(`
    INSERT INTO domains (
      name, base, tld, release_at, length,
      has_digit, has_hyphen, only_digits, only_letters,
      is_palindrome, has_repeat, is_cvcv, is_word,
      tranco_rank, majestic_rank, majestic_refsubnets,
      wayback_first, wayback_count, wayback_checked,
      dns_a, dns_mx, dns_ns, dns_checked
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const stats = {
    word: 0, tranco: 0, majestic: 0,
    waybackChecked: 0, waybackHits: 0,
    dnsChecked: 0, dnsAny: 0
  };

  const tx = db.transaction((items) => {
    for (const r of items) {
      const name = String(r.name).toLowerCase();
      const dot = name.lastIndexOf('.');
      const base = dot === -1 ? name : name.slice(0, dot);
      const tld = dot === -1 ? '' : name.slice(dot + 1);

      const word = words && words.has(base) ? 1 : 0;
      const trancoRank = tranco ? tranco.get(name) ?? null : null;
      const maj = majestic ? majestic.get(name) ?? null : null;
      const cache = cacheRows.get(name);

      const wbFirst = cache?.wayback_first ?? null;
      const wbCount = cache?.wayback_count ?? null;
      const wbChecked = cache?.wayback_checked_at ? 1 : 0;
      const dnsA = cache?.dns_a ?? null;
      const dnsMx = cache?.dns_mx ?? null;
      const dnsNs = cache?.dns_ns ?? null;
      const dnsChecked = cache?.dns_checked_at ? 1 : 0;

      if (word) stats.word++;
      if (trancoRank != null) stats.tranco++;
      if (maj) stats.majestic++;
      if (wbChecked) {
        stats.waybackChecked++;
        if ((wbCount ?? 0) > 0) stats.waybackHits++;
      }
      if (dnsChecked) {
        stats.dnsChecked++;
        if (dnsA || dnsMx || dnsNs) stats.dnsAny++;
      }

      insert.run(
        name, base, tld, r.release_at, base.length,
        /\d/.test(base) ? 1 : 0,
        base.includes('-') ? 1 : 0,
        /^\d+$/.test(base) ? 1 : 0,
        /^[a-zåäö]+$/.test(base) ? 1 : 0,
        isPalindrome(base) ? 1 : 0,
        hasRepeatedChar(base) ? 1 : 0,
        isCvcv(base) ? 1 : 0,
        word,
        trancoRank,
        maj?.rank ?? null,
        maj?.refSubNets ?? null,
        wbFirst, wbCount, wbChecked,
        dnsA, dnsMx, dnsNs, dnsChecked
      );
    }
  });

  tx(rows);

  log('→ Skapar index');
  db.exec(`
    CREATE INDEX idx_release_at ON domains(release_at);
    CREATE INDEX idx_tld_release ON domains(tld, release_at);
    CREATE INDEX idx_base ON domains(base);
    CREATE INDEX idx_length ON domains(length);
    CREATE INDEX idx_word ON domains(is_word);
    CREATE INDEX idx_tranco ON domains(tranco_rank);
    CREATE INDEX idx_majestic ON domains(majestic_rank);
    CREATE INDEX idx_wayback ON domains(wayback_count);
    CREATE INDEX idx_dns ON domains(dns_a);
  `);

  db.exec('VACUUM;');
  db.close();

  return stats;
}

// ─── Meta ───────────────────────────────────────────────────────────────────
function buildMeta(rows, stats, totalCount) {
  const byTld = {};
  const byDate = {};
  const byLen = {};
  let min = null, max = null;
  for (const r of rows) {
    byTld[r.tld] = (byTld[r.tld] || 0) + 1;
    byDate[r.release_at] = (byDate[r.release_at] || 0) + 1;
    const dot = r.name.lastIndexOf('.');
    const baseLen = dot === -1 ? r.name.length : dot;
    byLen[baseLen] = (byLen[baseLen] || 0) + 1;
    if (!min || r.release_at < min) min = r.release_at;
    if (!max || r.release_at > max) max = r.release_at;
  }
  return {
    generated_at: new Date().toISOString(),
    total: rows.length,
    by_tld: byTld,
    by_date: byDate,
    by_length: byLen,
    date_range: { min, max },
    sources: SOURCES.map((s) => s.url),
    enrichments: {
      word_hits: stats.word,
      tranco_hits: stats.tranco,
      majestic_hits: stats.majestic,
      wayback_checked: stats.waybackChecked,
      wayback_hits: stats.waybackHits,
      dns_checked: stats.dnsChecked,
      dns_active: stats.dnsAny,
      coverage_wayback: totalCount ? +(stats.waybackChecked / totalCount * 100).toFixed(1) : 0,
      coverage_dns: totalCount ? +(stats.dnsChecked / totalCount * 100).toFixed(1) : 0
    }
  };
}

// ─── Main ───────────────────────────────────────────────────────────────────
async function main() {
  mkdirSync(PUBLIC_DIR, { recursive: true });
  mkdirSync(CACHE_DIR, { recursive: true });

  // 1. Bardate
  const all = [];
  for (const src of SOURCES) {
    const rows = await fetchBardate(src.url);
    for (const r of rows) all.push({ name: String(r.name).toLowerCase(), release_at: r.release_at, tld: src.tld });
    log(`  ${rows.length.toLocaleString('sv-SE')} domäner från .${src.tld}`);
  }
  const allDomains = all.map((r) => r.name);
  const allSet = new Set(allDomains);

  // 2. Snabba berikningar parallellt
  const [words, tranco, majestic] = await Promise.all([
    loadWordSet(),
    loadTrancoMap(),
    loadMajestic()
  ]);

  // 3. Rolling enrichment
  const cache = new EnrichmentCache(CACHE_DB);
  const pruned = cache.prune(allSet);
  if (pruned > 0) log(`  Cache: rensade ${pruned} gamla rader`);

  if (process.env.SKIP_DNS !== '1') {
    const need = cache.needsUpdate('dns', allDomains, DNS_REFRESH_DAYS);
    await enrichDns(need, cache, { maxPerRun: DNS_MAX_PER_RUN, log });
  } else { warn('SKIP_DNS=1'); }

  if (process.env.SKIP_WAYBACK !== '1') {
    const need = cache.needsUpdate('wayback', allDomains, WAYBACK_REFRESH_DAYS);
    await enrichWayback(need, cache, { maxPerRun: WAYBACK_MAX_PER_RUN, log });
  } else { warn('SKIP_WAYBACK=1'); }

  // 4. Hämta cachade berikningar för alla aktuella domäner
  const cacheRows = cache.getMany(allDomains);
  cache.close();

  // 5. Bygg sqlite
  log(`→ Bygger SQLite med ${all.length.toLocaleString('sv-SE')} rader`);
  const stats = buildDatabase(all, words, tranco, majestic, cacheRows);

  log(`  Berikningar: ord=${stats.word}, tranco=${stats.tranco}, majestic=${stats.majestic}`);
  log(`  Wayback: ${stats.waybackChecked} kollade, ${stats.waybackHits} med snapshots`);
  log(`  DNS: ${stats.dnsChecked} kollade, ${stats.dnsAny} med aktiva records`);

  const meta = buildMeta(all, stats, all.length);
  writeFileSync(META_PATH, JSON.stringify(meta, null, 2), 'utf8');

  const sizeMB = (statSync(DB_PATH).size / 1024 / 1024).toFixed(2);
  log(`✓ Klar. domains.sqlite = ${sizeMB} MB, ${all.length} rader`);
}

main().catch((err) => {
  console.error('✗ Build-data misslyckades:', err);
  process.exit(1);
});
