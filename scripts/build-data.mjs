// Bygger public/domains.sqlite + public/meta.json från Internetstiftelsens
// bardate-data, berikat med ordlista, Tranco, Majestic, Open PageRank,
// Common Crawl web graph, Wayback och DNS.

import { mkdirSync, rmSync, existsSync, statSync, writeFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';

import { fetchJson, fetchText, fetchBuffer } from './lib/util.mjs';
import { EnrichmentCache } from './lib/cache.mjs';
import { enrichWayback } from './lib/wayback.mjs';
import { enrichDns, checkAvailability, checkRankedAvailability } from './lib/dnscheck.mjs';
import { loadMajesticMap } from './lib/backlinks.mjs';
import { loadOpenPageRankMap } from './lib/openpagerank.mjs';
import { loadCcDomainMap } from './lib/commoncrawl.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, '..');
const PUBLIC_DIR = join(ROOT, 'public');
const CACHE_DIR = join(ROOT, 'cache');
const DB_PATH = join(PUBLIC_DIR, 'domains.sqlite');
const META_PATH = join(PUBLIC_DIR, 'meta.json');
const CACHE_DB = process.env.CACHE_DB || join(CACHE_DIR, 'enrichment.sqlite');

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
// Hur många dagar bakåt nysläppta domäner visas med tillgänglighetsstatus
const RELEASED_DAYS = parseInt(process.env.RELEASED_DAYS || '90', 10);
// Rankade kandidater betas av snabbt första dygnet, därefter är endast nya och
// förfallna återkontroller normalt aktuella.
const RANKED_AVAIL_MAX_PER_RUN = parseInt(process.env.RANKED_AVAIL_MAX || '5000', 10);
const RANKED_FREE_REFRESH_HOURS = parseInt(process.env.RANKED_FREE_REFRESH_HOURS || '23', 10);
const RANKED_OCCUPIED_REFRESH_DAYS = parseInt(process.env.RANKED_OCCUPIED_REFRESH_DAYS || '30', 10);

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
    if (count < 1000) throw new Error(`Oväntat få Tranco-domäner: ${count}`);
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

async function loadOpr() {
  if (process.env.SKIP_OPR === '1') { warn('SKIP_OPR=1'); return null; }
  try {
    return await loadOpenPageRankMap(log);
  } catch (err) {
    warn(`Open PageRank misslyckades: ${err.message}`);
    return null;
  }
}

async function loadCc() {
  if (process.env.SKIP_CC === '1') { warn('SKIP_CC=1'); return null; }
  try {
    return await loadCcDomainMap(log, CACHE_DIR);
  } catch (err) {
    warn(`Common Crawl misslyckades: ${err.message}`);
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

const COMMERCIAL_TERMS = [
  'advokat', 'bil', 'bostad', 'bygg', 'butik', 'el', 'energi', 'finans',
  'flytt', 'försäkring', 'hotell', 'jobb', 'jurist', 'kredit', 'lån',
  'mäklare', 'renovering', 'resa', 'restaurang', 'sol', 'tandläkare', 'vård'
];
const RISK_TERMS = [
  'adult', 'betting', 'casino', 'escort', 'gambling', 'poker', 'porn',
  'sex', 'slots', 'viagra', 'xxx'
];

function clampScore(value) {
  return Math.max(0, Math.min(100, Math.round(value)));
}

function logPoints(value, cap, points) {
  if (!value || value <= 0) return 0;
  return Math.min(points, Math.log10(value + 1) / Math.log10(cap + 1) * points);
}

// Fyra transparenta delpoäng byggda enbart från data som redan finns i pipelinen.
// Efterfrågan är indikativ tills faktisk sökvolym kopplas på i ett senare steg.
function scoreDomain({ base, word, trancoRank, maj, oprData, ccHosts, wbFirst, wbCount }) {
  const asciiOrSwedishLetters = /^[a-zåäö]+$/.test(base);
  const hasDigit = /\d/.test(base);
  const hyphens = (base.match(/-/g) || []).length;
  const repeated = hasRepeatedChar(base);
  const cvcv = isCvcv(base);
  const commercial = COMMERCIAL_TERMS.some((term) => base.includes(term));
  const risky = RISK_TERMS.some((term) => base.includes(term));

  let brand = 0;
  if (asciiOrSwedishLetters) brand += 18;
  if (!hasDigit && hyphens === 0) brand += 12;
  if (base.length >= 4 && base.length <= 10) brand += 35;
  else if (base.length <= 14) brand += 25;
  else if (base.length <= 18) brand += 12;
  else brand += 4;
  if (word) brand += 25;
  if (cvcv) brand += 10;
  if (repeated) brand -= 6;
  if (base.startsWith('xn--')) brand -= 20;

  let authority = 0;
  authority += logPoints(maj?.refSubNets, 1000, 35);
  authority += Math.min(25, Math.max(0, (oprData?.score ?? 0) / 10 * 25));
  authority += logPoints(wbCount, 1000, 15);
  authority += logPoints(ccHosts, 1000, 10);
  if (wbFirst) {
    const firstYear = parseInt(String(wbFirst).slice(0, 4), 10);
    if (firstYear > 1990) authority += Math.min(10, (new Date().getUTCFullYear() - firstYear) * 0.7);
  }
  if (trancoRank) {
    authority += Math.max(0, 15 * (1 - Math.log10(Math.max(1, trancoRank)) / 6));
  }

  let demand = 0;
  if (word) demand += 35;
  if (commercial) demand += 30;
  if (base.length >= 4 && base.length <= 12) demand += 10;
  if (trancoRank) demand += Math.max(5, 20 * (1 - Math.log10(Math.max(1, trancoRank)) / 6));
  demand += logPoints(maj?.refSubNets, 100, 10);
  demand += logPoints(ccHosts, 100, 5);

  let risk = 0;
  if (base.startsWith('xn--')) risk += 30;
  if (hasDigit) risk += 15;
  if (/\d{3,}/.test(base)) risk += 10;
  if (hyphens) risk += Math.min(20, hyphens * 8);
  if (/^\d+$/.test(base)) risk += 30;
  if (risky) risk += 35;
  if (base.length > 24) risk += 10;
  if (repeated) risk += 5;

  const scores = {
    brand: clampScore(brand),
    authority: clampScore(authority),
    demand: clampScore(demand),
    risk: clampScore(risk)
  };
  scores.total = clampScore(
    scores.brand * 0.45 + scores.authority * 0.30 + scores.demand * 0.25 - scores.risk * 0.35
  );
  return scores;
}

// ─── Bygg sqlite ───────────────────────────────────────────────────────────
function buildDatabase(rows, words, tranco, majestic, opr, cc, cacheRows) {
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
      dns_checked       INTEGER NOT NULL,
      dns_status        TEXT,
      dns_error         TEXT,
      opr_rank          INTEGER,
      opr_score         REAL,
      cc_hosts          INTEGER,
      score_brand       INTEGER NOT NULL,
      score_authority   INTEGER NOT NULL,
      score_demand      INTEGER NOT NULL,
      score_risk        INTEGER NOT NULL,
      score_total       INTEGER NOT NULL,
      released          INTEGER NOT NULL DEFAULT 0,
      taken             INTEGER,
      taken_at          TEXT,
      avail_checked_at  TEXT,
      availability_status TEXT,
      availability_error TEXT,
      in_release_feed   INTEGER NOT NULL DEFAULT 1,
      ranked_candidate  INTEGER NOT NULL DEFAULT 0,
      ranking_first_seen_at TEXT,
      ranking_last_seen_at TEXT,
      first_free_at     TEXT
    );
  `);

  const insert = db.prepare(`
    INSERT INTO domains (
      name, base, tld, release_at, length,
      has_digit, has_hyphen, only_digits, only_letters,
      is_palindrome, has_repeat, is_cvcv, is_word,
      tranco_rank, majestic_rank, majestic_refsubnets,
      wayback_first, wayback_count, wayback_checked,
      dns_a, dns_mx, dns_ns, dns_checked, dns_status, dns_error,
      opr_rank, opr_score, cc_hosts,
      score_brand, score_authority, score_demand, score_risk, score_total,
      released, taken, taken_at, avail_checked_at, availability_status, availability_error,
      in_release_feed, ranked_candidate, ranking_first_seen_at, ranking_last_seen_at, first_free_at
    ) VALUES (
      @name, @base, @tld, @release_at, @length,
      @has_digit, @has_hyphen, @only_digits, @only_letters,
      @is_palindrome, @has_repeat, @is_cvcv, @is_word,
      @tranco_rank, @majestic_rank, @majestic_refsubnets,
      @wayback_first, @wayback_count, @wayback_checked,
      @dns_a, @dns_mx, @dns_ns, @dns_checked, @dns_status, @dns_error,
      @opr_rank, @opr_score, @cc_hosts,
      @score_brand, @score_authority, @score_demand, @score_risk, @score_total,
      @released, @taken, @taken_at, @avail_checked_at, @availability_status, @availability_error,
      @in_release_feed, @ranked_candidate, @ranking_first_seen_at, @ranking_last_seen_at, @first_free_at
    )
  `);

  const stats = {
    word: 0, tranco: 0, majestic: 0, opr: 0, cc: 0,
    waybackChecked: 0, waybackHits: 0,
    dnsChecked: 0, dnsAny: 0, dnsErrors: 0,
    rankedFree: 0
  };

  const tx = db.transaction((items) => {
    for (const r of items) {
      const name = String(r.name).toLowerCase();
      const dot = name.lastIndexOf('.');
      const base = dot === -1 ? name : name.slice(0, dot);
      const tld = dot === -1 ? '' : name.slice(dot + 1);

      const word = words && words.has(base) ? 1 : 0;
      const trancoRank = r.tranco_rank ?? (tranco ? tranco.get(name) ?? null : null);
      const maj = r.majestic_rank != null
        ? { rank: r.majestic_rank, refSubNets: r.majestic_refsubnets ?? 0 }
        : (majestic ? majestic.get(name) ?? null : null);
      const oprData = r.opr_rank != null
        ? { rank: r.opr_rank, score: r.opr_score ?? 0 }
        : (opr ? opr.get(name) ?? null : null);
      const ccHosts = cc ? cc.get(name) ?? null : null;
      const cache = cacheRows.get(name);

      const wbFirst = cache?.wayback_first ?? null;
      const wbCount = cache?.wayback_count ?? null;
      const wbChecked = cache?.wayback_checked_at ? 1 : 0;
      const dnsA = cache?.dns_a ?? null;
      const dnsMx = cache?.dns_mx ?? null;
      const dnsNs = cache?.dns_ns ?? null;
      const dnsChecked = cache?.dns_checked_at ? 1 : 0;
      const dnsStatus = cache?.dns_status ?? null;
      const dnsError = cache?.dns_error ?? null;
      const scores = scoreDomain({
        base, word, trancoRank, maj, oprData, ccHosts,
        wbFirst, wbCount
      });

      if (word) stats.word++;
      if (trancoRank != null) stats.tranco++;
      if (maj) stats.majestic++;
      if (oprData) stats.opr++;
      if (ccHosts != null) stats.cc++;
      if (wbChecked) {
        stats.waybackChecked++;
        if ((wbCount ?? 0) > 0) stats.waybackHits++;
      }
      if (dnsChecked) {
        stats.dnsChecked++;
        if (dnsA || dnsMx || dnsNs) stats.dnsAny++;
      }
      if (dnsStatus === 'error') stats.dnsErrors++;
      if (r.ranked_candidate) stats.rankedFree++;

      insert.run({
        name,
        base,
        tld,
        release_at: r.release_at,
        length: base.length,
        has_digit: /\d/.test(base) ? 1 : 0,
        has_hyphen: base.includes('-') ? 1 : 0,
        only_digits: /^\d+$/.test(base) ? 1 : 0,
        only_letters: /^[a-zåäö]+$/.test(base) ? 1 : 0,
        is_palindrome: isPalindrome(base) ? 1 : 0,
        has_repeat: hasRepeatedChar(base) ? 1 : 0,
        is_cvcv: isCvcv(base) ? 1 : 0,
        is_word: word,
        tranco_rank: trancoRank,
        majestic_rank: maj?.rank ?? null,
        majestic_refsubnets: maj?.refSubNets ?? null,
        wayback_first: wbFirst,
        wayback_count: wbCount,
        wayback_checked: wbChecked,
        dns_a: dnsA,
        dns_mx: dnsMx,
        dns_ns: dnsNs,
        dns_checked: dnsChecked,
        dns_status: dnsStatus,
        dns_error: dnsError,
        opr_rank: oprData?.rank ?? null,
        opr_score: oprData?.score ?? null,
        cc_hosts: ccHosts,
        score_brand: scores.brand,
        score_authority: scores.authority,
        score_demand: scores.demand,
        score_risk: scores.risk,
        score_total: scores.total,
        released: r.released ?? 0,
        taken: r.taken ?? null,
        taken_at: r.taken_at ?? null,
        avail_checked_at: r.avail_checked_at ?? null,
        availability_status: r.avail_status ?? null,
        availability_error: r.avail_error ?? null,
        in_release_feed: r.in_release_feed ?? 1,
        ranked_candidate: r.ranked_candidate ?? 0,
        ranking_first_seen_at: r.ranking_first_seen_at ?? null,
        ranking_last_seen_at: r.ranking_last_seen_at ?? null,
        first_free_at: r.first_free_at ?? null
      });
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
    CREATE INDEX idx_score_total ON domains(score_total);
    CREATE INDEX idx_available_score ON domains(released, availability_status, score_total);
    CREATE INDEX idx_opr ON domains(opr_rank);
    CREATE INDEX idx_cc ON domains(cc_hosts);
    CREATE INDEX idx_released ON domains(released, release_at);
    CREATE INDEX idx_release_feed ON domains(in_release_feed, released, release_at);
    CREATE INDEX idx_ranked_free ON domains(ranked_candidate, availability_status, score_total);
    CREATE INDEX idx_ranked_first_free ON domains(first_free_at);
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
      opr_hits: stats.opr,
      cc_hits: stats.cc,
      wayback_checked: stats.waybackChecked,
      wayback_hits: stats.waybackHits,
      dns_checked: stats.dnsChecked,
      dns_active: stats.dnsAny,
      dns_errors: stats.dnsErrors,
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
  const now = new Date();
  const todayStr = now.toISOString().slice(0, 10);
  // Frisläppningen startar tidigast 04.00 UTC. Före dess räknas dagens
  // domäner fortfarande som kommande så att "Nästa 24 h" inte tappar dem.
  const releasedThrough = now.getUTCHours() >= 4
    ? todayStr
    : new Date(now.getTime() - 86400000).toISOString().slice(0, 10);
  const releasedFrom = new Date(
    new Date(`${releasedThrough}T00:00:00Z`).getTime() - Math.max(0, RELEASED_DAYS - 1) * 86400000
  ).toISOString().slice(0, 10);
  // Kvar i karens = releasefönstret har inte börjat ännu.
  const future = all.filter((r) => r.release_at > releasedThrough);

  // 2. Snabba berikningar parallellt
  const [words, tranco, majestic, opr, cc] = await Promise.all([
    loadWordSet(),
    loadTrancoMap(),
    loadMajestic(),
    loadOpr(),
    loadCc()
  ]);

  // 3. Rolling enrichment — prioritera domäner som frisläpps snarast så att
  // dagens batch täcker det användarna faktiskt tittar på
  const releaseAt = new Map(all.map((r) => [r.name, r.release_at]));
  const sortByRelease = (names) =>
    names.sort((a, b) => (releaseAt.get(a) ?? '').localeCompare(releaseAt.get(b) ?? ''));
  const batchSpan = (names, max) => {
    const n = Math.min(names.length, Math.max(0, max));
    if (!n) return 'ingen (batch=0)';
    return `${releaseAt.get(names[0])} → ${releaseAt.get(names[n - 1])}`;
  };

  const cache = new EnrichmentCache(CACHE_DB);

  const rankedSync = cache.recordRankedCandidates({ tranco, majestic, opr });
  log(`  Rankningskandidater: ${rankedSync.active.toLocaleString('sv-SE')} aktiva · ${rankedSync.newCandidates.toLocaleString('sv-SE')} nya · källor ${rankedSync.sourcesUpdated.join(', ') || 'inga'}`);

  // Nysläppta: anteckna dagens karensdata, kolla om frisläppta domäner tagits
  cache.recordKarens(all);
  cache.pruneKarens(releasedFrom);
  let released = cache.getReleased(releasedFrom, releasedThrough);
  log(`  Nysläppta: ${released.length.toLocaleString('sv-SE')} domäner i fönstret ${releasedFrom} → ${releasedThrough}`);
  if (process.env.SKIP_AVAIL !== '1' && released.length) {
    await checkAvailability(released, cache, { log });
    released = cache.getReleased(releasedFrom, releasedThrough);
  } else if (process.env.SKIP_AVAIL === '1') { warn('SKIP_AVAIL=1'); }
  const releasedRows = released.map((r) => ({
    name: r.domain, tld: r.tld, release_at: r.release_at,
    released: 1,
    taken: r.taken,
    taken_at: r.taken_at,
    avail_checked_at: r.avail_checked_at,
    avail_status: r.avail_status,
    avail_error: r.avail_error
  }));
  const releasedNames = releasedRows.map((r) => r.name);
  const rankedSyncedFromKarens = cache.syncRankedAvailabilityFromKarens();
  if (rankedSyncedFromKarens) log(`  Rankningskö: återanvände ${rankedSyncedFromKarens.toLocaleString('sv-SE')} färska karenskontroller`);

  let rankedCheck = { checked: 0, occupied: 0, free: 0, errors: 0 };
  if (process.env.SKIP_RANKED_AVAIL !== '1' && RANKED_AVAIL_MAX_PER_RUN > 0) {
    const rankedDue = cache.getRankedDue(RANKED_AVAIL_MAX_PER_RUN, {
      freeHours: RANKED_FREE_REFRESH_HOURS,
      occupiedDays: RANKED_OCCUPIED_REFRESH_DAYS
    });
    rankedCheck = await checkRankedAvailability(rankedDue, cache, { log });
  } else if (process.env.SKIP_RANKED_AVAIL === '1') {
    warn('SKIP_RANKED_AVAIL=1');
  }

  const rankedFree = cache.getRankedFree();
  const rankedFreeNames = rankedFree.map((r) => r.domain);
  const rankedStats = cache.getRankedStats();
  log(`  Äldre guldkorn: ${rankedStats.free.toLocaleString('sv-SE')} lediga · ${rankedStats.occupied.toLocaleString('sv-SE')} upptagna · ${rankedStats.unchecked.toLocaleString('sv-SE')} väntar`);

  const pruned = cache.prune(new Set([...allDomains, ...releasedNames, ...rankedFreeNames]));
  if (pruned > 0) log(`  Cache: rensade ${pruned} gamla rader`);

  if (process.env.SKIP_DNS !== '1') {
    const need = sortByRelease(cache.needsUpdate('dns', allDomains, DNS_REFRESH_DAYS));
    if (need.length) log(`  DNS-batch täcker frisläpp ${batchSpan(need, DNS_MAX_PER_RUN)}`);
    await enrichDns(need, cache, { maxPerRun: DNS_MAX_PER_RUN, log });
  } else { warn('SKIP_DNS=1'); }

  if (process.env.SKIP_WAYBACK !== '1') {
    const waybackDomains = [...new Set([...rankedFreeNames, ...allDomains])];
    const need = sortByRelease(cache.needsUpdate('wayback', waybackDomains, WAYBACK_REFRESH_DAYS));
    if (need.length) log(`  Wayback-batch täcker frisläpp ${batchSpan(need, WAYBACK_MAX_PER_RUN)}`);
    await enrichWayback(need, cache, { maxPerRun: WAYBACK_MAX_PER_RUN, log });
  } else { warn('SKIP_WAYBACK=1'); }

  // 4. Hämta cachade berikningar för alla aktuella domäner
  const cacheRows = cache.getMany([...allDomains, ...releasedNames, ...rankedFreeNames]);
  cache.close();

  // 5. Bygg sqlite. Rankade lediga domäner slås ihop med karensrader när de
  // överlappar; äldre kandidater får första observerade ledighetsdatum som
  // internt listdatum men hålls utanför de vanliga frisläppningsvyerna.
  const dbRowMap = new Map();
  for (const row of releasedRows) dbRowMap.set(row.name, { ...row, in_release_feed: 1 });
  for (const row of future) {
    dbRowMap.set(row.name, {
      ...row,
      released: 0,
      taken: null,
      taken_at: null,
      avail_checked_at: null,
      in_release_feed: 1
    });
  }
  for (const ranked of rankedFree) {
    const existing = dbRowMap.get(ranked.domain);
    const rankedFields = {
      ranked_candidate: 1,
      ranking_first_seen_at: ranked.first_seen_at,
      ranking_last_seen_at: ranked.last_seen_at,
      first_free_at: ranked.first_free_at,
      tranco_rank: ranked.tranco_rank,
      majestic_rank: ranked.majestic_rank,
      majestic_refsubnets: ranked.majestic_refsubnets,
      opr_rank: ranked.opr_rank,
      opr_score: ranked.opr_score
    };
    if (existing) {
      Object.assign(existing, rankedFields);
      if (!existing.avail_checked_at || (ranked.avail_checked_at && ranked.avail_checked_at > existing.avail_checked_at)) {
        existing.taken = ranked.taken;
        existing.avail_checked_at = ranked.avail_checked_at;
        existing.avail_status = ranked.avail_status;
        existing.avail_error = ranked.avail_error;
      }
    } else {
      dbRowMap.set(ranked.domain, {
        name: ranked.domain,
        tld: ranked.tld,
        release_at: (ranked.first_free_at || todayStr).slice(0, 10),
        released: 1,
        taken: 0,
        taken_at: null,
        avail_checked_at: ranked.avail_checked_at,
        avail_status: ranked.avail_status,
        avail_error: ranked.avail_error,
        in_release_feed: 0,
        ...rankedFields
      });
    }
  }
  const dbRows = [...dbRowMap.values()];
  log(`→ Bygger SQLite med ${dbRows.length.toLocaleString('sv-SE')} rader (${releasedRows.length} nysläppta · ${rankedFree.length} rankade lediga)`);
  const stats = buildDatabase(dbRows, words, tranco, majestic, opr, cc, cacheRows);

  log(`  Berikningar: ord=${stats.word}, tranco=${stats.tranco}, majestic=${stats.majestic}, opr=${stats.opr}, cc=${stats.cc}`);
  log(`  Wayback: ${stats.waybackChecked} kollade, ${stats.waybackHits} med snapshots`);
  log(`  DNS: ${stats.dnsChecked} kollade, ${stats.dnsAny} med aktiva records`);

  const meta = buildMeta(future, stats, dbRows.length);
  meta.released = {
    window_days: RELEASED_DAYS,
    total: releasedRows.length,
    taken: releasedRows.filter((r) => r.taken === 1).length,
    free: releasedRows.filter((r) => r.avail_status === 'free').length,
    errors: releasedRows.filter((r) => r.avail_status === 'error').length,
    unchecked: releasedRows.filter((r) => r.avail_status == null).length
  };
  meta.ranked = {
    candidates: rankedStats.active,
    free: rankedStats.free,
    occupied: rankedStats.occupied,
    errors: rankedStats.errors,
    unchecked: rankedStats.unchecked,
    new_this_run: rankedSync.newCandidates,
    checked_this_run: rankedCheck.checked,
    sources_updated: rankedSync.sourcesUpdated,
    date_range: {
      min: rankedFree.length ? rankedFree[0].first_free_at.slice(0, 10) : null,
      max: rankedFree.length ? rankedFree[rankedFree.length - 1].first_free_at.slice(0, 10) : null
    }
  };
  writeFileSync(META_PATH, JSON.stringify(meta, null, 2), 'utf8');

  const sizeMB = (statSync(DB_PATH).size / 1024 / 1024).toFixed(2);
  log(`✓ Klar. domains.sqlite = ${sizeMB} MB, ${dbRows.length} rader`);
}

main().catch((err) => {
  console.error('✗ Build-data misslyckades:', err);
  process.exit(1);
});
