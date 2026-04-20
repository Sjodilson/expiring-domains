import './style.css';
import initSqlJs, { type Database, type SqlValue } from 'sql.js';
import wasmUrl from 'sql.js/dist/sql-wasm.wasm?url';

// ───────────────────────────────────────────────────────────────────────────
// Konstanter och typer
// ───────────────────────────────────────────────────────────────────────────
const ROW_HEIGHT = 36;
const BUFFER_ROWS = 8;
const PAGE_SIZE = 500;
const WATCHLIST_KEY = 'watchlist:v1';
const NOTES_KEY = 'notes:v1';
const SAVED_KEY = 'saved:v1';
const THEME_KEY = 'theme';

interface Meta {
  generated_at: string;
  total: number;
  by_tld: Record<string, number>;
  by_date: Record<string, number>;
  by_length: Record<string, number>;
  date_range: { min: string; max: string };
  enrichments?: {
    word_hits?: number;
    tranco_hits?: number;
    majestic_hits?: number;
    wayback_checked?: number;
    wayback_hits?: number;
    dns_checked?: number;
    dns_active?: number;
    coverage_wayback?: number;
    coverage_dns?: number;
  };
}

interface Row {
  name: string;
  base: string;
  tld: string;
  length: number;
  release_at: string;
  has_digit: number;
  has_hyphen: number;
  only_digits: number;
  only_letters: number;
  is_palindrome: number;
  has_repeat: number;
  is_cvcv: number;
  is_word: number;
  tranco_rank: number | null;
  majestic_rank: number | null;
  majestic_refsubnets: number | null;
  wayback_first: string | null;
  wayback_count: number | null;
  wayback_checked: number;
  dns_a: number | null;
  dns_mx: number | null;
  dns_ns: number | null;
  dns_checked: number;
  opr_rank: number | null;
  opr_score: number | null;
  cc_hosts: number | null;
}

interface Filters {
  q: string;
  regex: boolean;
  tld: string;
  from: string;
  to: string;
  sortKey: 'release_at' | 'name' | 'length' | 'majestic_refsubnets' | 'opr_score';
  sortDir: 'asc' | 'desc';
  noDigit: boolean;
  noHyphen: boolean;
  onlyDigits: boolean;
  onlyLetters: boolean;
  palindrome: boolean;
  repeat: boolean;
  cvcv: boolean;
  word: boolean;
  tranco: boolean;
  majestic: boolean;
  opr: boolean;
  cc: boolean;
  wayback: boolean;
  dns: boolean;
  watch: boolean;
  notes: boolean;
  minLen: number | null;
  maxLen: number | null;
}

const DEFAULT_FILTERS: Filters = {
  q: '', regex: false, tld: '', from: '', to: '',
  sortKey: 'release_at', sortDir: 'asc',
  noDigit: false, noHyphen: false, onlyDigits: false, onlyLetters: false,
  palindrome: false, repeat: false, cvcv: false,
  word: false, tranco: false, majestic: false, opr: false, cc: false, wayback: false, dns: false,
  watch: false, notes: false, minLen: null, maxLen: null
};

// ───────────────────────────────────────────────────────────────────────────
// DOM
// ───────────────────────────────────────────────────────────────────────────
const $ = <T extends HTMLElement = HTMLElement>(id: string) =>
  document.getElementById(id) as T;

const els = {
  meta: $('meta'),
  status: $('status'),
  q: $<HTMLInputElement>('q'),
  regex: $<HTMLInputElement>('regex'),
  from: $<HTMLInputElement>('from'),
  to: $<HTMLInputElement>('to'),
  dateCustom: $('date-custom'),
  dateCustomToggle: $<HTMLButtonElement>('date-custom-toggle'),
  fNoDigit: $<HTMLInputElement>('f-nodigit'),
  fNoHyphen: $<HTMLInputElement>('f-nohyphen'),
  fOnlyDigits: $<HTMLInputElement>('f-only-digits'),
  fOnlyLetters: $<HTMLInputElement>('f-only-letters'),
  fPalindrome: $<HTMLInputElement>('f-palindrome'),
  fRepeat: $<HTMLInputElement>('f-repeat'),
  fCvcv: $<HTMLInputElement>('f-cvcv'),
  fWord: $<HTMLInputElement>('f-word'),
  fTranco: $<HTMLInputElement>('f-tranco'),
  fMajestic: $<HTMLInputElement>('f-majestic'),
  fOpr: $<HTMLInputElement>('f-opr'),
  fCc: $<HTMLInputElement>('f-cc'),
  fWayback: $<HTMLInputElement>('f-wayback'),
  fDns: $<HTMLInputElement>('f-dns'),
  fWatch: $<HTMLInputElement>('f-watch'),
  fNotes: $<HTMLInputElement>('f-notes'),
  fMinLen: $<HTMLInputElement>('f-minlen'),
  fMaxLen: $<HTMLInputElement>('f-maxlen'),
  reset: $<HTMLButtonElement>('reset'),
  viewport: $('viewport'),
  spacer: $('spacer'),
  rows: $('rows'),
  empty: $('empty'),
  thead: $('thead'),
  histogram: $('histogram'),
  histTotal: $('hist-total'),
  datebar: $('datebar'),
  activePills: $('active-pills'),
  exportCsv: $<HTMLButtonElement>('export-csv'),
  exportJson: $<HTMLButtonElement>('export-json'),
  copyLink: $<HTMLButtonElement>('copy-link'),
  themeToggle: $<HTMLButtonElement>('theme-toggle'),
  help: $<HTMLButtonElement>('help'),
  helpModal: $('help-modal'),
  helpClose: $<HTMLButtonElement>('help-close'),
  drawer: $('drawer'),
  drawerBackdrop: $('drawer-backdrop'),
  drawerClose: $<HTMLButtonElement>('drawer-close'),
  drawerTitle: $('drawer-title'),
  drawerSub: $('drawer-sub'),
  drawerBody: $('drawer-body'),
  propsBtn: $<HTMLButtonElement>('props-btn'),
  propsPop: $('props-pop'),
  propsBadge: $('props-badge'),
  qualBtn: $<HTMLButtonElement>('qual-btn'),
  qualPop: $('qual-pop'),
  qualBadge: $('qual-badge'),
  savedBtn: $<HTMLButtonElement>('saved-btn'),
  savedPop: $('saved-pop'),
  savedName: $<HTMLInputElement>('saved-name'),
  savedAdd: $<HTMLButtonElement>('saved-add'),
  savedList: $('saved-list')
};

// ───────────────────────────────────────────────────────────────────────────
// Tillstånd
// ───────────────────────────────────────────────────────────────────────────
let db: Database | null = null;
let meta: Meta | null = null;
let activeIdx = -1;
const cache = new Map<number, Row>();

type Mode =
  | { kind: 'paged'; total: number }
  | { kind: 'array'; rows: Row[] };
let mode: Mode = { kind: 'paged', total: 0 };

const watchlist: Set<string> = loadJson<string[], Set<string>>(WATCHLIST_KEY, [], (a) => new Set(a));
const notes: Map<string, string> = loadJson<Record<string, string>, Map<string, string>>(NOTES_KEY, {}, (o) => new Map(Object.entries(o)));
let saved: { id: string; name: string; qs: string }[] = loadJson(SAVED_KEY, [], (v) => v as { id: string; name: string; qs: string }[]);

let activeTld = '';
let activePreset: '' | 'all' | 'today' | '7d' | '30d' = '';
let drawerTab: 'overview' | 'links' | 'similar' | 'notes' = 'overview';
let drawerRow: Row | null = null;

// ───────────────────────────────────────────────────────────────────────────
// Storage helpers
// ───────────────────────────────────────────────────────────────────────────
function loadJson<TRaw, TParsed>(key: string, fallback: TRaw, parse: (raw: TRaw) => TParsed): TParsed {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return parse(fallback);
    return parse(JSON.parse(raw) as TRaw);
  } catch {
    return parse(fallback);
  }
}
function saveWatchlist() {
  try { localStorage.setItem(WATCHLIST_KEY, JSON.stringify([...watchlist])); } catch {}
}
function saveNotes() {
  try { localStorage.setItem(NOTES_KEY, JSON.stringify(Object.fromEntries(notes))); } catch {}
}
function saveSaved() {
  try { localStorage.setItem(SAVED_KEY, JSON.stringify(saved)); } catch {}
}

function toggleWatch(name: string) {
  if (watchlist.has(name)) watchlist.delete(name); else watchlist.add(name);
  saveWatchlist();
}

// ───────────────────────────────────────────────────────────────────────────
// Helpers
// ───────────────────────────────────────────────────────────────────────────
function setStatus(msg: string) {
  els.status.textContent = msg;
}
function escapeHtml(s: string): string {
  return s.replace(/[&<>"']/g, (c) =>
    c === '&' ? '&amp;' : c === '<' ? '&lt;' : c === '>' ? '&gt;' : c === '"' ? '&quot;' : '&#39;');
}
function debounce<T extends (...args: never[]) => void>(fn: T, ms: number): T {
  let t: ReturnType<typeof setTimeout> | null = null;
  return ((...args: never[]) => { if (t) clearTimeout(t); t = setTimeout(() => fn(...args), ms); }) as T;
}
function todayISO(): string { return new Date().toISOString().slice(0, 10); }
function isoPlusDays(days: number): string {
  const d = new Date(); d.setDate(d.getDate() + days);
  return d.toISOString().slice(0, 10);
}

// ───────────────────────────────────────────────────────────────────────────
// Filter <-> form
// ───────────────────────────────────────────────────────────────────────────
function readFilters(): Filters {
  return {
    q: els.q.value.trim(),
    regex: els.regex.checked,
    tld: activeTld,
    from: els.from.value,
    to: els.to.value,
    sortKey: currentSort.key,
    sortDir: currentSort.dir,
    noDigit: els.fNoDigit.checked,
    noHyphen: els.fNoHyphen.checked,
    onlyDigits: els.fOnlyDigits.checked,
    onlyLetters: els.fOnlyLetters.checked,
    palindrome: els.fPalindrome.checked,
    repeat: els.fRepeat.checked,
    cvcv: els.fCvcv.checked,
    word: els.fWord.checked,
    tranco: els.fTranco.checked,
    majestic: els.fMajestic.checked,
    opr: els.fOpr.checked,
    cc: els.fCc.checked,
    wayback: els.fWayback.checked,
    dns: els.fDns.checked,
    watch: els.fWatch.checked,
    notes: els.fNotes.checked,
    minLen: els.fMinLen.value ? parseInt(els.fMinLen.value, 10) : null,
    maxLen: els.fMaxLen.value ? parseInt(els.fMaxLen.value, 10) : null
  };
}

function writeFilters(f: Partial<Filters>) {
  if (f.q !== undefined) els.q.value = f.q;
  if (f.regex !== undefined) els.regex.checked = f.regex;
  if (f.tld !== undefined) { activeTld = f.tld; updateTldPills(); }
  if (f.from !== undefined) els.from.value = f.from;
  if (f.to !== undefined) els.to.value = f.to;
  if (f.sortKey !== undefined) currentSort.key = f.sortKey;
  if (f.sortDir !== undefined) currentSort.dir = f.sortDir;
  if (f.noDigit !== undefined) els.fNoDigit.checked = f.noDigit;
  if (f.noHyphen !== undefined) els.fNoHyphen.checked = f.noHyphen;
  if (f.onlyDigits !== undefined) els.fOnlyDigits.checked = f.onlyDigits;
  if (f.onlyLetters !== undefined) els.fOnlyLetters.checked = f.onlyLetters;
  if (f.palindrome !== undefined) els.fPalindrome.checked = f.palindrome;
  if (f.repeat !== undefined) els.fRepeat.checked = f.repeat;
  if (f.cvcv !== undefined) els.fCvcv.checked = f.cvcv;
  if (f.word !== undefined) els.fWord.checked = f.word;
  if (f.tranco !== undefined) els.fTranco.checked = f.tranco;
  if (f.majestic !== undefined) els.fMajestic.checked = f.majestic;
  if (f.opr !== undefined) els.fOpr.checked = f.opr;
  if (f.cc !== undefined) els.fCc.checked = f.cc;
  if (f.wayback !== undefined) els.fWayback.checked = f.wayback;
  if (f.dns !== undefined) els.fDns.checked = f.dns;
  if (f.watch !== undefined) els.fWatch.checked = f.watch;
  if (f.notes !== undefined) els.fNotes.checked = f.notes;
  if (f.minLen !== undefined) els.fMinLen.value = f.minLen == null ? '' : String(f.minLen);
  if (f.maxLen !== undefined) els.fMaxLen.value = f.maxLen == null ? '' : String(f.maxLen);
}

const currentSort = { key: 'release_at' as Filters['sortKey'], dir: 'asc' as Filters['sortDir'] };

function buildOrderBy(f: Filters): string {
  const dir = f.sortDir === 'asc' ? 'ASC' : 'DESC';
  // Säkerställ deterministiskt: lägg alltid till name som tiebreaker
  if (f.sortKey === 'name') return `name ${dir}`;
  if (f.sortKey === 'length') return `length ${dir}, name ASC`;
  if (f.sortKey === 'majestic_refsubnets') {
    // NULLs last: sortera null-rader sist oavsett riktning
    return `CASE WHEN majestic_refsubnets IS NULL THEN 1 ELSE 0 END, majestic_refsubnets ${dir}, name ASC`;
  }
  if (f.sortKey === 'opr_score') {
    return `CASE WHEN opr_score IS NULL THEN 1 ELSE 0 END, opr_score ${dir}, name ASC`;
  }
  return `release_at ${dir}, name ASC`;
}

// ───────────────────────────────────────────────────────────────────────────
// URL permalink
// ───────────────────────────────────────────────────────────────────────────
const SHORT_KEY: Record<string, string> = {
  q: 'q', regex: 're', tld: 't', from: 'f', to: 'u',
  sortKey: 'sk', sortDir: 'sd',
  noDigit: 'nd', noHyphen: 'nh', onlyDigits: 'od', onlyLetters: 'ol',
  palindrome: 'pa', repeat: 'rp', cvcv: 'cv',
  word: 'w', tranco: 'tr', majestic: 'mj', opr: 'op', cc: 'cc', wayback: 'wb', dns: 'dn',
  watch: 'wl', notes: 'nt', minLen: 'mn', maxLen: 'mx'
};
const LONG_KEY: Record<string, keyof Filters> = Object.fromEntries(
  Object.entries(SHORT_KEY).map(([k, v]) => [v, k as keyof Filters])
);

function filtersToQs(f: Filters): string {
  const p = new URLSearchParams();
  if (f.q) p.set(SHORT_KEY.q, f.q);
  if (f.regex) p.set(SHORT_KEY.regex, '1');
  if (f.tld) p.set(SHORT_KEY.tld, f.tld);
  if (f.from) p.set(SHORT_KEY.from, f.from);
  if (f.to) p.set(SHORT_KEY.to, f.to);
  if (f.sortKey !== 'release_at') p.set(SHORT_KEY.sortKey, f.sortKey);
  if (f.sortDir !== 'asc') p.set(SHORT_KEY.sortDir, f.sortDir);
  for (const k of ['noDigit','noHyphen','onlyDigits','onlyLetters','palindrome','repeat','cvcv','word','tranco','majestic','wayback','dns','watch','notes'] as const) {
    if (f[k]) p.set(SHORT_KEY[k], '1');
  }
  if (f.minLen != null) p.set(SHORT_KEY.minLen, String(f.minLen));
  if (f.maxLen != null) p.set(SHORT_KEY.maxLen, String(f.maxLen));
  return p.toString();
}

function qsToFilters(qs: string): Partial<Filters> {
  const p = new URLSearchParams(qs);
  const f: Record<string, unknown> = {};
  for (const [short, val] of p.entries()) {
    const long = LONG_KEY[short];
    if (!long) continue;
    if (long === 'minLen' || long === 'maxLen') {
      const n = parseInt(val, 10);
      f[long] = isNaN(n) ? null : n;
    } else if (long === 'q' || long === 'tld' || long === 'from' || long === 'to') {
      f[long] = val;
    } else if (long === 'sortKey') {
      f[long] = val === 'name' || val === 'length' ? val : 'release_at';
    } else if (long === 'sortDir') {
      f[long] = val === 'desc' ? 'desc' : 'asc';
    } else {
      f[long] = val === '1';
    }
  }
  return f as Partial<Filters>;
}

function syncUrl(f: Filters) {
  const qs = filtersToQs(f);
  const url = location.pathname + (qs ? '?' + qs : '') + location.hash;
  history.replaceState(null, '', url);
}

// ───────────────────────────────────────────────────────────────────────────
// SQL filter
// ───────────────────────────────────────────────────────────────────────────
function buildWhere(f: Filters, includeQ: boolean, includeNotes: boolean): { sql: string; params: SqlValue[] } {
  const clauses: string[] = [];
  const params: SqlValue[] = [];
  if (includeQ && f.q && !f.regex) {
    const q = f.q.toLowerCase();
    const hasWildcard = q.includes('%') || q.includes('_');
    clauses.push('base LIKE ?'); params.push(hasWildcard ? q : `%${q}%`);
  }
  if (f.tld) { clauses.push('tld = ?'); params.push(f.tld); }
  if (f.from) { clauses.push('release_at >= ?'); params.push(f.from); }
  if (f.to) { clauses.push('release_at <= ?'); params.push(f.to); }
  if (f.noDigit) clauses.push('has_digit = 0');
  if (f.noHyphen) clauses.push('has_hyphen = 0');
  if (f.onlyDigits) clauses.push('only_digits = 1');
  if (f.onlyLetters) clauses.push('only_letters = 1');
  if (f.palindrome) clauses.push('is_palindrome = 1');
  if (f.repeat) clauses.push('has_repeat = 1');
  if (f.cvcv) clauses.push('is_cvcv = 1');
  if (f.word) clauses.push('is_word = 1');
  if (f.tranco) clauses.push('tranco_rank IS NOT NULL');
  if (f.majestic) clauses.push('majestic_rank IS NOT NULL');
  if (f.opr) clauses.push('opr_rank IS NOT NULL');
  if (f.cc) clauses.push('cc_hosts IS NOT NULL');
  if (f.wayback) clauses.push('wayback_count > 0');
  if (f.dns) clauses.push('(dns_a = 1 OR dns_mx = 1 OR dns_ns = 1)');
  if (f.minLen != null) { clauses.push('length >= ?'); params.push(f.minLen); }
  if (f.maxLen != null) { clauses.push('length <= ?'); params.push(f.maxLen); }
  if (f.watch) {
    if (watchlist.size === 0) { clauses.push('1 = 0'); }
    else {
      clauses.push(`name IN (${[...watchlist].map(() => '?').join(',')})`);
      for (const n of watchlist) params.push(n);
    }
  }
  if (includeNotes && f.notes) {
    if (notes.size === 0) { clauses.push('1 = 0'); }
    else {
      clauses.push(`name IN (${[...notes.keys()].map(() => '?').join(',')})`);
      for (const n of notes.keys()) params.push(n);
    }
  }
  return { sql: clauses.length ? 'WHERE ' + clauses.join(' AND ') : '', params };
}

function rowFromObject(o: Record<string, unknown>): Row {
  const num = (k: string) => (o[k] == null ? null : Number(o[k]));
  return {
    name: String(o.name), base: String(o.base), tld: String(o.tld),
    length: Number(o.length), release_at: String(o.release_at),
    has_digit: Number(o.has_digit), has_hyphen: Number(o.has_hyphen),
    only_digits: Number(o.only_digits), only_letters: Number(o.only_letters),
    is_palindrome: Number(o.is_palindrome), has_repeat: Number(o.has_repeat),
    is_cvcv: Number(o.is_cvcv), is_word: Number(o.is_word),
    tranco_rank: num('tranco_rank'),
    majestic_rank: num('majestic_rank'),
    majestic_refsubnets: num('majestic_refsubnets'),
    wayback_first: o.wayback_first == null ? null : String(o.wayback_first),
    wayback_count: num('wayback_count'),
    wayback_checked: Number(o.wayback_checked),
    dns_a: num('dns_a'), dns_mx: num('dns_mx'), dns_ns: num('dns_ns'),
    dns_checked: Number(o.dns_checked),
    opr_rank: num('opr_rank'), opr_score: num('opr_score'),
    cc_hosts: num('cc_hosts')
  };
}

const SELECT_COLS = 'name, base, tld, release_at, length, has_digit, has_hyphen, only_digits, only_letters, is_palindrome, has_repeat, is_cvcv, is_word, tranco_rank, majestic_rank, majestic_refsubnets, wayback_first, wayback_count, wayback_checked, dns_a, dns_mx, dns_ns, dns_checked, opr_rank, opr_score, cc_hosts';

function runCount(f: Filters): number {
  if (!db) return 0;
  const { sql, params } = buildWhere(f, true, true);
  const stmt = db.prepare(`SELECT COUNT(*) AS c FROM domains ${sql}`);
  stmt.bind(params); stmt.step();
  const row = stmt.getAsObject() as { c: number };
  stmt.free();
  return row.c;
}

function runPage(f: Filters, offset: number, limit: number): Row[] {
  if (!db) return [];
  const { sql, params } = buildWhere(f, true, true);
  const stmt = db.prepare(`SELECT ${SELECT_COLS} FROM domains ${sql} ORDER BY ${buildOrderBy(f)} LIMIT ? OFFSET ?`);
  stmt.bind([...params, limit, offset]);
  const out: Row[] = [];
  while (stmt.step()) out.push(rowFromObject(stmt.getAsObject()));
  stmt.free();
  return out;
}

function runAll(f: Filters, includeQ: boolean): Row[] {
  if (!db) return [];
  const { sql, params } = buildWhere(f, includeQ, true);
  const stmt = db.prepare(`SELECT ${SELECT_COLS} FROM domains ${sql} ORDER BY ${buildOrderBy(f)}`);
  stmt.bind(params);
  const out: Row[] = [];
  while (stmt.step()) out.push(rowFromObject(stmt.getAsObject()));
  stmt.free();
  return out;
}

function runHistogram(f: Filters): Array<[number, number]> {
  if (!db) return [];
  const { sql, params } = buildWhere(f, true, true);
  const stmt = db.prepare(`SELECT length, COUNT(*) AS c FROM domains ${sql} GROUP BY length ORDER BY length`);
  stmt.bind(params);
  const out: Array<[number, number]> = [];
  while (stmt.step()) {
    const o = stmt.getAsObject() as { length: number; c: number };
    out.push([o.length, o.c]);
  }
  stmt.free();
  return out;
}

function runDateBar(f: Filters, limit: number): Array<[string, number]> {
  if (!db) return [];
  const { sql, params } = buildWhere(f, true, true);
  const stmt = db.prepare(`SELECT release_at AS d, COUNT(*) AS c FROM domains ${sql} GROUP BY release_at ORDER BY release_at LIMIT ?`);
  stmt.bind([...params, limit]);
  const out: Array<[string, number]> = [];
  while (stmt.step()) {
    const o = stmt.getAsObject() as { d: string; c: number };
    out.push([o.d, o.c]);
  }
  stmt.free();
  return out;
}

// ───────────────────────────────────────────────────────────────────────────
// Mode dispatch
// ───────────────────────────────────────────────────────────────────────────
function getRow(idx: number): Row | undefined {
  if (mode.kind === 'array') return mode.rows[idx];
  if (cache.has(idx)) return cache.get(idx);
  const pageIdx = Math.floor(idx / PAGE_SIZE);
  const f = readFilters();
  const startIdx = pageIdx * PAGE_SIZE;
  const rows = runPage(f, startIdx, PAGE_SIZE);
  for (let i = 0; i < rows.length; i++) cache.set(startIdx + i, rows[i]);
  return cache.get(idx);
}

function getTotal(): number {
  return mode.kind === 'paged' ? mode.total : mode.rows.length;
}

// ───────────────────────────────────────────────────────────────────────────
// Rendering
// ───────────────────────────────────────────────────────────────────────────
function signalsHtml(r: Row): string {
  const out: string[] = [];
  if (r.is_word) out.push('<span class="sig word" title="Är ett svenskt ord">📖</span>');
  if (r.tranco_rank != null) out.push(`<span class="sig tranco" title="Tranco-rank ${r.tranco_rank.toLocaleString('sv-SE')}">⭐</span>`);
  if (r.majestic_rank != null) out.push(`<span class="sig majestic" title="Majestic-rank ${r.majestic_rank.toLocaleString('sv-SE')} · ${r.majestic_refsubnets ?? 0} ref-subnät">🔗</span>`);
  if (r.opr_rank != null) out.push(`<span class="sig opr" title="Open PageRank #${r.opr_rank.toLocaleString('sv-SE')} · poäng ${r.opr_score ?? '?'}">📊</span>`);
  if (r.cc_hosts != null) out.push(`<span class="sig cc" title="Common Crawl: ${r.cc_hosts} host(s)">🌐</span>`);
  if ((r.wayback_count ?? 0) > 0) out.push(`<span class="sig wayback" title="${r.wayback_count} Wayback-snapshots">⏳</span>`);
  if (r.dns_checked && (r.dns_a || r.dns_mx || r.dns_ns)) {
    const parts = [r.dns_a && 'A', r.dns_mx && 'MX', r.dns_ns && 'NS'].filter(Boolean).join(' ');
    out.push(`<span class="sig dns" title="Aktiva DNS-records: ${parts}">📡</span>`);
  }
  if (r.is_palindrome) out.push('<span class="sig" title="Palindrom">↔</span>');
  if (r.is_cvcv) out.push('<span class="sig" title="Uttalbart CVCV-mönster">🗣</span>');
  if (notes.has(r.name)) out.push('<span class="sig" title="Har anteckning">📝</span>');
  const bl = r.majestic_refsubnets ?? r.cc_hosts;
  out.push(`<span class="metric" title="Backlinks: ${bl ?? 'saknas'}">BL ${bl ?? '-'}</span>`);
  out.push(`<span class="metric" title="Open PageRank (DA): ${r.opr_score != null ? r.opr_score.toFixed(2) : 'saknas'}">DA ${r.opr_score != null ? r.opr_score.toFixed(1) : '-'}</span>`);
  return out.join('');
}

function renderRow(idx: number, top: number): string {
  const row = getRow(idx);
  if (!row) {
    return `<div class="row" style="position:absolute;top:${top}px;left:0;right:0;height:${ROW_HEIGHT}px;">
      <span></span><span class="text-slate-300">…</span><span></span><span></span><span></span><span></span><span></span><span></span>
    </div>`;
  }
  const url = `https://internetstiftelsen.se/domain/${encodeURIComponent(row.name)}/`;
  const starred = watchlist.has(row.name);
  const activeCls = idx === activeIdx ? ' active' : '';
  return `<div class="row${activeCls}" data-idx="${idx}" style="position:absolute;top:${top}px;left:0;right:0;height:${ROW_HEIGHT}px;">
    <button class="star${starred ? ' active' : ''}" data-action="star" title="Bevaka (s)">${starred ? '★' : '☆'}</button>
    <a class="name" href="${url}" target="_blank" rel="noopener" data-action="open">${escapeHtml(row.name)}</a>
    <span class="signals">${signalsHtml(row)}</span>
    <span class="len">${row.length}</span>
    <span class="date">${escapeHtml(row.release_at)}</span>
    <button class="more" data-action="more" title="Detaljer (Enter)">⋯</button>
  </div>`;
}

function render() {
  const total = getTotal();
  els.empty.classList.toggle('hidden', total > 0);
  const scrollTop = els.viewport.scrollTop;
  const viewportH = els.viewport.clientHeight;
  const startIdx = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - BUFFER_ROWS);
  const endIdx = Math.min(total, Math.ceil((scrollTop + viewportH) / ROW_HEIGHT) + BUFFER_ROWS);
  let html = '';
  for (let i = startIdx; i < endIdx; i++) html += renderRow(i, i * ROW_HEIGHT);
  els.rows.innerHTML = html;
}

function renderHistogram() {
  if (!db) return;
  const f = readFilters();
  const data = runHistogram(f);
  if (!data.length) {
    els.histogram.innerHTML = '<div class="text-slate-400 italic">Inga träffar</div>';
    els.histTotal.textContent = '';
    return;
  }
  const total = data.reduce((s, [, c]) => s + c, 0);
  const max = Math.max(...data.map(([, c]) => c));
  els.histTotal.textContent = `${total.toLocaleString('sv-SE')} st`;
  let html = '';
  for (const [len, cnt] of data) {
    const pct = (cnt / max) * 100;
    html += `<div class="hist-row" data-len="${len}" title="Klicka för att filtrera på längd ${len}">
      <span class="lbl">${len}</span>
      <div class="bar-wrap"><div class="bar" style="width:${pct.toFixed(1)}%"></div></div>
      <span class="cnt">${cnt.toLocaleString('sv-SE')}</span>
    </div>`;
  }
  els.histogram.innerHTML = html;
}

function renderDateBar() {
  if (!db) return;
  const f = readFilters();
  const data = runDateBar(f, 30);
  if (!data.length) {
    els.datebar.innerHTML = '<div class="text-slate-400 italic">Inga träffar</div>';
    return;
  }
  const max = Math.max(...data.map(([, c]) => c));
  let html = '';
  for (const [d, cnt] of data) {
    const pct = (cnt / max) * 100;
    const short = d.slice(5); // MM-DD
    html += `<div class="date-row" data-date="${d}" title="Klicka för att filtrera på ${d}">
      <span class="lbl">${short}</span>
      <div class="bar-wrap"><div class="bar" style="width:${pct.toFixed(1)}%"></div></div>
      <span class="cnt">${cnt.toLocaleString('sv-SE')}</span>
    </div>`;
  }
  els.datebar.innerHTML = html;
}

// ───────────────────────────────────────────────────────────────────────────
// Active pills, badges, sort indicators
// ───────────────────────────────────────────────────────────────────────────
function renderActivePills() {
  const f = readFilters();
  const pills: { label: string; clear: () => void }[] = [];
  if (f.q) pills.push({ label: `"${f.q}"${f.regex ? ' regex' : ''}`, clear: () => { writeFilters({ q: '', regex: false }); refresh(); } });
  if (f.tld) pills.push({ label: `.${f.tld}`, clear: () => { writeFilters({ tld: '' }); refresh(); } });
  if (f.from || f.to) pills.push({ label: `${f.from || '…'} → ${f.to || '…'}`, clear: () => { writeFilters({ from: '', to: '' }); activePreset = ''; updateDatePresets(); refresh(); } });
  const propLabels: Record<string, string> = {
    noDigit: 'inga siffror', noHyphen: 'inga bindestreck', onlyDigits: 'endast siffror', onlyLetters: 'endast bokstäver',
    palindrome: 'palindrom', repeat: 'upprepning', cvcv: 'uttalbart',
    word: '📖 ord', tranco: '⭐ Tranco', majestic: '🔗 backlinks', wayback: '⏳ wayback', dns: '📡 dns',
    watch: '🔖 bevakade', notes: '📝 antecknade'
  };
  for (const k of Object.keys(propLabels) as (keyof Filters)[]) {
    if (f[k]) pills.push({ label: propLabels[k as string], clear: () => { writeFilters({ [k]: false } as Partial<Filters>); refresh(); } });
  }
  if (f.minLen != null) pills.push({ label: `len≥${f.minLen}`, clear: () => { writeFilters({ minLen: null }); refresh(); } });
  if (f.maxLen != null) pills.push({ label: `len≤${f.maxLen}`, clear: () => { writeFilters({ maxLen: null }); refresh(); } });

  if (pills.length === 0) {
    els.activePills.classList.add('hidden');
    return;
  }
  els.activePills.classList.remove('hidden');
  const inner = els.activePills.querySelector('div')!;
  inner.innerHTML = '';
  for (const p of pills) {
    const span = document.createElement('span');
    span.className = 'pill';
    span.innerHTML = `${escapeHtml(p.label)} <button type="button" aria-label="Ta bort">✕</button>`;
    span.querySelector('button')!.addEventListener('click', p.clear);
    inner.appendChild(span);
  }
}

function renderBadges() {
  // Räkna props/qual som är aktiva för att visa siffra på popover-knapp
  const f = readFilters();
  const propsActive = [f.noDigit, f.noHyphen, f.onlyDigits, f.onlyLetters, f.palindrome, f.repeat, f.cvcv, f.minLen != null, f.maxLen != null].filter(Boolean).length;
  const qualActive = [f.word, f.tranco, f.majestic, f.wayback, f.dns, f.watch, f.notes].filter(Boolean).length;
  if (propsActive) { els.propsBadge.textContent = String(propsActive); els.propsBadge.classList.remove('hidden'); els.propsBtn.classList.add('active'); }
  else { els.propsBadge.classList.add('hidden'); els.propsBtn.classList.remove('active'); }
  if (qualActive) { els.qualBadge.textContent = String(qualActive); els.qualBadge.classList.remove('hidden'); els.qualBtn.classList.add('active'); }
  else { els.qualBadge.classList.add('hidden'); els.qualBtn.classList.remove('active'); }
}

function renderSortIndicators() {
  for (const btn of els.thead.querySelectorAll<HTMLButtonElement>('[data-sort]')) {
    const ind = btn.querySelector('.sort-ind')!;
    if (btn.dataset.sort === currentSort.key) {
      btn.classList.add('active');
      ind.textContent = currentSort.dir === 'asc' ? '▲' : '▼';
    } else {
      btn.classList.remove('active');
      ind.textContent = '';
    }
  }
}

function updateTldPills() {
  for (const btn of document.querySelectorAll<HTMLButtonElement>('.tld-pill')) {
    btn.classList.toggle('active', btn.dataset.tld === activeTld);
  }
}
function updateDatePresets() {
  for (const btn of document.querySelectorAll<HTMLButtonElement>('.date-preset')) {
    btn.classList.toggle('active', btn.dataset.preset === activePreset);
  }
}

// ───────────────────────────────────────────────────────────────────────────
// Refresh pipeline
// ───────────────────────────────────────────────────────────────────────────
function refresh() {
  if (!db) return;
  const f = readFilters();
  syncUrl(f);
  cache.clear();
  activeIdx = -1;

  const t0 = performance.now();
  const useArray = f.regex && !!f.q;
  let warning = '';
  if (useArray) {
    let arr = runAll(f, false);
    if (f.q) {
      try {
        const re = new RegExp(f.q, 'i');
        arr = arr.filter((r) => re.test(r.base));
      } catch (err) {
        warning = ` · ⚠ regex: ${(err as Error).message}`;
      }
    }
    mode = { kind: 'array', rows: arr };
  } else {
    mode = { kind: 'paged', total: runCount(f) };
  }
  const total = getTotal();
  els.spacer.style.height = `${total * ROW_HEIGHT}px`;
  els.viewport.scrollTop = 0;

  renderHistogram();
  renderDateBar();
  renderActivePills();
  renderBadges();
  renderSortIndicators();

  const t1 = performance.now();
  setStatus(`${total.toLocaleString('sv-SE')} träffar · ${(t1 - t0).toFixed(0)} ms${warning}`);
  render();
}

// ───────────────────────────────────────────────────────────────────────────
// Drawer
// ───────────────────────────────────────────────────────────────────────────
function openDrawer(row: Row) {
  drawerRow = row;
  els.drawerTitle.textContent = row.name;
  const days = Math.ceil((new Date(row.release_at).getTime() - Date.now()) / 86400000);
  els.drawerSub.textContent = `${row.length} tecken · frisläpps ${row.release_at} (${days >= 0 ? `om ${days} d` : `${-days} d sedan`})`;
  drawerTab = 'overview';
  updateDrawerTabs();
  renderDrawerBody();
  els.drawer.classList.add('open');
  els.drawer.setAttribute('aria-hidden', 'false');
  els.drawerBackdrop.classList.remove('hidden');
}
function closeDrawer() {
  els.drawer.classList.remove('open');
  els.drawer.setAttribute('aria-hidden', 'true');
  els.drawerBackdrop.classList.add('hidden');
  drawerRow = null;
}
function updateDrawerTabs() {
  for (const btn of document.querySelectorAll<HTMLButtonElement>('.drawer-tab')) {
    btn.classList.toggle('active', btn.dataset.tab === drawerTab);
  }
}

function renderDrawerBody() {
  if (!drawerRow) return;
  const r = drawerRow;
  if (drawerTab === 'overview') els.drawerBody.innerHTML = drawerOverviewHtml(r);
  else if (drawerTab === 'links') els.drawerBody.innerHTML = drawerLinksHtml(r);
  else if (drawerTab === 'similar') els.drawerBody.innerHTML = drawerSimilarHtml(r);
  else if (drawerTab === 'notes') els.drawerBody.innerHTML = drawerNotesHtml(r);
  wireDrawerActions();
}

function signalDetail(active: boolean, icon: string, label: string, value: string): string {
  return `<div class="signal-detail${active ? '' : ' dim'}"><span class="icon">${icon}</span><div><div>${label}</div><div class="text-slate-500 text-xs">${value}</div></div></div>`;
}

function drawerOverviewHtml(r: Row): string {
  const starred = watchlist.has(r.name);
  const days = Math.ceil((new Date(r.release_at).getTime() - Date.now()) / 86400000);
  const wbFirst = r.wayback_first ? `${r.wayback_first.slice(0,4)}-${r.wayback_first.slice(4,6)}-${r.wayback_first.slice(6,8)}` : null;

  return `
    <section class="mb-4">
      <div class="grid grid-cols-2 gap-2 mb-3">
        <button class="link-btn" data-drawer-action="star"><span>${starred ? '★' : '☆'}</span><span>${starred ? 'Sluta bevaka' : 'Bevaka'}</span></button>
        <button class="link-btn" data-drawer-action="copy"><span>📋</span><span>Kopiera</span></button>
      </div>
      <h3>Fakta</h3>
      <dl class="kv">
        <dt>TLD</dt><dd>.${r.tld}</dd>
        <dt>Längd</dt><dd>${r.length} tecken</dd>
        <dt>Frisläpps</dt><dd>${r.release_at} (${days >= 0 ? `om ${days} d` : `${-days} d sedan`})</dd>
      </dl>
    </section>
    <section class="mb-4">
      <h3>Värderingssignaler</h3>
      ${signalDetail(!!r.is_word, '📖', 'Svenskt ord', r.is_word ? 'Ja, finns i ordlistan' : 'Nej')}
      ${signalDetail(r.tranco_rank != null, '⭐', 'Tranco-rank', r.tranco_rank != null ? `#${r.tranco_rank.toLocaleString('sv-SE')} av 1 000 000` : 'Inte i topp 1M')}
      ${signalDetail(r.majestic_rank != null, '🔗', 'Backlinks (Majestic)', r.majestic_rank != null ? `Rank #${r.majestic_rank.toLocaleString('sv-SE')} · ${r.majestic_refsubnets ?? 0} ref-subnät` : 'Inga registrerade backlinks')}
      ${signalDetail(r.opr_rank != null, '📊', 'Open PageRank', r.opr_rank != null ? `Rank #${r.opr_rank.toLocaleString('sv-SE')} · poäng ${r.opr_score?.toFixed(2) ?? '?'}` : 'Inte i topp 10M')}
      ${signalDetail(r.cc_hosts != null, '🌐', 'Common Crawl', r.cc_hosts != null ? `${r.cc_hosts} host(s) i CC web graph` : 'Finns ej i CC web graph')}
      ${signalDetail((r.wayback_count ?? 0) > 0, '⏳', 'Wayback-historik',
        r.wayback_checked
          ? ((r.wayback_count ?? 0) > 0 ? `${r.wayback_count} snapshots${wbFirst ? ` · första ${wbFirst}` : ''}` : 'Inga snapshots')
          : 'Inte kollat ännu (kommer i nästa build)'
      )}
      ${signalDetail(!!(r.dns_a || r.dns_mx || r.dns_ns), '📡', 'Aktiva DNS-records',
        r.dns_checked
          ? ((r.dns_a || r.dns_mx || r.dns_ns) ? [r.dns_a && 'A', r.dns_mx && 'MX', r.dns_ns && 'NS'].filter(Boolean).join(', ') : 'Inga records')
          : 'Inte kollat ännu'
      )}
      ${signalDetail(!!r.is_palindrome, '↔', 'Palindrom', r.is_palindrome ? 'Ja' : 'Nej')}
      ${signalDetail(!!r.is_cvcv, '🗣', 'Uttalbart (CVCV)', r.is_cvcv ? 'Ja' : 'Nej')}
    </section>
  `;
}

function drawerLinksHtml(r: Row): string {
  const enc = encodeURIComponent(r.name);
  const links = [
    { url: `https://internetstiftelsen.se/domain/${enc}/`, icon: '🌐', label: 'Internetstiftelsen WHOIS' },
    { url: `https://web.archive.org/web/*/${enc}`, icon: '⏳', label: 'Wayback Machine — historik' },
    { url: `https://web.archive.org/web/2*/${enc}`, icon: '📷', label: 'Wayback senaste snapshot' },
    { url: `https://crt.sh/?q=${enc}`, icon: '🔐', label: 'crt.sh — TLS-certifikat' },
    { url: `https://urlscan.io/domain/${enc}`, icon: '🔍', label: 'urlscan.io — tidigare aktivitet' },
    { url: `https://who.is/whois/${enc}`, icon: '📜', label: 'who.is — WHOIS-lookup' },
    { url: `https://majestic.com/reports/site-explorer?q=${enc}`, icon: '🔗', label: 'Majestic Site Explorer' },
    { url: `https://${enc}`, icon: '↗', label: 'Försök öppna domänen' }
  ];
  return `<section><div class="grid grid-cols-1 gap-2">${links.map((l) =>
    `<a class="link-btn" href="${l.url}" target="_blank" rel="noopener"><span>${l.icon}</span><span>${escapeHtml(l.label)}</span></a>`
  ).join('')}</div></section>`;
}

function findSimilar(r: Row): string[] {
  if (!db) return [];
  const base = r.base;
  const prefix = base.slice(0, Math.min(3, base.length));
  const suffix = base.slice(-Math.min(3, base.length));
  const stmt = db.prepare(
    `SELECT name FROM domains WHERE tld = ? AND name != ? AND (base LIKE ? OR base LIKE ?) ORDER BY length ASC, name ASC LIMIT 60`
  );
  stmt.bind([r.tld, r.name, `${prefix}%`, `%${suffix}`]);
  const out: string[] = [];
  while (stmt.step()) out.push((stmt.getAsObject() as { name: string }).name);
  stmt.free();
  return out;
}

function drawerSimilarHtml(r: Row): string {
  const similar = findSimilar(r);
  if (!similar.length) return '<div class="text-slate-500 italic">Inga liknande hittades.</div>';
  return `<section>
    <h3>Domäner som börjar med "${escapeHtml(r.base.slice(0, 3))}" eller slutar på "${escapeHtml(r.base.slice(-3))}"</h3>
    <div class="similar-list">${similar.map((s) => `<a href="https://internetstiftelsen.se/domain/${encodeURIComponent(s)}/" target="_blank" rel="noopener">${escapeHtml(s)}</a>`).join('')}</div>
  </section>`;
}

function drawerNotesHtml(r: Row): string {
  const note = notes.get(r.name) || '';
  return `<section>
    <h3>Anteckningar</h3>
    <p class="text-xs text-slate-500 dark:text-slate-400 mb-2">Sparas lokalt i din webbläsare. Tomt för att radera.</p>
    <textarea id="notes-input" placeholder="Skriv anteckningar om ${escapeHtml(r.name)}…">${escapeHtml(note)}</textarea>
    <p id="notes-status" class="text-xs text-slate-500 dark:text-slate-400 mt-1"></p>
  </section>`;
}

function wireDrawerActions() {
  const r = drawerRow!;
  els.drawerBody.querySelector<HTMLButtonElement>('[data-drawer-action="star"]')?.addEventListener('click', () => {
    toggleWatch(r.name); renderDrawerBody(); render();
  });
  els.drawerBody.querySelector<HTMLButtonElement>('[data-drawer-action="copy"]')?.addEventListener('click', () => {
    copyToClipboard(r.name);
  });
  const ta = els.drawerBody.querySelector<HTMLTextAreaElement>('#notes-input');
  if (ta) {
    const status = els.drawerBody.querySelector('#notes-status')!;
    const save = debounce(() => {
      const v = ta.value.trim();
      if (v) notes.set(r.name, v); else notes.delete(r.name);
      saveNotes();
      status.textContent = `Sparat ${new Date().toLocaleTimeString('sv-SE')}`;
      render();
    }, 400);
    ta.addEventListener('input', save);
  }
}

// ───────────────────────────────────────────────────────────────────────────
// Saved searches
// ───────────────────────────────────────────────────────────────────────────
function renderSaved() {
  if (!saved.length) {
    els.savedList.innerHTML = '<div class="text-slate-400 italic text-xs px-1">Inga sparade sökningar än.</div>';
    return;
  }
  let html = '';
  for (const s of saved) {
    html += `<div class="saved-item" data-id="${escapeHtml(s.id)}">
      <span class="name">${escapeHtml(s.name)}</span>
      <button class="del" data-del="${escapeHtml(s.id)}" title="Radera">✕</button>
    </div>`;
  }
  els.savedList.innerHTML = html;
}

function applySaved(id: string) {
  const s = saved.find((x) => x.id === id);
  if (!s) return;
  // Återställ allt och applicera
  resetAll(false);
  const f = qsToFilters(s.qs);
  writeFilters(f);
  refresh();
  closeAllPopovers();
}

// ───────────────────────────────────────────────────────────────────────────
// Export / share
// ───────────────────────────────────────────────────────────────────────────
function collectAll(): Row[] {
  if (mode.kind === 'array') return mode.rows;
  return runAll(readFilters(), true);
}
function download(filename: string, content: string, mime: string) {
  const blob = new Blob([content], { type: mime + ';charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a'); a.href = url; a.download = filename;
  document.body.appendChild(a); a.click();
  setTimeout(() => { document.body.removeChild(a); URL.revokeObjectURL(url); }, 100);
}
function exportCsv() {
  const rows = collectAll();
  const headers = ['name','tld','release_at','length','is_word','tranco_rank','majestic_rank','opr_rank','opr_score','cc_hosts','wayback_count','dns_a','dns_mx','dns_ns'];
  const esc = (v: unknown) => { const s = v == null ? '' : String(v); return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s; };
  const lines = [headers.join(',')];
  for (const r of rows) {
    lines.push([r.name, r.tld, r.release_at, r.length, r.is_word, r.tranco_rank ?? '', r.majestic_rank ?? '', r.opr_rank ?? '', r.opr_score ?? '', r.cc_hosts ?? '', r.wayback_count ?? '', r.dns_a ?? '', r.dns_mx ?? '', r.dns_ns ?? ''].map(esc).join(','));
  }
  download(`expiring-domains-${todayISO()}.csv`, lines.join('\n'), 'text/csv');
}
function exportJson() {
  const rows = collectAll();
  download(`expiring-domains-${todayISO()}.json`, JSON.stringify(rows, null, 2), 'application/json');
}
async function copyToClipboard(text: string) {
  try { await navigator.clipboard.writeText(text); flash(`Kopierat: ${text}`); }
  catch { flash('Kunde inte kopiera till urklipp'); }
}
let flashTimer: ReturnType<typeof setTimeout> | null = null;
function flash(msg: string) {
  setStatus(msg);
  if (flashTimer) clearTimeout(flashTimer);
  flashTimer = setTimeout(() => refresh(), 1500);
}

// ───────────────────────────────────────────────────────────────────────────
// Reset
// ───────────────────────────────────────────────────────────────────────────
function resetAll(doRefresh = true) {
  writeFilters(DEFAULT_FILTERS);
  activeTld = ''; activePreset = '';
  updateTldPills(); updateDatePresets();
  els.dateCustom.classList.add('hidden');
  if (doRefresh) refresh();
}

// ───────────────────────────────────────────────────────────────────────────
// Date presets
// ───────────────────────────────────────────────────────────────────────────
function applyDatePreset(p: typeof activePreset) {
  activePreset = p;
  switch (p) {
    case 'all': writeFilters({ from: '', to: '' }); break;
    case 'today': writeFilters({ from: todayISO(), to: todayISO() }); break;
    case '7d': writeFilters({ from: todayISO(), to: isoPlusDays(7) }); break;
    case '30d': writeFilters({ from: todayISO(), to: isoPlusDays(30) }); break;
    default: break;
  }
  updateDatePresets();
}

// ───────────────────────────────────────────────────────────────────────────
// Popovers
// ───────────────────────────────────────────────────────────────────────────
function closeAllPopovers() {
  els.propsPop.classList.add('hidden');
  els.qualPop.classList.add('hidden');
  els.savedPop.classList.add('hidden');
}
function togglePopover(pop: HTMLElement) {
  const open = !pop.classList.contains('hidden');
  closeAllPopovers();
  if (!open) pop.classList.remove('hidden');
}

// ───────────────────────────────────────────────────────────────────────────
// Keyboard
// ───────────────────────────────────────────────────────────────────────────
function handleKey(e: KeyboardEvent) {
  const tag = (e.target as HTMLElement | null)?.tagName;
  const inField = tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT';

  if (e.key === '/' && !inField) { e.preventDefault(); els.q.focus(); els.q.select(); return; }
  if (e.key === '?' && !inField) { e.preventDefault(); els.helpModal.classList.remove('hidden'); return; }
  if (e.key === 'Escape') {
    if (!els.helpModal.classList.contains('hidden')) { els.helpModal.classList.add('hidden'); return; }
    if (els.drawer.classList.contains('open')) { closeDrawer(); return; }
    closeAllPopovers();
    if (inField) { (e.target as HTMLInputElement).blur(); return; }
    if (els.q.value) { els.q.value = ''; refresh(); return; }
    return;
  }
  if (inField) return;

  const total = getTotal();
  if (e.key === 'j') { e.preventDefault(); activeIdx = Math.min(total - 1, activeIdx + 1); scrollToActive(); render(); }
  else if (e.key === 'k') { e.preventDefault(); activeIdx = Math.max(0, activeIdx === -1 ? 0 : activeIdx - 1); scrollToActive(); render(); }
  else if (e.key === 'g') { activeIdx = 0; scrollToActive(); render(); }
  else if (e.key === 'G') { activeIdx = total - 1; scrollToActive(); render(); }
  else if (e.key === 'Enter' && activeIdx >= 0) { const r = getRow(activeIdx); if (r) openDrawer(r); }
  else if (e.key === 's' && activeIdx >= 0) { const r = getRow(activeIdx); if (r) { toggleWatch(r.name); render(); } }
}

function scrollToActive() {
  if (activeIdx < 0) return;
  const top = activeIdx * ROW_HEIGHT;
  const viewportH = els.viewport.clientHeight;
  if (top < els.viewport.scrollTop) els.viewport.scrollTop = top;
  else if (top + ROW_HEIGHT > els.viewport.scrollTop + viewportH) els.viewport.scrollTop = top + ROW_HEIGHT - viewportH;
}

// ───────────────────────────────────────────────────────────────────────────
// Theme
// ───────────────────────────────────────────────────────────────────────────
function toggleTheme() {
  const isDark = document.documentElement.classList.toggle('dark');
  try { localStorage.setItem(THEME_KEY, isDark ? 'dark' : 'light'); } catch {}
}

// ───────────────────────────────────────────────────────────────────────────
// Wiring
// ───────────────────────────────────────────────────────────────────────────
function wireUi() {
  const debouncedRefresh = debounce(refresh, 200);

  // Sök
  els.q.addEventListener('input', debouncedRefresh);
  els.regex.addEventListener('change', refresh);

  // TLD
  for (const btn of document.querySelectorAll<HTMLButtonElement>('.tld-pill')) {
    btn.addEventListener('click', () => {
      activeTld = btn.dataset.tld || '';
      updateTldPills();
      refresh();
    });
  }

  // Datumpresets
  for (const btn of document.querySelectorAll<HTMLButtonElement>('.date-preset')) {
    btn.addEventListener('click', () => {
      const p = btn.dataset.preset as typeof activePreset;
      applyDatePreset(p);
      refresh();
    });
  }
  els.dateCustomToggle.addEventListener('click', () => {
    els.dateCustom.classList.toggle('hidden');
  });
  els.from.addEventListener('change', () => { activePreset = ''; updateDatePresets(); refresh(); });
  els.to.addEventListener('change', () => { activePreset = ''; updateDatePresets(); refresh(); });

  // Filter-checkboxar
  for (const el of [
    els.fNoDigit, els.fNoHyphen, els.fOnlyDigits, els.fOnlyLetters,
    els.fPalindrome, els.fRepeat, els.fCvcv,
    els.fWord, els.fTranco, els.fMajestic, els.fWayback, els.fDns, els.fWatch, els.fNotes
  ]) {
    el.addEventListener('change', refresh);
  }
  els.fMinLen.addEventListener('input', debouncedRefresh);
  els.fMaxLen.addEventListener('input', debouncedRefresh);

  // Reset
  els.reset.addEventListener('click', () => resetAll());

  // Sortable headers
  for (const btn of els.thead.querySelectorAll<HTMLButtonElement>('[data-sort]')) {
    btn.addEventListener('click', () => {
      const k = btn.dataset.sort as Filters['sortKey'];
      if (currentSort.key === k) {
        currentSort.dir = currentSort.dir === 'asc' ? 'desc' : 'asc';
      } else {
        currentSort.key = k;
        currentSort.dir = k === 'release_at' ? 'asc' : 'asc';
      }
      refresh();
    });
  }

  // Viewport
  els.viewport.addEventListener('scroll', () => requestAnimationFrame(render), { passive: true });
  window.addEventListener('resize', render);

  // Row interaction
  els.rows.addEventListener('click', (e) => {
    const target = e.target as HTMLElement;
    const rowEl = target.closest<HTMLElement>('[data-idx]');
    if (!rowEl) return;
    const idx = parseInt(rowEl.dataset.idx!, 10);
    activeIdx = idx;
    const action = target.closest<HTMLElement>('[data-action]')?.dataset.action;
    if (action === 'star') {
      e.preventDefault();
      const r = getRow(idx); if (r) { toggleWatch(r.name); render(); }
    } else if (action === 'more') {
      e.preventDefault();
      const r = getRow(idx); if (r) openDrawer(r);
    } else if (action !== 'open') {
      render();
    }
  });

  // Histogram + datebar
  els.histogram.addEventListener('click', (e) => {
    const r = (e.target as HTMLElement).closest<HTMLElement>('[data-len]');
    if (!r) return;
    const len = parseInt(r.dataset.len!, 10);
    writeFilters({ minLen: len, maxLen: len }); refresh();
  });
  els.datebar.addEventListener('click', (e) => {
    const r = (e.target as HTMLElement).closest<HTMLElement>('[data-date]');
    if (!r) return;
    const d = r.dataset.date!;
    activePreset = ''; updateDatePresets();
    writeFilters({ from: d, to: d }); refresh();
  });

  // Popover-knappar
  els.propsBtn.addEventListener('click', () => togglePopover(els.propsPop));
  els.qualBtn.addEventListener('click', () => togglePopover(els.qualPop));
  els.savedBtn.addEventListener('click', () => togglePopover(els.savedPop));
  document.addEventListener('click', (e) => {
    const t = e.target as HTMLElement;
    if (!t.closest('.popover') && !t.closest('.popover-btn')) closeAllPopovers();
  });

  // Saved searches
  els.savedAdd.addEventListener('click', () => {
    const name = els.savedName.value.trim();
    if (!name) return;
    const f = readFilters();
    saved = saved.filter((s) => s.name !== name);
    saved.unshift({ id: crypto.randomUUID(), name, qs: filtersToQs(f) });
    saveSaved();
    els.savedName.value = '';
    renderSaved();
  });
  els.savedList.addEventListener('click', (e) => {
    const t = e.target as HTMLElement;
    const del = t.closest<HTMLButtonElement>('[data-del]');
    if (del) {
      e.stopPropagation();
      saved = saved.filter((s) => s.id !== del.dataset.del);
      saveSaved();
      renderSaved();
      return;
    }
    const item = t.closest<HTMLElement>('.saved-item');
    if (item) applySaved(item.dataset.id!);
  });

  // Drawer
  els.drawerClose.addEventListener('click', closeDrawer);
  els.drawerBackdrop.addEventListener('click', closeDrawer);
  for (const btn of document.querySelectorAll<HTMLButtonElement>('.drawer-tab')) {
    btn.addEventListener('click', () => {
      drawerTab = btn.dataset.tab as typeof drawerTab;
      updateDrawerTabs();
      renderDrawerBody();
    });
  }

  // Export / share / theme / help
  els.exportCsv.addEventListener('click', exportCsv);
  els.exportJson.addEventListener('click', exportJson);
  els.copyLink.addEventListener('click', () => copyToClipboard(location.href));
  els.themeToggle.addEventListener('click', toggleTheme);
  els.help.addEventListener('click', () => els.helpModal.classList.remove('hidden'));
  els.helpClose.addEventListener('click', () => els.helpModal.classList.add('hidden'));
  els.helpModal.addEventListener('click', (e) => {
    if (e.target === els.helpModal) els.helpModal.classList.add('hidden');
  });

  document.addEventListener('keydown', handleKey);
}

function setMeta(m: Meta | null) {
  if (!m) { els.meta.textContent = ''; return; }
  const updated = new Date(m.generated_at).toLocaleString('sv-SE');
  const se = (m.by_tld.se ?? 0).toLocaleString('sv-SE');
  const nu = (m.by_tld.nu ?? 0).toLocaleString('sv-SE');
  const enr = m.enrichments;
  const extra = enr ? ` · 📖 ${(enr.word_hits ?? 0).toLocaleString('sv-SE')} · ⭐ ${(enr.tranco_hits ?? 0).toLocaleString('sv-SE')} · 🔗 ${(enr.majestic_hits ?? 0).toLocaleString('sv-SE')} · ⏳ ${(enr.wayback_hits ?? 0).toLocaleString('sv-SE')}/${(enr.coverage_wayback ?? 0)}% · 📡 ${(enr.dns_active ?? 0).toLocaleString('sv-SE')}/${(enr.coverage_dns ?? 0)}%` : '';
  els.meta.textContent = `${se} .se · ${nu} .nu · uppd. ${updated}${extra}`;
}

async function loadMeta(): Promise<Meta | null> {
  try { const res = await fetch('./meta.json', { cache: 'no-cache' }); if (!res.ok) return null; return await res.json(); }
  catch { return null; }
}
async function loadDb(): Promise<Database> {
  setStatus('Hämtar databas…');
  const SQL = await initSqlJs({ locateFile: () => wasmUrl });
  const res = await fetch('./domains.sqlite', { cache: 'no-cache' });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const buf = new Uint8Array(await res.arrayBuffer());
  setStatus('Öppnar databas…');
  return new SQL.Database(buf);
}

async function main() {
  try {
    const [m, database] = await Promise.all([loadMeta(), loadDb()]);
    db = database; meta = m;
    setMeta(m);
    if (m?.date_range?.min) { els.from.min = m.date_range.min; els.to.min = m.date_range.min; }
    if (m?.date_range?.max) { els.from.max = m.date_range.max; els.to.max = m.date_range.max; }

    const fromUrl = qsToFilters(location.search.replace(/^\?/, ''));
    if (Object.keys(fromUrl).length) writeFilters(fromUrl);
    updateTldPills();
    if (fromUrl.from || fromUrl.to) els.dateCustom.classList.remove('hidden');

    wireUi();
    renderSaved();
    refresh();
  } catch (err) {
    console.error(err);
    setStatus('Det gick inte att ladda databasen. Kör npm run build:data först.');
  }
}

void meta; // tystar oanvänd-varning
main();
