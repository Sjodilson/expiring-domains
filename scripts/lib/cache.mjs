// Persistent berikningscache (sqlite). Lagras under cache/ och bevaras
// mellan körningar via GitHub Actions cache.

import { mkdirSync, existsSync } from 'node:fs';
import Database from 'better-sqlite3';

function candidateBase(domain) {
  const match = String(domain).toLowerCase().match(/^([a-z0-9åäö](?:[a-z0-9åäö-]{0,61}[a-z0-9åäö])?)\.(se|nu)$/);
  return match ? { base: match[1], tld: match[2] } : null;
}

function candidatePriority(domain, { words, ccHosts = 0, wikipediaLinks = 0, historical = false } = {}) {
  const parsed = candidateBase(domain);
  if (!parsed) return null;
  const { base } = parsed;
  const letters = /^[a-zåäö]+$/.test(base);
  const word = words instanceof Set && words.has(base);
  let score = 0;

  if (word) score += 350;
  if (wikipediaLinks > 0) score += 250 + Math.min(140, Math.log10(wikipediaLinks + 1) * 70);
  if (ccHosts > 0) score += (historical ? 90 : 60) + Math.min(100, Math.log10(ccHosts + 1) * 50);
  if (letters) score += 80;
  if (!base.includes('-')) score += 40;
  if (!/\d/.test(base)) score += 30;

  if (base.length <= 4) score += 220;
  else if (base.length <= 6) score += 180;
  else if (base.length <= 8) score += 140;
  else if (base.length <= 10) score += 100;
  else if (base.length <= 12) score += 70;
  else if (base.length <= 18) score += 30;
  else score -= 30;

  if (base.includes('-')) score -= 35;
  if (/\d/.test(base)) score -= 45;
  return Math.max(0, Math.round(score));
}

export class EnrichmentCache {
  constructor(path) {
    mkdirSync(path.replace(/\\[^\\]+$/, '').replace(/\/[^/]+$/, ''), { recursive: true });
    const fresh = !existsSync(path);
    this.db = new Database(path);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL');
    if (fresh) {
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS enrichment (
          domain              TEXT PRIMARY KEY,
          wayback_first       TEXT,
          wayback_count       INTEGER,
          wayback_checked_at  TEXT,
          dns_a               INTEGER,
          dns_mx              INTEGER,
          dns_ns              INTEGER,
          dns_checked_at      TEXT,
          dns_attempted_at    TEXT,
          dns_status          TEXT,
          dns_error           TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_wayback_check ON enrichment(wayback_checked_at);
        CREATE INDEX IF NOT EXISTS idx_dns_check ON enrichment(dns_checked_at);
      `);
    } else {
      // Säkerställ schema (idempotent)
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS enrichment (
          domain              TEXT PRIMARY KEY,
          wayback_first       TEXT,
          wayback_count       INTEGER,
          wayback_checked_at  TEXT,
          dns_a               INTEGER,
          dns_mx              INTEGER,
          dns_ns              INTEGER,
          dns_checked_at      TEXT,
          dns_attempted_at    TEXT,
          dns_status          TEXT,
          dns_error           TEXT
        );
      `);
    }
    this.ensureColumn('enrichment', 'dns_attempted_at', 'TEXT');
    this.ensureColumn('enrichment', 'dns_status', 'TEXT');
    this.ensureColumn('enrichment', 'dns_error', 'TEXT');
    this.ensureColumn('enrichment', 'ahrefs_dr', 'REAL');
    this.ensureColumn('enrichment', 'ahrefs_dr_checked_at', 'TEXT');
    this.ensureColumn('enrichment', 'ahrefs_dr_attempted_at', 'TEXT');
    this.ensureColumn('enrichment', 'ahrefs_dr_status', 'TEXT');
    this.ensureColumn('enrichment', 'ahrefs_dr_error', 'TEXT');
    // Historik över domäner vi sett i karens — låter oss visa nysläppta
    // domäner även efter att de försvunnit ur källfilen
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS karens (
        domain           TEXT PRIMARY KEY,
        tld              TEXT NOT NULL,
        release_at       TEXT NOT NULL,
        taken            INTEGER,
        taken_at         TEXT,
        avail_checked_at TEXT,
        avail_attempted_at TEXT,
        avail_status     TEXT,
        avail_error      TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_karens_release ON karens(release_at);
    `);
    this.ensureColumn('karens', 'avail_attempted_at', 'TEXT');
    this.ensureColumn('karens', 'avail_status', 'TEXT');
    this.ensureColumn('karens', 'avail_error', 'TEXT');

    // Alla .se/.nu-domäner som förekommer i minst en ranking- eller
    // discoverykälla. Tabellen är separat från karenshistoriken eftersom äldre
    // lediga domäner saknar ett känt frisläppningsdatum.
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS ranked_candidates (
        domain                  TEXT PRIMARY KEY,
        tld                     TEXT NOT NULL,
        first_seen_at           TEXT NOT NULL,
        last_seen_at            TEXT NOT NULL,
        active                  INTEGER NOT NULL DEFAULT 1,
        missing_runs            INTEGER NOT NULL DEFAULT 0,
        in_tranco               INTEGER NOT NULL DEFAULT 0,
        tranco_rank             INTEGER,
        in_majestic             INTEGER NOT NULL DEFAULT 0,
        majestic_rank           INTEGER,
        majestic_refsubnets     INTEGER,
        in_opr                  INTEGER NOT NULL DEFAULT 0,
        opr_rank                INTEGER,
        opr_score               REAL,
        in_commoncrawl          INTEGER NOT NULL DEFAULT 0,
        in_commoncrawl_history  INTEGER NOT NULL DEFAULT 0,
        cc_hosts                INTEGER,
        cc_graph_count          INTEGER NOT NULL DEFAULT 0,
        cc_first_graph          TEXT,
        cc_last_graph           TEXT,
        in_wikipedia            INTEGER NOT NULL DEFAULT 0,
        wikipedia_links         INTEGER,
        candidate_priority      INTEGER NOT NULL DEFAULT 0,
        taken                   INTEGER,
        first_free_at           TEXT,
        avail_checked_at        TEXT,
        avail_attempted_at      TEXT,
        avail_status            TEXT,
        avail_error             TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_ranked_active ON ranked_candidates(active);
      CREATE INDEX IF NOT EXISTS idx_ranked_avail ON ranked_candidates(active, avail_status, avail_checked_at);
      CREATE INDEX IF NOT EXISTS idx_ranked_free ON ranked_candidates(active, first_free_at) WHERE avail_status = 'free';
    `);
    this.ensureColumn('ranked_candidates', 'in_commoncrawl', 'INTEGER NOT NULL DEFAULT 0');
    this.ensureColumn('ranked_candidates', 'in_commoncrawl_history', 'INTEGER NOT NULL DEFAULT 0');
    this.ensureColumn('ranked_candidates', 'cc_hosts', 'INTEGER');
    this.ensureColumn('ranked_candidates', 'cc_graph_count', 'INTEGER NOT NULL DEFAULT 0');
    this.ensureColumn('ranked_candidates', 'cc_first_graph', 'TEXT');
    this.ensureColumn('ranked_candidates', 'cc_last_graph', 'TEXT');
    this.ensureColumn('ranked_candidates', 'in_wikipedia', 'INTEGER NOT NULL DEFAULT 0');
    this.ensureColumn('ranked_candidates', 'wikipedia_links', 'INTEGER');
    this.ensureColumn('ranked_candidates', 'candidate_priority', 'INTEGER NOT NULL DEFAULT 0');
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS source_sync (
        source        TEXT NOT NULL,
        version       TEXT NOT NULL,
        cursor        INTEGER NOT NULL DEFAULT 0,
        total         INTEGER NOT NULL DEFAULT 0,
        completed_at  TEXT,
        updated_at    TEXT NOT NULL,
        PRIMARY KEY (source, version)
      );
      CREATE INDEX IF NOT EXISTS idx_source_sync_complete
        ON source_sync(source, completed_at);
      CREATE INDEX IF NOT EXISTS idx_ranked_priority
        ON ranked_candidates(active, avail_status, candidate_priority);
    `);
    this.upsertKarensStmt = this.db.prepare(`
      INSERT INTO karens (domain, tld, release_at) VALUES (?, ?, ?)
      ON CONFLICT(domain) DO UPDATE SET tld = excluded.tld, release_at = excluded.release_at
    `);
    this.setAvailStmt = this.db.prepare(`
      UPDATE karens SET
        taken = ?,
        taken_at = CASE WHEN ? = 1 AND taken_at IS NULL THEN ? ELSE taken_at END,
        avail_checked_at = ?,
        avail_attempted_at = ?,
        avail_status = ?,
        avail_error = NULL
      WHERE domain = ?
    `);
    this.setAvailErrorStmt = this.db.prepare(`
      UPDATE karens SET
        avail_attempted_at = ?,
        avail_status = 'error',
        avail_error = ?
      WHERE domain = ?
    `);
    this.setRankedAvailStmt = this.db.prepare(`
      UPDATE ranked_candidates SET
        taken = ?,
        first_free_at = CASE WHEN ? = 0 AND first_free_at IS NULL THEN ? ELSE first_free_at END,
        avail_checked_at = ?,
        avail_attempted_at = ?,
        avail_status = ?,
        avail_error = NULL
      WHERE domain = ?
    `);
    this.setRankedAvailErrorStmt = this.db.prepare(`
      UPDATE ranked_candidates SET
        avail_attempted_at = ?,
        avail_status = 'error',
        avail_error = ?
      WHERE domain = ?
    `);

    this.getStmt = this.db.prepare('SELECT * FROM enrichment WHERE domain = ?');
    this.upsertWayback = this.db.prepare(`
      INSERT INTO enrichment (domain, wayback_first, wayback_count, wayback_checked_at)
      VALUES (?, ?, ?, ?)
      ON CONFLICT(domain) DO UPDATE SET
        wayback_first = excluded.wayback_first,
        wayback_count = excluded.wayback_count,
        wayback_checked_at = excluded.wayback_checked_at
    `);
    this.upsertDns = this.db.prepare(`
      INSERT INTO enrichment (
        domain, dns_a, dns_mx, dns_ns,
        dns_checked_at, dns_attempted_at, dns_status, dns_error
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(domain) DO UPDATE SET
        dns_a = excluded.dns_a,
        dns_mx = excluded.dns_mx,
        dns_ns = excluded.dns_ns,
        dns_checked_at = excluded.dns_checked_at,
        dns_attempted_at = excluded.dns_attempted_at,
        dns_status = excluded.dns_status,
        dns_error = excluded.dns_error
    `);
    this.upsertDnsError = this.db.prepare(`
      INSERT INTO enrichment (domain, dns_attempted_at, dns_status, dns_error)
      VALUES (?, ?, 'error', ?)
      ON CONFLICT(domain) DO UPDATE SET
        dns_attempted_at = excluded.dns_attempted_at,
        dns_status = 'error',
        dns_error = excluded.dns_error
    `);
    this.upsertAhrefsDr = this.db.prepare(`
      INSERT INTO enrichment (
        domain, ahrefs_dr, ahrefs_dr_checked_at,
        ahrefs_dr_attempted_at, ahrefs_dr_status, ahrefs_dr_error
      ) VALUES (?, ?, ?, ?, 'ok', NULL)
      ON CONFLICT(domain) DO UPDATE SET
        ahrefs_dr = excluded.ahrefs_dr,
        ahrefs_dr_checked_at = excluded.ahrefs_dr_checked_at,
        ahrefs_dr_attempted_at = excluded.ahrefs_dr_attempted_at,
        ahrefs_dr_status = 'ok',
        ahrefs_dr_error = NULL
    `);
    this.upsertAhrefsDrError = this.db.prepare(`
      INSERT INTO enrichment (
        domain, ahrefs_dr_attempted_at, ahrefs_dr_status, ahrefs_dr_error
      ) VALUES (?, ?, 'error', ?)
      ON CONFLICT(domain) DO UPDATE SET
        ahrefs_dr_attempted_at = excluded.ahrefs_dr_attempted_at,
        ahrefs_dr_status = 'error',
        ahrefs_dr_error = excluded.ahrefs_dr_error
    `);
  }

  ensureColumn(table, column, definition) {
    const columns = new Set(this.db.prepare(`PRAGMA table_info(${table})`).all().map((r) => r.name));
    if (!columns.has(column)) this.db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
  }

  get(domain) {
    return this.getStmt.get(domain) || null;
  }

  setWayback(domain, first, count) {
    this.upsertWayback.run(domain, first, count, new Date().toISOString());
  }

  setDns(domain, a, mx, ns, status = 'ok', error = null) {
    const now = new Date().toISOString();
    const value = (v) => v == null ? null : (v ? 1 : 0);
    this.upsertDns.run(domain, value(a), value(mx), value(ns), now, now, status, error);
  }

  setDnsError(domain, error) {
    this.upsertDnsError.run(domain, new Date().toISOString(), error);
  }

  setAhrefsDr(domain, rating) {
    const value = Number(rating);
    if (!Number.isFinite(value) || value < 0 || value > 100) {
      throw new Error(`Ogiltig Ahrefs DR för ${domain}`);
    }
    const now = new Date().toISOString();
    this.upsertAhrefsDr.run(domain, value, now, now);
  }

  setAhrefsDrError(domain, error) {
    this.upsertAhrefsDrError.run(domain, new Date().toISOString(), String(error).slice(0, 250));
  }

  needsAhrefsDr(domains, maxAgeDays = 30, errorHours = 6) {
    const checkedCutoff = new Date(Date.now() - maxAgeDays * 86400000).toISOString();
    const retryCutoff = new Date(Date.now() - errorHours * 3600000).toISOString();
    const known = new Map();
    const stmt = this.db.prepare(`
      SELECT domain, ahrefs_dr_checked_at AS checked,
        ahrefs_dr_attempted_at AS attempted, ahrefs_dr_status AS status
      FROM enrichment
    `);
    for (const row of stmt.iterate()) known.set(row.domain, row);

    const out = [];
    for (const domain of domains) {
      const row = known.get(domain);
      if (row?.status === 'error') {
        if (!row.attempted || row.attempted < retryCutoff) out.push(domain);
      } else if (!row?.checked || row.checked < checkedCutoff) {
        out.push(domain);
      }
    }
    return out;
  }

  // Returnerar domäner som behöver uppdateras (aldrig kollat eller äldre än maxAgeDays)
  needsUpdate(field, allDomains, maxAgeDays) {
    const cutoff = new Date(Date.now() - maxAgeDays * 86400000).toISOString();
    const ck = field === 'wayback' ? 'wayback_checked_at' : 'dns_checked_at';
    const dnsRetryCutoff = new Date(Date.now() - 6 * 3600000).toISOString();
    // Bygg en map av kända checked_at
    const extra = field === 'dns' ? ', dns_attempted_at AS attempted, dns_status AS status' : '';
    const stmt = this.db.prepare(`SELECT domain, ${ck} AS checked${extra} FROM enrichment`);
    const known = new Map();
    for (const row of stmt.iterate()) known.set(row.domain, row);

    const out = [];
    for (const d of allDomains) {
      const row = known.get(d);
      if (field === 'dns' && row?.status === 'error') {
        if (!row.attempted || row.attempted < dnsRetryCutoff) out.push(d);
      } else if (field === 'dns' && row?.status == null) {
        // Äldre cache saknar felstatus och kan innehålla timeouts sparade som falska nollor.
        out.push(d);
      } else if (!row?.checked || row.checked < cutoff) {
        out.push(d);
      }
    }
    return out;
  }

  // Hämtar alla rader för domains, returnerar Map<domain, row>
  getMany(domains) {
    const m = new Map();
    const stmt = this.db.prepare('SELECT * FROM enrichment WHERE domain = ?');
    for (const d of domains) {
      const r = stmt.get(d);
      if (r) m.set(d, r);
    }
    return m;
  }

  // Anteckna alla domäner som just nu är i karens
  recordKarens(rows) {
    const tx = this.db.transaction((items) => {
      for (const r of items) this.upsertKarensStmt.run(r.name, r.tld, r.release_at);
    });
    tx(rows);
  }

  // Synka den aktuella unionen av Tranco, Majestic och Open PageRank. En källa
  // nollställs bara när den faktiskt laddats, så ett tillfälligt källfel kan
  // aldrig avregistrera kandidater. Tre lyckade frånvarokörningar krävs innan
  // en domän markeras inaktiv.
  recordRankedCandidates({ tranco, majestic, opr }) {
    const sources = [];
    if (tranco instanceof Map && tranco.size > 0) sources.push('tranco');
    if (majestic instanceof Map && majestic.size > 0) sources.push('majestic');
    if (opr instanceof Map && opr.size > 0) sources.push('opr');
    if (sources.length === 0) {
      return { sourcesUpdated: [], newCandidates: 0, active: this.getRankedStats().active };
    }

    const before = this.db.prepare('SELECT COUNT(*) AS c FROM ranked_candidates').get().c;
    const now = new Date().toISOString();
    const upsertTranco = this.db.prepare(`
      INSERT INTO ranked_candidates (
        domain, tld, first_seen_at, last_seen_at, active, in_tranco, tranco_rank
      ) VALUES (?, ?, ?, ?, 1, 1, ?)
      ON CONFLICT(domain) DO UPDATE SET
        tld = excluded.tld,
        last_seen_at = excluded.last_seen_at,
        active = 1,
        in_tranco = 1,
        tranco_rank = excluded.tranco_rank
    `);
    const upsertMajestic = this.db.prepare(`
      INSERT INTO ranked_candidates (
        domain, tld, first_seen_at, last_seen_at, active,
        in_majestic, majestic_rank, majestic_refsubnets
      ) VALUES (?, ?, ?, ?, 1, 1, ?, ?)
      ON CONFLICT(domain) DO UPDATE SET
        tld = excluded.tld,
        last_seen_at = excluded.last_seen_at,
        active = 1,
        in_majestic = 1,
        majestic_rank = excluded.majestic_rank,
        majestic_refsubnets = excluded.majestic_refsubnets
    `);
    const upsertOpr = this.db.prepare(`
      INSERT INTO ranked_candidates (
        domain, tld, first_seen_at, last_seen_at, active, in_opr, opr_rank, opr_score
      ) VALUES (?, ?, ?, ?, 1, 1, ?, ?)
      ON CONFLICT(domain) DO UPDATE SET
        tld = excluded.tld,
        last_seen_at = excluded.last_seen_at,
        active = 1,
        in_opr = 1,
        opr_rank = excluded.opr_rank,
        opr_score = excluded.opr_score
    `);
    const tldOf = (domain) => domain.endsWith('.nu') ? 'nu' : 'se';

    const tx = this.db.transaction(() => {
      if (sources.includes('tranco')) {
        this.db.exec('UPDATE ranked_candidates SET in_tranco = 0, tranco_rank = NULL');
        for (const [domain, rank] of tranco) {
          upsertTranco.run(domain, tldOf(domain), now, now, rank);
        }
      }
      if (sources.includes('majestic')) {
        this.db.exec('UPDATE ranked_candidates SET in_majestic = 0, majestic_rank = NULL, majestic_refsubnets = NULL');
        for (const [domain, data] of majestic) {
          upsertMajestic.run(domain, tldOf(domain), now, now, data.rank, data.refSubNets ?? 0);
        }
      }
      if (sources.includes('opr')) {
        this.db.exec('UPDATE ranked_candidates SET in_opr = 0, opr_rank = NULL, opr_score = NULL');
        for (const [domain, data] of opr) {
          upsertOpr.run(domain, tldOf(domain), now, now, data.rank, data.score);
        }
      }

      this.db.prepare(`
        UPDATE ranked_candidates SET
          active = CASE
            WHEN in_tranco = 1 OR in_majestic = 1 OR in_opr = 1
              OR in_commoncrawl = 1 OR in_commoncrawl_history = 1 OR in_wikipedia = 1
            THEN 1
            WHEN missing_runs + 1 >= 3 THEN 0
            ELSE active
          END,
          missing_runs = CASE
            WHEN in_tranco = 1 OR in_majestic = 1 OR in_opr = 1
              OR in_commoncrawl = 1 OR in_commoncrawl_history = 1 OR in_wikipedia = 1
            THEN 0
            ELSE missing_runs + 1
          END
      `).run();
    });
    tx();

    const after = this.db.prepare('SELECT COUNT(*) AS c FROM ranked_candidates').get().c;
    return {
      sourcesUpdated: sources,
      newCandidates: Math.max(0, after - before),
      active: this.getRankedStats().active
    };
  }

  getCompletedHistoricalCcIds() {
    return new Set(
      this.db.prepare(`
        SELECT version FROM source_sync
        WHERE source = 'commoncrawl_history' AND completed_at IS NOT NULL
      `).all().map((row) => row.version)
    );
  }

  recordDiscoveryCandidates(
    { commoncrawl = null, wikipedia = null, historicalCc = null, words = null },
    { maxPerRun = 10000 } = {}
  ) {
    const budget = Math.max(0, Math.floor(maxPerRun));
    const before = this.db.prepare('SELECT COUNT(*) AS c FROM ranked_candidates').get().c;
    const sourceRows = [
      { source: 'wikipedia', map: wikipedia, kind: 'wikipedia', historical: false },
      { source: 'commoncrawl', map: commoncrawl, kind: 'commoncrawl', historical: false },
      { source: 'commoncrawl_history', map: historicalCc, kind: 'commoncrawl', historical: true }
    ];
    const stateStmt = this.db.prepare(`
      SELECT * FROM source_sync WHERE source = ? AND version = ?
    `);
    const initState = this.db.prepare(`
      INSERT INTO source_sync (source, version, cursor, total, completed_at, updated_at)
      VALUES (?, ?, 0, ?, NULL, ?)
      ON CONFLICT(source, version) DO UPDATE SET
        total = excluded.total,
        updated_at = excluded.updated_at
    `);
    const updateState = this.db.prepare(`
      UPDATE source_sync SET
        cursor = ?,
        total = ?,
        completed_at = ?,
        updated_at = ?
      WHERE source = ? AND version = ?
    `);
    const upsert = this.db.prepare(`
      INSERT INTO ranked_candidates (
        domain, tld, first_seen_at, last_seen_at, active,
        in_commoncrawl, in_commoncrawl_history, cc_hosts,
        cc_graph_count, cc_first_graph, cc_last_graph,
        in_wikipedia, wikipedia_links, candidate_priority
      ) VALUES (
        @domain, @tld, @now, @now, 1,
        @in_commoncrawl, @in_commoncrawl_history, @cc_hosts,
        @cc_graph_count, @cc_first_graph, @cc_last_graph,
        @in_wikipedia, @wikipedia_links, @candidate_priority
      )
      ON CONFLICT(domain) DO UPDATE SET
        tld = excluded.tld,
        last_seen_at = excluded.last_seen_at,
        active = 1,
        in_commoncrawl = MAX(ranked_candidates.in_commoncrawl, excluded.in_commoncrawl),
        in_commoncrawl_history = MAX(ranked_candidates.in_commoncrawl_history, excluded.in_commoncrawl_history),
        cc_hosts = CASE
          WHEN excluded.cc_hosts IS NULL THEN ranked_candidates.cc_hosts
          WHEN ranked_candidates.cc_hosts IS NULL THEN excluded.cc_hosts
          ELSE MAX(ranked_candidates.cc_hosts, excluded.cc_hosts)
        END,
        cc_graph_count = ranked_candidates.cc_graph_count + excluded.cc_graph_count,
        cc_first_graph = COALESCE(ranked_candidates.cc_first_graph, excluded.cc_first_graph),
        cc_last_graph = COALESCE(excluded.cc_last_graph, ranked_candidates.cc_last_graph),
        in_wikipedia = MAX(ranked_candidates.in_wikipedia, excluded.in_wikipedia),
        wikipedia_links = CASE
          WHEN excluded.wikipedia_links IS NULL THEN ranked_candidates.wikipedia_links
          WHEN ranked_candidates.wikipedia_links IS NULL THEN excluded.wikipedia_links
          ELSE MAX(ranked_candidates.wikipedia_links, excluded.wikipedia_links)
        END,
        candidate_priority = MAX(ranked_candidates.candidate_priority, excluded.candidate_priority)
    `);

    let remaining = budget;
    const sources = [];
    const now = new Date().toISOString();

    for (const source of sourceRows) {
      if (!(source.map instanceof Map) || source.map.size === 0) continue;
      const version = String(source.map.sourceVersion || source.map.fetchedAt || '');
      if (!version) continue;

      const existingState = stateStmt.get(source.source, version);
      if (existingState?.completed_at) {
        sources.push({
          source: source.source,
          version,
          imported: 0,
          cursor: existingState.cursor,
          total: existingState.total,
          completed: true
        });
        continue;
      }
      if (remaining <= 0) {
        if (!existingState) initState.run(source.source, version, source.map.size, now);
        const waitingState = stateStmt.get(source.source, version);
        sources.push({
          source: source.source,
          version,
          imported: 0,
          cursor: waitingState?.cursor ?? 0,
          total: waitingState?.total ?? source.map.size,
          completed: false
        });
        continue;
      }

      const candidates = [];
      for (const [domainRaw, rawValue] of source.map) {
        const domain = String(domainRaw).toLowerCase();
        const parsed = candidateBase(domain);
        if (!parsed) continue;
        const value = Math.max(0, Number(rawValue) || 0);
        const priority = candidatePriority(domain, {
          words,
          ccHosts: source.kind === 'commoncrawl' ? value : 0,
          wikipediaLinks: source.kind === 'wikipedia' ? value : 0,
          historical: source.historical
        });
        if (priority == null) continue;
        candidates.push({ domain, tld: parsed.tld, value, priority });
      }
      candidates.sort((a, b) => b.priority - a.priority || a.domain.localeCompare(b.domain, 'sv'));

      if (!existingState) initState.run(source.source, version, candidates.length, now);
      const state = stateStmt.get(source.source, version);
      const cursor = Math.min(state?.cursor ?? 0, candidates.length);
      const end = Math.min(candidates.length, cursor + remaining);
      const slice = candidates.slice(cursor, end);
      const completed = end >= candidates.length;

      const tx = this.db.transaction(() => {
        for (const candidate of slice) {
          const isCc = source.kind === 'commoncrawl';
          const graph = isCc ? version : null;
          upsert.run({
            domain: candidate.domain,
            tld: candidate.tld,
            now,
            in_commoncrawl: isCc && !source.historical ? 1 : 0,
            in_commoncrawl_history: isCc && source.historical ? 1 : 0,
            cc_hosts: isCc ? candidate.value : null,
            cc_graph_count: isCc ? 1 : 0,
            cc_first_graph: graph,
            cc_last_graph: graph,
            in_wikipedia: source.kind === 'wikipedia' ? 1 : 0,
            wikipedia_links: source.kind === 'wikipedia' ? candidate.value : null,
            candidate_priority: candidate.priority
          });
        }
        updateState.run(
          end,
          candidates.length,
          completed ? now : null,
          now,
          source.source,
          version
        );
      });
      tx();

      remaining -= slice.length;
      sources.push({
        source: source.source,
        version,
        imported: slice.length,
        cursor: end,
        total: candidates.length,
        completed
      });
    }

    const after = this.db.prepare('SELECT COUNT(*) AS c FROM ranked_candidates').get().c;
    return {
      imported: budget - remaining,
      newCandidates: Math.max(0, after - before),
      sources
    };
  }

  getDiscoverySourceStats() {
    return this.db.prepare(`
      SELECT source, version, cursor, total, completed_at, updated_at
      FROM source_sync
      ORDER BY updated_at DESC
    `).all();
  }

  getRankedDue(
    limit,
    { errorHours = 6, freeHours = 23, discoveryFreeDays = 7, occupiedDays = 30 } = {}
  ) {
    if (!Number.isFinite(limit) || limit <= 0) return [];
    const cutoff = (hours) => new Date(Date.now() - hours * 3600000).toISOString();
    return this.db.prepare(`
      SELECT * FROM ranked_candidates
      WHERE active = 1 AND (
        avail_status IS NULL
        OR (avail_status = 'error' AND (avail_attempted_at IS NULL OR avail_attempted_at < ?))
        OR (
          avail_status = 'free' AND (
            avail_checked_at IS NULL
            OR (
              (in_tranco = 1 OR in_majestic = 1 OR in_opr = 1)
              AND avail_checked_at < ?
            )
            OR (
              in_tranco = 0 AND in_majestic = 0 AND in_opr = 0
              AND avail_checked_at < ?
            )
          )
        )
        OR (avail_status = 'occupied' AND (avail_checked_at IS NULL OR avail_checked_at < ?))
      )
      ORDER BY
        CASE WHEN avail_status IS NULL THEN 0 WHEN avail_status = 'error' THEN 1 WHEN avail_status = 'free' THEN 2 ELSE 3 END,
        CASE WHEN in_wikipedia = 1 THEN 0 WHEN in_tranco = 1 OR in_majestic = 1 OR in_opr = 1 THEN 1 ELSE 2 END,
        candidate_priority DESC,
        CASE WHEN avail_status IS NULL THEN first_seen_at END DESC,
        CASE WHEN tranco_rank IS NULL THEN 1 ELSE 0 END, tranco_rank ASC,
        CASE WHEN majestic_rank IS NULL THEN 1 ELSE 0 END, majestic_rank ASC,
        CASE WHEN opr_rank IS NULL THEN 1 ELSE 0 END, opr_rank ASC,
        domain ASC
      LIMIT ?
    `).all(
      cutoff(errorHours),
      cutoff(freeHours),
      cutoff(discoveryFreeDays * 24),
      cutoff(occupiedDays * 24),
      Math.floor(limit)
    );
  }

  setRankedAvailability(domain, taken) {
    const now = new Date().toISOString();
    this.setRankedAvailStmt.run(
      taken ? 1 : 0,
      taken ? 1 : 0,
      now,
      now,
      now,
      taken ? 'occupied' : 'free',
      domain
    );
  }

  setRankedAvailabilityError(domain, error) {
    this.setRankedAvailErrorStmt.run(new Date().toISOString(), error, domain);
  }

  syncRankedAvailabilityFromKarens() {
    return this.db.prepare(`
      UPDATE ranked_candidates AS r SET
        taken = (SELECT k.taken FROM karens k WHERE k.domain = r.domain),
        first_free_at = CASE
          WHEN first_free_at IS NULL AND (SELECT k.avail_status FROM karens k WHERE k.domain = r.domain) = 'free'
          THEN (SELECT k.avail_checked_at FROM karens k WHERE k.domain = r.domain)
          ELSE first_free_at
        END,
        avail_checked_at = (SELECT k.avail_checked_at FROM karens k WHERE k.domain = r.domain),
        avail_attempted_at = (SELECT k.avail_attempted_at FROM karens k WHERE k.domain = r.domain),
        avail_status = (SELECT k.avail_status FROM karens k WHERE k.domain = r.domain),
        avail_error = (SELECT k.avail_error FROM karens k WHERE k.domain = r.domain)
      WHERE EXISTS (
        SELECT 1 FROM karens k
        WHERE k.domain = r.domain
          AND k.avail_status IN ('free', 'occupied')
          AND k.avail_checked_at IS NOT NULL
          AND (r.avail_checked_at IS NULL OR k.avail_checked_at > r.avail_checked_at)
      )
    `).run().changes;
  }

  getRankedFree() {
    return this.db.prepare(`
      SELECT * FROM ranked_candidates
      WHERE active = 1 AND avail_status = 'free'
      ORDER BY first_free_at ASC, domain ASC
    `).all();
  }

  getRankedStats() {
    return this.db.prepare(`
      SELECT
        COUNT(*) AS total,
        COALESCE(SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END), 0) AS active,
        COALESCE(SUM(CASE WHEN active = 1 AND avail_status = 'free' THEN 1 ELSE 0 END), 0) AS free,
        COALESCE(SUM(CASE WHEN active = 1 AND avail_status = 'occupied' THEN 1 ELSE 0 END), 0) AS occupied,
        COALESCE(SUM(CASE WHEN active = 1 AND avail_status = 'error' THEN 1 ELSE 0 END), 0) AS errors,
        COALESCE(SUM(CASE WHEN active = 1 AND avail_status IS NULL THEN 1 ELSE 0 END), 0) AS unchecked
      FROM ranked_candidates
    `).get();
  }

  // Domäner vars release-datum passerat, nyaste först
  getReleased(fromDate, toDate) {
    return this.db
      .prepare(`
        SELECT k.*, e.dns_a, e.dns_mx, e.dns_ns,
          e.dns_attempted_at, e.dns_status, e.dns_error
        FROM karens k
        LEFT JOIN enrichment e ON e.domain = k.domain
        WHERE k.release_at >= ? AND k.release_at <= ?
        ORDER BY k.release_at DESC, k.domain ASC
      `)
      .all(fromDate, toDate);
  }

  setAvailability(domain, taken) {
    const now = new Date().toISOString();
    this.setAvailStmt.run(
      taken ? 1 : 0,
      taken ? 1 : 0,
      now,
      now,
      now,
      taken ? 'occupied' : 'free',
      domain
    );
  }

  setAvailabilityError(domain, error) {
    this.setAvailErrorStmt.run(new Date().toISOString(), error, domain);
  }

  // Släng released-rader äldre än fönstret
  pruneKarens(beforeDate) {
    return this.db.prepare('DELETE FROM karens WHERE release_at < ?').run(beforeDate).changes;
  }

  close() {
    this.db.close();
  }

  // Rensa rader för domäner som inte längre finns i karens (håller cachen liten)
  prune(activeDomainsSet) {
    const stmt = this.db.prepare('SELECT domain FROM enrichment');
    const toDelete = [];
    for (const row of stmt.iterate()) {
      if (!activeDomainsSet.has(row.domain)) toDelete.push(row.domain);
    }
    if (toDelete.length === 0) return 0;
    const del = this.db.prepare('DELETE FROM enrichment WHERE domain = ?');
    const tx = this.db.transaction((arr) => {
      for (const d of arr) del.run(d);
    });
    tx(toDelete);
    return toDelete.length;
  }
}
