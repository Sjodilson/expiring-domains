// Persistent berikningscache (sqlite). Lagras under cache/ och bevaras
// mellan körningar via GitHub Actions cache.

import { mkdirSync, existsSync } from 'node:fs';
import Database from 'better-sqlite3';

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

    // Alla .se/.nu-domäner som förekommer i minst en rankningskälla. Tabellen
    // är separat från karenshistoriken eftersom äldre lediga domäner saknar ett
    // känt frisläppningsdatum.
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
            WHEN in_tranco = 1 OR in_majestic = 1 OR in_opr = 1 THEN 1
            WHEN missing_runs + 1 >= 3 THEN 0
            ELSE active
          END,
          missing_runs = CASE
            WHEN in_tranco = 1 OR in_majestic = 1 OR in_opr = 1 THEN 0
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

  getRankedDue(limit, { errorHours = 6, freeHours = 23, occupiedDays = 30 } = {}) {
    if (!Number.isFinite(limit) || limit <= 0) return [];
    const cutoff = (hours) => new Date(Date.now() - hours * 3600000).toISOString();
    return this.db.prepare(`
      SELECT * FROM ranked_candidates
      WHERE active = 1 AND (
        avail_status IS NULL
        OR (avail_status = 'error' AND (avail_attempted_at IS NULL OR avail_attempted_at < ?))
        OR (avail_status = 'free' AND (avail_checked_at IS NULL OR avail_checked_at < ?))
        OR (avail_status = 'occupied' AND (avail_checked_at IS NULL OR avail_checked_at < ?))
      )
      ORDER BY
        CASE WHEN avail_status IS NULL THEN 0 WHEN avail_status = 'error' THEN 1 WHEN avail_status = 'free' THEN 2 ELSE 3 END,
        CASE WHEN avail_status IS NULL THEN first_seen_at END DESC,
        CASE WHEN tranco_rank IS NULL THEN 1 ELSE 0 END, tranco_rank ASC,
        CASE WHEN majestic_rank IS NULL THEN 1 ELSE 0 END, majestic_rank ASC,
        CASE WHEN opr_rank IS NULL THEN 1 ELSE 0 END, opr_rank ASC,
        domain ASC
      LIMIT ?
    `).all(cutoff(errorHours), cutoff(freeHours), cutoff(occupiedDays * 24), Math.floor(limit));
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
