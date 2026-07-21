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
