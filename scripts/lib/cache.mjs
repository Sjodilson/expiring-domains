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
          dns_checked_at      TEXT
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
          dns_checked_at      TEXT
        );
      `);
    }
    // Historik över domäner vi sett i karens — låter oss visa nysläppta
    // domäner även efter att de försvunnit ur källfilen
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS karens (
        domain           TEXT PRIMARY KEY,
        tld              TEXT NOT NULL,
        release_at       TEXT NOT NULL,
        taken            INTEGER,
        taken_at         TEXT,
        avail_checked_at TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_karens_release ON karens(release_at);
    `);
    this.upsertKarensStmt = this.db.prepare(`
      INSERT INTO karens (domain, tld, release_at) VALUES (?, ?, ?)
      ON CONFLICT(domain) DO UPDATE SET tld = excluded.tld, release_at = excluded.release_at
    `);
    this.setAvailStmt = this.db.prepare(`
      UPDATE karens SET
        taken = ?,
        taken_at = CASE WHEN ? = 1 AND taken_at IS NULL THEN ? ELSE taken_at END,
        avail_checked_at = ?
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
      INSERT INTO enrichment (domain, dns_a, dns_mx, dns_ns, dns_checked_at)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(domain) DO UPDATE SET
        dns_a = excluded.dns_a,
        dns_mx = excluded.dns_mx,
        dns_ns = excluded.dns_ns,
        dns_checked_at = excluded.dns_checked_at
    `);
  }

  get(domain) {
    return this.getStmt.get(domain) || null;
  }

  setWayback(domain, first, count) {
    this.upsertWayback.run(domain, first, count, new Date().toISOString());
  }

  setDns(domain, a, mx, ns) {
    this.upsertDns.run(domain, a ? 1 : 0, mx ? 1 : 0, ns ? 1 : 0, new Date().toISOString());
  }

  // Returnerar domäner som behöver uppdateras (aldrig kollat eller äldre än maxAgeDays)
  needsUpdate(field, allDomains, maxAgeDays) {
    const cutoff = new Date(Date.now() - maxAgeDays * 86400000).toISOString();
    const ck = field === 'wayback' ? 'wayback_checked_at' : 'dns_checked_at';
    // Bygg en map av kända checked_at
    const stmt = this.db.prepare(`SELECT domain, ${ck} AS checked FROM enrichment`);
    const known = new Map();
    for (const row of stmt.iterate()) known.set(row.domain, row.checked);

    const out = [];
    for (const d of allDomains) {
      const checked = known.get(d);
      if (!checked || checked < cutoff) out.push(d);
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
      .prepare('SELECT * FROM karens WHERE release_at >= ? AND release_at <= ? ORDER BY release_at DESC, domain ASC')
      .all(fromDate, toDate);
  }

  setAvailability(domain, taken) {
    const now = new Date().toISOString();
    this.setAvailStmt.run(taken ? 1 : 0, taken ? 1 : 0, now, now, domain);
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
