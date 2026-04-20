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
