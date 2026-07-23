import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { EnrichmentCache } from './cache.mjs';
import { extractWikipediaDomains } from './wikipedia.mjs';

function versionedMap(entries, version) {
  const map = new Map(entries);
  Object.defineProperty(map, 'sourceVersion', { value: version });
  return map;
}

test('Wikipedia-parsern gör registrerbara .se/.nu-domäner av omvända domänindex', () => {
  const counts = new Map();
  extractWikipediaDomains(
    "INSERT INTO `externallinks` VALUES " +
    "(14,3037342,'http://se.smhi.www.','/klimat')," +
    "(20,1,'https://nu.exempel.blogg.','/')," +
    "(21,1,'https://com.example.','/')",
    counts
  );

  assert.deepEqual(Object.fromEntries(counts), {
    'smhi.se': 1,
    'exempel.nu': 1
  });
});

test('discoveryimporten fortsätter från sparad cursor utan att dubbelräkna', () => {
  const dir = mkdtempSync(join(tmpdir(), 'expiring-domains-discovery-'));
  try {
    const cache = new EnrichmentCache(join(dir, 'cache.sqlite'));
    const commoncrawl = versionedMap([
      ['guldkorn.se', 4],
      ['kort.nu', 2],
      ['langreexempel.se', 1]
    ], 'cc-test-1');
    const words = new Set(['guldkorn', 'kort']);

    const first = cache.recordDiscoveryCandidates(
      { commoncrawl, words },
      { maxPerRun: 1 }
    );
    const second = cache.recordDiscoveryCandidates(
      { commoncrawl, words },
      { maxPerRun: 1 }
    );

    assert.equal(first.imported, 1);
    assert.equal(second.imported, 1);
    const state = cache.getDiscoverySourceStats().find((row) => row.source === 'commoncrawl');
    assert.equal(state.cursor, 2);
    assert.equal(state.total, 3);
    assert.equal(
      cache.db.prepare('SELECT COUNT(*) AS c FROM ranked_candidates').get().c,
      2
    );
    assert.equal(
      cache.db.prepare('SELECT SUM(in_commoncrawl) AS c FROM ranked_candidates').get().c,
      2
    );
    cache.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
