// Backlinks: Majestic Million topp 1M (gratis, daglig CSV).

import { fetchText } from './util.mjs';

const MAJESTIC_URL = 'https://downloads.majestic.com/majestic_million.csv';

export async function loadMajesticMap(log) {
  log(`→ Hämtar ${MAJESTIC_URL}`);
  const csv = await fetchText(MAJESTIC_URL);
  // Header: GlobalRank,TldRank,Domain,TLD,RefSubNets,RefIPs,IDN_Domain,IDN_TLD,PrevGlobalRank,PrevTldRank,PrevRefSubNets,PrevRefIPs
  const lines = csv.split('\n');
  const map = new Map();
  let count = 0;
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line) continue;
    const cols = line.split(',');
    if (cols.length < 6) continue;
    const rank = parseInt(cols[0], 10);
    const domain = cols[2]?.trim().toLowerCase();
    const refSubNets = parseInt(cols[4], 10) || 0;
    const refIps = parseInt(cols[5], 10) || 0;
    if (!rank || !domain) continue;
    if (domain.endsWith('.se') || domain.endsWith('.nu')) {
      map.set(domain, { rank, refSubNets, refIps });
      count++;
    }
  }
  log(`  Majestic: ${count.toLocaleString('sv-SE')} relevanta domäner`);
  return map;
}
