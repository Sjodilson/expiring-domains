// Open PageRank: DomCop top 10M domäner med PageRank-poäng.
// Nedladdning ~60 MB zip med CSV.

import { fetchBuffer } from './util.mjs';

const DOMCOP_URL =
  process.env.OPR_URL ||
  'https://www.domcop.com/files/top/top10milliondomains.csv.zip';

export async function loadOpenPageRankMap(log) {
  const AdmZip = (await import('adm-zip')).default;
  log(`→ Hämtar Open PageRank: ${DOMCOP_URL}`);
  const buf = await fetchBuffer(DOMCOP_URL);
  const zip = new AdmZip(buf);
  const csvEntry = zip.getEntries().find((e) => e.entryName.endsWith('.csv'));
  if (!csvEntry) throw new Error('Ingen CSV i Open PageRank-zip');
  const csv = csvEntry.getData().toString('utf8');
  const map = new Map();
  let count = 0;
  const unq = (s) => s.replace(/^"|"$/g, '').trim();
  for (const line of csv.split('\n')) {
    // Format: "Rank","Domain","Open Page Rank"
    const idx1 = line.indexOf(',');
    if (idx1 === -1) continue;
    const idx2 = line.indexOf(',', idx1 + 1);
    if (idx2 === -1) continue;
    const rank = parseInt(unq(line.slice(0, idx1)), 10);
    let domain = unq(line.slice(idx1 + 1, idx2)).toLowerCase();
    const score = parseFloat(unq(line.slice(idx2 + 1)));
    if (!rank || !domain || isNaN(score)) continue;
    // Strip www. prefix
    if (domain.startsWith('www.')) domain = domain.slice(4);
    if (domain.endsWith('.se') || domain.endsWith('.nu')) {
      // Keep highest rank (first seen = best rank since file is sorted)
      if (!map.has(domain)) {
        map.set(domain, { rank, score });
        count++;
      }
    }
  }
  log(`  Open PageRank: ${count.toLocaleString('sv-SE')} relevanta domäner`);
  return map;
}
