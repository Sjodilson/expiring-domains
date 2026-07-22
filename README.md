# Frisläppta domäner — .se & .nu

Statisk webbapp som visar .se- och .nu-domäner som snart lämnar karens hos
[Internetstiftelsen](https://internetstiftelsen.se/). All sökning sker lokalt i
webbläsaren via en SQLite-databas (sql.js) — inga uppgifter skickas vidare.

## Funktioner

- Sök på domännamn (substring, eller `%`/`_` som SQLite LIKE-wildcards)
- Filtrera på TLD, datumintervall, längd, "inga siffror", "inga bindestreck"
- Sortera på datum, namn eller längd
- Virtuell scroll — hanterar hundratusentals rader smidigt
- Direktlänk till Internetstiftelsens uppslag per domän
- Nysläppta-vy: 90 dagars historik med registerstatus från Internetstiftelsens DAS
- Guldkornspoäng för varumärke, SEO/historik, indikativ efterfrågan och risk
- Topplistor för bästa domäner nästa 24 timmar och bästa bekräftat lediga nu
- Äldre guldkorn: lediga .se/.nu från Tranco, Majestic Million eller Open PageRank Top 10M
- Beständig rankningsbevakning som prioriterar nya kandidater och återkontrollerar lediga dagligen
- Sorterbar och filtrerbar Ahrefs Domain Rating med direktlänk till förifylld Backlink Checker
- Uppdateras varje timme via GitHub Actions (full berikning kl 05 UTC)

## Kör lokalt

```bash
npm install
npm run build:data    # hämtar JSON och bygger public/domains.sqlite
npm run dev           # startar Vite på http://localhost:5173
```

För att bygga den statiska sajten:

```bash
npm run build         # build:data + vite build → dist/
npm run preview
```

## Deploy

Workflowen i [.github/workflows/deploy.yml](.github/workflows/deploy.yml) bygger
och deployar till GitHub Pages varje timme samt vid push till `main`. De
timvisa körningarna uppdaterar nysläppta domäner och deras registerstatus via
Internetstiftelsens Free/DAS. Upptagna domäners A-, MX- och NS-records sparas
separat. DNS-timeout och SERVFAIL markeras som kontrollfel och cachas aldrig
som "inga records". Den fulla berikningen (Wayback + stor DNS-batch) körs i
05-körningen (UTC). Rankningskandidater kontrolleras i batchar om högst 5 000
per körning. Nya och okontrollerade kandidater prioriteras, lediga kontrolleras
ungefär dagligen och upptagna ungefär månadsvis. En kandidat inaktiveras först
efter tre lyckade rankningskörningar där den saknas.

Ahrefs DR hämtas från det kostnadsfria API:t i högst 75 anrop per körning,
cachas i 30 dagar och stryps till under 60 anrop/minut. Lägg API-nyckeln som
repository secret med namnet `AHREFS_API_KEY`; om den saknas hoppas DR-steget
över utan att resten av bygget påverkas. API-nyckeln får aldrig läggas i en fil
eller i den publika SQLite-databasen.

Aktivera GitHub Pages: **Settings → Pages → Source: GitHub Actions**.

## Datakällor

- https://data.internetstiftelsen.se/bardate_domains.json
- https://data.internetstiftelsen.se/bardate_domains_nu.json
- https://tranco-list.eu/
- https://majestic.com/reports/majestic-million
- https://www.domcop.com/openpagerank/
- https://api.ahrefs.com/v3/public/domain-rating-free

Ahrefs-värden visas med attributionen
[Domain Rating by Ahrefs](https://ahrefs.com/) enligt deras licensvillkor.

Datan beskriver domäner i karenstid (perioden mellan utgångsdatum och
frisläppsdatum). `release_at` är datumet då domänen blir tillgänglig att
registrera igen.

## Arkitektur

```
scripts/build-data.mjs   ── hämtar båda JSON-filerna, bygger public/domains.sqlite
                            och public/meta.json med better-sqlite3
public/domains.sqlite    ── packad databas, serveras statiskt
src/main.ts              ── laddar sql.js + databasen, sköter UI, virtuell scroll
src/style.css            ── Tailwind + small custom rules
index.html               ── statisk markup
```

## Kostnad

Repot är publikt och använder standard-runners, vilket gör GitHub Actions och
GitHub Pages kostnadsfria. Rankningskön har hårda batchgränser och den beständiga
cachen ligger under GitHubs kostnadsfria 10 GB-gräns. Räkna med 0 kr/mån så
länge repot förblir publikt och inga större betalrunners aktiveras.
