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
- Uppdateras dagligen via GitHub Actions

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
och deployar till GitHub Pages dagligen kl. 05:00 UTC samt vid push till `main`.

Aktivera GitHub Pages: **Settings → Pages → Source: GitHub Actions**.

## Datakällor

- https://data.internetstiftelsen.se/bardate_domains.json
- https://data.internetstiftelsen.se/bardate_domains_nu.json

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

Allt körs på gratisnivåer: GitHub Actions (~5 min/dag) + GitHub Pages eller
Cloudflare Pages. Räkna med 0 kr/mån.
